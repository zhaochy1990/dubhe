package tracing

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"

	collector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
)

const (
	protobufType = "application/x-protobuf"
	maxOTLPBytes = 4 * 1024 * 1024
)

type service struct {
	writeStore store
	readStore  store
	auth       authenticator
}

func NewHandler(writeDB, readDB *gorm.DB, config AuthConfig) (http.Handler, error) {
	auth, err := newAPIKeyAuthenticator(config)
	if err != nil {
		return nil, err
	}
	srv := service{writeStore: store{writeDB}, readStore: store{readDB}, auth: auth}
	handler := http.NewServeMux()
	handler.HandleFunc("POST /v1/traces", srv.ingest)
	handler.HandleFunc("POST /internal/v1/observability/runs", srv.register)
	handler.HandleFunc("GET /v1/observability/traces/{trace_id}", srv.getTrace)
	handler.HandleFunc("GET /v1/observability/nodes/{node_id}", srv.getNode)
	return handler, nil
}

func (s service) authenticate(r *http.Request, role authRole) (authenticatedIdentity, bool, error) {
	credential, ok := readCredential(r.Header)
	if !ok {
		return authenticatedIdentity{}, false, nil
	}
	return s.auth.authenticate(r.Context(), credential, role)
}

func (s service) ingest(w http.ResponseWriter, r *http.Request) {
	identity, authorized, authErr := s.authenticate(r, workloadRole)
	if authErr != nil {
		writeOTLPError(w, http.StatusServiceUnavailable, code.Code_UNAVAILABLE, "Authentication service unavailable")
		return
	}
	if !authorized {
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeOTLPError(w, http.StatusUnauthorized, code.Code_UNAUTHENTICATED, "Workload authentication required")
		return
	}
	contentType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	encoding := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding")))
	if contentType != protobufType || (encoding != "" && encoding != "identity" && encoding != "gzip") {
		writeOTLPError(w, http.StatusUnsupportedMediaType, code.Code_INVALID_ARGUMENT, "Unsupported OTLP encoding")
		return
	}
	body, err := readOTLPBody(w, r, encoding)
	var limitError *http.MaxBytesError
	if errors.As(err, &limitError) {
		writeOTLPError(w, http.StatusRequestEntityTooLarge, code.Code_RESOURCE_EXHAUSTED, "OTLP request exceeds the size limit")
		return
	}
	var batch collector.ExportTraceServiceRequest
	if err != nil || proto.Unmarshal(body, &batch) != nil {
		writeOTLPError(w, http.StatusBadRequest, code.Code_INVALID_ARGUMENT, "Invalid OTLP trace request")
		return
	}

	var records []spanRecord
	var rejected int64
	for _, resource := range batch.GetResourceSpans() {
		for _, scope := range resource.GetScopeSpans() {
			for _, span := range scope.GetSpans() {
				record, err := normalizeSpan(span)
				if err != nil {
					rejected++
					continue
				}
				records = append(records, record)
			}
		}
	}
	if len(records) != 0 {
		ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
		defer cancel()
		rejectedByStore, err := s.writeStore.ingest(ctx, identity, records)
		if err != nil {
			writeOTLPError(w, http.StatusServiceUnavailable, code.Code_UNAVAILABLE, "Tracing storage unavailable")
			return
		}
		rejected += rejectedByStore
	}

	acknowledgement := &collector.ExportTraceServiceResponse{}
	if rejected != 0 {
		acknowledgement.PartialSuccess = &collector.ExportTracePartialSuccess{
			RejectedSpans: rejected,
			ErrorMessage:  "One or more spans failed validation or authorization",
		}
	}
	writeProto(w, http.StatusOK, acknowledgement)
}

func readOTLPBody(w http.ResponseWriter, r *http.Request, encoding string) ([]byte, error) {
	var stream io.Reader = http.MaxBytesReader(w, r.Body, maxOTLPBytes)
	if encoding == "gzip" {
		compressed, err := gzip.NewReader(stream)
		if err != nil {
			return nil, err
		}
		defer compressed.Close()
		stream = io.LimitReader(compressed, maxOTLPBytes+1)
	}
	body, err := io.ReadAll(stream)
	if len(body) > maxOTLPBytes {
		return nil, &http.MaxBytesError{Limit: maxOTLPBytes}
	}
	return body, err
}

func writeOTLPError(w http.ResponseWriter, httpStatus int, rpcStatus code.Code, message string) {
	writeProto(w, httpStatus, &status.Status{Code: int32(rpcStatus), Message: message})
}

func writeProto(w http.ResponseWriter, httpStatus int, message proto.Message) {
	body, err := proto.Marshal(message)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", protobufType)
	w.WriteHeader(httpStatus)
	_, _ = w.Write(body)
}

func (s service) register(w http.ResponseWriter, r *http.Request) {
	identity, authorized, authErr := s.authenticate(r, registrarRole)
	if authErr != nil {
		writeDetail(w, http.StatusServiceUnavailable, "Authentication service unavailable")
		return
	}
	if !authorized {
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeDetail(w, http.StatusUnauthorized, "Registration authentication required")
		return
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16*1024))
	decoder.DisallowUnknownFields()
	var input registrationRequest
	if err := decoder.Decode(&input); err != nil || decoder.Decode(new(any)) != io.EOF {
		writeDetail(w, http.StatusBadRequest, "Invalid trusted run registration")
		return
	}
	run, ok := input.normalize()
	if !ok {
		writeDetail(w, http.StatusBadRequest, "Invalid trusted run registration")
		return
	}
	if identity.Scope != run.Scope || identity.ProducerID != run.RootProducerID {
		writeDetail(w, http.StatusForbidden, "Registration is outside the credential grants")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	switch err := s.writeStore.register(ctx, run); {
	case errors.Is(err, errRegistrationConflict):
		writeDetail(w, http.StatusConflict, errRegistrationConflict.Error())
	case err != nil:
		writeDetail(w, http.StatusServiceUnavailable, "Tracing storage unavailable")
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s service) authorizedReader(r *http.Request) (authenticatedIdentity, url.Values, bool, error) {
	identity, authorized, authErr := s.authenticate(r, readerRole)
	query, err := url.ParseQuery(r.URL.RawQuery)
	if !authorized || err != nil {
		return authenticatedIdentity{}, nil, false, authErr
	}
	tenant, environment, project := query["tenant_id"], query["environment_id"], query["project_id"]
	if len(tenant) != 1 || len(environment) != 1 || len(project) != 1 {
		return authenticatedIdentity{}, nil, false, nil
	}
	return identity, query, identity.Scope == (Scope{tenant[0], environment[0], project[0]}), nil
}

func (s service) getTrace(w http.ResponseWriter, r *http.Request) {
	identity, _, authorized, authErr := s.authorizedReader(r)
	if authErr != nil {
		writeDetail(w, http.StatusServiceUnavailable, "Authentication service unavailable")
		return
	}
	traceID, ok := parseID(r.PathValue("trace_id"), 16)
	if !authorized || !ok {
		writeDetail(w, http.StatusNotFound, "Not available")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	trace, err := s.readStore.trace(ctx, identity, traceID)
	switch {
	case err != nil:
		writeDetail(w, http.StatusServiceUnavailable, "Tracing storage unavailable")
	case trace == nil:
		writeDetail(w, http.StatusNotFound, "Not available")
	default:
		writeJSON(w, http.StatusOK, trace)
	}
}

func (s service) getNode(w http.ResponseWriter, r *http.Request) {
	identity, query, authorized, authErr := s.authorizedReader(r)
	if authErr != nil {
		writeDetail(w, http.StatusServiceUnavailable, "Authentication service unavailable")
		return
	}
	kind, rawID, _ := strings.Cut(r.PathValue("node_id"), ":")
	spanID, ok := parseID(rawID, 8)
	if !authorized || !ok || kind != "span" || len(query["expected_trace_id"]) > 1 {
		writeDetail(w, http.StatusNotFound, "Not available")
		return
	}
	var traceID []byte
	if values := query["expected_trace_id"]; len(values) == 1 {
		traceID, ok = parseID(values[0], 16)
		if !ok {
			writeDetail(w, http.StatusNotFound, "Not available")
			return
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	node, err := s.readStore.node(ctx, identity, spanID, traceID)
	switch {
	case err != nil:
		writeDetail(w, http.StatusServiceUnavailable, "Tracing storage unavailable")
	case node == nil:
		writeDetail(w, http.StatusNotFound, "Not available")
	default:
		writeJSON(w, http.StatusOK, node)
	}
}

func writeDetail(w http.ResponseWriter, httpStatus int, detail string) {
	writeJSON(w, httpStatus, struct {
		Detail string `json:"detail"`
	}{detail})
}

func writeJSON(w http.ResponseWriter, httpStatus int, payload any) {
	body, err := json.Marshal(payload)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	_, _ = fmt.Fprintln(w, string(body))
}
