package tracing

import (
	"compress/gzip"
	"context"
	"crypto/subtle"
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

type WorkloadCredential struct {
	Token      string `json:"-" mapstructure:"token"`
	Scope      Scope  `json:"scope" mapstructure:"scope"`
	ProducerID string `json:"producer_id" mapstructure:"producer_id"`
}

type ReaderCredential struct {
	Token    string   `json:"-" mapstructure:"token"`
	Scope    Scope    `json:"scope" mapstructure:"scope"`
	AgentIDs []string `json:"agent_ids" mapstructure:"agent_ids"`
}

type workload = WorkloadCredential
type reader = ReaderCredential

type AuthConfig struct {
	Workloads  []WorkloadCredential `mapstructure:"workloads"`
	Registrars []WorkloadCredential `mapstructure:"registrars"`
	Readers    []ReaderCredential   `mapstructure:"readers"`
}

type authConfig struct {
	Workloads  map[string]workload `json:"workloads"`
	Registrars map[string]workload `json:"registrars"`
	Readers    map[string]reader   `json:"readers"`
}

// ParseLegacyAuth supports the JSON environment configuration used by earlier builds.
func ParseLegacyAuth(raw string) (AuthConfig, error) {
	var legacy authConfig
	if err := json.Unmarshal([]byte(raw), &legacy); err != nil {
		return AuthConfig{}, errors.New("DUBHE_TRACING_AUTH must contain valid credential configuration")
	}
	var config AuthConfig
	for token, entry := range legacy.Workloads {
		entry.Token = token
		config.Workloads = append(config.Workloads, entry)
	}
	for token, entry := range legacy.Registrars {
		entry.Token = token
		config.Registrars = append(config.Registrars, entry)
	}
	for token, entry := range legacy.Readers {
		entry.Token = token
		config.Readers = append(config.Readers, entry)
	}
	return config, nil
}

func validateAuth(config AuthConfig) (authConfig, error) {
	auth := authConfig{
		Workloads:  make(map[string]workload),
		Registrars: make(map[string]workload),
		Readers:    make(map[string]reader),
	}
	tokens := make(map[string]bool)
	checkToken := func(token string) bool {
		if token == "" || strings.ContainsAny(token, " \t\r\n") || tokens[token] {
			return false
		}
		tokens[token] = true
		return true
	}
	addWorkloads := func(entries []WorkloadCredential, identities map[string]workload) error {
		for _, identity := range entries {
			if !checkToken(identity.Token) || !identity.Scope.valid() || !opaqueID(identity.ProducerID) {
				return errors.New("invalid workload or registration credential configuration")
			}
			identities[identity.Token] = identity
		}
		return nil
	}
	if err := addWorkloads(config.Workloads, auth.Workloads); err != nil {
		return authConfig{}, err
	}
	if err := addWorkloads(config.Registrars, auth.Registrars); err != nil {
		return authConfig{}, err
	}
	for _, identity := range config.Readers {
		if !checkToken(identity.Token) || !identity.Scope.valid() {
			return authConfig{}, errors.New("invalid reader credential configuration")
		}
		for _, id := range identity.AgentIDs {
			if !opaqueID(id) {
				return authConfig{}, errors.New("invalid reader Agent grant")
			}
		}
		auth.Readers[identity.Token] = identity
	}
	return auth, nil
}

func bearer[T any](request *http.Request, identities map[string]T) (T, bool) {
	var unknown T
	if len(request.Header.Values("Authorization")) != 1 {
		return unknown, false
	}
	scheme, token, ok := strings.Cut(request.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return unknown, false
	}
	for expected, identity := range identities {
		if subtle.ConstantTimeCompare([]byte(token), []byte(expected)) == 1 {
			return identity, true
		}
	}
	return unknown, false
}

type service struct {
	writeStore store
	readStore  store
	auth       authConfig
}

func NewHandler(writeDB, readDB *gorm.DB, config AuthConfig) (http.Handler, error) {
	auth, err := validateAuth(config)
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

func (s service) ingest(w http.ResponseWriter, r *http.Request) {
	identity, authorized := bearer(r, s.auth.Workloads)
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
	identity, authorized := bearer(r, s.auth.Registrars)
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

func authorizedReader(r *http.Request, readers map[string]reader) (reader, url.Values, bool) {
	identity, authorized := bearer(r, readers)
	query, err := url.ParseQuery(r.URL.RawQuery)
	if !authorized || err != nil {
		return reader{}, nil, false
	}
	tenant, environment, project := query["tenant_id"], query["environment_id"], query["project_id"]
	if len(tenant) != 1 || len(environment) != 1 || len(project) != 1 {
		return reader{}, nil, false
	}
	return identity, query, identity.Scope == (Scope{tenant[0], environment[0], project[0]})
}

func (s service) getTrace(w http.ResponseWriter, r *http.Request) {
	identity, _, authorized := authorizedReader(r, s.auth.Readers)
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
	identity, query, authorized := authorizedReader(r, s.auth.Readers)
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
