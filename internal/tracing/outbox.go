package tracing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"gorm.io/gorm"
)

type GatewayOutboxConfig struct {
	URL      string `mapstructure:"url"`
	APIKey   string `mapstructure:"api_key"`
	Scope    Scope  `mapstructure:"scope"`
	SourceID string `mapstructure:"source_id"`
}

type leasedAuditEvent struct {
	Token   string     `json:"lease_token"`
	Version int        `json:"version"`
	Event   auditEvent `json:"event"`
}

type GatewayOutbox struct {
	store  store
	source authenticatedIdentity
	url    *url.URL
	key    string
	client *http.Client
}

func NewGatewayOutbox(db *gorm.DB, options Options, config GatewayOutboxConfig) (*GatewayOutbox, error) {
	base, err := url.Parse(config.URL)
	if err != nil || base.Host == "" || (base.Scheme != "https" && base.Scheme != "http") ||
		base.User != nil || base.RawQuery != "" || base.Fragment != "" ||
		!config.Scope.valid() || !opaqueID(config.SourceID) || !credentialValue(config.APIKey) {
		return nil, errors.New("invalid Gateway Outbox configuration")
	}
	policy, err := compilePolicies(options)
	if err != nil {
		return nil, err
	}
	return &GatewayOutbox{
		store: store{db: db, policy: policy}, source: authenticatedIdentity{Scope: config.Scope, ProducerID: config.SourceID},
		url: base, key: config.APIKey, client: &http.Client{
			Timeout:       8 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}, nil
}

// Run polls the Gateway independently of OTLP and business execution. A
// delivery stays unacknowledged when the projection or acknowledgement fails.
func (o *GatewayOutbox) Run(ctx context.Context, report func(error)) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		if err := o.consume(ctx, report); err != nil && ctx.Err() == nil {
			report(err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (o *GatewayOutbox) consume(ctx context.Context, report func(error)) error {
	res, err := o.post(ctx, "lease", []byte(`{"limit":32}`))
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNoContent {
		return nil
	}
	if res.StatusCode != http.StatusOK {
		return errors.New("Gateway Outbox lease request failed")
	}
	var batch struct {
		Deliveries []leasedAuditEvent `json:"deliveries"`
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, maxOTLPBytes+1))
	if err != nil || len(body) > maxOTLPBytes {
		return errors.New("could not read Gateway Outbox lease")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&batch) != nil || decoder.Decode(new(any)) != io.EOF || len(batch.Deliveries) > 32 {
		return errors.New("invalid Gateway Outbox lease batch")
	}
	for _, delivery := range batch.Deliveries {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := o.project(ctx, delivery); err != nil && ctx.Err() == nil {
			report(err)
		}
	}
	return nil
}

func (o *GatewayOutbox) project(ctx context.Context, delivery leasedAuditEvent) error {
	if delivery.Version != 1 || delivery.Token == "" || len(delivery.Token) > 1024 {
		return errors.New("invalid Gateway Outbox delivery")
	}
	traceID, spanID, err := delivery.Event.normalize(o.source.Scope)
	if err != nil {
		return err
	}
	writeCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	err = o.store.ingestAudit(writeCtx, o.source, traceID, spanID, delivery.Event)
	cancel()
	if err != nil {
		if errors.Is(err, errAuditConflict) || errors.Is(err, errAuditBinding) || errors.Is(err, errAuditOrder) {
			return err
		}
		return errors.New("could not commit Gateway audit event")
	}
	body, err := json.Marshal(struct {
		Token string `json:"lease_token"`
	}{delivery.Token})
	if err != nil {
		return err
	}
	res, err := o.post(ctx, "ack", body)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		return errors.New("Gateway Outbox acknowledgement failed")
	}
	return nil
}

func (o *GatewayOutbox) post(ctx context.Context, operation string, body []byte) (*http.Response, error) {
	target := *o.url
	target.Path = strings.TrimRight(target.Path, "/") + "/" + operation
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(body))
	if err != nil {
		return nil, errors.New("invalid Gateway Outbox request")
	}
	request.Header.Set("X-API-Key", o.key)
	request.Header.Set("Content-Type", "application/json")
	res, err := o.client.Do(request)
	if err != nil {
		return nil, errors.New("Gateway Outbox unavailable")
	}
	return res, nil
}
