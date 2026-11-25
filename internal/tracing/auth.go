package tracing

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

type WorkloadCredential struct {
	APIKey     string `json:"-" mapstructure:"api_key"`
	Token      string `json:"-" mapstructure:"token"`
	Scope      Scope  `json:"scope" mapstructure:"scope"`
	ProducerID string `json:"producer_id" mapstructure:"producer_id"`
}

type ReaderCredential struct {
	APIKey   string   `json:"-" mapstructure:"api_key"`
	Token    string   `json:"-" mapstructure:"token"`
	Scope    Scope    `json:"scope" mapstructure:"scope"`
	AgentIDs []string `json:"agent_ids" mapstructure:"agent_ids"`
}

type AuthConfig struct {
	Workloads  []WorkloadCredential `mapstructure:"workloads"`
	Registrars []WorkloadCredential `mapstructure:"registrars"`
	Readers    []ReaderCredential   `mapstructure:"readers"`
}

type authRole uint8

const (
	workloadRole authRole = iota
	registrarRole
	readerRole
)

type credentialScheme uint8

const (
	apiKeyScheme credentialScheme = iota
	bearerScheme
)

type requestCredential struct {
	scheme credentialScheme
	value  string
}

type authenticatedIdentity struct {
	Scope      Scope
	ProducerID string
	AgentIDs   []string
}

// An authenticator resolves a presented credential to a scoped identity for
// the requested role. Each endpoint then applies its existing authorization.
type authenticator interface {
	authenticate(context.Context, requestCredential, authRole) (authenticatedIdentity, bool, error)
}

type apiKeyGrant struct {
	identity     authenticatedIdentity
	role         authRole
	legacyBearer bool
}

type apiKeyAuthenticator struct {
	grants map[[sha256.Size]byte]apiKeyGrant
}

// ParseLegacyAuth supports the JSON environment configuration used by earlier builds.
func ParseLegacyAuth(raw string) (AuthConfig, error) {
	var legacy struct {
		Workloads  map[string]WorkloadCredential `json:"workloads"`
		Registrars map[string]WorkloadCredential `json:"registrars"`
		Readers    map[string]ReaderCredential   `json:"readers"`
	}
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

func newAPIKeyAuthenticator(config AuthConfig) (*apiKeyAuthenticator, error) {
	auth := &apiKeyAuthenticator{grants: make(map[[sha256.Size]byte]apiKeyGrant)}
	for _, group := range []struct {
		entries []WorkloadCredential
		role    authRole
	}{{config.Workloads, workloadRole}, {config.Registrars, registrarRole}} {
		for _, entry := range group.entries {
			if !entry.Scope.valid() || !opaqueID(entry.ProducerID) {
				return nil, errors.New("invalid workload or registration identity configuration")
			}
			identity := authenticatedIdentity{Scope: entry.Scope, ProducerID: entry.ProducerID}
			if err := auth.add(entry.APIKey, entry.Token, identity, group.role); err != nil {
				return nil, err
			}
		}
	}
	for _, entry := range config.Readers {
		if !entry.Scope.valid() {
			return nil, errors.New("invalid reader identity configuration")
		}
		for _, id := range entry.AgentIDs {
			if !opaqueID(id) {
				return nil, errors.New("invalid reader Agent grant")
			}
		}
		identity := authenticatedIdentity{Scope: entry.Scope, AgentIDs: entry.AgentIDs}
		if err := auth.add(entry.APIKey, entry.Token, identity, readerRole); err != nil {
			return nil, err
		}
	}
	return auth, nil
}

func credentialValue(value string) bool {
	return value != "" && !strings.ContainsAny(value, " \t\r\n")
}

func (auth *apiKeyAuthenticator) add(key, legacy string, identity authenticatedIdentity, role authRole) error {
	if key != "" && legacy != "" {
		return errors.New("configure api_key or the legacy token for an identity")
	}
	if key == "" {
		key = legacy
	}
	hash := sha256.Sum256([]byte(key))
	if _, exists := auth.grants[hash]; !credentialValue(key) || exists {
		return errors.New("invalid or duplicate API key")
	}
	auth.grants[hash] = apiKeyGrant{identity: identity, role: role, legacyBearer: legacy != ""}
	return nil
}

func readCredential(headers http.Header) (requestCredential, bool) {
	keys, authorization := headers.Values("X-API-Key"), headers.Values("Authorization")
	if len(keys) == 1 && len(authorization) == 0 && credentialValue(keys[0]) {
		return requestCredential{apiKeyScheme, keys[0]}, true
	}
	if len(authorization) != 1 || len(keys) != 0 {
		return requestCredential{}, false
	}
	scheme, token, ok := strings.Cut(authorization[0], " ")
	token = strings.TrimLeft(token, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || !credentialValue(token) {
		return requestCredential{}, false
	}
	return requestCredential{bearerScheme, token}, true
}

func (auth *apiKeyAuthenticator) authenticate(_ context.Context, credential requestCredential, role authRole) (authenticatedIdentity, bool, error) {
	grant, ok := auth.grants[sha256.Sum256([]byte(credential.value))]
	if !ok || grant.role != role {
		return authenticatedIdentity{}, false, nil
	}
	switch credential.scheme {
	case apiKeyScheme:
		return grant.identity, true, nil
	case bearerScheme:
		if grant.legacyBearer {
			return grant.identity, true, nil
		}
	}
	return authenticatedIdentity{}, false, nil
}
