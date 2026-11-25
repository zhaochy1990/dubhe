package tracing

import (
	"net/http"
	"testing"
)

func TestAPIKeyAuthentication(t *testing.T) {
	auth, err := newAPIKeyAuthenticator(AuthConfig{
		Workloads:  []WorkloadCredential{{APIKey: "Ingest-Key", Scope: testScope, ProducerID: "worker"}},
		Registrars: []WorkloadCredential{{APIKey: "Register-Key", Scope: testScope, ProducerID: "worker"}},
		Readers: []ReaderCredential{
			{APIKey: "Read-Key", Scope: testScope, AgentIDs: []string{"agent"}},
			{Token: "legacy", Scope: testScope, AgentIDs: []string{"agent"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		role   authRole
		header http.Header
		want   bool
	}{
		{"workload key", workloadRole, http.Header{"X-Api-Key": {"Ingest-Key"}}, true},
		{"registrar key", registrarRole, http.Header{"X-Api-Key": {"Register-Key"}}, true},
		{"reader key", readerRole, http.Header{"X-Api-Key": {"Read-Key"}}, true},
		{"missing key", workloadRole, nil, false},
		{"case-sensitive key", workloadRole, http.Header{"X-Api-Key": {"ingest-key"}}, false},
		{"wrong role", registrarRole, http.Header{"X-Api-Key": {"Ingest-Key"}}, false},
		{"duplicate header", workloadRole, http.Header{"X-Api-Key": {"Ingest-Key", "Ingest-Key"}}, false},
		{"mixed headers", readerRole, http.Header{"X-Api-Key": {"Read-Key"}, "Authorization": {"Bearer legacy"}}, false},
		{"API key presented as Bearer", readerRole, http.Header{"Authorization": {"Bearer Read-Key"}}, false},
		{"legacy Bearer", readerRole, http.Header{"Authorization": {"Bearer legacy"}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			credential, parsed := readCredential(tt.header)
			var authorized bool
			if parsed {
				identity, ok, err := auth.authenticate(t.Context(), credential, tt.role)
				if err != nil {
					t.Fatal(err)
				}
				authorized = ok
				if authorized && identity.Scope != testScope {
					t.Fatalf("authentication changed the configured scope: %+v", identity.Scope)
				}
			}
			if authorized != tt.want {
				t.Fatalf("authorized = %t, want %t", authorized, tt.want)
			}
		})
	}
}

func TestAPIKeyConfigurationRejectsDuplicateKeysAcrossRoles(t *testing.T) {
	_, err := newAPIKeyAuthenticator(AuthConfig{
		Workloads: []WorkloadCredential{{APIKey: "shared", Scope: testScope, ProducerID: "worker"}},
		Readers:   []ReaderCredential{{Token: "shared", Scope: testScope, AgentIDs: []string{"agent"}}},
	})
	if err == nil {
		t.Fatal("expected duplicate API key to be rejected")
	}
}
