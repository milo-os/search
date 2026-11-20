package evaluation

import "testing"

func TestEvalResult_Transform_TenantDefaults(t *testing.T) {
	tests := []struct {
		name        string
		tenant      string
		tenantType  string
		multiTenant bool
		wantTenant  string
		wantType    string
	}{
		{"multi-tenant empty tenant is unscoped", "", "", true, "unscoped", "unscoped"},
		{"multi-tenant type without name is unscoped", "", "Project", true, "unscoped", "unscoped"},
		{"multi-tenant name without type is unscoped", "acme", "", true, "unscoped", "unscoped"},
		{"multi-tenant owner is kept", "acme", "Organization", true, "acme", "Organization"},
		{"single-tenant empty tenant is platform", "", "", false, "platform", "platform"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &EvalResult{Object: map[string]any{}, Version: "v1", Kind: "Pod", Tenant: tt.tenant, TenantType: tt.tenantType, MultiTenant: tt.multiTenant}
			doc := r.Transform()
			if doc["_tenant"] != tt.wantTenant || doc["_tenant_type"] != tt.wantType {
				t.Errorf("Transform() tenant = %v/%v, want %s/%s", doc["_tenant_type"], doc["_tenant"], tt.wantType, tt.wantTenant)
			}
		})
	}
}
