package tenant

import (
	"context"

	"k8s.io/client-go/dynamic"
)

// TenantInfo holds the identity of a single tenant.
type TenantInfo struct {
	// Name is the tenant identifier. "platform" for the platform tenant,
	// or the project name for project tenants.
	Name string

	// Type is the tenant type. One of "platform" or "project".
	Type string
}

// TenantRegistry provides the set of active tenants and per-tenant clients.
type TenantRegistry interface {
	// ListTenants returns all currently active tenants.
	// In single-tenant mode, always returns [{Name: "platform", Type: "platform"}].
	// In multi-tenant mode, returns platform + all ready projects.
	ListTenants() []TenantInfo

	// GetTenantClient returns a dynamic client for the given tenant.
	// For the platform tenant, returns the local cluster dynamic client.
	// For project tenants, returns a client routed through the project control plane proxy.
	// Returns nil if the tenant is not found.
	GetTenantClient(tenantName string) dynamic.Interface
}

// TenantEngagementCallback is called when a new project control plane becomes available.
// The implementation must be safe to call concurrently.
type TenantEngagementCallback func(ctx context.Context, tenant TenantInfo, client dynamic.Interface)

// TenantDisengagementCallback is called when a project control plane is removed.
// The implementation must be safe to call concurrently.
type TenantDisengagementCallback func(tenantName string)

const (
	TenantTypePlatform = "platform"
	TenantTypeProject  = "Project"
)

// PlatformTenantInfo is the canonical TenantInfo for the platform tenant.
var PlatformTenantInfo = TenantInfo{Name: "platform", Type: TenantTypePlatform}

// SingleTenantRegistry always returns only the platform tenant.
// It is used when --multi-tenant=false (the default).
type SingleTenantRegistry struct {
	platformClient dynamic.Interface
}

// NewSingleTenantRegistry creates a registry for single-tenant mode.
func NewSingleTenantRegistry(client dynamic.Interface) *SingleTenantRegistry {
	return &SingleTenantRegistry{platformClient: client}
}

func (r *SingleTenantRegistry) ListTenants() []TenantInfo {
	return []TenantInfo{PlatformTenantInfo}
}

func (r *SingleTenantRegistry) GetTenantClient(tenantName string) dynamic.Interface {
	if tenantName == "platform" {
		return r.platformClient
	}
	return nil
}
