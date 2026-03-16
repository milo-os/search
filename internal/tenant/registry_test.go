package tenant

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// stubDynamicClient is a minimal implementation of dynamic.Interface used only
// to verify identity (pointer equality). It does not need to do anything.
type stubDynamicClient struct {
	dynamic.Interface
}

func TestSingleTenantRegistry_ListTenants(t *testing.T) {
	r := NewSingleTenantRegistry(&stubDynamicClient{})

	tenants := r.ListTenants()

	require.Len(t, tenants, 1)
	assert.Equal(t, TenantInfo{Name: "platform", Type: TenantTypePlatform}, tenants[0])
}

func TestSingleTenantRegistry_GetTenantClient_Platform(t *testing.T) {
	client := &stubDynamicClient{}
	r := NewSingleTenantRegistry(client)

	got := r.GetTenantClient("platform")

	assert.Same(t, client, got.(*stubDynamicClient))
}

func TestSingleTenantRegistry_GetTenantClient_Unknown(t *testing.T) {
	r := NewSingleTenantRegistry(&stubDynamicClient{})

	got := r.GetTenantClient("some-project")

	assert.Nil(t, got)
}

func TestMultiTenantRegistry_ListTenants_OnlyPlatformInitially(t *testing.T) {
	platformClient := &stubDynamicClient{}
	r := NewMultiTenantRegistry(
		&rest.Config{Host: "https://example.com"},
		platformClient,
		"",
		nil,
		nil,
	)

	// Do NOT call Run; the registry should still expose the platform tenant.
	tenants := r.ListTenants()

	require.Len(t, tenants, 1)
	assert.Equal(t, PlatformTenantInfo, tenants[0])
}

func TestMultiTenantRegistry_AddRemoveProject(t *testing.T) {
	platformClient := &stubDynamicClient{}

	var engaged []TenantInfo
	var disengaged []string

	onEngage := func(_ context.Context, tenant TenantInfo, _ dynamic.Interface) {
		engaged = append(engaged, tenant)
	}
	onDisengage := func(name string) {
		disengaged = append(disengaged, name)
	}

	r := NewMultiTenantRegistry(
		// Use a fake host that will fail dynamic.NewForConfig — but addProject
		// calls dynamic.NewForConfig, so we need a host that succeeds.
		// An empty Host is accepted by rest.Config and dynamic.NewForConfig.
		&rest.Config{Host: "http://localhost:0"},
		platformClient,
		"",
		onEngage,
		onDisengage,
	)

	ctx := context.Background()

	// Initially only platform tenant.
	assert.Len(t, r.ListTenants(), 1)

	// Simulate informer Add event.
	r.addProject(ctx, "my-project")

	// Give the goroutine spawned by addProject (for onEngage) a chance to run.
	// We use a simple poll rather than time.Sleep to avoid flakiness.
	assert.Eventually(t, func() bool {
		tenants := r.ListTenants()
		return len(tenants) == 2
	}, testTimeout, testPoll, "expected 2 tenants after addProject")

	// Verify the project tenant is present.
	tenants := r.ListTenants()
	var found bool
	for _, ti := range tenants {
		if ti.Name == "my-project" && ti.Type == TenantTypeProject {
			found = true
		}
	}
	assert.True(t, found, "project tenant 'my-project' should be in ListTenants")

	// GetTenantClient should return a non-nil client for the new project.
	assert.NotNil(t, r.GetTenantClient("my-project"))

	// Simulate informer Delete event.
	r.removeProject("my-project")

	tenants = r.ListTenants()
	require.Len(t, tenants, 1, "only platform tenant should remain after removeProject")
	assert.Nil(t, r.GetTenantClient("my-project"))

	// onDisengage should have been called with the project name.
	assert.Contains(t, disengaged, "my-project")
}

func TestMultiTenantRegistry_GetTenantClient_Platform(t *testing.T) {
	platformClient := &stubDynamicClient{}
	r := NewMultiTenantRegistry(
		&rest.Config{Host: "http://localhost:0"},
		platformClient,
		"",
		nil,
		nil,
	)

	got := r.GetTenantClient("platform")

	assert.Same(t, platformClient, got.(*stubDynamicClient))
}

func TestMultiTenantRegistry_GetTenantClient_UnknownReturnsNil(t *testing.T) {
	r := NewMultiTenantRegistry(
		&rest.Config{Host: "http://localhost:0"},
		&stubDynamicClient{},
		"",
		nil,
		nil,
	)

	assert.Nil(t, r.GetTenantClient("nonexistent-project"))
}

func TestMultiTenantRegistry_ProjectRestConfig(t *testing.T) {
	tests := []struct {
		name        string
		baseHost    string
		projectName string
		wantHost    string
	}{
		{
			name:        "base host without trailing slash",
			baseHost:    "https://api.example.com",
			projectName: "my-project",
			wantHost:    "https://api.example.com/projects/my-project/control-plane",
		},
		{
			name:        "base host with trailing slash",
			baseHost:    "https://api.example.com/",
			projectName: "my-project",
			wantHost:    "https://api.example.com/projects/my-project/control-plane",
		},
		{
			name:        "base host with path suffix",
			baseHost:    "https://api.example.com/prefix",
			projectName: "other-project",
			wantHost:    "https://api.example.com/prefix/projects/other-project/control-plane",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewMultiTenantRegistry(
				&rest.Config{Host: tt.baseHost},
				&stubDynamicClient{},
				"",
				nil,
				nil,
			)

			cfg := r.projectRestConfig(tt.projectName)

			assert.Equal(t, tt.wantHost, cfg.Host)
		})
	}
}
