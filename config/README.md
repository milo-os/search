# Kubernetes Deployment Configuration

This directory contains Kustomize-based Kubernetes deployment manifests for the search aggregated API server.

## Structure

```
config/
├── base/                      # Base Kubernetes resources
│   ├── deployment.yaml        # API server deployment
│   ├── service.yaml          # ClusterIP service
│   ├── serviceaccount.yaml   # Service account
│   ├── rbac-*.yaml           # RBAC permissions
│   ├── secret.yaml           # TLS certificates placeholder
│   └── kustomization.yaml
├── components/               # Optional add-ons (cert-manager, observability, etc.)
│   ├── namespace/
│   ├── api-registration/     # APIService registration (required)
│   ├── cert-manager-ca/      # cert-manager integration
│   ├── observability/        # Prometheus ServiceMonitor, alerts, dashboards
│   └── tracing/             # OpenTelemetry tracing config
├── milo/                     # Milo IAM integration (optional)
│   └── iam/
│       ├── resources/        # Protected resources
│       └── roles/           # IAM roles
└── overlays/                # Environment-specific configuration
    └── dev/                 # Development environment
```

## Quick Start

### 1. Deploy to Development Environment

```bash
# Apply the dev overlay (includes namespace and API registration)
kubectl apply -k config/overlays/dev

# Check deployment status
kubectl get pods -n search-system
kubectl get apiservice v1alpha1.search.miloapis.com
```

### 2. Verify API Registration

```bash
# Check if the APIService is available
kubectl get apiservices | grep search

# List API resources (once storage is implemented)
kubectl api-resources | grep datum

# Try creating a resource (once storage is implemented)
kubectl get resourcesearchqueries
```

## Components

Components are optional features that can be enabled in overlays. See [components/README.md](components/README.md) for details.

### Required Components

- **api-registration**: Registers the aggregated API with Kubernetes (enabled by default in dev overlay)

### Optional Components

- **namespace**: Manages the namespace via kustomize
- **cert-manager-ca**: Uses cert-manager for TLS certificates
- **observability**: Adds Prometheus metrics, alerts, and Grafana dashboards
- **tracing**: Enables OpenTelemetry tracing

### Milo IAM Integration

- **milo/iam**: Integrates with Milo for advanced IAM capabilities
  - See <https://github.com/datum-cloud/milo>

## Customization

### Creating a New Environment Overlay

```bash
mkdir -p config/overlays/production
```

```yaml
# config/overlays/production/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: search-system

resources:
  - ../../base

components:
  - ../../components/namespace
  - ../../components/api-registration
  - ../../components/cert-manager-ca
  - ../../components/observability

images:
  - name: ghcr.io/datum-cloud/search
    newTag: v1.0.0

# Production-specific patches
patches:
  # Set production replicas
  - patch: |-
      - op: replace
        path: /spec/replicas
        value: 3
    target:
      kind: Deployment
      name: search-apiserver

  # Production resource limits
  - patch: |-
      - op: replace
        path: /spec/template/spec/containers/0/resources/limits/cpu
        value: "2000m"
      - op: replace
        path: /spec/template/spec/containers/0/resources/limits/memory
        value: "2Gi"
    target:
      kind: Deployment
      name: search-apiserver
```

## TLS Certificates

The deployment requires TLS certificates. Choose one option:

### Option 1: cert-manager (Recommended for Production)

```bash
# Install cert-manager
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.14.0/cert-manager.yaml

# Enable cert-manager component in your overlay
components:
  - ../../components/cert-manager-ca
```

### Option 2: Manual Certificates (Testing Only)

```bash
# Generate self-signed certificate
openssl req -x509 -newkey rsa:4096 -keyout tls.key -out tls.crt \
  -days 365 -nodes -subj "/CN=search-apiserver.search-system.svc"

# Create secret
kubectl create secret tls search-tls \
  --cert=tls.crt --key=tls.key -n search-system
```

## Observability

Enable monitoring with the observability component:

```yaml
components:
  - ../../components/observability
```

**Requires:**

- Prometheus Operator (for ServiceMonitor)
- Grafana (for dashboards)

**Customize:**

- Alerts: `components/observability/alerts/`
- Dashboards: `components/observability/dashboards/`

## Troubleshooting

### APIService shows "FailedDiscoveryCheck"

```bash
# Check API server logs
kubectl logs -n search-system -l app=search-apiserver --tail=50

# Check APIService status
kubectl get apiservice v1alpha1.search.miloapis.com -o yaml
```

**Common causes:**

- TLS certificate issues (check secret exists and is valid)
- Service not ready (check pod status)
- RBAC permissions missing

### Pods CrashLoopBackOff

```bash
# Describe pod for detailed events
kubectl describe pod -n search-system -l app=search-apiserver

# Check logs
kubectl logs -n search-system -l app=search-apiserver --previous
```

### Image Pull Errors

```bash
# Check events
kubectl get events -n search-system --sort-by='.lastTimestamp'

# Verify image exists
docker pull ghcr.io/datum-cloud/search:dev
```

## Production Checklist

Before deploying to production:

- [ ] Use proper TLS certificates (cert-manager or external CA)
- [ ] Set `insecureSkipTLSVerify: false` in APIService
- [ ] Configure appropriate resource limits
- [ ] Enable observability and alerting
- [ ] Set up log aggregation
- [ ] Configure PodDisruptionBudget for HA
- [ ] Review and tighten RBAC permissions
- [ ] Enable network policies
- [ ] Configure backup/disaster recovery
- [ ] Document runbooks for common operations

## References

- [Kubernetes Aggregated API Servers](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/apiserver-aggregation/)
- [Kustomize Documentation](https://kustomize.io/)
- [cert-manager](https://cert-manager.io/)
- [Prometheus Operator](https://prometheus-operator.dev/)
- [Milo IAM](https://github.com/datum-cloud/milo)
