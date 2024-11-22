# GCP Managed Prometheus Authentication Guide

## Method 1: Service Account Key Authentication
Using service account keys is the current recommended method for authenticating with Google Cloud Managed Prometheus.

```yaml
prometheus:
  base_url: "https://monitoring.googleapis.com/v1/projects/[PROJECT_ID]/location/global/prometheus"
  auth:
    gcloud_service_account_path: "/path/to/service-account.json"
    gcloud_target_principal: "prometheus-reader@[PROJECT_ID].iam.gserviceaccount.com"
```

### Required Permissions
The service account needs these IAM roles:
- `roles/monitoring.viewer` - For reading metrics data
- `roles/iam.serviceAccountTokenCreator` - For token generation
- `roles/iam.serviceAccountUser` - For service account impersonation

### Required OAuth Scopes
- `https://www.googleapis.com/auth/monitoring.read`
- `https://www.googleapis.com/auth/cloud-platform`

### Complete Configuration Example

```yaml
inputs:
  - integration: prometheus
    slaos_key: prometheus_metrics
    type: metrics
    prometheus:
      base_url: "https://monitoring.googleapis.com/v1/projects/rated-network/location/global/prometheus"
      auth:
        gcloud_service_account_path: "/path/to/service-account.json"
        gcloud_target_principal: "prometheus-reader@rated-network.iam.gserviceaccount.com"
    queries:
      # Request Rate
      - query: 'rate(http_requests_total{job="api"}[5m])'
        step:
          value: 30
          unit: "s"
        slaos_metric_name: "request_rate"
        organization_identifier: "customer_id"
```

## Method 2: Workload Identity (Work in progress)
Workload Identity enables pods running in GKE to automatically authenticate with Google Cloud services without managing service account keys. This method provides better security by eliminating the need to store and manage service account key files.

### Prerequisites
1. Enable Workload Identity on your GKE cluster
2. Create a Google Cloud service account (GSA)
3. Create a Kubernetes service account (KSA)
4. Configure the IAM binding between the GSA and KSA

### Required Instance Permissions
The GKE node pool must have the following configurations:
- Workload Identity enabled at the cluster level
- Node pool configured with the appropriate service account
- Service account must have these IAM roles:
  - `roles/monitoring.viewer` - For reading metrics data
  - `roles/iam.workloadIdentityUser` - For Workload Identity federation

### Configuration Steps

Ensure your GKE node pool has:
- Workload Identity enabled
- Service account with:
  - `roles/monitoring.viewer` - For reading metrics data
  - `roles/iam.workloadIdentityUser` - For Workload Identity federation


### Configuration Example

```yaml
inputs:
  - integration: prometheus
    slaos_key: prometheus_metrics
    type: metrics
    prometheus:
      base_url: "https://monitoring.googleapis.com/v1/projects/[PROJECT_ID]/location/global/prometheus"
      auth:
        assume_identity: "gcloud_workload"  # Use Workload Identity
      queries:
        - query: 'up'
          step:
            value: 60
            unit: "s"
          slaos_metric_name: "requests_by_endpoint"
          organization_identifier: "user_id"
```

### Security Benefits
- No service account keys to manage or rotate
- Reduced risk of key exposure
- Automatic credential management
- Fine-grained access control through KSA-GSA binding
- Audit logging of service account usage
