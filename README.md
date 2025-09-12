# Namespace Watcher

A Kubernetes controller that automatically manages infrastructure resources when namespaces are created or deleted.

## Features

- **Certificate Management**: Automatically creates SSL certificates using cert-manager with DNS-01 validation
- **DNS Management**: Creates DNS A records in DNS Made Easy after certificate issuance
- **AWS Resources**: Creates SQS queues and SNS topics
- **Kong API Gateway**: Configures routes and uploads certificates
- **Apollo Configuration**: Manages configuration database
- **Zadig Workflow**: Updates workflow configurations

## Architecture

The application uses:
- Async/await pattern for concurrent operations
- Modular manager classes for different resources
- Configuration through environment variables
- Kubernetes secrets for sensitive data
- Graceful shutdown handling

## Installation

### Local Development

1. Clone the repository
2. Copy `.env.example` to `.env` and update values
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Run the application:
   ```bash
   python -m src.main
   ```

### Kubernetes Deployment

1. Update secrets in `k8s/secret.yaml`
2. Update configuration in `k8s/configmap.yaml`
3. Build and push Docker image:
   ```bash
   docker build -t namespace-watcher:latest .
   docker push your-registry/namespace-watcher:latest
   ```
4. Deploy to Kubernetes:
   ```bash
   kubectl apply -f k8s/
   ```

## Configuration

All configuration is done through environment variables. See `.env.example` for available options.

### Feature Flags

You can enable/disable specific features:
- `ENABLE_CERT_MANAGEMENT`: SSL certificate management
- `ENABLE_DNS_MANAGEMENT`: DNS record management
- `ENABLE_AWS_RESOURCES`: AWS SQS/SNS management
- `ENABLE_KONG_ROUTES`: Kong route management
- `ENABLE_APOLLO_CONFIG`: Apollo configuration
- `ENABLE_ZADIG_WORKFLOW`: Zadig workflow updates

## Security

- All credentials are stored in Kubernetes secrets
- Uses service account with minimal permissions
- Non-root container execution
- Certificates managed through Kubernetes-native cert-manager

## Monitoring

The application logs all operations with structured logging. Monitor the pod logs:

```bash
kubectl logs -f deployment/namespace-watcher
```

## Troubleshooting

1. Check pod status:
   ```bash
   kubectl get pods -l app=namespace-watcher
   ```

2. View logs:
   ```bash
   kubectl logs -f deployment/namespace-watcher
   ```

3. Verify permissions:
   ```bash
   kubectl auth can-i --as=system:serviceaccount:default:namespace-watcher list namespaces
   ```

## License

MIT