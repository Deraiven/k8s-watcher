# Namespace Creation Requirements

## Important: Label Requirements for Namespace Detection

The namespace watcher **only** processes ADDED events. This means namespaces must be created with the required label in a **single operation** for automatic resource provisioning to work.

### ✅ Correct: Single Operation Creation

Use a YAML manifest that includes the label:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: test102
  labels:
    createdBy: koderover
```

Apply with:
```bash
kubectl apply -f namespace.yaml
```

### ❌ Incorrect: Two-Step Creation

Do NOT use this approach:
```bash
# This will NOT trigger resource creation
kubectl create namespace test101
kubectl label namespace test101 createdBy=koderover
```

The ADDED event fires immediately after `kubectl create namespace`, before the label is applied. Since the watcher only handles ADDED events (not MODIFIED), the namespace won't be processed.

## Technical Details

The watcher checks two conditions for ADDED events:
1. `should_track`: Namespace has prefix "test" and label `createdBy=koderover`
2. `not in_stored`: Namespace is not already being tracked

Both conditions must be true at the time of the ADDED event for automatic resource creation to trigger.

## Resources Created Automatically

When a properly labeled namespace is created, the following resources are provisioned:
- DNS record: `*.{namespace}.shub.us`
- SSL certificate via cert-manager
- Kong API Gateway routes
- AWS SQS queues and SNS topics
- Zadig workflow updates