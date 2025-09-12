# Execution Flow

## Namespace Creation Sequence

The namespace creation process follows this optimized sequence:

```mermaid
sequenceDiagram
    participant NS as Namespace Event
    participant W as Watcher
    participant CM as Cert Manager
    participant DNS as DNS Manager
    participant AWS as AWS Manager
    participant K as Kong Manager
    participant Z as Zadig Manager
    
    NS->>W: ADDED Event
    W->>W: Check should_track
    
    par Certificate Creation
        W->>CM: Create Certificate
        CM->>CM: Create Certificate CRD
        CM->>CM: Wait for Secret (DNS-01 validation)
        CM->>K: Upload Certificate to Kong
    and Parallel Tasks
        W->>AWS: Create SQS Queues
        W->>AWS: Create SNS Topics
        W->>K: Create Routes
        W->>Z: Update Workflows
    end
    
    CM-->>W: Certificate Ready
    W->>DNS: Create DNS A Record
    DNS-->>W: DNS Record Created
    
    W->>W: Mark as stored
```

## Key Changes

1. **Certificate First**: Certificate creation starts immediately using cert-manager's DNS-01 validation
2. **Parallel Execution**: AWS, Kong routes, and Zadig updates run in parallel with certificate creation
3. **DNS After Certificate**: DNS A record is created after certificate is ready to ensure proper domain resolution

## Cancellation Support

If a namespace is deleted during creation:

1. All running tasks are cancelled
2. Resources already created are cleaned up in the deletion process
3. The creation is marked as cancelled and deletion is skipped if not fully created

## Resource Dependencies

- **Certificate**: Independent (uses DNS-01 validation)
- **DNS Record**: Depends on Certificate completion
- **AWS Resources**: Independent
- **Kong Routes**: Independent
- **Zadig Workflows**: Independent

This ensures maximum parallelization while respecting necessary dependencies.