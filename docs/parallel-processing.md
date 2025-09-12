# Parallel Processing Capabilities

## Overview

The namespace watcher supports **full parallel processing** of multiple namespace events. This means:

- Multiple namespace creations can be processed simultaneously
- Multiple namespace deletions can be processed simultaneously
- Creation and deletion of different namespaces can happen in parallel

## Implementation Details

### Event Processing

When namespace events are received, they are processed asynchronously:

```python
# Instead of blocking with await:
# await self.handle_namespace_created(namespace_name)

# We create async tasks for parallel execution:
task = asyncio.create_task(self.handle_namespace_created(namespace_name))
```

### Concurrency Example

If you create 3 namespaces at the same time:
```bash
kubectl apply -f namespace1.yaml &
kubectl apply -f namespace2.yaml &
kubectl apply -f namespace3.yaml &
```

The processing timeline looks like:

```
Time →
namespace1: [--Certificate--][--DNS--][Done]
namespace2: [--Certificate--][--DNS--][Done]
namespace3: [--Certificate--][--DNS--][Done]
            ↑ All start simultaneously
```

### Resource-Level Parallelism

Within each namespace, resources are also created in parallel:

```
Namespace Creation
├── Certificate Creation
├── AWS SQS Queues (parallel)
├── AWS SNS Topics (parallel)
├── Kong Routes (parallel)
└── Zadig Workflows (parallel)
    └──> DNS Record (after certificate)
```

## Safety Mechanisms

### 1. Duplicate Prevention
- `processing_namespaces` set prevents duplicate processing
- `stored_namespaces` set tracks completed namespaces

### 2. Cancellation Support
- If deletion happens during creation, all tasks are cancelled
- Tasks are tracked in `creation_tasks` dictionary per namespace

### 3. Graceful Shutdown
- All active tasks are tracked in `namespace_tasks` set
- On shutdown, waits for all tasks to complete

## Performance Benefits

With parallel processing:
- **Sequential**: 3 namespaces × 10 seconds = 30 seconds total
- **Parallel**: 3 namespaces = ~10 seconds total (all processed simultaneously)

## Monitoring

You can see parallel processing in action through logs:

```
2025-09-12 16:55:10 - src.main - INFO - Creating the namespace test101
2025-09-12 16:55:10 - src.main - INFO - Creating the namespace test102
2025-09-12 16:55:10 - src.main - INFO - Creating the namespace test103
2025-09-12 16:55:10 - src.main - INFO - Processing namespace creation: test101
2025-09-12 16:55:10 - src.main - INFO - Processing namespace creation: test102
2025-09-12 16:55:10 - src.main - INFO - Processing namespace creation: test103
```

All three start processing immediately without waiting for each other.