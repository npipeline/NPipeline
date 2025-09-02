# Release Notes - NPipeline.Analyzers

## Rules

| Rule ID | Title                                                           | Category    | Severity |
|---------|-----------------------------------------------------------------|-------------|----------|
| NP9001  | RestartNode decision requires complete resilience configuration | Resilience  | Warning  |
| NP9102  | Blocking operations in async methods                            | Performance | Warning  |
| NP9103  | Swallowing OperationCanceledException                           | Reliability | Warning  |

## Summary

### NP9001

**Introduced in:** 1.0.0

Detects when `PipelineErrorDecision.RestartNode` can be returned but three mandatory prerequisites are missing. Missing prerequisites will silently disable
restart functionality, causing entire pipeline to fail instead of recovering failed node.

**Mandatory prerequisites:**

1. ResilientExecutionStrategy wrapping the node
2. MaxNodeRestartAttempts > 0 in PipelineRetryOptions
3. MaxMaterializedItems != null in PipelineRetryOptions

**Helpful links:**

- [Node Restart Quick Start Guide](https://npipeline.dev/docs/core-concepts/resilience/node-restart-quickstart)
- [Resilient Execution Strategy](https://npipeline.dev/docs/core-concepts/resilience/resilient-execution-strategy)

### NP9102

**Introduced in:** 1.0.0

Detects blocking operations in async methods that can lead to deadlocks, thread pool starvation, and reduced performance. The analyzer identifies blocking
patterns such as Task.Result, Task.Wait(), Thread.Sleep, and synchronous I/O operations.

**Detected patterns:**

1. Task.Result and Task.Wait() calls that block the current thread
2. GetAwaiter().GetResult() patterns that synchronously wait for task completion
3. Thread.Sleep() in async methods (should use Task.Delay instead)
4. Synchronous file I/O operations (File.ReadAllText, File.WriteAllBytes, etc.)
5. Synchronous network I/O operations (WebClient.DownloadString, unawaited HttpClient calls)
6. Unawaited StreamReader/Writer operations (ReadToEnd, WriteLine without await)

**Helpful links:**

- [Async Programming Best Practices](https://npipeline.dev/docs/core-concepts/async-programming/best-practices)
- [Avoiding Deadlocks in Async Code](https://npipeline.dev/docs/core-concepts/async-programming/avoiding-deadlocks)

### NP9103

**Introduced in:** 1.0.0

Detects problematic catch patterns that can swallow `OperationCanceledException`, preventing proper cancellation propagation in pipelines. Swallowing
cancellation exceptions breaks the cancellation chain and can cause operations to hang indefinitely.

**Detected patterns:**

1. Broad catch of Exception that doesn't re-throw OperationCanceledException
2. Direct catch of OperationCanceledException without re-throw
3. Catch of AggregateException without checking inner exceptions for OperationCanceledException

**Why this matters:**

- Prevents hanging operations that ignore cancellation requests
- Avoids resource leaks when cancellation isn't properly handled
- Ensures responsive user experience with proper cancellation handling
- Prevents pipeline deadlocks when downstream stages wait indefinitely

**Helpful links:**

- [Cancellation in NPipeline](https://npipeline.dev/docs/core-concepts/cancellation/cancellation-overview)
- [Graceful Shutdown Patterns](https://npipeline.dev/docs/core-concepts/cancellation/graceful-shutdown)
