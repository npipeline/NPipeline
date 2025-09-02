---
title: Dead-Letter Queues
description: Learn how to implement dead-letter queues in NPipeline using IDeadLetterSink to capture and analyze problematic items.
sidebar_position: 9
---

# Dead-Letter Queues

Dead-letter queues in NPipeline provide a mechanism to capture and store items that repeatedly fail processing, allowing you to analyze and potentially reprocess problematic data outside the main pipeline flow.

## Overview

When an `INodeErrorHandler` returns `NodeErrorDecision.DeadLetter`, the failed item is sent to an `IDeadLetterSink`. This pattern allows your pipeline to continue processing other items while isolating problematic ones for later analysis and potential reprocessing.

## IDeadLetterSink Interface

The `IDeadLetterSink` interface defines the contract for dead-letter queue implementations:

```csharp
public interface IDeadLetterSink
{
    /// <summary>
    ///     Handles a failed item by persisting it for later analysis.
    /// </summary>
    /// <param name="nodeId">The ID of node where error occurred.</param>
    /// <param name="item">The item that failed processing.</param>
    /// <param name="error">The exception that was thrown.</param>
    /// <param name="context">The current pipeline context.</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests.</param>
    Task HandleAsync(string nodeId, object item, Exception error, PipelineContext context, CancellationToken cancellationToken);
}
```

* **`nodeId`**: The ID of the node where the error occurred.
* **`item`**: The item that failed processing (non-generic object type).
* **`error`**: The exception that was thrown.
* **`context`**: The current pipeline context.
* **`cancellationToken`**: A token to observe for cancellation requests.
* **`HandleAsync`**: Called when an item needs to be sent to dead-letter queue. Receives the node ID, failed item, exception, pipeline context, and a cancellation token.

## Implementing a Custom Dead Letter Sink

### File-based Dead Letter Sink

```csharp
public class FileDeadLetterSink : IDeadLetterSink
{
    private readonly string _filePath;
    private readonly ILogger _logger;

    public FileDeadLetterSink(string filePath, ILogger logger)
    {
        _filePath = filePath;
        _logger = logger;
    }

    public async Task HandleAsync(string nodeId, object item, Exception error, PipelineContext context, CancellationToken cancellationToken)
    {
        var deadLetterEntry = new
        {
            Timestamp = DateTime.UtcNow,
            NodeId = nodeId,
            ItemType = item?.GetType().Name ?? "Unknown",
            Item = item,
            Error = error.Message,
            StackTrace = error.StackTrace,
            ErrorType = error.GetType().Name
        };

        var json = JsonSerializer.Serialize(deadLetterEntry, new JsonSerializerOptions { WriteIndented = true });

        await File.AppendAllTextAsync(_filePath, json + Environment.NewLine, cancellationToken);
        _logger.LogWarning("Item from node {NodeId} sent to dead-letter queue: {ItemType}", nodeId, item?.GetType().Name);
    }
}
```

### Database-based Dead Letter Sink

```csharp
public class DatabaseDeadLetterSink : IDeadLetterSink
{
    private readonly IDbConnection _connection;
    private readonly ILogger _logger;
    private readonly string _tableName;

    public DatabaseDeadLetterSink(IDbConnection connection, string tableName, ILogger logger)
    {
        _connection = connection;
        _tableName = tableName;
        _logger = logger;
    }

    public async Task HandleAsync(string nodeId, object item, Exception error, PipelineContext context, CancellationToken cancellationToken)
    {
        try
        {
            var command = _connection.CreateCommand();
            command.CommandText = $@"
                INSERT INTO {_tableName} (Timestamp, NodeId, ItemData, ErrorType, ErrorMessage, StackTrace, ItemType)
                VALUES (@Timestamp, @NodeId, @ItemData, @ErrorType, @ErrorMessage, @StackTrace, @ItemType)";

            command.Parameters.Add(new Parameter("@Timestamp", DateTime.UtcNow));
            command.Parameters.Add(new Parameter("@NodeId", nodeId));
            command.Parameters.Add(new Parameter("@ItemData", JsonSerializer.Serialize(item)));
            command.Parameters.Add(new Parameter("@ErrorType", error.GetType().Name));
            command.Parameters.Add(new Parameter("@ErrorMessage", error.Message));
            command.Parameters.Add(new Parameter("@StackTrace", error.StackTrace));
            command.Parameters.Add(new Parameter("@ItemType", item?.GetType().Name ?? "Unknown"));

            await command.ExecuteNonQueryAsync(cancellationToken);
            _logger.LogWarning("Item from node {NodeId} sent to dead-letter table: {ItemType}", nodeId, item?.GetType().Name);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send item to dead-letter queue");
            throw;
        }
    }
}
```

### Message Queue-based Dead Letter Sink

```csharp
public class MessageQueueDeadLetterSink : IDeadLetterSink
{
    private readonly IMessageQueue _messageQueue;
    private readonly string _queueName;
    private readonly ILogger _logger;

    public MessageQueueDeadLetterSink(IMessageQueue messageQueue, string queueName, ILogger logger)
    {
        _messageQueue = messageQueue;
        _queueName = queueName;
        _logger = logger;
    }

    public async Task HandleAsync(string nodeId, object item, Exception error, PipelineContext context, CancellationToken cancellationToken)
    {
        var deadLetterMessage = new DeadLetterMessage
        {
            Id = Guid.NewGuid().ToString(),
            Timestamp = DateTime.UtcNow,
            NodeId = nodeId,
            Item = item,
            ErrorType = error.GetType().Name,
            ErrorMessage = error.Message,
            StackTrace = error.StackTrace,
            ItemType = item?.GetType().Name ?? "Unknown",
            RetryCount = 0 // Can be incremented if item is reprocessed
        };

        await _messageQueue.SendMessageAsync(_queueName, deadLetterMessage, cancellationToken);
        _logger.LogWarning("Item {ItemId} from node {NodeId} sent to dead-letter queue", deadLetterMessage.Id, nodeId);
    }

    private class DeadLetterMessage
    {
        public string Id { get; set; }
        public DateTime Timestamp { get; set; }
        public string NodeId { get; set; }
        public object Item { get; set; }
        public string ErrorType { get; set; }
        public string ErrorMessage { get; set; }
        public string StackTrace { get; set; }
        public string ItemType { get; set; }
        public int RetryCount { get; set; }
    }
}
```

## Registering a Dead Letter Sink

You register a dead letter sink with your DI container:

```csharp
// File-based dead letter sink
services.AddSingleton<IDeadLetterSink>(provider =>
    new FileDeadLetterSink("dead-letters.json", provider.GetRequiredService<ILogger<FileDeadLetterSink>>()));

// Database-based dead letter sink
services.AddSingleton<IDeadLetterSink>(provider =>
{
    var connection = provider.GetRequiredService<IDbConnection>();
    var logger = provider.GetRequiredService<ILogger<DatabaseDeadLetterSink>>();
    return new DatabaseDeadLetterSink(connection, "DeadLetterItems", logger);
});

// Message queue-based dead letter sink
services.AddSingleton<IDeadLetterSink>(provider =>
{
    var messageQueue = provider.GetRequiredService<IMessageQueue>();
    var logger = provider.GetRequiredService<ILogger<MessageQueueDeadLetterSink>>();
    return new MessageQueueDeadLetterSink(messageQueue, "dead-letter-queue", logger);
});
```

## Complete Pipeline Configuration Example

Here's a practical example showing how to configure a full pipeline with dead-letter queue support:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NPipeline;
using NPipeline.ErrorHandling;

public class Program
{
    public static async Task Main(string[] args)
    {
        var services = new ServiceCollection();

        // Configure logging
        services.AddLogging(builder =>
            builder.AddConsole()
                .SetMinimumLevel(LogLevel.Information));

        // Step 1: Register the dead-letter sink
        services.AddSingleton<IDeadLetterSink>(provider =>
            new FileDeadLetterSink(
                "dead-letters.json",
                provider.GetRequiredService<ILogger<FileDeadLetterSink>>()));

        // Step 2: Register error handler that uses the dead-letter sink
        services.AddSingleton<INodeErrorHandler<ITransformNode<string, string>, string>>(provider =>
            new DeadLetterAwareErrorHandler(
                provider.GetRequiredService<ILogger<DeadLetterAwareErrorHandler>>(),
                provider.GetRequiredService<IDeadLetterSink>()));

        var serviceProvider = services.BuildServiceProvider();

        // Step 3: Build and execute the pipeline
        var pipeline = new PipelineBuilder<string>()
            .AddTransform("ValidationNode", async (item, context, ct) =>
            {
                // Simulate validation that might fail
                if (item.Contains("INVALID"))
                    throw new ValidationException("Item contains invalid content");
                return item.ToUpper();
            })
            .AddTransform("ProcessingNode", async (item, context, ct) =>
            {
                // Simulate processing
                return $"Processed: {item}";
            })
            .Build();

        // Get the error handler from DI
        var errorHandler = serviceProvider.GetRequiredService<INodeErrorHandler<ITransformNode<string, string>, string>>();

        var context = PipelineContext.Default;
        var items = new[] { "valid1", "INVALID_ITEM", "valid2", "INVALID_ITEM2" };

        foreach (var item in items)
        {
            try
            {
                await pipeline.ExecuteAsync(item, context, CancellationToken.None);
                Console.WriteLine($"✓ Successfully processed: {item}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"✗ Failed to process: {item}");
                Console.WriteLine($"  Error: {ex.Message}");
            }
        }
    }
}

public class DeadLetterAwareErrorHandler : INodeErrorHandler<ITransformNode<string, string>, string>
{
    private readonly ILogger _logger;
    private readonly IDeadLetterSink _deadLetterSink;

    public DeadLetterAwareErrorHandler(
        ILogger logger,
        IDeadLetterSink deadLetterSink)
    {
        _logger = logger;
        _deadLetterSink = deadLetterSink;
    }

    public async Task<NodeErrorDecision> HandleAsync(
        ITransformNode<string, string> node,
        string failedItem,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        _logger.LogError(error, "Error processing item in node {NodeName}", node.Name);

        // Send validation errors to dead-letter queue
        if (error is ValidationException)
        {
            _logger.LogInformation("Validation error, redirecting to dead-letter queue");
            await _deadLetterSink.HandleAsync(node.Id, failedItem, error, context, cancellationToken);
            return NodeErrorDecision.DeadLetter;
        }

        // Log and skip other errors
        _logger.LogWarning("Unexpected error, skipping item");
        return NodeErrorDecision.Skip;
    }
}

public class FileDeadLetterSink : IDeadLetterSink
{
    private readonly string _filePath;
    private readonly ILogger _logger;

    public FileDeadLetterSink(string filePath, ILogger logger)
    {
        _filePath = filePath;
        _logger = logger;
    }

    public async Task HandleAsync(string nodeId, object item, Exception error, PipelineContext context, CancellationToken cancellationToken)
    {
        var deadLetterEntry = new
        {
            Timestamp = DateTime.UtcNow,
            NodeId = nodeId,
            ItemType = item?.GetType().Name ?? "Unknown",
            Item = item,
            Error = error.Message,
            StackTrace = error.StackTrace,
            ErrorType = error.GetType().Name
        };

        var json = JsonSerializer.Serialize(deadLetterEntry, new JsonSerializerOptions { WriteIndented = true });

        await File.AppendAllTextAsync(_filePath, json + Environment.NewLine, cancellationToken);
        _logger.LogWarning("Item from node {NodeId} sent to dead-letter queue: {Item}", nodeId, item);
    }
}

public class ValidationException : Exception
{
    public ValidationException(string message) : base(message) { }
}
```

**Key configuration steps:**

1. **Register the sink**: Register your chosen `IDeadLetterSink` implementation in the DI container.
2. **Register the error handler**: Register an `INodeErrorHandler` that uses the dead-letter sink to send failed items to the queue.
3. **Build the pipeline**: Create your pipeline with the appropriate nodes.
4. **Execute and handle errors**: When items fail, the error handler determines whether to redirect them to the dead-letter queue or skip them.

## Using Dead Letter Queues with Error Handlers

### Basic Usage with Node Error Handler

```csharp
public class DeadLetterAwareErrorHandler : INodeErrorHandler<ITransformNode<string, string>, string>
{
    private readonly ILogger _logger;
    private readonly IDeadLetterSink _deadLetterSink;

    public DeadLetterAwareErrorHandler(
        ILogger logger,
        IDeadLetterSink deadLetterSink)
    {
        _logger = logger;
        _deadLetterSink = deadLetterSink;
    }

    public async Task<NodeErrorDecision> HandleAsync(
        ITransformNode<string, string> node,
        string failedItem,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        _logger.LogError(error, "Error processing item in node {NodeName}", node.Name);

        if (error is ValidationException)
        {
            _logger.LogInformation("Validation error, redirecting to dead-letter queue");
            await _deadLetterSink.HandleAsync(node.Id, failedItem, error, context, cancellationToken);
            return NodeErrorDecision.DeadLetter;
        }
        else if (error is FormatException)
        {
            _logger.LogInformation("Format error, redirecting to dead-letter queue");
            await _deadLetterSink.HandleAsync(node.Id, failedItem, error, context, cancellationToken);
            return NodeErrorDecision.DeadLetter;
        }
        else
        {
            _logger.LogWarning("Unexpected error, skipping item");
            return NodeErrorDecision.Skip;
        }
    }
}
```

### Advanced Dead Letter Processing with Metadata

```csharp
public class EnhancedDeadLetterSink : IDeadLetterSink<object>
{
    private readonly string _filePath;
    private readonly ILogger _logger;
    private readonly IMetrics _metrics;

    public EnhancedDeadLetterSink(string filePath, ILogger logger, IMetrics metrics)
    {
        _filePath = filePath;
        _logger = logger;
        _metrics = metrics;
    }

    public async Task SendAsync(object item, Exception error, CancellationToken cancellationToken)
    {
        var deadLetterEntry = new DeadLetterEntry
        {
            Id = Guid.NewGuid().ToString(),
            Timestamp = DateTime.UtcNow,
            Item = item,
            ItemType = item.GetType().Name,
            Error = new ErrorInfo
            {
                Type = error.GetType().Name,
                Message = error.Message,
                StackTrace = error.StackTrace,
                Source = error.Source,
                HResult = error.HResult
            },
            Metadata = new Dictionary<string, object>
            {
                ["Environment"] = Environment.GetEnvironmentVariable("ENVIRONMENT") ?? "Unknown",
                ["MachineName"] = Environment.MachineName,
                ["ProcessId"] = Environment.ProcessId
            }
        };

        var json = JsonSerializer.Serialize(deadLetterEntry, new JsonSerializerOptions { WriteIndented = true });

        await File.AppendAllTextAsync(_filePath, json + Environment.NewLine, cancellationToken);

        _logger.LogWarning("Item {ItemId} sent to dead-letter queue: {ErrorType}",
            deadLetterEntry.Id, error.GetType().Name);

        _metrics.Increment("dead_letter_items", new[]
        {
            new KeyValuePair<string, object>("error_type", error.GetType().Name),
            new KeyValuePair<string, object>("item_type", item.GetType().Name)
        });
    }

    private class DeadLetterEntry
    {
        public string Id { get; set; }
        public DateTime Timestamp { get; set; }
        public object Item { get; set; }
        public string ItemType { get; set; }
        public ErrorInfo Error { get; set; }
        public Dictionary<string, object> Metadata { get; set; }
    }

    private class ErrorInfo
    {
        public string Type { get; set; }
        public string Message { get; set; }
        public string StackTrace { get; set; }
        public string Source { get; set; }
        public int HResult { get; set; }
    }
}
```

## Reprocessing Dead Letter Items

### Simple Reprocessing Service

```csharp
public class DeadLetterReprocessor
{
    private readonly IDeadLetterSink<object> _deadLetterSink;
    private readonly ILogger<DeadLetterReprocessor> _logger;
    private readonly ITransformNode<string, string> _targetNode;

    public DeadLetterReprocessor(
        IDeadLetterSink<object> deadLetterSink,
        ILogger<DeadLetterReprocessor> logger,
        ITransformNode<string, string> targetNode)
    {
        _deadLetterSink = deadLetterSink;
        _logger = logger;
        _targetNode = targetNode;
    }

    public async Task ReprocessItemsAsync(string filePath, CancellationToken cancellationToken = default)
    {
        var lines = await File.ReadAllLinesAsync(filePath, cancellationToken);
        var reprocessedCount = 0;
        var failedCount = 0;

        foreach (var line in lines)
        {
            try
            {
                var deadLetterEntry = JsonSerializer.Deserialize<DeadLetterEntry>(line);

                if (deadLetterEntry?.Item is string item)
                {
                    _logger.LogInformation("Reprocessing item: {ItemId}", deadLetterEntry.Id);

                    // Attempt to reprocess the item
                    var result = await _targetNode.ExecuteAsync(
                        item,
                        PipelineContext.Default,
                        cancellationToken);

                    reprocessedCount++;
                    _logger.LogInformation("Successfully reprocessed item: {ItemId}", deadLetterEntry.Id);
                }
            }
            catch (Exception ex)
            {
                failedCount++;
                _logger.LogError(ex, "Failed to reprocess dead letter item");
            }
        }

        _logger.LogInformation("Reprocessing complete. Success: {SuccessCount}, Failed: {FailedCount}",
            reprocessedCount, failedCount);
    }
}
```

## Best Practices

1. **Include sufficient context**: Store not just the failed item but also error details, timestamps, and any relevant metadata.

2. **Monitor dead-letter queues**: Set up monitoring and alerting for items being added to dead-letter queues.

3. **Implement reprocessing workflows**: Create processes to analyze and potentially reprocess dead-letter items.

4. **Consider retention policies**: Implement policies for how long dead-letter items should be retained.

5. **Use appropriate storage**: Choose storage that matches your performance and durability requirements.

6. **Implement proper error handling**: Ensure your dead-letter sink itself has robust error handling.

7. **Track metrics**: Monitor the volume and types of items being sent to dead-letter queues to identify systemic issues.

## Production Example

Here's a comprehensive example that combines multiple dead-letter queue concepts:

```csharp
using NPipeline;
using NPipeline.ErrorHandling;

public class ProductionDeadLetterErrorHandler : INodeErrorHandler<ITransformNode<string, string>, string>
{
    private readonly ILogger _logger;
    private readonly IMetrics _metrics;
    private readonly IDeadLetterSink<string> _deadLetterSink;
    private readonly ConcurrentDictionary<string, int> _itemRetryCounts = new();
    private readonly int _maxRetriesBeforeDeadLetter = 3;

    public ProductionDeadLetterErrorHandler(
        ILogger logger,
        IMetrics metrics,
        IDeadLetterSink<string> deadLetterSink)
    {
        _logger = logger;
        _metrics = metrics;
        _deadLetterSink = deadLetterSink;
    }

    public async Task<NodeErrorDecision> HandleAsync(
        ITransformNode<string, string> node,
        string failedItem,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var itemKey = $"{node.Id}:{failedItem.GetHashCode()}";
        var retryCount = _itemRetryCounts.AddOrUpdate(itemKey, 1, (_, count) => count + 1);

        // Record metrics
        _metrics.Increment("node_error_handling_attempts", new[]
        {
            new KeyValuePair<string, object>("node_id", node.Id),
            new KeyValuePair<string, object>("error_type", error.GetType().Name),
            new KeyValuePair<string, object>("retry_count", retryCount)
        });

        _logger.LogError(error, "Error processing item in node {NodeName} (attempt {RetryCount})",
            node.Name, retryCount);

        if (IsTransientError(error) && retryCount <= _maxRetriesBeforeDeadLetter)
        {
            _logger.LogInformation("Retrying item (attempt {RetryCount}/{MaxRetries})", retryCount, _maxRetriesBeforeDeadLetter);
            return NodeErrorDecision.Retry;
        }

        // Send to dead-letter queue
        try
        {
            await _deadLetterSink.SendAsync(failedItem, error, cancellationToken);

            _metrics.Increment("dead_letter_items_sent", new[]
            {
                new KeyValuePair<string, object>("node_id", node.Id),
                new KeyValuePair<string, object>("error_type", error.GetType().Name),
                new KeyValuePair<string, object>("retry_count", retryCount)
            });

            _logger.LogWarning("Item sent to dead-letter queue after {RetryCount} attempts", retryCount);
            _itemRetryCounts.TryRemove(itemKey, out _);
            return NodeErrorDecision.DeadLetter;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send item to dead-letter queue");
            return NodeErrorDecision.Skip;
        }
    }

    private static bool IsTransientError(Exception error)
    {
        return error is TimeoutException or HttpRequestException or OperationCanceledException;
    }
}
```

## Configuration Guidance

For comprehensive setup guidance that integrates dead-letter queues with other resilience features, see the [Configuration Guide](../resilience/configuration-guide.md) in the resilience section.

## See Also

* **[Resilience Overview](../resilience/index.md)**: Comprehensive guide to building fault-tolerant pipelines
* **[Configuration Guide](../resilience/configuration-guide.md)**: Practical implementation guidance with code examples
* **[Dependency Chains](../resilience/dependency-chains.md)**: Understanding critical prerequisite relationships for resilience features
* **[Troubleshooting](../resilience/troubleshooting.md)**: Diagnose and resolve common resilience issues

## Related Topics

* **[Node-level Error Handling](error-handling-guide.md)**: Learn about handling errors for individual items.
* **[Pipeline-level Error Handling](error-handling-guide.md)**: Learn about handling errors that affect entire node streams.
* **[Retry Configuration](retry-configuration.md)**: Configure retry behavior for items and node restarts.
* **[Error Handling Overview](error-handling-guide.md)**: Return to the error handling overview.

