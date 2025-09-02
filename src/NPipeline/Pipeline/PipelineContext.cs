using System.Diagnostics.CodeAnalysis;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Lineage;
using NPipeline.Observability;
using NPipeline.Observability.Logging;
using NPipeline.Observability.Tracing;
using NPipeline.State;

namespace NPipeline.Pipeline;

/// <summary>
///     Provides a context for a single pipeline run, allowing for the passing of runtime configuration and state.
/// </summary>
public sealed class PipelineContext
{
    // Composite disposal registry for lifecycle-managed IAsyncDisposable resources
    private readonly List<IAsyncDisposable> _disposables = new();
    private bool _disposed;
    private IExecutionObserver _executionObserver = NullExecutionObserver.Instance;

    /// <summary>
    ///     Creates a new <see cref="PipelineContext" /> with the specified configuration.
    ///     All unspecified components use sensible defaults.
    /// </summary>
    /// <remarks>
    ///     This is the recommended way to create a context for most use cases.
    ///     Components not provided will use defaults:
    ///     <list type="bullet">
    ///         <item>
    ///             <description>ErrorHandlerFactory: <see cref="DefaultErrorHandlerFactory" /></description>
    ///         </item>
    ///         <item>
    ///             <description>LineageFactory: <see cref="DefaultLineageFactory" /></description>
    ///         </item>
    ///         <item>
    ///             <description>ObservabilityFactory: <see cref="DefaultObservabilityFactory" /></description>
    ///         </item>
    ///         <item>
    ///             <description>LoggerFactory: <see cref="NullPipelineLoggerFactory" /> (no-op)</description>
    ///         </item>
    ///         <item>
    ///             <description>Tracer: <see cref="NullPipelineTracer" /> (no-op)</description>
    ///         </item>
    ///         <item>
    ///             <description>RetryOptions: <see cref="PipelineRetryOptions.Default" /> (no retries)</description>
    ///         </item>
    ///     </list>
    /// </remarks>
    /// <example>
    ///     <code>
    ///         // Simple - all defaults
    ///         var context = new PipelineContext();
    /// 
    ///         // With configuration (cancellation token)
    ///         var context = new PipelineContext(
    ///             PipelineContextConfiguration.WithCancellation(cancellationToken));
    /// 
    ///         // With complex configuration
    ///         var config = new PipelineContextConfiguration(
    ///             CancellationToken: cancellationToken,
    ///             Parameters: parameters);
    ///         var context = new PipelineContext(config);
    ///     </code>
    /// </example>
    public PipelineContext(PipelineContextConfiguration? config = null)
    {
        config ??= PipelineContextConfiguration.Default;

        Parameters = config.Parameters ?? new Dictionary<string, object>();
        Items = config.Items ?? new Dictionary<string, object>();
        Properties = config.Properties ?? new Dictionary<string, object>();
        CancellationToken = config.CancellationToken;
        LoggerFactory = config.LoggerFactory ?? NullPipelineLoggerFactory.Instance;
        Tracer = config.Tracer ?? NullPipelineTracer.Instance;
        PipelineErrorHandler = config.PipelineErrorHandler;
        DeadLetterSink = config.DeadLetterSink;
        ErrorHandlerFactory = config.ErrorHandlerFactory ?? new DefaultErrorHandlerFactory();
        LineageFactory = config.LineageFactory ?? new DefaultLineageFactory();
        ObservabilityFactory = config.ObservabilityFactory ?? new DefaultObservabilityFactory();
        RetryOptions = config.RetryOptions ?? PipelineRetryOptions.Default;
    }

    /// <summary>
    ///     A dictionary to hold any runtime parameters for the pipeline.
    /// </summary>
    public Dictionary<string, object> Parameters { get; }

    /// <summary>
    ///     A dictionary for sharing state between pipeline nodes.
    /// </summary>
    public Dictionary<string, object> Items { get; }

    /// <summary>
    ///     A dictionary for storing properties that can be used by extensions and plugins.
    ///     This provides a way to extend the PipelineContext without modifying its core structure.
    /// </summary>
    public Dictionary<string, object> Properties { get; }

    /// <summary>
    ///     A cancellation token to monitor for pipeline cancellation requests.
    /// </summary>
    public CancellationToken CancellationToken { get; }

    /// <summary>
    ///     The logger factory for this pipeline run.
    /// </summary>
    public IPipelineLoggerFactory LoggerFactory { get; }

    /// <summary>
    ///     The tracer for this pipeline run.
    /// </summary>
    public IPipelineTracer Tracer { get; }

    /// <summary>
    ///     The error handler for the entire pipeline.
    /// </summary>
    public IPipelineErrorHandler? PipelineErrorHandler { get; internal set; }

    /// <summary>
    ///     The sink for items that have failed processing and have been redirected.
    /// </summary>
    public IDeadLetterSink? DeadLetterSink { get; internal set; }

    /// <summary>
    ///     The factory for creating error handlers and dead-letter sinks.
    /// </summary>
    public IErrorHandlerFactory ErrorHandlerFactory { get; }

    /// <summary>
    ///     The factory for creating lineage-related components.
    /// </summary>
    public ILineageFactory LineageFactory { get; }

    /// <summary>
    ///     The factory for resolving observability-related components.
    /// </summary>
    public IObservabilityFactory ObservabilityFactory { get; }

    /// <summary>
    ///     The ID of the node currently being executed.
    /// </summary>
    public string CurrentNodeId { get; private set; } = string.Empty;

    /// <summary>
    ///     Execution observer for instrumentation (node lifecycle, retries, queue/backpressure events).
    ///     Defaults to <see cref="NullExecutionObserver.Instance" /> which provides zero-overhead observation.
    ///     Set to a different observer implementation to enable actual instrumentation.
    ///     If set to null, automatically falls back to <see cref="NullExecutionObserver.Instance" />.
    /// </summary>
    public IExecutionObserver ExecutionObserver
    {
        get => _executionObserver;
        set => _executionObserver = value ?? NullExecutionObserver.Instance;
    }

    /// <summary>
    ///     Execution / retry configuration for this pipeline run.
    ///     Values here override builder defaults when provided.
    /// </summary>
    public PipelineRetryOptions RetryOptions { get; }

    /// <summary>
    ///     Creates a default pipeline context with all default values.
    /// </summary>
    public static PipelineContext Default => new(PipelineContextConfiguration.Default);


    /// <summary>
    ///     Gets the state manager for this pipeline run, if available.
    /// </summary>
    public IPipelineStateManager? StateManager => Properties.TryGetValue("NPipeline.StateManager", out var sm) && sm is IPipelineStateManager manager
        ? manager
        : null;

    /// <summary>
    ///     Gets the stateful registry for this pipeline run, if available.
    /// </summary>
    public IStatefulRegistry? StatefulRegistry => Properties.TryGetValue("NPipeline.State.StatefulRegistry", out var reg) && reg is IStatefulRegistry registry
        ? registry
        : null;

    /// <summary>
    ///     Registers an <see cref="IAsyncDisposable" /> resource to be disposed when the pipeline context is disposed.
    /// </summary>
    public void RegisterForDisposal(IAsyncDisposable disposable)
    {
        if (_disposed)
        {
            // If already disposed, dispose immediately to avoid leaks.
            _ = Task.Run(async () =>
            {
                try
                {
                    await disposable.DisposeAsync().ConfigureAwait(false); // CA2012 satisfied by awaiting inside background task
                }
                catch
                {
                    // Swallow disposal exceptions in late-registration path
                }
            });

            return;
        }

        _disposables.Add(disposable);
    }

    /// <summary>
    ///     Disposes all registered async disposables. Safe to call multiple times.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        List<Exception>? errors = null;

        foreach (var d in _disposables)
        {
            try
            {
                await d.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                errors ??= new List<Exception>();
                errors.Add(ex);
            }
        }

        _disposables.Clear();

        if (errors is { Count: > 0 })
            throw new AggregateException("One or more errors occurred disposing pipeline context resources.", errors);
    }

    /// <summary>
    ///     Attempts to get the stateful registry for this pipeline run.
    /// </summary>
    /// <param name="registry">The stateful registry if available.</param>
    /// <returns>True if a stateful registry is available, false otherwise.</returns>
    public bool TryGetStatefulRegistry([NotNullWhen(true)] out IStatefulRegistry? registry)
    {
        registry = StatefulRegistry;
        return registry is not null;
    }

    /// <summary>
    ///     Sets the CurrentNodeId for the duration of the returned disposable scope.
    /// </summary>
    /// <param name="nodeId">The ID of the node to set as current.</param>
    /// <returns>An IDisposable that will restore the original node ID upon disposal.</returns>
    public IDisposable ScopedNode(string nodeId)
    {
        return new NodeScope(this, nodeId);
    }

    private sealed class NodeScope : IDisposable
    {
        private readonly PipelineContext _context;
        private readonly string _previousNodeId;

        public NodeScope(PipelineContext context, string newNodeId)
        {
            _context = context;
            _previousNodeId = context.CurrentNodeId;
            context.CurrentNodeId = newNodeId;
        }

        public void Dispose()
        {
            _context.CurrentNodeId = _previousNodeId;
        }
    }
}
