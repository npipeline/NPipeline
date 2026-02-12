using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Pooling;
using NPipeline.Lineage;
using NPipeline.Observability;
using NPipeline.Observability.Logging;
using NPipeline.Observability.Tracing;
using NPipeline.State;

namespace NPipeline.Pipeline;

/// <summary>
///     Provides a context for a single pipeline run, allowing for the passing of runtime configuration and state.
/// </summary>
/// <remarks>
///     <para>
///         <strong>Thread Safety:</strong>
///     </para>
///     <para>
///         <see cref="PipelineContext" /> is designed for single-threaded execution within a pipeline run.
///         The <see cref="Parameters" />, <see cref="Items" />, and <see cref="Properties" /> dictionaries
///         are NOT thread-safe and should not be accessed concurrently from multiple threads.
///     </para>
///     <para>
///         <strong>Single-Pipeline Execution (Default):</strong>
///         All operations are inherently single-threaded. No synchronization needed.
///     </para>
///     <para>
///         <strong>Parallel Node Execution:</strong>
///         When using parallel execution strategies (e.g. ParallelExecutionStrategy),
///         each worker thread processes independent data items through the pipeline. The context itself is not shared across threads—
///         only the node instances and their configuration are shared. State updates during parallel execution should use:
///         <list type="bullet">
///             <item>
///                 <description>
///                     <see cref="IPipelineStateManager" /> for thread-safe, node-aware state management
///                 </description>
///             </item>
///             <item>
///                 <description>Node-level synchronization within custom node implementations</description>
///             </item>
///         </list>
///     </para>
///     <para>
///         <strong>Why Not ConcurrentDictionary?</strong>
///         Thread-safe dictionaries add overhead (locks, memory barriers, allocations) inappropriate for the common case
///         (single-threaded execution). NPipeline follows the principle of paying only for what you use. For the rare cases
///         requiring thread-safe shared state, use <see cref="IPipelineStateManager" /> instead.
///     </para>
/// </remarks>
public sealed class PipelineContext
{
    private readonly bool _ownsItemsDictionary;
    private readonly bool _ownsParametersDictionary;

    private readonly bool _ownsPropertiesDictionary;

    // Composite disposal registry for lifecycle-managed IAsyncDisposable resources (lazy initialized)
    private List<IAsyncDisposable>? _disposables;
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
    ///             <description>LoggerFactory: <see cref="NullLoggerFactory" /> (no-op)</description>
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

        if (config.Parameters is not null)
            Parameters = config.Parameters;
        else
        {
            Parameters = PipelineObjectPool.RentStringObjectDictionary();
            _ownsParametersDictionary = true;
        }

        if (config.Items is not null)
            Items = config.Items;
        else
        {
            Items = PipelineObjectPool.RentStringObjectDictionary();
            _ownsItemsDictionary = true;
        }

        if (config.Properties is not null)
            Properties = config.Properties;
        else
        {
            Properties = PipelineObjectPool.RentStringObjectDictionary();
            _ownsPropertiesDictionary = true;
        }

        CancellationToken = config.CancellationToken;
        LoggerFactory = config.LoggerFactory ?? NullLoggerFactory.Instance;
        Tracer = config.Tracer ?? NullPipelineTracer.Instance;
        PipelineErrorHandler = config.PipelineErrorHandler;
        DeadLetterSink = config.DeadLetterSink;
        ErrorHandlerFactory = config.ErrorHandlerFactory ?? new DefaultErrorHandlerFactory(LoggerFactory);
        LineageFactory = config.LineageFactory ?? new DefaultLineageFactory(LoggerFactory);
        ObservabilityFactory = config.ObservabilityFactory ?? new DefaultObservabilityFactory();
        RetryOptions = config.RetryOptions ?? PipelineRetryOptions.Default;
    }

    /// <summary>
    ///     A dictionary to hold any runtime parameters for the pipeline.
    /// </summary>
    /// <remarks>
    ///     <strong>Thread Safety:</strong> NOT thread-safe. Typically populated during pipeline initialization
    ///     and read during execution. Should not be modified concurrently.
    /// </remarks>
    public Dictionary<string, object> Parameters { get; }

    /// <summary>
    ///     A dictionary for sharing state between pipeline nodes.
    /// </summary>
    /// <remarks>
    ///     <strong>Thread Safety:</strong> NOT thread-safe. Used for node-to-node communication and metrics storage.
    ///     <para>
    ///         In parallel execution scenarios, if multiple worker threads need to share state, consider:
    ///         <list type="bullet">
    ///             <item>
    ///                 <description>Using <see cref="IPipelineStateManager" /> from the Properties dictionary</description>
    ///             </item>
    ///             <item>
    ///                 <description>Implementing node-level synchronization in custom transforms</description>
    ///             </item>
    ///             <item>
    ///                 <description>Using atomic operations for simple counters (with <see cref="System.Threading.Interlocked" />)</description>
    ///             </item>
    ///         </list>
    ///     </para>
    /// </remarks>
    public Dictionary<string, object> Items { get; }

    /// <summary>
    ///     A dictionary for storing properties that can be used by extensions and plugins.
    ///     This provides a way to extend the PipelineContext without modifying its core structure.
    /// </summary>
    /// <remarks>
    ///     <strong>Thread Safety:</strong> NOT thread-safe. Common uses include:
    ///     <list type="bullet">
    ///         <item>
    ///             <description>Storing <see cref="IPipelineStateManager" /> for thread-safe state management</description>
    ///         </item>
    ///         <item>
    ///             <description>Storing execution observers and configuration extensions</description>
    ///         </item>
    ///     </list>
    /// </remarks>
    public Dictionary<string, object> Properties { get; }

    /// <summary>
    ///     A cancellation token to monitor for pipeline cancellation requests.
    /// </summary>
    public CancellationToken CancellationToken { get; }

    /// <summary>
    ///     The logger factory for this pipeline run.
    /// </summary>
    public ILoggerFactory LoggerFactory { get; }

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
    public IPipelineStateManager? StateManager => Properties.TryGetValue(PipelineContextKeys.StateManager, out var sm) && sm is IPipelineStateManager manager
        ? manager
        : null;

    /// <summary>
    ///     Gets the stateful registry for this pipeline run, if available.
    /// </summary>
    public IStatefulRegistry? StatefulRegistry => Properties.TryGetValue(PipelineContextKeys.StatefulRegistry, out var reg) && reg is IStatefulRegistry registry
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
                catch (Exception ex)
                {
                    // Log but don't propagate - we're already past disposal
                    var logger = LoggerFactory.CreateLogger("PipelineContext");
                    PipelineContextLogMessages.LateRegistrationDisposalFailed(logger, ex.Message);
                }
            });

            return;
        }

        // Lazy initialize the disposables list only when needed
        _disposables ??= new List<IAsyncDisposable>(8);
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

        if (_disposables is not null)
        {
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
        }

        ReturnPooledDictionaries();

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

    private void ReturnPooledDictionaries()
    {
        if (_ownsParametersDictionary)
        {
            Parameters.Clear();
            PipelineObjectPool.Return(Parameters);
        }

        if (_ownsItemsDictionary)
        {
            Items.Clear();
            PipelineObjectPool.Return(Items);
        }

        if (_ownsPropertiesDictionary)
        {
            Properties.Clear();
            PipelineObjectPool.Return(Properties);
        }
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
