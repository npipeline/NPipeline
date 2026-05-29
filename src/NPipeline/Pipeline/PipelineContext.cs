using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;
using NPipeline.Configuration;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.CircuitBreaking;
using NPipeline.Execution.Pooling;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Observability.Logging;
using NPipeline.Observability.Tracing;
using NPipeline.Resilience;
using NPipeline.State;

namespace NPipeline.Pipeline;

/// <summary>
///     Provides a context for a single pipeline run, allowing for the passing of runtime configuration and state.
/// </summary>
/// <remarks>
///     <para>
///         <strong>Optimization Profile Controls Thread Safety:</strong>
///     </para>
///     <para>
///         <see cref="PipelineContext" /> chooses dictionary implementations by
///         <see cref="PipelineContextConfiguration.OptimizationProfile" />:
///         <list type="bullet">
///             <item>
///                 <description>
///                     <see cref="PipelineOptimizationProfile.Default" /> uses
///                     <see cref="ConcurrentDictionary{TKey,TValue}" /> for <see cref="Parameters" />,
///                     <see cref="Items" />, and <see cref="Properties" />.
///                 </description>
///             </item>
///             <item>
///                 <description>
///                     <see cref="PipelineOptimizationProfile.HighThroughput" /> uses pooled
///                     <see cref="Dictionary{TKey,TValue}" /> instances for minimum overhead.
///                 </description>
///             </item>
///         </list>
///     </para>
///     <para>
///         <strong>Single-Pipeline Execution (Default):</strong>
///         All operations are inherently single-threaded. No synchronization needed.
///     </para>
///     <para>
///         <strong>Parallel Node Execution:</strong>
///         When using parallel execution strategies (e.g. ParallelExecutionStrategy),
///         each worker thread processes independent data items through the pipeline. Context dictionaries remain profile-dependent:
///         concurrent-safe in <see cref="PipelineOptimizationProfile.Default" />, and not thread-safe in
///         <see cref="PipelineOptimizationProfile.HighThroughput" />. State updates during parallel execution should use:
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
///         Thread-safe dictionaries add overhead (locks, memory barriers, allocations). NPipeline follows
///         "pay for what you use" by using concurrent dictionaries in <see cref="PipelineOptimizationProfile.Default" />
///         and pooled dictionaries in <see cref="PipelineOptimizationProfile.HighThroughput" />.
///     </para>
///     <para>
///         <strong>Composition Model:</strong>
///         <see cref="PipelineContext" /> composes focused context objects (<see cref="RunIdentity" />, <see cref="ExecutionConfiguration" />,
///         <see cref="Observability" />, <see cref="NodeEnvironment" />, and <see cref="Lineage" />) and exposes compatibility properties
///         for existing node and extension code.
///     </para>
/// </remarks>
public sealed class PipelineContext
{
    private readonly bool _ownsItemsDictionary;
    private readonly bool _ownsParametersDictionary;
    private readonly bool _ownsPropertiesDictionary;

    private readonly bool _parametersIsPooled;
    private readonly bool _itemsIsPooled;
    private readonly bool _propertiesIsPooled;

    // Composite disposal registry for lifecycle-managed IAsyncDisposable resources (lazy initialized)
    private List<IAsyncDisposable>? _disposables;
    private bool _disposed;

    /// <summary>
    ///     Creates a new <see cref="PipelineContext" /> with the specified configuration.
    ///     All unspecified components use sensible defaults.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <strong>Optimization Profile:</strong>
    ///     </para>
    ///     <para>
    ///         When the <see cref="PipelineContextConfiguration.OptimizationProfile" /> is
    ///         <see cref="PipelineOptimizationProfile.Default" />, the <see cref="Parameters" />,
    ///         <see cref="Items" />, and <see cref="Properties" /> dictionaries are backed by
    ///         <see cref="ConcurrentDictionary{TKey,TValue}" /> to prevent race conditions when
    ///         scaling to parallel execution. When <see cref="PipelineOptimizationProfile.HighThroughput" />,
    ///         ordinary <see cref="Dictionary{TKey,TValue}" /> instances are used for zero locking overhead.
    ///     </para>
    ///     <para>
    ///         <strong>Thread Safety:</strong>
    ///     </para>
    ///     <para>
    ///         When using <see cref="PipelineOptimizationProfile.Default" />, the dictionaries support
    ///         concurrent reads and writes. When using <see cref="PipelineOptimizationProfile.HighThroughput" />,
    ///         they are NOT thread-safe. In both cases, <see cref="IPipelineStateManager" /> is recommended
    ///         for complex shared state in parallel execution scenarios.
    ///     </para>
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
        var profileBehavior = OptimizationProfileBehaviorRegistry.For(config.OptimizationProfile);

        if (config.Parameters is not null)
        {
            Parameters = config.Parameters;
        }
        else
        {
            var (dictionary, isPooled) = CreateOwnedDictionary(profileBehavior);
            Parameters = dictionary;
            _ownsParametersDictionary = true;
            _parametersIsPooled = isPooled;
        }

        if (config.Items is not null)
        {
            Items = config.Items;
        }
        else
        {
            var (dictionary, isPooled) = CreateOwnedDictionary(profileBehavior);
            Items = dictionary;
            _ownsItemsDictionary = true;
            _itemsIsPooled = isPooled;
        }

        if (config.Properties is not null)
        {
            Properties = config.Properties;
        }
        else
        {
            var (dictionary, isPooled) = CreateOwnedDictionary(profileBehavior);
            Properties = dictionary;
            _ownsPropertiesDictionary = true;
            _propertiesIsPooled = isPooled;
        }

        var loggerFactory = config.LoggerFactory ?? NullLoggerFactory.Instance;
        var tracer = config.Tracer ?? NullPipelineTracer.Instance;
        var observabilityFactory = config.ObservabilityFactory ?? new DefaultObservabilityFactory();
        var retryOptions = config.RetryOptions ?? PipelineRetryOptions.Default;
        var lineageFactory = config.LineageFactory ?? new DefaultLineageFactory(loggerFactory);

        CancellationToken = config.CancellationToken;
        DeadLetterSink = config.DeadLetterSink;
        ErrorHandlerFactory = config.ErrorHandlerFactory ?? new DefaultErrorHandlerFactory(loggerFactory);

        RunIdentity = new PipelineRunIdentityContext(DateTime.UtcNow);
        ExecutionConfiguration = new PipelineExecutionConfigurationContext(retryOptions, config.OptimizationProfile);
        if (config.ResiliencePolicy is not null)
            ExecutionConfiguration.ResiliencePolicy = config.ResiliencePolicy;
        Observability = new PipelineObservabilityContext(loggerFactory, tracer, observabilityFactory);
        NodeEnvironment = new PipelineNodeEnvironmentContext();
        Lineage = new PipelineLineageContext(lineageFactory);
    }

    private static (IDictionary<string, object> Dictionary, bool IsPooled) CreateOwnedDictionary(
        IOptimizationProfileBehavior profileBehavior)
    {
        if (profileBehavior.UsesThreadSafeContextDictionaries)
            return (new ConcurrentDictionary<string, object>(), false);

        return (PipelineObjectPool.RentStringObjectDictionary(), true);
    }

    /// <summary>
    ///     Focused run identity state for this execution.
    /// </summary>
    public PipelineRunIdentityContext RunIdentity { get; }

    /// <summary>
    ///     Focused execution configuration and resilience state for this execution.
    /// </summary>
    public PipelineExecutionConfigurationContext ExecutionConfiguration { get; }

    /// <summary>
    ///     Focused observability surface for this execution.
    /// </summary>
    public PipelineObservabilityContext Observability { get; }

    /// <summary>
    ///     Focused node environment state for this execution.
    /// </summary>
    public PipelineNodeEnvironmentContext NodeEnvironment { get; }

    /// <summary>
    ///     Focused lineage services and sink state for this execution.
    /// </summary>
    public PipelineLineageContext Lineage { get; }

    /// <summary>
    ///     A dictionary to hold any runtime parameters for the pipeline.
    /// </summary>
    /// <remarks>
    ///     <strong>Thread Safety:</strong> Thread-safe in <see cref="PipelineOptimizationProfile.Default" /> mode
    ///     (backed by <see cref="ConcurrentDictionary{TKey,TValue}" />). Not thread-safe in
    ///     <see cref="PipelineOptimizationProfile.HighThroughput" /> mode.
    /// </remarks>
    public IDictionary<string, object> Parameters { get; }

    /// <summary>
    ///     A dictionary for sharing state between pipeline nodes.
    /// </summary>
    /// <remarks>
    ///     <strong>Thread Safety:</strong> Thread-safe in <see cref="PipelineOptimizationProfile.Default" /> mode
    ///     (backed by <see cref="ConcurrentDictionary{TKey,TValue}" />). Not thread-safe in
    ///     <see cref="PipelineOptimizationProfile.HighThroughput" /> mode.
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
    public IDictionary<string, object> Items { get; }

    /// <summary>
    ///     Framework-managed services and execution state should be stored on strongly-typed members.
    ///     <see cref="Items" /> is reserved for user-defined values.
    /// </summary>
    public StatsCounter ProcessedItemsCounter
    {
        get => Observability.ProcessedItemsCounter;
        internal set => Observability.ProcessedItemsCounter = value;
    }

    /// <summary>
    ///     Effective global retry options for the current pipeline run.
    /// </summary>
    public PipelineRetryOptions GlobalRetryOptions
    {
        get => ExecutionConfiguration.GlobalRetryOptions;
        internal set => ExecutionConfiguration.GlobalRetryOptions = value;
    }

    /// <summary>
    ///     Per-node retry option overrides indexed by node id.
    /// </summary>
    public Dictionary<string, PipelineRetryOptions> NodeRetryOverrides => ExecutionConfiguration.NodeRetryOverrides;

    /// <summary>
    ///     Unified resilience policy used by runtime execution.
    /// </summary>
    public IResiliencePolicy ResiliencePolicy
    {
        get => ExecutionConfiguration.ResiliencePolicy;
        internal set => ExecutionConfiguration.ResiliencePolicy = value;
    }

    /// <summary>
    ///     Registry for node execution annotations, observability scopes, and runtime annotations.
    /// </summary>
    public NodeExecutionScopeRegistry NodeExecutionScopeRegistry => NodeEnvironment.NodeExecutionScopeRegistry;

    /// <summary>
    ///     Optional preconfigured node instances to seed graph construction.
    /// </summary>
    public Dictionary<string, INode> PreconfiguredNodeInstances => NodeEnvironment.PreconfiguredNodeInstances;

    /// <summary>
    ///     Indicates the current run uses parallel execution behavior.
    /// </summary>
    public bool IsParallelExecution
    {
        get => ExecutionConfiguration.IsParallelExecution;
        internal set => ExecutionConfiguration.IsParallelExecution = value;
    }

    /// <summary>
    ///     Indicates node lifetimes are owned externally (for example by DI container).
    /// </summary>
    public bool DiOwnedNodes
    {
        get => NodeEnvironment.DiOwnedNodes;
        set => NodeEnvironment.DiOwnedNodes = value;
    }

    /// <summary>
    ///     The last retry-exhausted exception observed in the pipeline.
    /// </summary>
    public RetryExhaustedException? LastRetryExhaustedException
    {
        get => ExecutionConfiguration.LastRetryExhaustedException;
        internal set => ExecutionConfiguration.LastRetryExhaustedException = value;
    }

    /// <summary>
    ///     The pipeline-level UTC start timestamp.
    /// </summary>
    public DateTime PipelineStartTimeUtc
    {
        get => RunIdentity.PipelineStartTimeUtc;
        internal set => RunIdentity.PipelineStartTimeUtc = value;
    }

    /// <summary>
    ///     Unique pipeline identity for this execution context.
    ///     This is stable for the lifetime of the context and is used for unambiguous lineage/metrics keying.
    /// </summary>
    public Guid PipelineId
    {
        get => RunIdentity.PipelineId;
        internal set => RunIdentity.PipelineId = value;
    }

    /// <summary>
    ///     Unique run identifier for this pipeline execution.
    ///     This can be inherited by child pipelines when composite run identity inheritance is enabled.
    /// </summary>
    public Guid RunId
    {
        get => RunIdentity.RunId;
        internal set => RunIdentity.RunId = value;
    }

    /// <summary>
    ///     Logical pipeline name for this execution context.
    ///     Used by observability and lineage to disambiguate nested node identities.
    /// </summary>
    public string? PipelineName
    {
        get => RunIdentity.PipelineName;
        internal set => RunIdentity.PipelineName = value;
    }

    /// <summary>
    ///     Circuit-breaker options for the current run.
    /// </summary>
    public PipelineCircuitBreakerOptions? CircuitBreakerOptions
    {
        get => ExecutionConfiguration.CircuitBreakerOptions;
        internal set => ExecutionConfiguration.CircuitBreakerOptions = value;
    }

    /// <summary>
    ///     Circuit-breaker memory management options for the current run.
    /// </summary>
    public CircuitBreakerMemoryManagementOptions? CircuitBreakerMemoryOptions
    {
        get => ExecutionConfiguration.CircuitBreakerMemoryOptions;
        internal set => ExecutionConfiguration.CircuitBreakerMemoryOptions = value;
    }

    /// <summary>
    ///     Circuit-breaker manager for the current run.
    /// </summary>
    internal ICircuitBreakerManager? CircuitBreakerManager
    {
        get => ExecutionConfiguration.CircuitBreakerManager;
        set => ExecutionConfiguration.CircuitBreakerManager = value;
    }

    /// <summary>
    ///     Item-level lineage sink resolved for the current run.
    /// </summary>
    public ILineageSink? LineageSink
    {
        get => Lineage.LineageSink;
        internal set => Lineage.LineageSink = value;
    }

    /// <summary>
    ///     Pipeline-level lineage sink resolved for the current run.
    /// </summary>
    public IPipelineLineageSink? PipelineLineageSink
    {
        get => Lineage.PipelineLineageSink;
        internal set => Lineage.PipelineLineageSink = value;
    }

    /// <summary>
    ///     Item-level lineage collector resolved for the current run.
    /// </summary>
    public ILineageCollector? LineageCollector
    {
        get => Lineage.LineageCollector;
        internal set => Lineage.LineageCollector = value;
    }

    /// <summary>
    ///     A dictionary for storing properties that can be used by extensions and plugins.
    ///     This provides a way to extend the PipelineContext without modifying its core structure.
    /// </summary>
    /// <remarks>
    ///     Thread-safe in <see cref="PipelineOptimizationProfile.Default" /> mode
    ///     (backed by <see cref="ConcurrentDictionary{TKey,TValue}" />). Common uses include:
    ///     <list type="bullet">
    ///         <item>
    ///             <description>Storing <see cref="IPipelineStateManager" /> for thread-safe state management</description>
    ///         </item>
    ///         <item>
    ///             <description>Storing execution observers and configuration extensions</description>
    ///         </item>
    ///     </list>
    /// </remarks>
    public IDictionary<string, object> Properties { get; }

    /// <summary>
    ///     A cancellation token to monitor for pipeline cancellation requests.
    /// </summary>
    public CancellationToken CancellationToken { get; }

    /// <summary>
    ///     The logger factory for this pipeline run.
    /// </summary>
    public ILoggerFactory LoggerFactory => Observability.LoggerFactory;

    /// <summary>
    ///     The tracer for this pipeline run.
    /// </summary>
    public IPipelineTracer Tracer => Observability.Tracer;

    /// <summary>
    ///     The sink for items that have failed processing and have been redirected.
    /// </summary>
    public IDeadLetterSink? DeadLetterSink { get; internal set; }

    /// <summary>
    ///     The factory for creating dead-letter sinks.
    /// </summary>
    public IErrorHandlerFactory ErrorHandlerFactory { get; }

    /// <summary>
    ///     The factory for creating lineage-related components.
    /// </summary>
    public ILineageFactory LineageFactory => Lineage.LineageFactory;

    /// <summary>
    ///     The factory for resolving observability-related components.
    /// </summary>
    public IObservabilityFactory ObservabilityFactory => Observability.ObservabilityFactory;

    /// <summary>
    ///     The ID of the node currently being executed.
    /// </summary>
    public string CurrentNodeId
    {
        get => NodeEnvironment.CurrentNodeId;
        private set => NodeEnvironment.CurrentNodeId = value;
    }

    /// <summary>
    ///     Execution observer for instrumentation (node lifecycle, retries, queue/backpressure events).
    ///     Defaults to <see cref="NullExecutionObserver.Instance" /> which provides zero-overhead observation.
    ///     Set to a different observer implementation to enable actual instrumentation.
    ///     If set to null, automatically falls back to <see cref="NullExecutionObserver.Instance" />.
    /// </summary>
    public IExecutionObserver ExecutionObserver
    {
        get => Observability.ExecutionObserver;
        set => Observability.ExecutionObserver = value;
    }

    /// <summary>
    ///     Execution / retry configuration for this pipeline run.
    ///     Values here override builder defaults when provided.
    /// </summary>
    public PipelineRetryOptions RetryOptions => ExecutionConfiguration.RetryOptions;

    /// <summary>
    ///     Creates a default pipeline context with all default values.
    /// </summary>
    public static PipelineContext Default => new(PipelineContextConfiguration.Default);


    /// <summary>
    ///     Gets the state manager for this pipeline run, if available.
    /// </summary>
    public IPipelineStateManager? StateManager { get; internal set; }

    /// <summary>
    ///     Gets the stateful registry for this pipeline run, if available.
    /// </summary>
    public IStatefulRegistry? StatefulRegistry { get; internal set; }

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
        NodeRetryOverrides.Clear();
        NodeExecutionScopeRegistry.Clear();

        if (_ownsParametersDictionary)
        {
            Parameters.Clear();

            if (_parametersIsPooled && Parameters is Dictionary<string, object> pooledParams)
                PipelineObjectPool.Return(pooledParams);
        }

        if (_ownsItemsDictionary)
        {
            Items.Clear();

            if (_itemsIsPooled && Items is Dictionary<string, object> pooledItems)
                PipelineObjectPool.Return(pooledItems);
        }

        if (_ownsPropertiesDictionary)
        {
            Properties.Clear();

            if (_propertiesIsPooled && Properties is Dictionary<string, object> pooledProps)
                PipelineObjectPool.Return(pooledProps);
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
