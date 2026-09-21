using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;
using NPipeline.Configuration;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.CircuitBreaking;
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
public sealed class PipelineContext : IAsyncDisposable
{
    private readonly bool _ownsItemsDictionary;
    private readonly bool _ownsParametersDictionary;
    private readonly bool _ownsPropertiesDictionary;

    // Composite disposal registry for lifecycle-managed IAsyncDisposable resources (lazy initialized).
    // Guarded by _disposalGate: terminal nodes below a fan-out drain concurrently and may each register a resource.
    // Typical pipeline runs put a handful of entries in each context dictionary.
    private const int DefaultContextDictionaryCapacity = 10;

    private readonly object _disposalGate = new();
    private List<IAsyncDisposable>? _disposables;

    // Background disposals started for resources registered after disposal completed. Tracked rather than
    // fire-and-forget so a subsequent DisposeAsync can await them instead of leaving unobserved work behind.
    private List<Task>? _lateDisposals;
    private volatile bool _disposed;

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
            Parameters = CreateOwnedDictionary(profileBehavior);
            _ownsParametersDictionary = true;
        }

        if (config.Items is not null)
        {
            Items = config.Items;
        }
        else
        {
            Items = CreateOwnedDictionary(profileBehavior);
            _ownsItemsDictionary = true;
        }

        if (config.Properties is not null)
        {
            Properties = config.Properties;
        }
        else
        {
            Properties = CreateOwnedDictionary(profileBehavior);
            _ownsPropertiesDictionary = true;
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

    private static IDictionary<string, object> CreateOwnedDictionary(IOptimizationProfileBehavior profileBehavior)
    {
        return profileBehavior.UsesThreadSafeContextDictionaries
            ? new ConcurrentDictionary<string, object>()
            : new Dictionary<string, object>(DefaultContextDictionaryCapacity);
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
    ///     The sink for items that have failed processing and have been redirected.
    /// </summary>
    public IDeadLetterSink? DeadLetterSink { get; internal set; }

    /// <summary>
    ///     The factory for creating dead-letter sinks.
    /// </summary>
    public IErrorHandlerFactory ErrorHandlerFactory { get; }

    /// <summary>
    ///     Creates a new pipeline context with all default values.
    /// </summary>
    /// <remarks>
    ///     A method, not a property: every call allocates a fresh context. As a property it read like a shared
    ///     singleton, and callers that mutated what they got back were relying on each access being new.
    /// </remarks>
    public static PipelineContext CreateDefault()
    {
        return new PipelineContext(PipelineContextConfiguration.Default);
    }

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
        ArgumentNullException.ThrowIfNull(disposable);

        lock (_disposalGate)
        {
            // Re-check under the gate: disposal may have started between the fast check above and here.
            if (!_disposed)
            {
                // Lazy initialize the disposables list only when needed
                _disposables ??= new List<IAsyncDisposable>(8);
                _disposables.Add(disposable);
                return;
            }
        }

        DisposeLateRegistration(disposable);
    }

    private void DisposeLateRegistration(IAsyncDisposable disposable)
    {
        // Registered after disposal completed: dispose immediately to avoid leaking the resource. Most stream
        // disposals complete synchronously, so try that first and avoid scheduling any background work at all.
        ValueTask disposal;

        try
        {
            disposal = disposable.DisposeAsync();
        }
        catch (Exception ex)
        {
            LogLateRegistrationFailure(ex);
            return;
        }

        if (disposal.IsCompletedSuccessfully)
            return;

        var pending = AwaitLateDisposalAsync(disposal);

        if (pending.IsCompleted)
            return;

        lock (_disposalGate)
        {
            _lateDisposals ??= new List<Task>(1);
            _lateDisposals.Add(pending);
        }
    }

    private async Task AwaitLateDisposalAsync(ValueTask disposal)
    {
        try
        {
            await disposal.ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Log but don't propagate - we're already past disposal.
            LogLateRegistrationFailure(ex);
        }
    }

    private void LogLateRegistrationFailure(Exception ex)
    {
        var logger = Observability.LoggerFactory.CreateLogger("PipelineContext");
        PipelineContextLogMessages.LateRegistrationDisposalFailed(logger, ex.Message);
    }

    /// <summary>
    ///     Disposes all registered async disposables. Safe to call multiple times.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        List<IAsyncDisposable>? disposables;
        bool alreadyDisposed;

        lock (_disposalGate)
        {
            alreadyDisposed = _disposed;
            _disposed = true;
            disposables = _disposables;
            _disposables = null;
        }

        if (alreadyDisposed)
        {
            // Await anything a late registration started, so a caller who disposes again has a way to observe that
            // work rather than leaving it running unwatched.
            await DrainLateDisposalsAsync().ConfigureAwait(false);
            return;
        }

        List<Exception>? errors = null;

        if (disposables is not null)
        {
            // Dispose in reverse registration order: decorators are registered after the streams they wrap, so
            // LIFO tears the outer layer down before the inner one it depends on.
            for (var i = disposables.Count - 1; i >= 0; i--)
            {
                try
                {
                    await disposables[i].DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    errors ??= new List<Exception>();
                    errors.Add(ex);
                }
            }

            disposables.Clear();
        }

        ClearOwnedDictionaries();

        if (errors is { Count: > 0 })
            throw new AggregateException("One or more errors occurred disposing pipeline context resources.", errors);
    }

    private async ValueTask DrainLateDisposalsAsync()
    {
        List<Task>? pending;

        lock (_disposalGate)
        {
            pending = _lateDisposals;
            _lateDisposals = null;
        }

        if (pending is not null)
            await Task.WhenAll(pending).ConfigureAwait(false);
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
    /// <returns>A non-allocating disposable scope that restores the original node ID upon disposal.</returns>
    /// <remarks>
    ///     Inert while nodes are running concurrently. <see cref="PipelineNodeEnvironmentContext.CurrentNodeId" /> is one field on a shared context,
    ///     so concurrent scopes would interleave their writes and restores and leave it pointing at whichever node
    ///     finished last. Freezing it is the honest behaviour: a stale id beats an arbitrary one.
    /// </remarks>
    public NodeScope ScopedNode(string nodeId)
    {
        return NodeEnvironment.NodesRunConcurrently
            ? default
            : new NodeScope(this, nodeId);
    }

    private void ClearOwnedDictionaries()
    {
        ExecutionConfiguration.NodeRetryOverrides.Clear();
        NodeEnvironment.NodeExecutionScopeRegistry.Clear();

        if (_ownsParametersDictionary)
            Parameters.Clear();

        if (_ownsItemsDictionary)
            Items.Clear();

        if (_ownsPropertiesDictionary)
            Properties.Clear();
    }

    /// <summary>
    ///     Allocation-free disposable scope that sets <see cref="PipelineNodeEnvironmentContext.CurrentNodeId" /> for its lifetime
    ///     and restores the previous node id on disposal.
    /// </summary>
    public readonly struct NodeScope : IDisposable
    {
        private readonly PipelineContext? _context;
        private readonly string _previousNodeId;

        internal NodeScope(PipelineContext context, string newNodeId)
        {
            _context = context;
            _previousNodeId = context.NodeEnvironment.CurrentNodeId;
            context.NodeEnvironment.CurrentNodeId = newNodeId;
        }

        /// <summary>
        ///     Restores the previous node id. A default-constructed scope tracks no context and does nothing.
        /// </summary>
        public void Dispose()
        {
            if (_context is not null)
                _context.NodeEnvironment.CurrentNodeId = _previousNodeId;
        }
    }
}
