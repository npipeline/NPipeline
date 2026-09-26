using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using NPipeline.Observability;

namespace NPipeline.Execution;

/// <summary>
///     Owns node-scoped execution state for a single pipeline run.
///     All members are safe for concurrent use; node output streams may be consumed
///     and finalized on different threads when nodes execute in parallel.
/// </summary>
public sealed class NodeExecutionScopeRegistry
{
    private readonly ConcurrentDictionary<string, object> _nodeExecutionAnnotations = new();
    private readonly ConcurrentDictionary<string, NodeObservabilityRegistration> _nodeObservabilityScopes = new();
    private readonly ConcurrentDictionary<string, object> _runtimeAnnotations = new();

    /// <summary>
    ///     Clears all per-run state.
    /// </summary>
    public void Clear()
    {
        DisposeAllNodeScopes();
        _nodeExecutionAnnotations.Clear();
        _runtimeAnnotations.Clear();
    }

    /// <summary>
    ///     The scope handed out for a node with no registration. Used to skip work that only matters when a node is
    ///     actually observed.
    /// </summary>
    internal static IAutoObservabilityScope NullScope => NullObservabilityScope.Instance;

    /// <summary>
    ///     Disposes all active node observability scopes.
    /// </summary>
    public void DisposeAllNodeScopes()
    {
        if (_nodeObservabilityScopes.IsEmpty)
            return;

        // Remove under the dictionary's own atomic semantics, so a scope a handle is disposing at the same time is
        // disposed exactly once by whichever side removes it.
        foreach (var key in _nodeObservabilityScopes.Keys)
        {
            if (_nodeObservabilityScopes.TryRemove(key, out var registration))
                DisposeRegistration(registration);
        }
    }

    /// <summary>
    ///     Disposes a removed registration exactly once: the scope's dispose is idempotent, and the callback fires only
    ///     for the side that removed the registration.
    /// </summary>
    private static void DisposeRegistration(NodeObservabilityRegistration registration)
    {
        registration.Scope.Dispose();
        registration.OnDisposed?.Invoke(registration.Scope, registration.Scope.GetFailureException());
    }

    /// <summary>
    ///     Sets a node execution annotation.
    /// </summary>
    public void SetNodeExecutionAnnotation(string nodeId, object annotation)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        ArgumentNullException.ThrowIfNull(annotation);

        _nodeExecutionAnnotations[nodeId] = annotation;
    }

    /// <summary>
    ///     Tries to retrieve a node execution annotation.
    /// </summary>
    public bool TryGetNodeExecutionAnnotation(string nodeId, [NotNullWhen(true)] out object? annotation)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        return _nodeExecutionAnnotations.TryGetValue(nodeId, out annotation);
    }

    /// <summary>
    ///     Removes a node execution annotation.
    /// </summary>
    public bool RemoveNodeExecutionAnnotation(string nodeId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        return _nodeExecutionAnnotations.TryRemove(nodeId, out _);
    }

    /// <summary>
    ///     Registers a per-node observability scope.
    /// </summary>
    public void RegisterNodeObservabilityScope(string nodeId, IAutoObservabilityScope scope,
        Action<IAutoObservabilityScope, Exception?>? onDisposed = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        ArgumentNullException.ThrowIfNull(scope);

        _nodeObservabilityScopes[nodeId] = new NodeObservabilityRegistration(scope, onDisposed);
    }

    /// <summary>
    ///     Begins node item-level observation tracking for the specified node.
    /// </summary>
    public IAutoObservabilityScope BeginNodeScope(string nodeId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);

        if (!_nodeObservabilityScopes.TryGetValue(nodeId, out var registration))
            return NullObservabilityScope.Instance;

        _ = Interlocked.Increment(ref registration.Handles);
        return new ScopedObservabilityHandle(this, nodeId, registration);
    }

    /// <summary>
    ///     Records node failure and disposes the registered scope, if present.
    /// </summary>
    public bool RecordNodeFailureAndDispose(string nodeId, Exception exception)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        ArgumentNullException.ThrowIfNull(exception);

        if (!_nodeObservabilityScopes.TryGetValue(nodeId, out var registration))
            return false;

        registration.Scope.RecordFailure(exception);

        // A terminal failure disposes the scope regardless of any handle still open: the run is over.
        _ = RemoveAndDisposeNodeScope(nodeId, registration);
        return true;
    }

    /// <summary>
    ///     Sets a runtime annotation value.
    /// </summary>
    public void SetRuntimeAnnotation(string key, object value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(value);

        _runtimeAnnotations[key] = value;
    }

    /// <summary>
    ///     Tries to retrieve a runtime annotation value.
    /// </summary>
    public bool TryGetRuntimeAnnotation(string key, [NotNullWhen(true)] out object? value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        return _runtimeAnnotations.TryGetValue(key, out value);
    }

    /// <summary>
    ///     Enumerates all runtime annotations.
    /// </summary>
    public IEnumerable<KeyValuePair<string, object>> EnumerateRuntimeAnnotations() => _runtimeAnnotations;

    /// <summary>
    ///     Enumerates runtime annotations whose keys start with the provided prefix.
    /// </summary>
    public IEnumerable<KeyValuePair<string, object>> EnumerateRuntimeAnnotationsWithPrefix(string keyPrefix)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(keyPrefix);

        foreach (var kv in _runtimeAnnotations)
        {
            if (kv.Key.StartsWith(keyPrefix, StringComparison.Ordinal))
                yield return kv;
        }
    }

    /// <summary>
    ///     Removes and disposes a node's registration. Returns true when this call removed the registration and ran its
    ///     disposal.
    /// </summary>
    /// <remarks>
    ///     Removal and the callback are tied together atomically, so when a handle and <see cref="DisposeAllNodeScopes" />
    ///     race, only the side that removes the registration disposes the scope and fires the callback.
    /// </remarks>
    private bool RemoveAndDisposeNodeScope(string nodeId, NodeObservabilityRegistration expectedRegistration)
    {
        if (!_nodeObservabilityScopes.TryRemove(
                new KeyValuePair<string, NodeObservabilityRegistration>(nodeId, expectedRegistration)))
            return false;

        DisposeRegistration(expectedRegistration);
        return true;
    }

    private sealed class NodeObservabilityRegistration(
        IAutoObservabilityScope scope,
        Action<IAutoObservabilityScope, Exception?>? onDisposed)
    {
        /// <summary>
        ///     How many scopes have been handed out for this registration and not yet disposed. The registration is
        ///     disposed when the last one goes.
        /// </summary>
        public int Handles;

        public IAutoObservabilityScope Scope { get; } = scope;

        public Action<IAutoObservabilityScope, Exception?>? OnDisposed { get; } = onDisposed;
    }

    private sealed class ScopedObservabilityHandle : IAutoObservabilityScope
    {
        private readonly IAutoObservabilityScope _inner;
        private readonly string _nodeId;
        private readonly NodeExecutionScopeRegistry _registry;
        private readonly NodeObservabilityRegistration _registration;
        private int _disposed;

        public ScopedObservabilityHandle(NodeExecutionScopeRegistry registry, string nodeId, NodeObservabilityRegistration registration)
        {
            _registry = registry;
            _nodeId = nodeId;
            _registration = registration;
            _inner = registration.Scope;
        }

        public void RecordItemCount(long processed, long emitted)
        {
            if (Volatile.Read(ref _disposed) == 1)
                return;

            _inner.RecordItemCount(processed, emitted);
        }

        public void IncrementProcessed()
        {
            if (Volatile.Read(ref _disposed) == 1)
                return;

            _inner.IncrementProcessed();
        }

        public void IncrementEmitted()
        {
            if (Volatile.Read(ref _disposed) == 1)
                return;

            _inner.IncrementEmitted();
        }

        public void RecordFailure(Exception exception)
        {
            if (Volatile.Read(ref _disposed) == 1)
                return;

            _inner.RecordFailure(exception);
        }

        public Exception? GetFailureException()
        {
            if (Volatile.Read(ref _disposed) == 1)
                return null;

            return _inner.GetFailureException();
        }

        public void ClearFailure()
        {
            if (Volatile.Read(ref _disposed) == 1)
                return;

            _inner.ClearFailure();
        }

        public void AddWork(TimeSpan duration)
        {
            if (Volatile.Read(ref _disposed) == 1)
                return;

            _inner.AddWork(duration);
        }

        public void AddInputWait(TimeSpan duration)
        {
            if (Volatile.Read(ref _disposed) == 1)
                return;

            _inner.AddInputWait(duration);
        }

        public void AddOutputBlock(TimeSpan duration)
        {
            if (Volatile.Read(ref _disposed) == 1)
                return;

            _inner.AddOutputBlock(duration);
        }

        public NodeTimingBreakdown GetTimingBreakdown()
        {
            if (Volatile.Read(ref _disposed) == 1)
                return NodeTimingBreakdown.Empty;

            return _inner.GetTimingBreakdown();
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 1)
                return;

            // The registration holds a handle for the scope it is about to hand out, so the scope is disposed only
            // once the last handle goes - a restart's second handle keeps it alive across the failed attempt.
            if (Interlocked.Decrement(ref _registration.Handles) == 0)
                _ = _registry.RemoveAndDisposeNodeScope(_nodeId, _registration);
        }
    }

    private sealed class NullObservabilityScope : IAutoObservabilityScope
    {
        private NullObservabilityScope()
        {
        }

        public static NullObservabilityScope Instance { get; } = new();

        public void RecordItemCount(long processed, long emitted)
        {
        }

        public void IncrementProcessed()
        {
        }

        public void IncrementEmitted()
        {
        }

        public void RecordFailure(Exception exception)
        {
        }

        public Exception? GetFailureException() => null;

        public void AddWork(TimeSpan duration)
        {
        }

        public void AddInputWait(TimeSpan duration)
        {
        }

        public void AddOutputBlock(TimeSpan duration)
        {
        }

        public NodeTimingBreakdown GetTimingBreakdown() => NodeTimingBreakdown.Empty;

        public void Dispose()
        {
        }
    }
}
