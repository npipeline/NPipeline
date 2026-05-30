using System.Diagnostics.CodeAnalysis;
using NPipeline.Observability;

namespace NPipeline.Execution;

/// <summary>
///     Owns node-scoped execution state for a single pipeline run.
/// </summary>
public sealed class NodeExecutionScopeRegistry
{
    private readonly Dictionary<string, object> _nodeExecutionAnnotations = new();
    private readonly Dictionary<string, NodeObservabilityRegistration> _nodeObservabilityScopes = new();
    private readonly Dictionary<string, object> _runtimeAnnotations = new();

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
    ///     Disposes all active node observability scopes.
    /// </summary>
    public void DisposeAllNodeScopes()
    {
        if (_nodeObservabilityScopes.Count == 0)
            return;

        var registrations = new List<NodeObservabilityRegistration>(_nodeObservabilityScopes.Values);
        _nodeObservabilityScopes.Clear();

        foreach (var registration in registrations)
        {
            registration.Scope.Dispose();
            registration.OnDisposed?.Invoke(registration.Scope, registration.Scope.GetFailureException());
        }
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
        return _nodeExecutionAnnotations.Remove(nodeId);
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

        return _nodeObservabilityScopes.TryGetValue(nodeId, out var registration)
            ? new ScopedObservabilityHandle(this, nodeId, registration.Scope)
            : NullObservabilityScope.Instance;
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
        DisposeNodeScope(nodeId, registration.Scope);
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
    public IEnumerable<KeyValuePair<string, object>> EnumerateRuntimeAnnotations()
    {
        return _runtimeAnnotations;
    }

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

    private void DisposeNodeScope(string nodeId, IAutoObservabilityScope expectedScope)
    {
        Action<IAutoObservabilityScope, Exception?>? onDisposed = null;

        if (_nodeObservabilityScopes.TryGetValue(nodeId, out var currentRegistration) &&
            ReferenceEquals(currentRegistration.Scope, expectedScope))
        {
            _ = _nodeObservabilityScopes.Remove(nodeId);
            onDisposed = currentRegistration.OnDisposed;
        }

        expectedScope.Dispose();
        onDisposed?.Invoke(expectedScope, expectedScope.GetFailureException());
    }

    private readonly struct NodeObservabilityRegistration(
        IAutoObservabilityScope scope,
        Action<IAutoObservabilityScope, Exception?>? onDisposed)
    {
        public IAutoObservabilityScope Scope { get; } = scope;

        public Action<IAutoObservabilityScope, Exception?>? OnDisposed { get; } = onDisposed;
    }

    private sealed class ScopedObservabilityHandle : IAutoObservabilityScope
    {
        private readonly NodeExecutionScopeRegistry _registry;
        private readonly string _nodeId;
        private readonly IAutoObservabilityScope _inner;
        private int _disposed;

        public ScopedObservabilityHandle(NodeExecutionScopeRegistry registry, string nodeId, IAutoObservabilityScope inner)
        {
            _registry = registry;
            _nodeId = nodeId;
            _inner = inner;
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

            _registry.DisposeNodeScope(_nodeId, _inner);
        }
    }

    private sealed class NullObservabilityScope : IAutoObservabilityScope
    {
        public static NullObservabilityScope Instance { get; } = new();

        private NullObservabilityScope()
        {
        }

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

        public Exception? GetFailureException()
        {
            return null;
        }

        public void AddWork(TimeSpan duration)
        {
        }

        public void AddInputWait(TimeSpan duration)
        {
        }

        public void AddOutputBlock(TimeSpan duration)
        {
        }

        public NodeTimingBreakdown GetTimingBreakdown()
        {
            return NodeTimingBreakdown.Empty;
        }

        public void Dispose()
        {
        }
    }
}