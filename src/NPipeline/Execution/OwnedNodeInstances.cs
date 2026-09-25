using NPipeline.Nodes;

namespace NPipeline.Execution;

/// <summary>
///     The disposable resources one pipeline run owns, tracked by reference identity.
/// </summary>
/// <remarks>
///     Registration and disposal both use reference identity, so an instance that appears both as a builder disposable
///     and as a preconfigured node instance, or under two node ids, is tracked and released exactly once. Disposal is
///     idempotent: a setup failure and the end-of-run cleanup can both call it, and the second call does nothing.
/// </remarks>
public sealed class OwnedNodeInstances
{
    private readonly object _gate = new();
    private readonly HashSet<object> _resources = new(ReferenceEqualityComparer.Instance);
    private bool _disposed;

    /// <summary>
    ///     Tracks <paramref name="resource" /> when it is disposable and not already tracked.
    /// </summary>
    /// <returns><see langword="true" /> when the resource was added; otherwise <see langword="false" />.</returns>
    public bool Add(object? resource)
    {
        if (resource is not (IAsyncDisposable or IDisposable))
            return false;

        lock (_gate)
            return _resources.Add(resource);
    }

    /// <summary>
    ///     Tracks <paramref name="node" /> when it is disposable and not already tracked.
    /// </summary>
    /// <returns><see langword="true" /> when the instance was added; otherwise <see langword="false" />.</returns>
    public bool Add(INode node) => Add((object)node);

    /// <summary>
    ///     Tracks every disposable entry of <paramref name="resources" />.
    /// </summary>
    public void AddRange(System.Collections.IEnumerable resources)
    {
        foreach (var resource in resources)
            Add(resource);
    }

    /// <summary>
    ///     Disposes every tracked resource, recording rather than propagating each failure so one bad disposal cannot
    ///     stop the rest or replace the run's real error. A second call does nothing.
    /// </summary>
    /// <returns>The disposal failures, or null when every disposal succeeded (including on a repeat call).</returns>
    public async ValueTask<List<Exception>?> DisposeAllAsync()
    {
        object[] resources;

        lock (_gate)
        {
            if (_disposed)
                return null;

            _disposed = true;
            resources = [.. _resources];
            _resources.Clear();
        }

        List<Exception>? errors = null;

        foreach (var resource in resources)
        {
            try
            {
                switch (resource)
                {
                    case IAsyncDisposable asyncDisposable:
                        await asyncDisposable.DisposeAsync().ConfigureAwait(false);
                        break;

                    case IDisposable disposable:
                        disposable.Dispose();
                        break;
                }
            }
            catch (Exception ex)
            {
                (errors ??= []).Add(ex);
            }
        }

        return errors;
    }
}
