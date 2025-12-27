using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.TestHarness;

/// <summary>
///     Test harness utilities for running transform nodes in unit tests without relying on now-removed generic ExecuteAsync.
/// </summary>
public static class TransformTestHarness
{
    /// <summary>
    ///     Executes a transform node against an in-memory set of input items using its configured execution strategy.
    /// </summary>
    public static async Task<IReadOnlyList<TOut>> RunAsync<TIn, TOut>(
        ITransformNode<TIn, TOut> node,
        IEnumerable<TIn> items,
        PipelineContext? context = null,
        CancellationToken cancellationToken = default)
    {
        context ??= PipelineContext.Default;

        // Use a lightweight in-memory pipe that does not enforce notnull constraint
        var list = items.ToList();
        var inputPipe = new HarnessListPipe<TIn>(list, "HarnessInput");
        var strategy = node.ExecutionStrategy;
        var outputPipe = await strategy.ExecuteAsync<TIn, TOut>(inputPipe, node, context, cancellationToken).ConfigureAwait(false);
        var results = new List<TOut>();

        await foreach (var o in outputPipe.WithCancellation(cancellationToken))
        {
            results.Add(o);
        }

        return results;
    }

    /// <summary>
    ///     Convenience method for single-item execution producing a single item result.
    /// </summary>
    public static async Task<TOut> RunSingleAsync<TIn, TOut>(
        ITransformNode<TIn, TOut> node,
        TIn item,
        PipelineContext? context = null,
        CancellationToken cancellationToken = default)
    {
        var results = await RunAsync(node, [item], context, cancellationToken).ConfigureAwait(false);
        return results.Single();
    }
}

file sealed class HarnessListPipe<T>(IReadOnlyList<T> items, string name) : IDataPipe<T>
{
    public string StreamName { get; } = name;

    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return Iterate(cancellationToken).GetAsyncEnumerator(cancellationToken);
    }

    public IAsyncEnumerable<object?> ToAsyncEnumerable(CancellationToken cancellationToken = default)
    {
        return Internal(cancellationToken);
    }

    public Type GetDataType()
    {
        return typeof(T);
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    private async IAsyncEnumerable<T> Iterate([EnumeratorCancellation] CancellationToken ct)
    {
        foreach (var i in items)
        {
            ct.ThrowIfCancellationRequested();
            yield return await Task.FromResult(i);
        }
    }

    private async IAsyncEnumerable<object?> Internal([EnumeratorCancellation] CancellationToken ct)
    {
        foreach (var i in items)
        {
            ct.ThrowIfCancellationRequested();
            yield return await Task.FromResult<object>(i!); // test harness permits null-forgiving
        }
    }
}
