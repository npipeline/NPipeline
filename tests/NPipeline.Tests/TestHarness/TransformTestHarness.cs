using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.TestHarness;

/// <summary>
///     Test harness utilities for running transform nodes in unit tests without relying on the generic execution strategy directly.
/// </summary>
public static class TransformTestHarness
{
    /// <summary>
    ///     Executes a transform node against an in-memory set of input items under the given execution strategy,
    ///     defaulting to sequential execution as the pipeline does.
    /// </summary>
    public static async Task<IReadOnlyList<TOut>> RunAsync<TIn, TOut>(
        ITransformNode<TIn, TOut> node,
        IEnumerable<TIn> items,
        PipelineContext? context = null,
        IExecutionStrategy? executionStrategy = null,
        CancellationToken cancellationToken = default)
    {
        context ??= PipelineContext.CreateDefault();

        // Use a lightweight in-memory pipe that does not enforce notnull constraint
        var list = items.ToList();
        var inputPipe = new HarnessListPipe<TIn>(list, "HarnessInput");
        var strategy = executionStrategy
                       ?? (node as IExecutionStrategyProvider)?.DefaultExecutionStrategy
                       ?? new SequentialExecutionStrategy();
        var outputPipe = await strategy.ExecuteAsync<TIn, TOut>(inputPipe, node, context, node.GetType().Name, cancellationToken).ConfigureAwait(false);
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
        IExecutionStrategy? executionStrategy = null,
        CancellationToken cancellationToken = default)
    {
        var results = await RunAsync(node, [item], context, executionStrategy, cancellationToken).ConfigureAwait(false);
        return results.Single();
    }
}

file sealed class HarnessListPipe<T>(IReadOnlyList<T> items, string name) : IDataStream<T>
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
