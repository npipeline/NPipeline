using System.Collections.Concurrent;
using System.Linq.Expressions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Graph;
using NPipeline.Nodes;

namespace NPipeline.Execution.Services;

/// <summary>
///     Service for merging data pipes using appropriate strategies.
/// </summary>
public sealed class PipeMergeService(IMergeStrategySelector strategySelector) : IPipeMergeService
{
    private static readonly ConcurrentDictionary<(Type DataType, MergeType MergeType),
        Func<IMergeStrategySelector, IEnumerable<IDataStream>, int?, CancellationToken, IDataStream>> MergeDelegateCache = new();

    /// <inheritdoc />
    public Task<IDataStream> MergeAsync(
        NodeDefinition nodeDef,
        INode nodeInstance,
        IEnumerable<IDataStream> inputPipes,
        int? mergeCapacity,
        CancellationToken cancellationToken = default)
    {
        var materializedInputPipes = inputPipes as IReadOnlyList<IDataStream> ?? inputPipes.ToList();

        if (materializedInputPipes.Count == 0)
            throw new InvalidOperationException(ErrorMessages.NodeMissingInputConnection(nodeDef.Id, nodeDef.Name, nodeDef.Kind.ToString()));

        // Special-case join nodes: they intentionally accept heterogeneous input types (the two sides of the join)
        // so we must not filter by nodeDef.InputType (which reflects only the first input's type). Their two inputs
        // must be read concurrently, or an unbounded live input starves the other side.
        if (nodeDef.Kind == NodeKind.Join)
        {
            var sources = new IAsyncEnumerable<object?>[materializedInputPipes.Count];
            for (var i = 0; i < sources.Length; i++)
                sources[i] = materializedInputPipes[i].ToAsyncEnumerable(cancellationToken);

            return Task.FromResult<IDataStream>(
                new DataStream<object?>(StreamInterleaver.Interleave(sources, mergeCapacity, cancellationToken), $"JoinMerge_{nodeDef.Id}"));
        }

        // Check for custom merge first
        if (nodeDef.HasCustomMerge && nodeDef.CustomMerge is not null)
            return nodeDef.CustomMerge(nodeInstance, materializedInputPipes, cancellationToken);

        // Use the effective runtime stream item type so lineage-wrapped streams merge correctly.
        var dataType = ResolveMergeDataType(nodeDef, materializedInputPipes);
        var mergeType = nodeDef.MergeStrategy ?? MergeType.Interleave;

        var cacheKey = (DataType: dataType, MergeType: mergeType);

        var mergeDelegate = MergeDelegateCache.GetOrAdd(cacheKey, static k => BuildMergeDelegate(k.DataType, k.MergeType));
        return Task.FromResult(mergeDelegate(strategySelector, materializedInputPipes, mergeCapacity, cancellationToken));
    }

    private static Type ResolveMergeDataType(NodeDefinition nodeDef, IReadOnlyList<IDataStream> inputPipes)
    {
        var distinctRuntimeTypes = inputPipes
            .Select(static pipe => pipe.GetDataType())
            .Distinct()
            .ToArray();

        if (distinctRuntimeTypes.Length != 1)
        {
            var formattedTypes = string.Join(", ", distinctRuntimeTypes.Select(TypeNameFormatter.GetAssemblyQualifiedName));

            throw new InvalidOperationException(
                $"Node '{nodeDef.Id}' received multiple runtime input stream types for merge: {formattedTypes}. " +
                "Non-join nodes require a single runtime stream type.");
        }

        return distinctRuntimeTypes[0];
    }

    /// <summary>
    ///     Builds a compiled delegate that merges data pipes of the specified type using the given merge strategy.
    /// </summary>
    /// <remarks>
    ///     This method uses expression trees to dynamically generate code that:
    ///     1. Casts input pipes to the expected IDataStream&lt;T&gt; type
    ///     2. Invokes the appropriate merge strategy's Merge method
    ///     The generated delegate is cached to avoid repeated compilation overhead. Only type-shaped code is cached;
    ///     the strategy selector is a delegate parameter, so the cache never captures a scoped instance.
    /// </remarks>
    /// <param name="dataType">The expected data type for the merge operation.</param>
    /// <param name="mergeType">The merge strategy to use (Interleave, Concatenate, etc.).</param>
    /// <returns>A compiled delegate that performs the merge operation.</returns>
    private static Func<IMergeStrategySelector, IEnumerable<IDataStream>, int?, CancellationToken, IDataStream> BuildMergeDelegate(
        Type dataType,
        MergeType mergeType)
    {
        // Define delegate parameters
        var selectorParam = Expression.Parameter(typeof(IMergeStrategySelector), "selector");
        var pipesParam = Expression.Parameter(typeof(IEnumerable<IDataStream>), "pipes");
        var capacityParam = Expression.Parameter(typeof(int?), "capacity");
        var ctParam = Expression.Parameter(typeof(CancellationToken), "ct");

        // Step 1: Get the merge strategy from the selector
        var getStrategyMethod = typeof(IMergeStrategySelector).GetMethod(nameof(IMergeStrategySelector.GetStrategy))!;
        var getStrategyCall = Expression.Call(selectorParam, getStrategyMethod, Expression.Constant(dataType), Expression.Constant(mergeType));

        // Step 2: Prepare variable for typed pipes (IEnumerable<IDataStream<T>>)
        var typedStreamType = typeof(IDataStream<>).MakeGenericType(dataType);
        var typedPipesType = typeof(IEnumerable<>).MakeGenericType(typedStreamType);
        var typedPipesVar = Expression.Variable(typedPipesType, "typedPipes");

        // Step 3: Cast pipes to the correct generic type
        // typedPipes = pipesParam.Cast<IDataStream<T>>()
        var castMethod = typeof(Enumerable).GetMethods().First(m => m.Name == nameof(Enumerable.Cast) && m.IsGenericMethod)
            .MakeGenericMethod(typedStreamType);

        var castCall = Expression.Call(castMethod, pipesParam);
        var assignTypedPipes = Expression.Assign(typedPipesVar, castCall);

        // Step 4: Call the merge strategy's Merge method
        // strategy.Merge(typedPipes, capacity, cancellationToken)
        var mergeMethod = typeof(IMergeStrategy<>).MakeGenericType(dataType).GetMethod(nameof(IMergeStrategy<object>.Merge),
            [typedPipesType, typeof(int?), typeof(CancellationToken)])!;

        var mergeCall = Expression.Call(Expression.Convert(getStrategyCall, typeof(IMergeStrategy<>).MakeGenericType(dataType)), mergeMethod, typedPipesVar,
            capacityParam, ctParam);

        // Step 5: Build the final expression block
        var block = Expression.Block(
            [typedPipesVar],
            assignTypedPipes,
            mergeCall
        );

        // Step 6: Compile and return the delegate
        var lambda = Expression.Lambda<Func<IMergeStrategySelector, IEnumerable<IDataStream>, int?, CancellationToken, IDataStream>>(block,
            selectorParam, pipesParam, capacityParam, ctParam);
        return lambda.Compile();
    }
}
