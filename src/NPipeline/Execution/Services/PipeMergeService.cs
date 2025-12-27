using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Graph;
using NPipeline.Nodes;

namespace NPipeline.Execution.Services;

/// <summary>
///     Service for merging data pipes using appropriate strategies.
/// </summary>
public sealed class PipeMergeService(IMergeStrategySelector strategySelector) : IPipeMergeService
{
    private static readonly ConcurrentDictionary<(Type DataType, MergeType MergeType), Func<IEnumerable<IDataPipe>, CancellationToken, IDataPipe>>
        MergeDelegateCache = new();

    /// <inheritdoc />
    public async Task<IDataPipe> MergeAsync(
        NodeDefinition nodeDef,
        INode nodeInstance,
        IEnumerable<IDataPipe> inputPipes,
        CancellationToken cancellationToken = default)
    {
        // Special-case join nodes: they intentionally accept heterogeneous input types (the two sides of the join)
        // so we must not filter by nodeDef.InputType (which reflects only the first input's type).
        if (nodeDef.Kind == NodeKind.Join)
        {
            async IAsyncEnumerable<object?> Hetero([EnumeratorCancellation] CancellationToken ct = default)
            {
                foreach (var pipe in inputPipes)
                {
                    await foreach (var item in pipe.ToAsyncEnumerable(ct).WithCancellation(ct))
                    {
                        yield return item; // allow nulls (object?)
                    }
                }
            }

            return new StreamingDataPipe<object?>(Hetero(cancellationToken), $"JoinMerge_{nodeDef.Id}");
        }

        // Check for custom merge first
        if (nodeDef.HasCustomMerge && nodeDef.CustomMerge is not null)
            return await nodeDef.CustomMerge(nodeInstance, inputPipes, cancellationToken);

        // Use standard merge strategy
        var dataType = nodeDef.InputType ?? throw new InvalidOperationException($"Node '{nodeDef.Id}' missing InputType metadata.");
        var mergeType = nodeDef.MergeStrategy ?? MergeType.Interleave;

        var cacheKey = (DataType: dataType, MergeType: mergeType);

        if (!MergeDelegateCache.TryGetValue(cacheKey, out var mergeDelegate))
        {
            mergeDelegate = BuildMergeDelegate(dataType, mergeType);
            MergeDelegateCache[cacheKey] = mergeDelegate;
        }

        return await Task.Run(() => mergeDelegate(inputPipes, cancellationToken), cancellationToken);
    }

    /// <summary>
    ///     Builds a compiled delegate that merges data pipes of the specified type using the given merge strategy.
    /// </summary>
    /// <remarks>
    ///     This method uses expression trees to dynamically generate code that:
    ///     1. Filters input pipes to only those that implement IDataPipe&lt;T&gt; for the expected data type
    ///     2. Casts the filtered pipes to the correct generic type
    ///     3. Invokes the appropriate merge strategy's Merge method
    ///     The generated delegate is cached to avoid repeated compilation overhead.
    ///     This approach provides excellent runtime performance (delegate call) while maintaining type safety
    ///     and supporting heterogeneous input scenarios (e.g., join nodes with different input types).
    /// </remarks>
    /// <param name="dataType">The expected data type for the merge operation.</param>
    /// <param name="mergeType">The merge strategy to use (Interleave, Concatenate, etc.).</param>
    /// <returns>A compiled delegate that performs the merge operation.</returns>
    private Func<IEnumerable<IDataPipe>, CancellationToken, IDataPipe> BuildMergeDelegate(Type dataType, MergeType mergeType)
    {
        // Define delegate parameters
        var pipesParam = Expression.Parameter(typeof(IEnumerable<IDataPipe>), "pipes");
        var ctParam = Expression.Parameter(typeof(CancellationToken), "ct");

        // Step 1: Get the merge strategy from the selector
        var strategySelectorExpr = Expression.Constant(strategySelector);
        var getStrategyMethod = typeof(IMergeStrategySelector).GetMethod(nameof(IMergeStrategySelector.GetStrategy))!;
        var getStrategyCall = Expression.Call(strategySelectorExpr, getStrategyMethod, Expression.Constant(dataType), Expression.Constant(mergeType));

        // Step 2: Prepare variable for typed pipes (IEnumerable<IDataPipe<T>>)
        var typedPipesVar = Expression.Variable(typeof(IEnumerable<>).MakeGenericType(typeof(IDataPipe<>).MakeGenericType(dataType)), "typedPipes");

        // Step 3: Build a predicate to filter pipes by their generic type
        // This is necessary because pipesParam is IEnumerable<IDataPipe> (non-generic),
        // but we need only those pipes that implement IDataPipe<T> for the expected type.
        // This handles heterogeneous inputs (e.g., join nodes) where not all pipes have the same type.
        var pipeVar = Expression.Parameter(typeof(IDataPipe), "p");
        var getTypeCall = Expression.Call(pipeVar, typeof(object).GetMethod("GetType")!);
        var interfacesCall = Expression.Call(getTypeCall, typeof(Type).GetMethod("GetInterfaces")!);
        var ifaceVar = Expression.Parameter(typeof(Type), "iface");
        var targetGeneric = typeof(IDataPipe<>);

        // Predicate: iface.IsGenericType && iface.GetGenericTypeDefinition() == IDataPipe<> && iface.GetGenericArguments()[0] == dataType
        var isMatchLambda = Expression.Lambda<Func<Type, bool>>(
            Expression.AndAlso(
                Expression.Property(ifaceVar, nameof(Type.IsGenericType)),
                Expression.AndAlso(
                    Expression.Equal(Expression.Call(ifaceVar, nameof(Type.GetGenericTypeDefinition), Type.EmptyTypes), Expression.Constant(targetGeneric)),
                    Expression.Equal(
                        Expression.ArrayIndex(Expression.Call(ifaceVar, nameof(Type.GetGenericArguments), Type.EmptyTypes), Expression.Constant(0)),
                        Expression.Constant(dataType)))
            ), ifaceVar);

        // Step 4: Filter pipes using the predicate
        // filteredPipes = pipesParam.Where(p => p.GetType().GetInterfaces().Any(iface => isMatch(iface)))
        var anyMethod = typeof(Enumerable).GetMethods().First(m => m.Name == nameof(Enumerable.Any) && m.GetParameters().Length == 2)
            .MakeGenericMethod(typeof(Type));

        var anyCall = Expression.Call(anyMethod, interfacesCall, isMatchLambda);
        var whereLambda = Expression.Lambda<Func<IDataPipe, bool>>(anyCall, pipeVar);

        var whereMethod = typeof(Enumerable).GetMethods().First(m => m.Name == nameof(Enumerable.Where) && m.GetParameters().Length == 2)
            .MakeGenericMethod(typeof(IDataPipe));

        var filtered = Expression.Call(whereMethod, pipesParam, whereLambda);

        // Step 5: Cast filtered pipes to the correct generic type
        // typedPipes = filtered.Cast<IDataPipe<T>>()
        var castMethod = typeof(Enumerable).GetMethods().First(m => m.Name == nameof(Enumerable.Cast) && m.IsGenericMethod)
            .MakeGenericMethod(typeof(IDataPipe<>).MakeGenericType(dataType));

        var castCall = Expression.Call(castMethod, filtered);
        var assignTypedPipes = Expression.Assign(typedPipesVar, castCall);

        // Step 6: Call the merge strategy's Merge method
        // strategy.Merge(typedPipes, cancellationToken)
        var mergeMethod = typeof(IMergeStrategy<>).MakeGenericType(dataType).GetMethod(nameof(IMergeStrategy<object>.Merge))!;

        var mergeCall = Expression.Call(Expression.Convert(getStrategyCall, typeof(IMergeStrategy<>).MakeGenericType(dataType)), mergeMethod, typedPipesVar,
            ctParam);

        // Step 7: Build the final expression block
        var block = Expression.Block(
            [typedPipesVar],
            assignTypedPipes,
            mergeCall
        );

        // Step 8: Compile and return the delegate
        var lambda = Expression.Lambda<Func<IEnumerable<IDataPipe>, CancellationToken, IDataPipe>>(block, pipesParam, ctParam);
        return lambda.Compile();
    }
}
