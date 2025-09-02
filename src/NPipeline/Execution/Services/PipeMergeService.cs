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
                await foreach (var item in pipe.ToAsyncEnumerable(ct).WithCancellation(ct))
                {
                    yield return item; // allow nulls (object?)
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

    private Func<IEnumerable<IDataPipe>, CancellationToken, IDataPipe> BuildMergeDelegate(Type dataType, MergeType mergeType)
    {
        var pipesParam = Expression.Parameter(typeof(IEnumerable<IDataPipe>), "pipes");
        var ctParam = Expression.Parameter(typeof(CancellationToken), "ct");

        // Get the strategy from selector
        var strategySelectorExpr = Expression.Constant(strategySelector);
        var getStrategyMethod = typeof(IMergeStrategySelector).GetMethod(nameof(IMergeStrategySelector.GetStrategy))!;
        var getStrategyCall = Expression.Call(strategySelectorExpr, getStrategyMethod, Expression.Constant(dataType), Expression.Constant(mergeType));

        // Convert pipes to typed pipes
        var typedPipesVar = Expression.Variable(typeof(IEnumerable<>).MakeGenericType(typeof(IDataPipe<>).MakeGenericType(dataType)), "typedPipes");

        // Instead of blind Cast (which throws when heterogeneous generic arguments exist e.g., join inputs of different types),
        // filter only those IDataPipe<TExpected> pipes whose generic argument matches the node's declared InputType.
        // pipesParam is IEnumerable<IDataPipe>; we project via Where + Select dynamic inspection.
        var pipeVar = Expression.Parameter(typeof(IDataPipe), "p");
        var getTypeCall = Expression.Call(pipeVar, typeof(object).GetMethod("GetType")!);
        var interfacesCall = Expression.Call(getTypeCall, typeof(Type).GetMethod("GetInterfaces")!);
        var ifaceVar = Expression.Parameter(typeof(Type), "iface");
        var targetGeneric = typeof(IDataPipe<>);

        var isMatchLambda = Expression.Lambda<Func<Type, bool>>(
            Expression.AndAlso(
                Expression.Property(ifaceVar, nameof(Type.IsGenericType)),
                Expression.AndAlso(
                    Expression.Equal(Expression.Call(ifaceVar, nameof(Type.GetGenericTypeDefinition), Type.EmptyTypes), Expression.Constant(targetGeneric)),
                    Expression.Equal(
                        Expression.ArrayIndex(Expression.Call(ifaceVar, nameof(Type.GetGenericArguments), Type.EmptyTypes), Expression.Constant(0)),
                        Expression.Constant(dataType)))
            ), ifaceVar);

        // interfacesCall.Any(isMatch)
        var anyMethod = typeof(Enumerable).GetMethods().First(m => m.Name == nameof(Enumerable.Any) && m.GetParameters().Length == 2)
            .MakeGenericMethod(typeof(Type));

        var anyCall = Expression.Call(anyMethod, interfacesCall, isMatchLambda);
        var whereLambda = Expression.Lambda<Func<IDataPipe, bool>>(anyCall, pipeVar);

        var whereMethod = typeof(Enumerable).GetMethods().First(m => m.Name == nameof(Enumerable.Where) && m.GetParameters().Length == 2)
            .MakeGenericMethod(typeof(IDataPipe));

        var filtered = Expression.Call(whereMethod, pipesParam, whereLambda);

        // Now cast filtered to IEnumerable<IDataPipe<TExpected>>
        var castMethod = typeof(Enumerable).GetMethods().First(m => m.Name == nameof(Enumerable.Cast) && m.IsGenericMethod)
            .MakeGenericMethod(typeof(IDataPipe<>).MakeGenericType(dataType));

        var castCall = Expression.Call(castMethod, filtered);
        var assignTypedPipes = Expression.Assign(typedPipesVar, castCall);

        // Call the strategy's Merge method
        var mergeMethod = typeof(IMergeStrategy<>).MakeGenericType(dataType).GetMethod(nameof(IMergeStrategy<object>.Merge))!;

        var mergeCall = Expression.Call(Expression.Convert(getStrategyCall, typeof(IMergeStrategy<>).MakeGenericType(dataType)), mergeMethod, typedPipesVar,
            ctParam);

        var block = Expression.Block(
            [typedPipesVar],
            assignTypedPipes,
            mergeCall
        );

        var lambda = Expression.Lambda<Func<IEnumerable<IDataPipe>, CancellationToken, IDataPipe>>(block, pipesParam, ctParam);
        return lambda.Compile();
    }
}
