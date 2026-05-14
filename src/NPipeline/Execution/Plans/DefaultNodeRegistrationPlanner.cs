using System.Collections.Concurrent;
using System.Reflection;
using NPipeline.Graph;
using NPipeline.Graph.PipelineDelegates;
using NPipeline.Nodes;
using NPipeline.Pipeline.Internals;

namespace NPipeline.Execution.Plans;

/// <summary>
///     Default implementation for build-time execution registration planning.
/// </summary>
public sealed class DefaultNodeRegistrationPlanner : INodeRegistrationPlanner
{
    /// <summary>
    ///     Shared singleton instance.
    /// </summary>
    public static readonly DefaultNodeRegistrationPlanner Instance = new();

    private static readonly Type JoinNodeBaseDefinition = typeof(BaseJoinNode<,,,>);

    private static readonly MethodInfo BuildStronglyTypedCustomMergeDelegateMethod = typeof(DefaultNodeRegistrationPlanner)
        .GetMethod(nameof(BuildStronglyTypedCustomMergeDelegate), BindingFlags.NonPublic | BindingFlags.Static)
        ?? throw new InvalidOperationException($"Method '{nameof(BuildStronglyTypedCustomMergeDelegate)}' not found on {nameof(DefaultNodeRegistrationPlanner)}.");

    private static readonly ConcurrentDictionary<Type, CustomMergeDelegate> CustomMergeDelegateCache = new();

    /// <inheritdoc />
    public void PrepareNode(NodeKind kind, Type nodeType)
    {
        ArgumentNullException.ThrowIfNull(nodeType);

        if (kind != NodeKind.Join)
            return;

        PrecompileJoinKeySelectors(nodeType);
    }

    /// <inheritdoc />
    public CustomMergeDelegate BuildCustomMergeDelegate(Type nodeType)
    {
        ArgumentNullException.ThrowIfNull(nodeType);
        return CustomMergeDelegateCache.GetOrAdd(nodeType, static t => BuildCustomMergeDelegateCore(t));
    }

    private static void PrecompileJoinKeySelectors(Type nodeType)
    {
        if (JoinKeySelectorRegistry.TryGetSelectors(nodeType, out var selector1, out var selector2) &&
            selector1 is not null && selector2 is not null)
            return;

        var genericArguments = TryExtractJoinGenericArguments(nodeType);

        if (!genericArguments.HasValue)
            return;

        var (keyType, input1Type, input2Type) = genericArguments.Value;
        var (compiledSelector1, compiledSelector2) = JoinKeySelectorRegistry.Compile(nodeType, keyType, input1Type, input2Type);

        if (compiledSelector1 is not null && compiledSelector2 is not null)
            JoinKeySelectorRegistry.Register(nodeType, compiledSelector1, compiledSelector2);
    }

    private static (Type KeyType, Type Input1Type, Type Input2Type)? TryExtractJoinGenericArguments(Type nodeType)
    {
        for (var baseType = nodeType.BaseType; baseType is not null; baseType = baseType.BaseType)
        {
            if (!baseType.IsGenericType)
                continue;

            if (baseType.GetGenericTypeDefinition() != JoinNodeBaseDefinition)
                continue;

            var args = baseType.GetGenericArguments();
            return (args[0], args[1], args[2]);
        }

        return null;
    }

    private static CustomMergeDelegate BuildCustomMergeDelegateCore(Type nodeType)
    {
        if (typeof(ICustomMergeNodeUntyped).IsAssignableFrom(nodeType))
            return static async (node, pipes, ct) => await ((ICustomMergeNodeUntyped)node).MergeAsyncUntyped(pipes, ct).ConfigureAwait(false);

        var genericInterface = nodeType.GetInterfaces()
                                   .FirstOrDefault(static i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ICustomMergeNode<>))
                               ?? throw new InvalidOperationException(
                                   $"Custom merge node '{nodeType.Name}' does not implement expected generic interface.");

        var inputType = genericInterface.GetGenericArguments()[0];
        var helper = BuildStronglyTypedCustomMergeDelegateMethod.MakeGenericMethod(inputType);
        return (CustomMergeDelegate)helper.Invoke(null, null)!;
    }

    private static CustomMergeDelegate BuildStronglyTypedCustomMergeDelegate<TIn>()
    {
        return static async (node, pipes, ct) => await ((ICustomMergeNode<TIn>)node).MergeAsync(pipes, ct).ConfigureAwait(false);
    }
}