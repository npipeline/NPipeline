using System.Reflection;
using NPipeline.Graph;
using NPipeline.Nodes;

namespace NPipeline.Pipeline.Internals;

/// <summary>
///     Builds custom merge delegates for nodes implementing either <see cref="ICustomMergeNodeUntyped" /> or
///     <c>ICustomMergeNode&lt;TIn&gt;</c>. Reflection is performed once per merge node at build time.
/// </summary>
internal static class CustomMergeDelegateFactory
{
    public static CustomMergeDelegate Build(Type nodeType)
    {
        if (typeof(ICustomMergeNodeUntyped).IsAssignableFrom(nodeType))
            return async (node, pipes, ct) => await ((ICustomMergeNodeUntyped)node).MergeAsyncUntyped(pipes, ct).ConfigureAwait(false);

        var genericIface = nodeType.GetInterfaces()
                               .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ICustomMergeNode<>))
                           ?? throw new InvalidOperationException($"Custom merge node '{nodeType.Name}' does not implement expected generic interface.");

        var inType = genericIface.GetGenericArguments()[0];

        var helper = typeof(CustomMergeDelegateFactory).GetMethod(nameof(BuildStronglyTyped), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(inType);

        return (CustomMergeDelegate)helper.Invoke(null, null)!;
    }

    private static CustomMergeDelegate BuildStronglyTyped<TIn>()
    {
        return async (node, pipes, ct) => await ((ICustomMergeNode<TIn>)node).MergeAsync(pipes, ct).ConfigureAwait(false);
    }
}
