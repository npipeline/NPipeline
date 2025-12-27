using NPipeline.Pipeline;

namespace NPipeline.Extensions.Testing;

/// <summary>
///     Provides extension methods for <see cref="PipelineContext" /> to support testing scenarios.
///     These methods facilitate setting up test data and retrieving sink instances from the pipeline context.
/// </summary>
public static class TestingContextExtensions
{
    /// <summary>
    ///     Stores source data for parameterless InMemorySourceNode&lt;T&gt; resolution.
    /// </summary>
    /// <typeparam name="T">The type of data items to store.</typeparam>
    /// <param name="context">The pipeline context to store the data in.</param>
    /// <param name="items">The collection of items to store as source data.</param>
    /// <param name="nodeId">Optional node identifier for node-scoped storage. If provided, data is stored under a node-scoped key in addition to the type-scoped key.</param>
    /// <remarks>
    ///     The method stores data in two ways:
    ///     - If nodeId is provided, data is stored under a node-scoped key
    ///     - Always stores a type-scoped key for convenience
    ///     This allows for both node-specific and type-based data retrieval during testing.
    /// </remarks>
    public static void SetSourceData<T>(this PipelineContext context, IEnumerable<T> items, string? nodeId = null)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(items);

        var list = items as IReadOnlyList<T> ?? items.ToList();

        if (!string.IsNullOrWhiteSpace(nodeId))
            context.Items[PipelineContextKeys.TestingSourceData(nodeId)] = list;

        var typeKey = PipelineContextKeys.TestingSourceDataByType(typeof(T));
        context.Items[typeKey] = list;
    }

    /// <summary>
    ///     Retrieves a sink instance (like InMemorySinkNode&lt;T&gt;) that was registered in the PipelineContext.Items.
    /// </summary>
    /// <typeparam name="T">The type of sink to retrieve.</typeparam>
    /// <param name="context">The pipeline context to search for the sink instance.</param>
    /// <returns>The sink instance of type T found in the context.</returns>
    /// <exception cref="InvalidOperationException">Thrown when no instance of the requested sink type is found in the context.</exception>
    /// <remarks>
    ///     The lookup strategy is:
    ///     1. First looks up by typeof(T).FullName (the convention used by InMemorySinkNode&lt;&gt;)
    ///     2. If not found, scans for a matching type in all context items
    ///     3. If still not found, checks parent context if available (for composite pipelines)
    /// </remarks>
    public static T GetSink<T>(this PipelineContext context) where T : class
    {
        ArgumentNullException.ThrowIfNull(context);

        var key = typeof(T).FullName!;

        if (context.Items.TryGetValue(key, out var obj) && obj is T sinkByKey)
            return sinkByKey;

        foreach (var kv in context.Items)
        {
            if (kv.Value is T sinkByType)
                return sinkByType;
        }

        // Check parent context if available (for composite pipelines)
        if (context.Items.TryGetValue(PipelineContextKeys.TestingParentContext, out var parentContextObj) && parentContextObj is PipelineContext parentContext)
        {
            if (parentContext.Items.TryGetValue(key, out var parentObj) && parentObj is T parentSinkByKey)
                return parentSinkByKey;

            foreach (var kv in parentContext.Items)
            {
                if (kv.Value is T parentSinkByType)
                    return parentSinkByType;
            }
        }

        string FriendlyTypeName(Type t)
        {
            string MapAlias(Type tt)
            {
                if (tt == typeof(int))
                    return "int";

                if (tt == typeof(long))
                    return "long";

                if (tt == typeof(short))
                    return "short";

                if (tt == typeof(byte))
                    return "byte";

                if (tt == typeof(bool))
                    return "bool";

                if (tt == typeof(string))
                    return "string";

                if (tt == typeof(object))
                    return "object";

                if (tt == typeof(void))
                    return "void";

                return tt.Name;
            }

            if (!t.IsGenericType)
                return MapAlias(t);

            var generic = t.GetGenericTypeDefinition().Name;
            var tick = generic.IndexOf('`');

            if (tick >= 0)
                generic = generic.Substring(0, tick);

            var args = string.Join(", ", t.GetGenericArguments().Select(a => MapAlias(a)));
            return $"{generic}<{args}>";
        }

        throw new InvalidOperationException(
            $"Could not find an instance of '{FriendlyTypeName(typeof(T))}' in the pipeline context. " +
            $"Ensure your pipeline registers it under context.Items[typeof({FriendlyTypeName(typeof(T))}).FullName] or use InMemorySinkNode<T> which registers itself.");
    }
}
