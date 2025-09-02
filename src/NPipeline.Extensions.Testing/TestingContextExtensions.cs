using NPipeline.Pipeline;

namespace NPipeline.Extensions.Testing;

public static class TestingContextExtensions
{
    // Stores source data for parameterless InMemorySourceNode<T> resolution
    // - If nodeId is provided, data is stored under a node-scoped key
    // - Always stores a type-scoped key for convenience
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

    // Retrieves a sink instance (like InMemorySinkNode<T>) that was registered in the PipelineContext.Items
    // Looks up by typeof(T).FullName first (the convention used by InMemorySinkNode<>), then scans for a matching type
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
