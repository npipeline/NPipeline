using System.Collections.Immutable;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Testing;

/// <summary>
///     A source node that produces data from an in-memory list or resolves items from PipelineContext when constructed parameterlessly.
/// </summary>
/// <typeparam name="T">The type of data to be emitted.</typeparam>
public sealed class InMemorySourceNode<T> : SourceNode<T>
{
    private const string SourceDataPrefix = "NPipeline.Testing.SourceData::";

    private readonly IReadOnlyList<T>? _items;
    private readonly bool _useContext;

    /// <summary>
    ///     Initializes a new instance of the <see cref="InMemorySourceNode{T}" /> class with an empty list.
    /// </summary>
    public InMemorySourceNode()
    {
        _items = null; // Will be resolved from context at execution time
        _useContext = true;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="InMemorySourceNode{T}" /> class with explicit items.
    /// </summary>
    /// <param name="items">The list of items to be emitted by the source node.</param>
    public InMemorySourceNode(IEnumerable<T> items)
    {
        _items = items as IReadOnlyList<T> ?? items.ToList();
        _useContext = false;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="InMemorySourceNode{T}" /> class that resolves items from PipelineContext.
    ///     Context resolution order:
    ///     - Node-scoped: "NPipeline.Testing.SourceData::{context.CurrentNodeId}"
    ///     - Type-scoped: "NPipeline.Testing.SourceData::{typeof(T).FullName}"
    /// </summary>
    /// <param name="context">The pipeline context to resolve items from.</param>
    /// <param name="nodeId">The node ID for context resolution.</param>
    public InMemorySourceNode(PipelineContext context, string nodeId)
    {
        ArgumentNullException.ThrowIfNull(context);
        var items = ResolveFromContext(context, nodeId) ?? [];
        _items = items as ImmutableList<T> ?? items.ToImmutableList();
        _useContext = false;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="InMemorySourceNode{T}" /> class that resolves items from PipelineContext.
    ///     Context resolution order:
    ///     - Node-scoped: "NPipeline.Testing.SourceData::{context.CurrentNodeId}"
    ///     - Type-scoped: "NPipeline.Testing.SourceData::{typeof(T).FullName}"
    /// </summary>
    /// <param name="context">The pipeline context to resolve items from.</param>
    public InMemorySourceNode(PipelineContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var items = ResolveFromContext(context, null) ?? [];
        _items = items as ImmutableList<T> ?? items.ToImmutableList();
        _useContext = false;
    }

    /// <inheritdoc />
    public override IDataPipe<T> Execute(PipelineContext context, CancellationToken cancellationToken)
    {
        if (!_useContext)
            return new InMemoryDataPipe<T>(_items!);

        var items = ResolveFromContext(context, context.CurrentNodeId)
                    ?? throw new InvalidOperationException(
                        $"No source data configured for node '{context.CurrentNodeId}' of type '{typeof(T).Name}'. " +
                        $"Set data via context.SetSourceData<{typeof(T).Name}>(...) before running, or use InMemorySourceNode<T>(IEnumerable<T>) constructor.");

        return new InMemoryDataPipe<T>(items);
    }

    /// <summary>
    ///     Resolves the data from the context.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="nodeId">The optional node ID for context resolution.</param>
    /// <returns>The data items.</returns>
    private static IReadOnlyList<T>? ResolveFromContext(PipelineContext context, string? nodeId)
    {
        // First try to find data in the current context
        if (TryResolveFromContext(context, nodeId, out var data))
            return data;

        // If not found and we're in a composite pipeline, check the parent context
        if (context.Items.TryGetValue(PipelineContextKeys.TestingParentContext, out var parentContextObj) &&
            parentContextObj is PipelineContext parentContext)
        {
            if (TryResolveFromContext(parentContext, nodeId, out var parentData))
                return parentData;
        }

        return null;
    }

    private static bool TryResolveFromContext(PipelineContext context, string? nodeId, out IReadOnlyList<T>? data)
    {
        data = null;

        // Try node-specific key first
        if (!string.IsNullOrWhiteSpace(nodeId))
        {
            var nodeKey = $"{SourceDataPrefix}{nodeId}";

            if (context.Items.TryGetValue(nodeKey, out var nodeData) && nodeData is IReadOnlyList<T> nodeList)
            {
                data = nodeList;
                return true;
            }
        }

        // Try type-specific key
        var typeKey = $"{SourceDataPrefix}{typeof(T).FullName}";

        if (context.Items.TryGetValue(typeKey, out var typeData) && typeData is IReadOnlyList<T> typeList)
        {
            data = typeList;
            return true;
        }

        return false;
    }
}
