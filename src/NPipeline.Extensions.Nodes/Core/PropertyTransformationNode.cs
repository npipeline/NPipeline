using System.Linq.Expressions;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes.Core;

/// <summary>
///     Base class for in-place property transformations on items of type <typeparamref name="T" />.
///     Derived classes register one or more property-level transforms (compiled from expressions) and this node
///     applies them per item with no reflection in hot paths.
/// </summary>
/// <remarks>
///     <para>
///         Only settable properties or non-readonly fields are supported. Registration throws if the member is not settable.
///     </para>
///     <para>
///         Transformations are value-level (Func&lt;TProp, TProp&gt;). If you need context-aware logic, lift it into the lambda.
///     </para>
///     <para>
///         Mutates the existing instance by design. If your items are immutable, consider a variant that reconstructs
///         a new instance using a copy-with approach (future enhancement).
///     </para>
/// </remarks>
public abstract class PropertyTransformationNode<T> : TransformNode<T, T>
{
    private readonly List<ICompiledRule<T>> _rules = [];

    /// <summary>
    ///     Registers a transformation for a specific property/field selected via expression.
    /// </summary>
    /// <typeparam name="TProp">Property/field type</typeparam>
    /// <param name="selector">Selector like x => x.Name (supports nested)</param>
    /// <param name="transform">Value transformer (should be pure/idempotent ideally)</param>
    /// <returns>This node for method chaining.</returns>
    public PropertyTransformationNode<T> Register<TProp>(
        Expression<Func<T, TProp>> selector,
        Func<TProp, TProp> transform)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(transform);

        var accessor = PropertyAccessor.Create(selector);
        _rules.Add(new CompiledRule<T, TProp>(accessor, transform));
        return this;
    }

    /// <summary>
    ///     Registers multiple selectors with the same transform function.
    /// </summary>
    /// <typeparam name="TProp">Property/field type</typeparam>
    /// <param name="selectors">Multiple selectors to apply the same transform to</param>
    /// <param name="transform">Value transformer to apply to all selectors</param>
    /// <returns>This node for method chaining.</returns>
    public PropertyTransformationNode<T> RegisterMany<TProp>(
        IEnumerable<Expression<Func<T, TProp>>> selectors,
        Func<TProp, TProp> transform)
    {
        ArgumentNullException.ThrowIfNull(selectors);

        foreach (var sel in selectors)
        {
            Register(sel, transform);
        }

        return this;
    }

    /// <summary>
    ///     Executes transformations on the item asynchronously.
    ///     Applies all registered rules in order.
    /// </summary>
    public override Task<T> ExecuteAsync(
        T item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Apply transformations
        foreach (var rule in _rules)
        {
            cancellationToken.ThrowIfCancellationRequested();
            rule.Apply(item);
        }

        return Task.FromResult(item);
    }

    private interface ICompiledRule<in TItem>
    {
        void Apply(TItem item);
    }

    private sealed class CompiledRule<TItem, TProp>(
        PropertyAccessor.Accessor<TItem, TProp> accessor,
        Func<TProp, TProp> transform) : ICompiledRule<TItem>
    {
        public void Apply(TItem item)
        {
            var current = accessor.Getter(item);
            var updated = transform(current);

            // Always assign to ensure semantic changes not captured by Equals
            // (e.g., DateTime.Kind) are applied.
            // EqualityComparer<DateTime> compares by ticks and ignores Kind,
            // which would skip needed updates.
            accessor.Setter(item, updated);
        }
    }
}
