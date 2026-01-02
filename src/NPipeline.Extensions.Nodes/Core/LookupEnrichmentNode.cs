using System.Linq.Expressions;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes.Core;

/// <summary>
/// Provides operations for enriching data with lookup values and computed properties.
/// Supports adding or replacing properties based on lookup dictionaries.
/// </summary>
/// <typeparam name="T">The type of the item being enriched.</typeparam>
public sealed class LookupEnrichmentNode<T> : PropertyTransformationNode<T>
{
    private readonly List<Action<T>> _lookupActions = [];

    /// <summary>
    /// Adds a property value from a lookup dictionary based on a key selector.
    /// </summary>
    /// <typeparam name="TKey">The type of the lookup key.</typeparam>
    /// <typeparam name="TValue">The type of the lookup value.</typeparam>
    /// <param name="keySelector">Expression selecting the key property.</param>
    /// <param name="lookup">Dictionary containing key-value pairs for lookup.</param>
    /// <param name="valueSetter">Expression selecting the property to set with the lookup value.</param>
    /// <returns>This node for method chaining.</returns>
    public LookupEnrichmentNode<T> AddProperty<TKey, TValue>(
        Expression<Func<T, TKey>> keySelector,
        IReadOnlyDictionary<TKey, TValue> lookup,
        Expression<Func<T, TValue>> valueSetter)
        where TKey : notnull
    {
        var keyGetter = keySelector.Compile();
        var setter = CompilePropertySetter(valueSetter);

        _lookupActions.Add(item =>
        {
            var key = keyGetter(item);
            if (lookup.TryGetValue(key, out var value))
            {
                setter(item, value);
            }
        });

        return this;
    }

    /// <summary>
    /// Replaces a property value with a lookup dictionary value based on a key selector.
    /// </summary>
    /// <typeparam name="TKey">The type of the lookup key.</typeparam>
    /// <typeparam name="TValue">The type of the lookup value.</typeparam>
    /// <param name="keySelector">Expression selecting the key property.</param>
    /// <param name="lookup">Dictionary containing key-value pairs for lookup.</param>
    /// <param name="valueSetter">Expression selecting the property to set with the lookup value.</param>
    /// <returns>This node for method chaining.</returns>
    public LookupEnrichmentNode<T> ReplaceProperty<TKey, TValue>(
        Expression<Func<T, TKey>> keySelector,
        IReadOnlyDictionary<TKey, TValue> lookup,
        Expression<Func<T, TValue>> valueSetter)
        where TKey : notnull
    {
        var keyGetter = keySelector.Compile();
        var setter = CompilePropertySetter(valueSetter);

        _lookupActions.Add(item =>
        {
            var key = keyGetter(item);
            var value = lookup.TryGetValue(key, out var lookupValue) ? lookupValue : default!;
            setter(item, value);
        });

        return this;
    }

    /// <summary>
    /// Adds multiple properties from a lookup based on a key selector.
    /// </summary>
    /// <typeparam name="TKey">The type of the lookup key.</typeparam>
    /// <typeparam name="TValue">The type of the lookup values.</typeparam>
    /// <param name="keySelector">Expression selecting the key property.</param>
    /// <param name="lookup">Dictionary containing key-value pairs for lookup.</param>
    /// <param name="valueSetters">Array of expressions selecting properties to set with lookup values.</param>
    /// <returns>This node for method chaining.</returns>
    public LookupEnrichmentNode<T> AddProperties<TKey, TValue>(
        Expression<Func<T, TKey>> keySelector,
        IReadOnlyDictionary<TKey, TValue> lookup,
        params Expression<Func<T, TValue>>[] valueSetters)
        where TKey : notnull
    {
        var keyGetter = keySelector.Compile();

        foreach (var setter in valueSetters)
        {
            var compiledSetter = CompilePropertySetter(setter);

            _lookupActions.Add(item =>
            {
                var key = keyGetter(item);
                if (lookup.TryGetValue(key, out var value))
                {
                    compiledSetter(item, value);
                }
            });
        }

        return this;
    }

    /// <summary>
    /// Adds a computed property based on existing property values.
    /// </summary>
    /// <typeparam name="TValue">The type of the computed value.</typeparam>
    /// <param name="selector">Expression selecting the property to set with computed value.</param>
    /// <param name="computeValue">Function to compute the property value from the item.</param>
    /// <returns>This node for method chaining.</returns>
    public LookupEnrichmentNode<T> AddComputedProperty<TValue>(
        Expression<Func<T, TValue>> selector,
        Func<T, TValue> computeValue)
    {
        var setter = CompilePropertySetter(selector);

        _lookupActions.Add(item =>
        {
            var value = computeValue(item);
            setter(item, value);
        });

        return this;
    }

    /// <summary>
    /// Executes lookup operations on the item asynchronously.
    /// </summary>
    protected override ValueTask<T> ExecuteValueTaskAsync(
        T item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Apply all lookup actions
        foreach (var action in _lookupActions)
        {
            cancellationToken.ThrowIfCancellationRequested();
            action(item);
        }

        // Apply base transformations
        return base.ExecuteValueTaskAsync(item, context, cancellationToken);
    }

    /// <summary>
    /// Compiles a property setter expression into an action delegate.
    /// </summary>
    private static Action<T, TValue> CompilePropertySetter<TValue>(
        Expression<Func<T, TValue>> selector)
    {
        if (selector.Body is not MemberExpression memberExpr)
        {
            throw new ArgumentException(
                "Selector must be a simple property access expression.",
                nameof(selector));
        }

        var property = memberExpr.Member as System.Reflection.PropertyInfo
            ?? throw new ArgumentException(
                "Selector must target a property.",
                nameof(selector));

        if (!property.CanWrite)
        {
            throw new ArgumentException(
                $"Property '{property.Name}' does not have a setter.",
                nameof(selector));
        }

        var paramExpr = Expression.Parameter(typeof(T), "item");
        var valueExpr = Expression.Parameter(typeof(TValue), "value");
        var assignExpr = Expression.Assign(
            Expression.Property(paramExpr, property),
            valueExpr);

        var lambda = Expression.Lambda<Action<T, TValue>>(assignExpr, paramExpr, valueExpr);
        return lambda.Compile();
    }
}
