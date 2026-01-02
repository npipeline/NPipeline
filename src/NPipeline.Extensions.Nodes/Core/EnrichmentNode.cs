using System.Linq.Expressions;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes.Core;

/// <summary>
/// Enriches data by setting property values from lookups, computations, or defaults.
/// Combines lookup enrichment and default value functionality into a unified, fluent API.
/// </summary>
/// <typeparam name="T">The type of the item being enriched.</typeparam>
public sealed class EnrichmentNode<T> : PropertyTransformationNode<T>
{
    private readonly List<Action<T>> _enrichmentActions = [];

    /// <inheritdoc />
    protected override ValueTask<T> ExecuteValueTaskAsync(
        T item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Apply all enrichment actions in the order they were registered
        // This includes Lookup, Set, Compute, and Default operations
        foreach (var action in _enrichmentActions)
        {
            cancellationToken.ThrowIfCancellationRequested();
            action(item);
        }

        return ValueTask.FromResult(item);
    }

    #region Lookup Operations

    /// <summary>
    /// Enriches a property by looking up a value from a dictionary.
    /// Only sets the property if the key exists in the lookup.
    /// </summary>
    /// <typeparam name="TKey">The type of the lookup key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    /// <param name="propertySelector">Expression selecting the property to enrich.</param>
    /// <param name="lookup">Dictionary containing key-value pairs.</param>
    /// <param name="keySelector">Expression selecting the property to use as the lookup key.</param>
    /// <returns>This node for method chaining.</returns>
    public EnrichmentNode<T> Lookup<TKey, TValue>(
        Expression<Func<T, TValue>> propertySelector,
        IReadOnlyDictionary<TKey, TValue> lookup,
        Expression<Func<T, TKey>> keySelector)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(propertySelector);
        ArgumentNullException.ThrowIfNull(lookup);
        ArgumentNullException.ThrowIfNull(keySelector);

        var keyGetter = keySelector.Compile();
        var propertySetter = CompilePropertySetter(propertySelector);

        _enrichmentActions.Add(item =>
        {
            var key = keyGetter(item);
            if (lookup.TryGetValue(key, out var value))
            {
                propertySetter(item, value);
            }
        });

        return this;
    }

    /// <summary>
    /// Sets a property value from a lookup dictionary, setting to default(TValue) if key not found.
    /// </summary>
    /// <typeparam name="TKey">The type of the lookup key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    /// <param name="propertySelector">Expression selecting the property to set.</param>
    /// <param name="lookup">Dictionary containing key-value pairs.</param>
    /// <param name="keySelector">Expression selecting the property to use as the lookup key.</param>
    /// <returns>This node for method chaining.</returns>
    public EnrichmentNode<T> Set<TKey, TValue>(
        Expression<Func<T, TValue>> propertySelector,
        IReadOnlyDictionary<TKey, TValue> lookup,
        Expression<Func<T, TKey>> keySelector)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(propertySelector);
        ArgumentNullException.ThrowIfNull(lookup);
        ArgumentNullException.ThrowIfNull(keySelector);

        var keyGetter = keySelector.Compile();
        var propertySetter = CompilePropertySetter(propertySelector);

        _enrichmentActions.Add(item =>
        {
            var key = keyGetter(item);
            var value = lookup.TryGetValue(key, out var lookupValue) ? lookupValue : default!;
            propertySetter(item, value);
        });

        return this;
    }

    #endregion

    #region Computed Properties

    /// <summary>
    /// Sets a property to a computed value based on the item.
    /// </summary>
    /// <typeparam name="TValue">The type of the property value.</typeparam>
    /// <param name="propertySelector">Expression selecting the property to set.</param>
    /// <param name="computeValue">Function to compute the property value from the item.</param>
    /// <returns>This node for method chaining.</returns>
    public EnrichmentNode<T> Compute<TValue>(
        Expression<Func<T, TValue>> propertySelector,
        Func<T, TValue> computeValue)
    {
        ArgumentNullException.ThrowIfNull(propertySelector);
        ArgumentNullException.ThrowIfNull(computeValue);

        var propertySetter = CompilePropertySetter(propertySelector);

        _enrichmentActions.Add(item =>
        {
            var value = computeValue(item);
            propertySetter(item, value);
        });

        return this;
    }

    #endregion

    #region Default Values

    /// <summary>
    /// Helper method to register a default value transformation.
    /// </summary>
    private void RegisterDefault<TValue>(
        Expression<Func<T, TValue>> propertySelector,
        Func<TValue, TValue> transform)
    {
        var propertyGetter = propertySelector.Compile();
        var propertySetter = CompilePropertySetter(propertySelector);

        _enrichmentActions.Add(item =>
        {
            var currentValue = propertyGetter(item);
            var newValue = transform(currentValue);
            if (!EqualityComparer<TValue>.Default.Equals(currentValue, newValue))
            {
                propertySetter(item, newValue);
            }
        });
    }

    /// <summary>
    /// Sets a default value for a property if it is currently null.
    /// </summary>
    /// <typeparam name="TValue">The type of the property.</typeparam>
    /// <param name="propertySelector">Expression selecting the property.</param>
    /// <param name="defaultValue">The default value to use if property is null.</param>
    /// <returns>This node for method chaining.</returns>
    public EnrichmentNode<T> DefaultIfNull<TValue>(
        Expression<Func<T, TValue>> propertySelector,
        TValue defaultValue)
    {
        RegisterDefault(propertySelector, value => value == null ? defaultValue : value);
        return this;
    }

    /// <summary>
    /// Sets a default value for a string property if it is null or empty.
    /// </summary>
    /// <param name="propertySelector">Expression selecting the string property.</param>
    /// <param name="defaultValue">The default value to use if property is null or empty.</param>
    /// <returns>This node for method chaining.</returns>
    public EnrichmentNode<T> DefaultIfEmpty(
        Expression<Func<T, string?>> propertySelector,
        string defaultValue)
    {
        RegisterDefault(propertySelector, value => string.IsNullOrEmpty(value) ? defaultValue : value);
        return this;
    }

    /// <summary>
    /// Sets a default value for a string property if it is null, empty, or whitespace.
    /// </summary>
    /// <param name="propertySelector">Expression selecting the string property.</param>
    /// <param name="defaultValue">The default value to use if property is null, empty, or whitespace.</param>
    /// <returns>This node for method chaining.</returns>
    public EnrichmentNode<T> DefaultIfWhitespace(
        Expression<Func<T, string?>> propertySelector,
        string defaultValue)
    {
        RegisterDefault(propertySelector, value => string.IsNullOrWhiteSpace(value) ? defaultValue : value);
        return this;
    }

    /// <summary>
    /// Sets a default value for a property if it equals the default value for its type.
    /// </summary>
    /// <typeparam name="TValue">The type of the property.</typeparam>
    /// <param name="propertySelector">Expression selecting the property.</param>
    /// <param name="defaultValue">The default value to use if property equals default(TValue).</param>
    /// <returns>This node for method chaining.</returns>
    public EnrichmentNode<T> DefaultIfDefault<TValue>(
        Expression<Func<T, TValue>> propertySelector,
        TValue defaultValue)
        where TValue : struct, IEquatable<TValue>
    {
        var comparer = EqualityComparer<TValue>.Default;
        RegisterDefault(propertySelector, value => comparer.Equals(value, default) ? defaultValue : value);
        return this;
    }

    /// <summary>
    /// Sets a default value for a property if it matches a condition.
    /// </summary>
    /// <typeparam name="TValue">The type of the property.</typeparam>
    /// <param name="propertySelector">Expression selecting the property.</param>
    /// <param name="condition">Predicate that determines if default should be applied.</param>
    /// <param name="defaultValue">The default value to use if condition is true.</param>
    /// <returns>This node for method chaining.</returns>
    public EnrichmentNode<T> DefaultWhen<TValue>(
        Expression<Func<T, TValue>> propertySelector,
        Func<TValue, bool> condition,
        TValue defaultValue)
    {
        RegisterDefault(propertySelector, value => condition(value) ? defaultValue : value);
        return this;
    }

    /// <summary>
    /// Sets a default value for an integer property if it is zero.
    /// </summary>
    /// <param name="propertySelector">Expression selecting the integer property.</param>
    /// <param name="defaultValue">The default value to use if property is zero.</param>
    /// <returns>This node for method chaining.</returns>
    public EnrichmentNode<T> DefaultIfZero(
        Expression<Func<T, int>> propertySelector,
        int defaultValue)
    {
        RegisterDefault(propertySelector, value => value == 0 ? defaultValue : value);
        return this;
    }

    /// <summary>
    /// Sets a default value for a decimal property if it is zero.
    /// </summary>
    /// <param name="propertySelector">Expression selecting the decimal property.</param>
    /// <param name="defaultValue">The default value to use if property is zero.</param>
    /// <returns>This node for method chaining.</returns>
    public EnrichmentNode<T> DefaultIfZero(
        Expression<Func<T, decimal>> propertySelector,
        decimal defaultValue)
    {
        RegisterDefault(propertySelector, value => value == decimal.Zero ? defaultValue : value);
        return this;
    }

    /// <summary>
    /// Sets a default value for a double property if it is zero.
    /// </summary>
    /// <param name="propertySelector">Expression selecting the double property.</param>
    /// <param name="defaultValue">The default value to use if property is zero.</param>
    /// <returns>This node for method chaining.</returns>
    public EnrichmentNode<T> DefaultIfZero(
        Expression<Func<T, double>> propertySelector,
        double defaultValue)
    {
        RegisterDefault(propertySelector, value => value == 0.0 ? defaultValue : value);
        return this;
    }

    /// <summary>
    /// Sets a default collection for a collection property if it is null or empty.
    /// </summary>
    /// <typeparam name="TItem">The type of items in the collection.</typeparam>
    /// <param name="propertySelector">Expression selecting the collection property.</param>
    /// <param name="defaultValue">The default collection to use if property is null or empty.</param>
    /// <returns>This node for method chaining.</returns>
    public EnrichmentNode<T> DefaultIfEmptyCollection<TItem>(
        Expression<Func<T, IEnumerable<TItem>>> propertySelector,
        IEnumerable<TItem> defaultValue)
    {
        RegisterDefault(propertySelector, value =>
        {
            if (value is null)
            {
                return defaultValue;
            }

            if (value is ICollection<TItem> collection)
            {
                return collection.Count == 0 ? defaultValue : value;
            }

            using var enumerator = value.GetEnumerator();
            return enumerator.MoveNext() ? value : defaultValue;
        });
        return this;
    }

    #endregion

    #region Helper Methods

    /// <summary>
    /// Compiles a property setter expression into an action delegate.
    /// </summary>
    private static Action<T, TValue> CompilePropertySetter<TValue>(
        Expression<Func<T, TValue>> propertySelector)
    {
        if (propertySelector.Body is not MemberExpression memberExpr)
        {
            throw new ArgumentException(
                "Selector must be a simple property access expression.",
                nameof(propertySelector));
        }

        var property = memberExpr.Member as System.Reflection.PropertyInfo
            ?? throw new ArgumentException(
                "Selector must target a property.",
                nameof(propertySelector));

        if (!property.CanWrite)
        {
            throw new ArgumentException(
                $"Property '{property.Name}' does not have a setter.",
                nameof(propertySelector));
        }

        var paramExpr = Expression.Parameter(typeof(T), "item");
        var valueExpr = Expression.Parameter(typeof(TValue), "value");
        var assignExpr = Expression.Assign(
            Expression.Property(paramExpr, property),
            valueExpr);

        var lambda = Expression.Lambda<Action<T, TValue>>(assignExpr, paramExpr, valueExpr);
        return lambda.Compile();
    }

    #endregion
}
