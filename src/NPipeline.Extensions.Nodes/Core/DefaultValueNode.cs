using System.Linq.Expressions;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes.Core;

/// <summary>
/// Provides operations for setting default values on properties that are null or meet certain conditions.
/// Supports DefaultIfNull, DefaultIfNullOrEmpty, DefaultIfDefault, and conditional defaults.
/// </summary>
/// <typeparam name="T">The type of the item being enriched.</typeparam>
public sealed class DefaultValueNode<T> : PropertyTransformationNode<T>
{
    /// <summary>
    /// Sets a default value for a property if it is currently null.
    /// </summary>
    /// <typeparam name="TProp">The type of the property.</typeparam>
    /// <param name="selector">Expression selecting the property.</param>
    /// <param name="defaultValue">The default value to use if property is null.</param>
    /// <returns>This node for method chaining.</returns>
    public DefaultValueNode<T> DefaultIfNull<TProp>(
        Expression<Func<T, TProp>> selector,
        TProp defaultValue)
    {
        Register(selector, value => value == null ? defaultValue : value);
        return this;
    }

    /// <summary>
    /// Sets a default value for a string property if it is null or empty.
    /// </summary>
    /// <param name="selector">Expression selecting the string property.</param>
    /// <param name="defaultValue">The default value to use if property is null or empty.</param>
    /// <returns>This node for method chaining.</returns>
    public DefaultValueNode<T> DefaultIfNullOrEmpty(
        Expression<Func<T, string?>> selector,
        string defaultValue)
    {
        Register(selector, value => string.IsNullOrEmpty(value) ? defaultValue : value);
        return this;
    }

    /// <summary>
    /// Sets a default value for a string property if it is null or whitespace.
    /// </summary>
    /// <param name="selector">Expression selecting the string property.</param>
    /// <param name="defaultValue">The default value to use if property is null or whitespace.</param>
    /// <returns>This node for method chaining.</returns>
    public DefaultValueNode<T> DefaultIfNullOrWhitespace(
        Expression<Func<T, string?>> selector,
        string defaultValue)
    {
        Register(selector, value => string.IsNullOrWhiteSpace(value) ? defaultValue : value);
        return this;
    }

    /// <summary>
    /// Sets a default value for a property if it equals the default value for its type.
    /// </summary>
    /// <typeparam name="TProp">The type of the property.</typeparam>
    /// <param name="selector">Expression selecting the property.</param>
    /// <param name="defaultValue">The default value to use if property equals default(TProp).</param>
    /// <returns>This node for method chaining.</returns>
    public DefaultValueNode<T> DefaultIfDefault<TProp>(
        Expression<Func<T, TProp>> selector,
        TProp defaultValue)
        where TProp : struct, IEquatable<TProp>
    {
        var comparer = EqualityComparer<TProp>.Default;
        Register(selector, value => comparer.Equals(value, default) ? defaultValue : value);
        return this;
    }

    /// <summary>
    /// Sets a default value for a property if it matches a condition.
    /// </summary>
    /// <typeparam name="TProp">The type of the property.</typeparam>
    /// <param name="selector">Expression selecting the property.</param>
    /// <param name="condition">Predicate that determines if default should be applied.</param>
    /// <param name="defaultValue">The default value to use if condition is true.</param>
    /// <returns>This node for method chaining.</returns>
    public DefaultValueNode<T> DefaultIfCondition<TProp>(
        Expression<Func<T, TProp>> selector,
        Func<TProp, bool> condition,
        TProp defaultValue)
    {
        Register(selector, value => condition(value) ? defaultValue : value);
        return this;
    }

    /// <summary>
    /// Sets a default value for an integer property if it is zero.
    /// </summary>
    /// <param name="selector">Expression selecting the integer property.</param>
    /// <param name="defaultValue">The default value to use if property is zero.</param>
    /// <returns>This node for method chaining.</returns>
    public DefaultValueNode<T> DefaultIfZero(
        Expression<Func<T, int>> selector,
        int defaultValue)
    {
        Register(selector, value => value == 0 ? defaultValue : value);
        return this;
    }

    /// <summary>
    /// Sets a default value for a decimal property if it is zero.
    /// </summary>
    /// <param name="selector">Expression selecting the decimal property.</param>
    /// <param name="defaultValue">The default value to use if property is zero.</param>
    /// <returns>This node for method chaining.</returns>
    public DefaultValueNode<T> DefaultIfZero(
        Expression<Func<T, decimal>> selector,
        decimal defaultValue)
    {
        Register(selector, value => value == decimal.Zero ? defaultValue : value);
        return this;
    }

    /// <summary>
    /// Sets a default value for a double property if it is zero.
    /// </summary>
    /// <param name="selector">Expression selecting the double property.</param>
    /// <param name="defaultValue">The default value to use if property is zero.</param>
    /// <returns>This node for method chaining.</returns>
    public DefaultValueNode<T> DefaultIfZero(
        Expression<Func<T, double>> selector,
        double defaultValue)
    {
        Register(selector, value => value == 0.0 ? defaultValue : value);
        return this;
    }

    /// <summary>
    /// Sets a default value for a collection property if it is empty.
    /// </summary>
    /// <typeparam name="TItem">The type of items in the collection.</typeparam>
    /// <param name="selector">Expression selecting the collection property.</param>
    /// <param name="defaultValue">The default value to use if collection is empty.</param>
    /// <returns>This node for method chaining.</returns>
    public DefaultValueNode<T> DefaultIfEmpty<TItem>(
        Expression<Func<T, IEnumerable<TItem>>> selector,
        IEnumerable<TItem> defaultValue)
    {
        Register(selector, value =>
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

    /// <summary>
    /// Extracts a property info from a property selector expression.
    /// </summary>
    private static string GetPropertyName<TProp>(
        Expression<Func<T, TProp>> selector)
    {
        if (selector.Body is not MemberExpression memberExpr)
        {
            throw new ArgumentException(
                "Selector must be a simple property access expression.",
                nameof(selector));
        }

        return memberExpr.Member.Name;
    }
}
