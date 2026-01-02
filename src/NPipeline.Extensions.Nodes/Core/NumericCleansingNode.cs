using System.Linq.Expressions;

namespace NPipeline.Extensions.Nodes.Core;

/// <summary>
///     A cleansing node for numeric properties that provides operations like clamping, rounding, and scaling.
/// </summary>
public sealed class NumericCleansingNode<T> : PropertyTransformationNode<T>
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="NumericCleansingNode{T}" /> class.
    /// </summary>
    public NumericCleansingNode()
    {
    }

    /// <summary>
    ///     Clamps a numeric property to a range [min, max]. Throws ArgumentException if min > max.
    /// </summary>
    public NumericCleansingNode<T> Clamp<TNumeric>(
        Expression<Func<T, TNumeric>> selector,
        TNumeric min,
        TNumeric max)
        where TNumeric : IComparable<TNumeric>
    {
        ArgumentNullException.ThrowIfNull(selector);

        if (min.CompareTo(max) > 0)
            throw new ArgumentException("Minimum value cannot be greater than maximum value.", nameof(min));

        Register(selector, value =>
        {
            if (value.CompareTo(min) < 0)
                return min;

            if (value.CompareTo(max) > 0)
                return max;

            return value;
        });

        return this;
    }

    /// <summary>
    ///     Clamps a nullable numeric property to a range [min, max]. Returns null if the value is null.
    /// </summary>
    public NumericCleansingNode<T> Clamp<TNumeric>(
        Expression<Func<T, TNumeric?>> selector,
        TNumeric min,
        TNumeric max)
        where TNumeric : struct, IComparable<TNumeric>
    {
        ArgumentNullException.ThrowIfNull(selector);

        if (min.CompareTo(max) > 0)
            throw new ArgumentException("Minimum value cannot be greater than maximum value.", nameof(min));

        Register(selector, value =>
        {
            if (!value.HasValue)
                return null;

            var val = value.Value;
            if (val.CompareTo(min) < 0)
                return min;

            if (val.CompareTo(max) > 0)
                return max;

            return val;
        });

        return this;
    }

    /// <summary>
    ///     Ensures a numeric property is at least the specified minimum value.
    /// </summary>
    public NumericCleansingNode<T> Min<TNumeric>(
        Expression<Func<T, TNumeric>> selector,
        TNumeric minValue)
        where TNumeric : IComparable<TNumeric>
    {
        ArgumentNullException.ThrowIfNull(selector);

        Register(selector, value => value.CompareTo(minValue) < 0 ? minValue : value);
        return this;
    }

    /// <summary>
    ///     Ensures a numeric property is at most the specified maximum value.
    /// </summary>
    public NumericCleansingNode<T> Max<TNumeric>(
        Expression<Func<T, TNumeric>> selector,
        TNumeric maxValue)
        where TNumeric : IComparable<TNumeric>
    {
        ArgumentNullException.ThrowIfNull(selector);

        Register(selector, value => value.CompareTo(maxValue) > 0 ? maxValue : value);
        return this;
    }

    /// <summary>
    ///     Rounds a double property to the specified number of decimal places.
    /// </summary>
    public NumericCleansingNode<T> Round(
        Expression<Func<T, double>> selector,
        int digits = 2)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentOutOfRangeException.ThrowIfNegative(digits);

        Register(selector, value => Math.Round(value, digits));
        return this;
    }

    /// <summary>
    ///     Rounds a decimal property to the specified number of decimal places.
    /// </summary>
    public NumericCleansingNode<T> Round(
        Expression<Func<T, decimal>> selector,
        int digits = 2)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentOutOfRangeException.ThrowIfNegative(digits);

        Register(selector, value => Math.Round(value, digits));
        return this;
    }

    /// <summary>
    ///     Rounds a nullable double property to the specified number of decimal places.
    /// </summary>
    public NumericCleansingNode<T> Round(
        Expression<Func<T, double?>> selector,
        int digits = 2)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentOutOfRangeException.ThrowIfNegative(digits);

        Register(selector, value => value.HasValue
            ? Math.Round(value.Value, digits)
            : null);

        return this;
    }

    /// <summary>
    ///     Rounds a nullable decimal property to the specified number of decimal places.
    /// </summary>
    public NumericCleansingNode<T> Round(
        Expression<Func<T, decimal?>> selector,
        int digits = 2)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentOutOfRangeException.ThrowIfNegative(digits);

        Register(selector, value => value.HasValue
            ? Math.Round(value.Value, digits)
            : null);

        return this;
    }

    /// <summary>
    ///     Rounds a double down to the nearest integer.
    /// </summary>
    public NumericCleansingNode<T> Floor(Expression<Func<T, double>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        Register(selector, value => Math.Floor(value));
        return this;
    }

    /// <summary>
    ///     Rounds a double up to the nearest integer.
    /// </summary>
    public NumericCleansingNode<T> Ceiling(Expression<Func<T, double>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        Register(selector, value => Math.Ceiling(value));
        return this;
    }

    /// <summary>
    ///     Gets the absolute value of a numeric property.
    /// </summary>
    public NumericCleansingNode<T> AbsoluteValue(Expression<Func<T, int>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        Register(selector, value => Math.Abs(value));
        return this;
    }

    /// <summary>
    ///     Gets the absolute value of a nullable numeric property.
    /// </summary>
    public NumericCleansingNode<T> AbsoluteValue(Expression<Func<T, int?>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        Register(selector, value => value.HasValue
            ? Math.Abs(value.Value)
            : null);

        return this;
    }

    /// <summary>
    ///     Gets the absolute value of a double property.
    /// </summary>
    public NumericCleansingNode<T> AbsoluteValue(Expression<Func<T, double>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        Register(selector, value => Math.Abs(value));
        return this;
    }

    /// <summary>
    ///     Gets the absolute value of a decimal property.
    /// </summary>
    public NumericCleansingNode<T> AbsoluteValue(Expression<Func<T, decimal>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        Register(selector, value => Math.Abs(value));
        return this;
    }

    /// <summary>
    ///     Sets value to zero if it is negative.
    /// </summary>
    public NumericCleansingNode<T> ToZeroIfNegative(Expression<Func<T, int>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        Register(selector, value => value < 0
            ? 0
            : value);

        return this;
    }

    /// <summary>
    ///     Sets value to zero if it is negative (double).
    /// </summary>
    public NumericCleansingNode<T> ToZeroIfNegative(Expression<Func<T, double>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        Register(selector, value => value < 0
            ? 0.0
            : value);

        return this;
    }

    /// <summary>
    ///     Sets value to zero if it is negative (decimal).
    /// </summary>
    public NumericCleansingNode<T> ToZeroIfNegative(Expression<Func<T, decimal>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        Register(selector, value => value < 0
            ? 0m
            : value);

        return this;
    }

    /// <summary>
    ///     Scales a numeric value by multiplying by a factor.
    /// </summary>
    public NumericCleansingNode<T> Scale(
        Expression<Func<T, double>> selector,
        double factor)
    {
        ArgumentNullException.ThrowIfNull(selector);
        Register(selector, value => value * factor);
        return this;
    }

    /// <summary>
    ///     Scales a decimal value by multiplying by a factor.
    /// </summary>
    public NumericCleansingNode<T> Scale(
        Expression<Func<T, decimal>> selector,
        decimal factor)
    {
        ArgumentNullException.ThrowIfNull(selector);
        Register(selector, value => value * factor);
        return this;
    }

    /// <summary>
    ///     Provides a default value if the property is null.
    /// </summary>
    public NumericCleansingNode<T> DefaultIfNull<TNumeric>(
        Expression<Func<T, TNumeric?>> selector,
        TNumeric defaultValue)
        where TNumeric : struct
    {
        ArgumentNullException.ThrowIfNull(selector);
        Register(selector, value => value ?? defaultValue);
        return this;
    }
}
