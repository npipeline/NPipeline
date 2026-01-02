using System.Linq.Expressions;

namespace NPipeline.Extensions.Nodes.Core;

/// <summary>
///     A validation node for numeric properties that provides validators like range checks, sign validation, and type checks.
/// </summary>
public sealed class NumericValidationNode<T> : ValidationNode<T>
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="NumericValidationNode{T}" /> class.
    /// </summary>
    public NumericValidationNode()
    {
    }

    /// <summary>
    ///     Validates that an integer is positive.
    /// </summary>
    public NumericValidationNode<T> IsPositive(Expression<Func<T, int>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsPositive";
        var message = errorMessage ?? "Value must be positive";
        Register(selector, value => value > 0, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a double is positive.
    /// </summary>
    public NumericValidationNode<T> IsPositive(Expression<Func<T, double>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsPositive";
        var message = errorMessage ?? "Value must be positive";
        Register(selector, value => value > 0, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a decimal is positive.
    /// </summary>
    public NumericValidationNode<T> IsPositive(Expression<Func<T, decimal>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsPositive";
        var message = errorMessage ?? "Value must be positive";
        Register(selector, value => value > 0, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that an integer is negative.
    /// </summary>
    public NumericValidationNode<T> IsNegative(Expression<Func<T, int>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsNegative";
        var message = errorMessage ?? "Value must be negative";
        Register(selector, value => value < 0, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a double is negative.
    /// </summary>
    public NumericValidationNode<T> IsNegative(Expression<Func<T, double>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsNegative";
        var message = errorMessage ?? "Value must be negative";
        Register(selector, value => value < 0, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a decimal is negative.
    /// </summary>
    public NumericValidationNode<T> IsNegative(Expression<Func<T, decimal>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsNegative";
        var message = errorMessage ?? "Value must be negative";
        Register(selector, value => value < 0, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that an integer is zero or positive (non-negative).
    /// </summary>
    public NumericValidationNode<T> IsZeroOrPositive(Expression<Func<T, int>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsZeroOrPositive";
        var message = errorMessage ?? "Value must be zero or positive";
        Register(selector, value => value >= 0, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that an integer is not negative. Alias for IsZeroOrPositive.
    /// </summary>
    public NumericValidationNode<T> IsNotNegative(Expression<Func<T, int>> selector, string? errorMessage = null)
        => IsZeroOrPositive(selector, errorMessage);

    /// <summary>
    ///     Validates that a double is zero or positive (non-negative).
    /// </summary>
    public NumericValidationNode<T> IsZeroOrPositive(Expression<Func<T, double>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsZeroOrPositive";
        var message = errorMessage ?? "Value must be zero or positive";
        Register(selector, value => value >= 0, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a double is not negative. Alias for IsZeroOrPositive.
    /// </summary>
    public NumericValidationNode<T> IsNotNegative(Expression<Func<T, double>> selector, string? errorMessage = null)
        => IsZeroOrPositive(selector, errorMessage);

    /// <summary>
    ///     Validates that a decimal is zero or positive (non-negative).
    /// </summary>
    public NumericValidationNode<T> IsZeroOrPositive(Expression<Func<T, decimal>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsZeroOrPositive";
        var message = errorMessage ?? "Value must be zero or positive";
        Register(selector, value => value >= 0, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a decimal is not negative. Alias for IsZeroOrPositive.
    /// </summary>
    public NumericValidationNode<T> IsNotNegative(Expression<Func<T, decimal>> selector, string? errorMessage = null)
        => IsZeroOrPositive(selector, errorMessage);

    /// <summary>
    ///     Validates that an integer is not zero.
    /// </summary>
    public NumericValidationNode<T> IsNonZero(Expression<Func<T, int>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsNonZero";
        var message = errorMessage ?? "Value must not be zero";
        Register(selector, value => value != 0, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a double is not zero.
    /// </summary>
    public NumericValidationNode<T> IsNonZero(Expression<Func<T, double>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsNonZero";
        var message = errorMessage ?? "Value must not be zero";
        Register(selector, value => value != 0, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a decimal is not zero.
    /// </summary>
    public NumericValidationNode<T> IsNonZero(Expression<Func<T, decimal>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsNonZero";
        var message = errorMessage ?? "Value must not be zero";
        Register(selector, value => value != 0, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that an integer is between two values (inclusive).
    /// </summary>
    public NumericValidationNode<T> IsBetween(
        Expression<Func<T, int>> selector,
        int minValue,
        int maxValue,
        string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);

        if (minValue > maxValue)
            throw new ArgumentException("minValue must be less than or equal to maxValue.", nameof(minValue));

        var ruleName = "IsBetween";
        var message = errorMessage ?? $"Value must be between {minValue} and {maxValue}";
        Register(selector, value => value >= minValue && value <= maxValue, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a double is between two values (inclusive).
    /// </summary>
    public NumericValidationNode<T> IsBetween(
        Expression<Func<T, double>> selector,
        double minValue,
        double maxValue,
        string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);

        if (minValue > maxValue)
            throw new ArgumentException("minValue must be less than or equal to maxValue.", nameof(minValue));

        var ruleName = "IsBetween";
        var message = errorMessage ?? $"Value must be between {minValue} and {maxValue}";
        Register(selector, value => value >= minValue && value <= maxValue, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a decimal is between two values (inclusive).
    /// </summary>
    public NumericValidationNode<T> IsBetween(
        Expression<Func<T, decimal>> selector,
        decimal minValue,
        decimal maxValue,
        string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);

        if (minValue > maxValue)
            throw new ArgumentException("minValue must be less than or equal to maxValue.", nameof(minValue));

        var ruleName = "IsBetween";
        var message = errorMessage ?? $"Value must be between {minValue} and {maxValue}";
        Register(selector, value => value >= minValue && value <= maxValue, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that an integer is even.
    /// </summary>
    public NumericValidationNode<T> IsEven(Expression<Func<T, int>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsEven";
        var message = errorMessage ?? "Value must be even";
        Register(selector, value => value % 2 == 0, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that an integer is odd.
    /// </summary>
    public NumericValidationNode<T> IsOdd(Expression<Func<T, int>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsOdd";
        var message = errorMessage ?? "Value must be odd";
        Register(selector, value => value % 2 != 0, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a double is finite (not NaN or Infinity).
    /// </summary>
    public NumericValidationNode<T> IsFinite(Expression<Func<T, double>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsFinite";
        var message = errorMessage ?? "Value must be a finite number";
        Register(selector, value => double.IsFinite(value), ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a double is an integer value (no fractional part).
    /// </summary>
    public NumericValidationNode<T> IsIntegerValue(Expression<Func<T, double>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsIntegerValue";
        var message = errorMessage ?? "Value must be an integer";
        Register(selector, value => value == Math.Floor(value), ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a decimal is an integer value (no fractional part).
    /// </summary>
    public NumericValidationNode<T> IsIntegerValue(Expression<Func<T, decimal>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsIntegerValue";
        var message = errorMessage ?? "Value must be an integer";
        Register(selector, value => value == decimal.Floor(value), ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that an integer is greater than a minimum value.
    /// </summary>
    public NumericValidationNode<T> IsGreaterThan(Expression<Func<T, int>> selector, int minValue, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsGreaterThan";
        var message = errorMessage ?? $"Value must be greater than {minValue}";
        Register(selector, value => value > minValue, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a double is greater than a minimum value.
    /// </summary>
    public NumericValidationNode<T> IsGreaterThan(Expression<Func<T, double>> selector, double minValue, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsGreaterThan";
        var message = errorMessage ?? $"Value must be greater than {minValue}";
        Register(selector, value => value > minValue, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a decimal is greater than a minimum value.
    /// </summary>
    public NumericValidationNode<T> IsGreaterThan(Expression<Func<T, decimal>> selector, decimal minValue, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsGreaterThan";
        var message = errorMessage ?? $"Value must be greater than {minValue}";
        Register(selector, value => value > minValue, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that an integer is less than a maximum value.
    /// </summary>
    public NumericValidationNode<T> IsLessThan(Expression<Func<T, int>> selector, int maxValue, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsLessThan";
        var message = errorMessage ?? $"Value must be less than {maxValue}";
        Register(selector, value => value < maxValue, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a double is less than a maximum value.
    /// </summary>
    public NumericValidationNode<T> IsLessThan(Expression<Func<T, double>> selector, double maxValue, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsLessThan";
        var message = errorMessage ?? $"Value must be less than {maxValue}";
        Register(selector, value => value < maxValue, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a decimal is less than a maximum value.
    /// </summary>
    public NumericValidationNode<T> IsLessThan(Expression<Func<T, decimal>> selector, decimal maxValue, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsLessThan";
        var message = errorMessage ?? $"Value must be less than {maxValue}";
        Register(selector, value => value < maxValue, ruleName, _ => message);
        return this;
    }

    #region Nullable Validations

    /// <summary>
    ///     Validates that a nullable integer is not null.
    /// </summary>
    public NumericValidationNode<T> IsNotNull(Expression<Func<T, int?>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsNotNull";
        var message = errorMessage ?? "Value must not be null";
        Register(selector, value => value.HasValue, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a nullable double is not null.
    /// </summary>
    public NumericValidationNode<T> IsNotNull(Expression<Func<T, double?>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsNotNull";
        var message = errorMessage ?? "Value must not be null";
        Register(selector, value => value.HasValue, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a nullable decimal is not null.
    /// </summary>
    public NumericValidationNode<T> IsNotNull(Expression<Func<T, decimal?>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsNotNull";
        var message = errorMessage ?? "Value must not be null";
        Register(selector, value => value.HasValue, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a nullable integer is positive (if not null).
    /// </summary>
    public NumericValidationNode<T> IsPositive(Expression<Func<T, int?>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsPositive";
        var message = errorMessage ?? "Value must be positive";
        Register(selector, value => !value.HasValue || value.Value > 0, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a nullable double is positive (if not null).
    /// </summary>
    public NumericValidationNode<T> IsPositive(Expression<Func<T, double?>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsPositive";
        var message = errorMessage ?? "Value must be positive";
        Register(selector, value => !value.HasValue || value.Value > 0, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a nullable decimal is positive (if not null).
    /// </summary>
    public NumericValidationNode<T> IsPositive(Expression<Func<T, decimal?>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsPositive";
        var message = errorMessage ?? "Value must be positive";
        Register(selector, value => !value.HasValue || value.Value > 0, ruleName, _ => message);
        return this;
    }

    #endregion
}
