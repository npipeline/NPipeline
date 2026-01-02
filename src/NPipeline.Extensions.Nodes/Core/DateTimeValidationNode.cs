using System.Linq.Expressions;

namespace NPipeline.Extensions.Nodes.Core;

/// <summary>
///     A validation node for DateTime properties that provides validators like temporal checks, timezone validation, and day-of-week checks.
/// </summary>
public sealed class DateTimeValidationNode<T> : ValidationNode<T>
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="DateTimeValidationNode{T}" /> class.
    /// </summary>
    public DateTimeValidationNode()
    {
    }

    /// <summary>
    ///     Validates that a DateTime is in the future.
    /// </summary>
    public DateTimeValidationNode<T> IsInFuture(Expression<Func<T, DateTime>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsInFuture";
        var message = errorMessage ?? "Value must be in the future";
        Register(selector, value => value > DateTime.UtcNow, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a nullable DateTime is in the future or null.
    /// </summary>
    public DateTimeValidationNode<T> IsInFuture(Expression<Func<T, DateTime?>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsInFuture";
        var message = errorMessage ?? "Value must be in the future";
        Register(selector, value => !value.HasValue || value.Value > DateTime.UtcNow, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a DateTime is in the past.
    /// </summary>
    public DateTimeValidationNode<T> IsInPast(Expression<Func<T, DateTime>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsInPast";
        var message = errorMessage ?? "Value must be in the past";
        Register(selector, value => value < DateTime.UtcNow, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a nullable DateTime is in the past or null.
    /// </summary>
    public DateTimeValidationNode<T> IsInPast(Expression<Func<T, DateTime?>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsInPast";
        var message = errorMessage ?? "Value must be in the past";
        Register(selector, value => !value.HasValue || value.Value < DateTime.UtcNow, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a DateTime is today.
    /// </summary>
    public DateTimeValidationNode<T> IsToday(Expression<Func<T, DateTime>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsToday";
        var message = errorMessage ?? "Value must be today";
        var today = DateTime.UtcNow.Date;
        Register(selector, value => value.Date == today, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a DateTime is on a weekday (Monday-Friday).
    /// </summary>
    public DateTimeValidationNode<T> IsWeekday(Expression<Func<T, DateTime>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsWeekday";
        var message = errorMessage ?? "Value must be a weekday";
        Register(selector, value => value.DayOfWeek >= DayOfWeek.Monday && value.DayOfWeek <= DayOfWeek.Friday, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a DateTime is on a weekend (Saturday or Sunday).
    /// </summary>
    public DateTimeValidationNode<T> IsWeekend(Expression<Func<T, DateTime>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsWeekend";
        var message = errorMessage ?? "Value must be a weekend day";
        Register(selector, value => value.DayOfWeek == DayOfWeek.Saturday || value.DayOfWeek == DayOfWeek.Sunday, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a DateTime is UTC.
    /// </summary>
    public DateTimeValidationNode<T> IsUtc(Expression<Func<T, DateTime>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsUtc";
        var message = errorMessage ?? "Value must be in UTC";
        Register(selector, value => value.Kind == DateTimeKind.Utc, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a DateTime is local time.
    /// </summary>
    public DateTimeValidationNode<T> IsLocal(Expression<Func<T, DateTime>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsLocal";
        var message = errorMessage ?? "Value must be in local time";
        Register(selector, value => value.Kind == DateTimeKind.Local, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a DateTime is not MinValue.
    /// </summary>
    public DateTimeValidationNode<T> IsNotMinValue(Expression<Func<T, DateTime>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsNotMinValue";
        var message = errorMessage ?? "Value must not be the minimum date";
        Register(selector, value => value != DateTime.MinValue, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a DateTime is not MaxValue.
    /// </summary>
    public DateTimeValidationNode<T> IsNotMaxValue(Expression<Func<T, DateTime>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsNotMaxValue";
        var message = errorMessage ?? "Value must not be the maximum date";
        Register(selector, value => value != DateTime.MaxValue, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a DateTime is before a given date.
    /// </summary>
    public DateTimeValidationNode<T> IsBefore(Expression<Func<T, DateTime>> selector, DateTime maxDate, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsBefore";
        var message = errorMessage ?? $"Value must be before {maxDate:O}";
        Register(selector, value => value < maxDate, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a DateTime is after a given date.
    /// </summary>
    public DateTimeValidationNode<T> IsAfter(Expression<Func<T, DateTime>> selector, DateTime minDate, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsAfter";
        var message = errorMessage ?? $"Value must be after {minDate:O}";
        Register(selector, value => value > minDate, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a DateTime is between two dates (inclusive).
    /// </summary>
    public DateTimeValidationNode<T> IsBetween(
        Expression<Func<T, DateTime>> selector,
        DateTime minDate,
        DateTime maxDate,
        string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);

        if (minDate > maxDate)
            throw new ArgumentException("minDate must be less than or equal to maxDate.", nameof(minDate));

        var ruleName = "IsBetween";
        var message = errorMessage ?? $"Value must be between {minDate:O} and {maxDate:O}";
        Register(selector, value => value >= minDate && value <= maxDate, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a DateTime is on a specific day of the week.
    /// </summary>
    public DateTimeValidationNode<T> IsDayOfWeek(Expression<Func<T, DateTime>> selector, DayOfWeek dayOfWeek, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsDayOfWeek";
        var message = errorMessage ?? $"Value must be a {dayOfWeek}";
        Register(selector, value => value.DayOfWeek == dayOfWeek, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a DateTime has a specific year.
    /// </summary>
    public DateTimeValidationNode<T> IsInYear(Expression<Func<T, DateTime>> selector, int year, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);

        if (year < 1 || year > 9999)
            throw new ArgumentException("Year must be between 1 and 9999.", nameof(year));

        var ruleName = "IsInYear";
        var message = errorMessage ?? $"Value must be in year {year}";
        Register(selector, value => value.Year == year, ruleName, _ => message);
        return this;
    }

    /// <summary>
    ///     Validates that a DateTime has a specific month.
    /// </summary>
    public DateTimeValidationNode<T> IsInMonth(Expression<Func<T, DateTime>> selector, int month, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);

        if (month < 1 || month > 12)
            throw new ArgumentException("Month must be between 1 and 12.", nameof(month));

        var ruleName = "IsInMonth";
        var message = errorMessage ?? $"Value must be in month {month}";
        Register(selector, value => value.Month == month, ruleName, _ => message);
        return this;
    }
}
