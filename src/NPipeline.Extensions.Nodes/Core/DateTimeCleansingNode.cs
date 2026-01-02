using System.Linq.Expressions;

namespace NPipeline.Extensions.Nodes.Core;

/// <summary>
///     A cleansing node for DateTime properties that provides operations like truncation, UTC conversion, and timezone normalization.
/// </summary>
public sealed class DateTimeCleansingNode<T> : PropertyTransformationNode<T>
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="DateTimeCleansingNode{T}" /> class.
    /// </summary>
    public DateTimeCleansingNode()
    {
    }

    /// <summary>
    ///     Specifies the DateTimeKind for a DateTime property.
    /// </summary>
    public DateTimeCleansingNode<T> SpecifyKind(
        Expression<Func<T, DateTime>> selector,
        DateTimeKind kind)
    {
        ArgumentNullException.ThrowIfNull(selector);
        Register(selector, value => DateTime.SpecifyKind(value, kind));
        return this;
    }

    /// <summary>
    ///     Specifies the DateTimeKind for a nullable DateTime property.
    /// </summary>
    public DateTimeCleansingNode<T> SpecifyKind(
        Expression<Func<T, DateTime?>> selector,
        DateTimeKind kind)
    {
        ArgumentNullException.ThrowIfNull(selector);

        Register(selector, value => value.HasValue
            ? DateTime.SpecifyKind(value.Value, kind)
            : null);

        return this;
    }

    /// <summary>
    ///     Converts a DateTime to UTC. Assumes the value is in the specified source timezone.
    /// </summary>
    public DateTimeCleansingNode<T> ToUtc(
        Expression<Func<T, DateTime>> selector,
        TimeZoneInfo? sourceTimeZone = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var tz = sourceTimeZone ?? TimeZoneInfo.Local;

        Register(selector, value =>
        {
            if (value.Kind == DateTimeKind.Utc)
                return value;

            if (value.Kind == DateTimeKind.Local)
                return value.ToUniversalTime();

            // Unspecified - assume it's in the source timezone
            return TimeZoneInfo.ConvertTimeToUtc(value, tz);
        });

        return this;
    }

    /// <summary>
    ///     Converts a nullable DateTime to UTC.
    /// </summary>
    public DateTimeCleansingNode<T> ToUtc(
        Expression<Func<T, DateTime?>> selector,
        TimeZoneInfo? sourceTimeZone = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var tz = sourceTimeZone ?? TimeZoneInfo.Local;

        Register(selector, value =>
        {
            if (!value.HasValue)
                return null;

            var dt = value.Value;

            if (dt.Kind == DateTimeKind.Utc)
                return dt;

            if (dt.Kind == DateTimeKind.Local)
                return dt.ToUniversalTime();

            return TimeZoneInfo.ConvertTimeToUtc(dt, tz);
        });

        return this;
    }

    /// <summary>
    ///     Converts a UTC DateTime to local time.
    /// </summary>
    public DateTimeCleansingNode<T> ToLocal(Expression<Func<T, DateTime>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        Register(selector, value =>
        {
            if (value.Kind == DateTimeKind.Local)
                return value;

            if (value.Kind == DateTimeKind.Utc)
                return value.ToLocalTime();

            // Unspecified - assume it's UTC
            return DateTime.SpecifyKind(value, DateTimeKind.Utc).ToLocalTime();
        });

        return this;
    }

    /// <summary>
    ///     Converts a nullable UTC DateTime to local time.
    /// </summary>
    public DateTimeCleansingNode<T> ToLocal(Expression<Func<T, DateTime?>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        Register(selector, value =>
        {
            if (!value.HasValue)
                return null;

            var dt = value.Value;

            if (dt.Kind == DateTimeKind.Local)
                return dt;

            if (dt.Kind == DateTimeKind.Utc)
                return dt.ToLocalTime();

            return DateTime.SpecifyKind(dt, DateTimeKind.Utc).ToLocalTime();
        });

        return this;
    }

    /// <summary>
    ///     Removes the time component from a DateTime (sets to midnight).
    /// </summary>
    public DateTimeCleansingNode<T> StripTime(Expression<Func<T, DateTime>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        Register(selector, value => value.Date);
        return this;
    }

    /// <summary>
    ///     Removes the time component from a nullable DateTime.
    /// </summary>
    public DateTimeCleansingNode<T> StripTime(Expression<Func<T, DateTime?>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        Register(selector, value => value?.Date);
        return this;
    }

    /// <summary>
    ///     Truncates a DateTime to the specified precision (e.g., to nearest second).
    /// </summary>
    public DateTimeCleansingNode<T> Truncate(
        Expression<Func<T, DateTime>> selector,
        TimeSpan precision)
    {
        ArgumentNullException.ThrowIfNull(selector);

        if (precision <= TimeSpan.Zero)
            throw new ArgumentException("Precision must be greater than zero.", nameof(precision));

        Register(selector, value => value.AddTicks(-(value.Ticks % precision.Ticks)));
        return this;
    }

    /// <summary>
    ///     Truncates a nullable DateTime to the specified precision.
    /// </summary>
    public DateTimeCleansingNode<T> Truncate(
        Expression<Func<T, DateTime?>> selector,
        TimeSpan precision)
    {
        ArgumentNullException.ThrowIfNull(selector);

        if (precision <= TimeSpan.Zero)
            throw new ArgumentException("Precision must be greater than zero.", nameof(precision));

        Register(selector, value =>
            value.HasValue
                ? value.Value.AddTicks(-(value.Value.Ticks % precision.Ticks))
                : null);

        return this;
    }

    /// <summary>
    ///     Rounds a DateTime to the nearest minute.
    /// </summary>
    public DateTimeCleansingNode<T> RoundToMinute(Expression<Func<T, DateTime>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        Register(selector, value =>
        {
            var ticks = value.Ticks;
            var ticksInMinute = TimeSpan.FromMinutes(1).Ticks;
            var remainder = ticks % ticksInMinute;

            return remainder < ticksInMinute / 2
                ? value.AddTicks(-remainder)
                : value.AddTicks(ticksInMinute - remainder);
        });

        return this;
    }

    /// <summary>
    ///     Rounds a nullable DateTime to the nearest minute.
    /// </summary>
    public DateTimeCleansingNode<T> RoundToMinute(Expression<Func<T, DateTime?>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        Register(selector, value =>
        {
            if (!value.HasValue)
                return null;

            var ticks = value.Value.Ticks;
            var ticksInMinute = TimeSpan.FromMinutes(1).Ticks;
            var remainder = ticks % ticksInMinute;

            return remainder < ticksInMinute / 2
                ? value.Value.AddTicks(-remainder)
                : value.Value.AddTicks(ticksInMinute - remainder);
        });

        return this;
    }

    /// <summary>
    ///     Rounds a DateTime to the nearest hour.
    /// </summary>
    public DateTimeCleansingNode<T> RoundToHour(Expression<Func<T, DateTime>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        Register(selector, value =>
        {
            var ticks = value.Ticks;
            var ticksInHour = TimeSpan.FromHours(1).Ticks;
            var remainder = ticks % ticksInHour;

            return remainder < ticksInHour / 2
                ? value.AddTicks(-remainder)
                : value.AddTicks(ticksInHour - remainder);
        });

        return this;
    }

    /// <summary>
    ///     Rounds a nullable DateTime to the nearest hour.
    /// </summary>
    public DateTimeCleansingNode<T> RoundToHour(Expression<Func<T, DateTime?>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        Register(selector, value =>
        {
            if (!value.HasValue)
                return null;

            var ticks = value.Value.Ticks;
            var ticksInHour = TimeSpan.FromHours(1).Ticks;
            var remainder = ticks % ticksInHour;

            return remainder < ticksInHour / 2
                ? value.Value.AddTicks(-remainder)
                : value.Value.AddTicks(ticksInHour - remainder);
        });

        return this;
    }

    /// <summary>
    ///     Rounds a DateTime to the nearest day.
    /// </summary>
    public DateTimeCleansingNode<T> RoundToDay(Expression<Func<T, DateTime>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        Register(selector, value =>
        {
            var ticks = value.Ticks;
            var ticksInDay = TimeSpan.FromDays(1).Ticks;
            var remainder = ticks % ticksInDay;

            return remainder < ticksInDay / 2
                ? value.AddTicks(-remainder)
                : value.AddTicks(ticksInDay - remainder);
        });

        return this;
    }

    /// <summary>
    ///     Rounds a nullable DateTime to the nearest day.
    /// </summary>
    public DateTimeCleansingNode<T> RoundToDay(Expression<Func<T, DateTime?>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        Register(selector, value =>
        {
            if (!value.HasValue)
                return null;

            var ticks = value.Value.Ticks;
            var ticksInDay = TimeSpan.FromDays(1).Ticks;
            var remainder = ticks % ticksInDay;

            return remainder < ticksInDay / 2
                ? value.Value.AddTicks(-remainder)
                : value.Value.AddTicks(ticksInDay - remainder);
        });

        return this;
    }

    /// <summary>
    ///     Clamps a DateTime value between minimum and maximum bounds.
    /// </summary>
    public DateTimeCleansingNode<T> Clamp(
        Expression<Func<T, DateTime>> selector,
        DateTime min,
        DateTime max)
    {
        ArgumentNullException.ThrowIfNull(selector);

        if (min > max)
            throw new ArgumentException("Minimum value cannot be greater than maximum value.", nameof(min));

        Register(selector, value =>
        {
            if (value < min)
                return min;

            if (value > max)
                return max;

            return value;
        });

        return this;
    }

    /// <summary>
    ///     Clamps a nullable DateTime value between minimum and maximum bounds.
    /// </summary>
    public DateTimeCleansingNode<T> Clamp(
        Expression<Func<T, DateTime?>> selector,
        DateTime min,
        DateTime max)
    {
        ArgumentNullException.ThrowIfNull(selector);

        if (min > max)
            throw new ArgumentException("Minimum value cannot be greater than maximum value.", nameof(min));

        Register(selector, value =>
        {
            if (!value.HasValue)
                return null;

            if (value.Value < min)
                return min;

            if (value.Value > max)
                return max;

            return value.Value;
        });

        return this;
    }

    /// <summary>
    ///     Provides a default value if the property equals DateTime.MinValue.
    /// </summary>
    public DateTimeCleansingNode<T> DefaultIfMinValue(
        Expression<Func<T, DateTime>> selector,
        DateTime defaultValue)
    {
        ArgumentNullException.ThrowIfNull(selector);

        Register(selector, value => value == DateTime.MinValue
            ? defaultValue
            : value);

        return this;
    }

    /// <summary>
    ///     Provides a default value if the property equals DateTime.MaxValue.
    /// </summary>
    public DateTimeCleansingNode<T> DefaultIfMaxValue(
        Expression<Func<T, DateTime>> selector,
        DateTime defaultValue)
    {
        ArgumentNullException.ThrowIfNull(selector);

        Register(selector, value => value == DateTime.MaxValue
            ? defaultValue
            : value);

        return this;
    }

    /// <summary>
    ///     Provides a default value if a nullable DateTime is null.
    /// </summary>
    public DateTimeCleansingNode<T> DefaultIfNull(
        Expression<Func<T, DateTime?>> selector,
        DateTime defaultValue)
    {
        ArgumentNullException.ThrowIfNull(selector);
        Register(selector, value => value ?? defaultValue);
        return this;
    }
}
