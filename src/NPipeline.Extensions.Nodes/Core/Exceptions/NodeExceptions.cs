using NPipeline.ErrorHandling;

namespace NPipeline.Extensions.Nodes.Core.Exceptions;

/// <summary>
///     Exception thrown when validation fails in a validation node.
///     Contains property path, rule name, and the value that failed validation.
/// </summary>
public sealed class ValidationException : Exception
{
    /// <summary>
    ///     Gets the path to the property that failed validation (e.g., "Customer.Email").
    /// </summary>
    public string PropertyPath { get; }

    /// <summary>
    ///     Gets the name of the validation rule that failed.
    /// </summary>
    public string RuleName { get; }

    /// <summary>
    ///     Gets the actual value that failed validation.
    /// </summary>
    public object? Value { get; }

    /// <summary>
    ///     Initializes a new instance of the <see cref="ValidationException"/> class.
    /// </summary>
    /// <param name="propertyPath">The path to the property that failed validation.</param>
    /// <param name="ruleName">The name of the validation rule that failed.</param>
    /// <param name="value">The actual value that failed validation.</param>
    /// <param name="message">A descriptive error message.</param>
    public ValidationException(string propertyPath, string ruleName, object? value, string message)
        : base(message)
    {
        PropertyPath = propertyPath;
        RuleName = ruleName;
        Value = value;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="ValidationException"/> class with inner exception.
    /// </summary>
    public ValidationException(string propertyPath, string ruleName, object? value, string message, Exception innerException)
        : base(message, innerException)
    {
        PropertyPath = propertyPath;
        RuleName = ruleName;
        Value = value;
    }
}

/// <summary>
///     Exception thrown when filtering rejects an item.
///     Contains the reason the item was filtered out.
/// </summary>
public sealed class FilteringException : Exception
{
    /// <summary>
    ///     Gets the reason why the item was filtered out.
    /// </summary>
    public string Reason { get; }

    /// <summary>
    ///     Initializes a new instance of the <see cref="FilteringException"/> class.
    /// </summary>
    /// <param name="reason">The reason why the item was filtered out.</param>
    public FilteringException(string reason)
        : base(reason)
    {
        Reason = reason;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="FilteringException"/> class with inner exception.
    /// </summary>
    public FilteringException(string reason, Exception innerException)
        : base(reason, innerException)
    {
        Reason = reason;
    }
}

/// <summary>
///     Exception thrown when type conversion fails.
///     Contains the source and target types and the value that couldn't be converted.
/// </summary>
public sealed class TypeConversionException : Exception
{
    /// <summary>
    ///     Gets the source type that was being converted from.
    /// </summary>
    public Type SourceType { get; }

    /// <summary>
    ///     Gets the target type that conversion was attempted to.
    /// </summary>
    public Type TargetType { get; }

    /// <summary>
    ///     Gets the value that failed conversion.
    /// </summary>
    public object? Value { get; }

    /// <summary>
    ///     Initializes a new instance of the <see cref="TypeConversionException"/> class.
    /// </summary>
    /// <param name="sourceType">The source type being converted from.</param>
    /// <param name="targetType">The target type being converted to.</param>
    /// <param name="value">The value that failed conversion.</param>
    /// <param name="message">A descriptive error message.</param>
    public TypeConversionException(Type sourceType, Type targetType, object? value, string message)
        : base(message)
    {
        SourceType = sourceType;
        TargetType = targetType;
        Value = value;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="TypeConversionException"/> class with inner exception.
    /// </summary>
    public TypeConversionException(Type sourceType, Type targetType, object? value, string message, Exception innerException)
        : base(message, innerException)
    {
        SourceType = sourceType;
        TargetType = targetType;
        Value = value;
    }
}
