using System.Linq.Expressions;
using System.Text.RegularExpressions;

namespace NPipeline.Extensions.Nodes.Core;

/// <summary>
/// A validation node for string properties that provides validators for common patterns and constraints.
/// </summary>
public sealed partial class StringValidationNode<T> : ValidationNode<T>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="StringValidationNode{T}" /> class.
    /// </summary>
    public StringValidationNode()
    {
    }

    /// <summary>
    /// Validates that a string is not null or empty.
    /// </summary>
    public StringValidationNode<T> IsNotEmpty(Expression<Func<T, string?>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsNotEmpty";
        var message = errorMessage ?? "Value must not be empty";
        Register(selector, value => !string.IsNullOrEmpty(value), ruleName, _ => message);
        return this;
    }

    /// <summary>
    /// Validates that a string is not null, empty, or whitespace-only.
    /// </summary>
    public StringValidationNode<T> IsNotWhitespace(Expression<Func<T, string?>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsNotWhitespace";
        var message = errorMessage ?? "Value must not be empty or whitespace";
        Register(selector, value => !string.IsNullOrWhiteSpace(value), ruleName, _ => message);
        return this;
    }

    /// <summary>
    /// Validates that a string has a minimum length.
    /// </summary>
    public StringValidationNode<T> HasMinLength(Expression<Func<T, string?>> selector, int minLength, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        if (minLength < 0)
            throw new ArgumentException("Minimum length cannot be negative.", nameof(minLength));

        var ruleName = "HasMinLength";
        var message = errorMessage ?? $"Value must be at least {minLength} characters";
        Register(selector, value => value == null || value.Length >= minLength, ruleName, _ => message);
        return this;
    }

    /// <summary>
    /// Validates that a string has a maximum length.
    /// </summary>
    public StringValidationNode<T> HasMaxLength(Expression<Func<T, string?>> selector, int maxLength, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        if (maxLength < 0)
            throw new ArgumentException("Maximum length cannot be negative.", nameof(maxLength));

        var ruleName = "HasMaxLength";
        var message = errorMessage ?? $"Value must not exceed {maxLength} characters";
        Register(selector, value => value == null || value.Length <= maxLength, ruleName, _ => message);
        return this;
    }

    /// <summary>
    /// Validates that a string length is within a range.
    /// </summary>
    public StringValidationNode<T> HasLengthBetween(
        Expression<Func<T, string?>> selector,
        int minLength,
        int maxLength,
        string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        if (minLength < 0)
            throw new ArgumentException("Minimum length cannot be negative.", nameof(minLength));
        if (maxLength < minLength)
            throw new ArgumentException("Maximum length must be greater than or equal to minimum length.", nameof(maxLength));

        var ruleName = "HasLengthBetween";
        var message = errorMessage ?? $"Value length must be between {minLength} and {maxLength} characters";
        Register(selector, value => value == null || (value.Length >= minLength && value.Length <= maxLength), ruleName, _ => message);
        return this;
    }

    /// <summary>
    /// Validates that a string matches an email format.
    /// </summary>
    public StringValidationNode<T> IsEmail(Expression<Func<T, string?>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsEmail";
        var message = errorMessage ?? "Value must be a valid email address";
        // Simple email validation - checks for @ and . with basic structure
        Register(selector, value => value == null || EmailRegex().IsMatch(value), ruleName, _ => message);
        return this;
    }

    /// <summary>
    /// Validates that a string is a valid URL (HTTP/HTTPS).
    /// </summary>
    public StringValidationNode<T> IsUrl(Expression<Func<T, string?>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsUrl";
        var message = errorMessage ?? "Value must be a valid HTTP/HTTPS URL";
        Register(selector, value =>
        {
            if (value == null)
                return true;
            return Uri.TryCreate(value, UriKind.Absolute, out var uri) &&
                   (uri.Scheme == Uri.UriSchemeHttp || uri.Scheme == Uri.UriSchemeHttps);
        }, ruleName, _ => message);
        return this;
    }

    /// <summary>
    /// Validates that a string is a valid GUID format.
    /// </summary>
    public StringValidationNode<T> IsGuid(Expression<Func<T, string?>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsGuid";
        var message = errorMessage ?? "Value must be a valid GUID";
        Register(selector, value => value == null || Guid.TryParse(value, out _), ruleName, _ => message);
        return this;
    }

    /// <summary>
    /// Validates that a string contains only alphanumeric characters.
    /// </summary>
    public StringValidationNode<T> IsAlphanumeric(Expression<Func<T, string?>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsAlphanumeric";
        var message = errorMessage ?? "Value must contain only letters and digits";
        Register(selector, value => value == null || value.All(char.IsLetterOrDigit), ruleName, _ => message);
        return this;
    }

    /// <summary>
    /// Validates that a string contains only alphabetic characters.
    /// </summary>
    public StringValidationNode<T> IsAlphabetic(Expression<Func<T, string?>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsAlphabetic";
        var message = errorMessage ?? "Value must contain only letters";
        Register(selector, value => value == null || value.All(char.IsLetter), ruleName, _ => message);
        return this;
    }

    /// <summary>
    /// Validates that a string contains only numeric digits.
    /// </summary>
    public StringValidationNode<T> IsDigitsOnly(Expression<Func<T, string?>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsDigitsOnly";
        var message = errorMessage ?? "Value must contain only digits";
        Register(selector, value => value == null || value.All(char.IsDigit), ruleName, _ => message);
        return this;
    }

    /// <summary>
    /// Validates that a string is in a valid numeric format.
    /// </summary>
    public StringValidationNode<T> IsNumeric(Expression<Func<T, string?>> selector, string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var ruleName = "IsNumeric";
        var message = errorMessage ?? "Value must be a valid number";
        Register(selector, value => value == null || double.TryParse(value, out _), ruleName, _ => message);
        return this;
    }

    /// <summary>
    /// Validates that a string matches a regex pattern.
    /// </summary>
    public StringValidationNode<T> Matches(
        Expression<Func<T, string?>> selector,
        string pattern,
        RegexOptions options = RegexOptions.None,
        string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(pattern);

        var ruleName = "Matches";
        var message = errorMessage ?? $"Value must match pattern {pattern}";
        // Note: User-provided pattern, compiled at runtime for flexibility
        var regex = new Regex(pattern, options | RegexOptions.Compiled, TimeSpan.FromSeconds(1));
        Register(selector, value => value == null || regex.IsMatch(value), ruleName, _ => message);
        return this;
    }

    /// <summary>
    /// Validates that a string contains a substring.
    /// </summary>
    public StringValidationNode<T> Contains(
        Expression<Func<T, string?>> selector,
        string substring,
        StringComparison comparison = StringComparison.Ordinal,
        string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(substring);

        var ruleName = "Contains";
        var message = errorMessage ?? $"Value must contain '{substring}'";
        Register(selector, value => value == null || value.Contains(substring, comparison), ruleName, _ => message);
        return this;
    }

    /// <summary>
    /// Validates that a string starts with a prefix.
    /// </summary>
    public StringValidationNode<T> StartsWith(
        Expression<Func<T, string?>> selector,
        string prefix,
        StringComparison comparison = StringComparison.Ordinal,
        string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(prefix);

        var ruleName = "StartsWith";
        var message = errorMessage ?? $"Value must start with '{prefix}'";
        Register(selector, value => value == null || value.StartsWith(prefix, comparison), ruleName, _ => message);
        return this;
    }

    /// <summary>
    /// Validates that a string ends with a suffix.
    /// </summary>
    public StringValidationNode<T> EndsWith(
        Expression<Func<T, string?>> selector,
        string suffix,
        StringComparison comparison = StringComparison.Ordinal,
        string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(suffix);

        var ruleName = "EndsWith";
        var message = errorMessage ?? $"Value must end with '{suffix}'";
        Register(selector, value => value == null || value.EndsWith(suffix, comparison), ruleName, _ => message);
        return this;
    }

    /// <summary>
    /// Validates that a string is in a list of allowed values.
    /// </summary>
    public StringValidationNode<T> IsInList(
        Expression<Func<T, string?>> selector,
        IEnumerable<string> allowedValues,
        StringComparison comparison = StringComparison.Ordinal,
        string? errorMessage = null)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(allowedValues);

        var allowedList = allowedValues.ToList();
        var ruleName = "IsInList";
        var message = errorMessage ?? $"Value must be one of: {string.Join(", ", allowedList)}";
        Register(selector, value => value == null || allowedList.Any(v => string.Equals(value, v, comparison)), ruleName, _ => message);
        return this;
    }

    // Generated regex patterns for compile-time safety
    [GeneratedRegex(@"^[^\s@]+@[^\s@]+\.[^\s@]+$", RegexOptions.CultureInvariant)]
    private static partial Regex EmailRegex();

    [GeneratedRegex(@"^https?://", RegexOptions.CultureInvariant | RegexOptions.IgnoreCase)]
    private static partial Regex UrlRegex();

    [GeneratedRegex(@"^[{]?[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}[}]?$", RegexOptions.CultureInvariant | RegexOptions.IgnoreCase)]
    private static partial Regex GuidRegex();

    [GeneratedRegex(@"^[a-zA-Z0-9]+$", RegexOptions.CultureInvariant)]
    private static partial Regex AlphanumericRegex();

    [GeneratedRegex(@"^[a-zA-Z]+$", RegexOptions.CultureInvariant)]
    private static partial Regex AlphabeticRegex();

    [GeneratedRegex(@"^[0-9]+$", RegexOptions.CultureInvariant)]
    private static partial Regex DigitsRegex();

    [GeneratedRegex(@"^-?(\d+\.?\d*|\.\d+)$", RegexOptions.CultureInvariant)]
    private static partial Regex NumericRegex();
}
