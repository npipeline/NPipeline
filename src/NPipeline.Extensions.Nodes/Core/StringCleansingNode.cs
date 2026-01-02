using System.Globalization;
using System.Linq.Expressions;
using System.Text;

namespace NPipeline.Extensions.Nodes.Core;

/// <summary>
///     A cleansing node for string properties that provides operations for normalization and transformation.
/// </summary>
public sealed class StringCleansingNode<T> : PropertyTransformationNode<T>
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="StringCleansingNode{T}" /> class.
    /// </summary>
    public StringCleansingNode()
    {
    }

    /// <summary>
    ///     Removes leading and trailing whitespace from a string.
    /// </summary>
    public StringCleansingNode<T> Trim(Expression<Func<T, string?>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        Register(selector, value => value?.Trim());
        return this;
    }

    /// <summary>
    ///     Removes leading whitespace from a string.
    /// </summary>
    public StringCleansingNode<T> TrimStart(Expression<Func<T, string?>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        Register(selector, value => value?.TrimStart());
        return this;
    }

    /// <summary>
    ///     Removes trailing whitespace from a string.
    /// </summary>
    public StringCleansingNode<T> TrimEnd(Expression<Func<T, string?>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        Register(selector, value => value?.TrimEnd());
        return this;
    }

    /// <summary>
    ///     Collapses multiple consecutive whitespace characters into a single space, and trims the result.
    /// </summary>
    public StringCleansingNode<T> CollapseWhitespace(Expression<Func<T, string?>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        Register(selector, value =>
        {
            if (string.IsNullOrWhiteSpace(value))
                return value;

            var sb = new StringBuilder();
            var inWhitespace = false;

            foreach (var ch in value)
            {
                if (char.IsWhiteSpace(ch))
                {
                    if (!inWhitespace)
                    {
                        sb.Append(' ');
                        inWhitespace = true;
                    }
                }
                else
                {
                    sb.Append(ch);
                    inWhitespace = false;
                }
            }

            return sb.ToString().Trim();
        });

        return this;
    }

    /// <summary>
    ///     Removes all whitespace characters from a string.
    /// </summary>
    public StringCleansingNode<T> RemoveWhitespace(Expression<Func<T, string?>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        Register(selector, value => value == null
            ? null
            : new string(value.Where(c => !char.IsWhiteSpace(c)).ToArray()));

        return this;
    }

    /// <summary>
    ///     Converts a string to lowercase using the invariant culture.
    /// </summary>
    public StringCleansingNode<T> ToLower(Expression<Func<T, string?>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        Register(selector, value => value?.ToLowerInvariant());
        return this;
    }

    /// <summary>
    ///     Converts a string to uppercase using the invariant culture.
    /// </summary>
    public StringCleansingNode<T> ToUpper(Expression<Func<T, string?>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        Register(selector, value => value?.ToUpperInvariant());
        return this;
    }

    /// <summary>
    ///     Converts a string to title case using the invariant culture.
    /// </summary>
    public StringCleansingNode<T> ToTitleCase(Expression<Func<T, string?>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        Register(selector, value => value == null
            ? null
            : CultureInfo.InvariantCulture.TextInfo.ToTitleCase(value.ToLowerInvariant()));

        return this;
    }

    /// <summary>
    ///     Removes all non-alphanumeric characters from a string.
    /// </summary>
    public StringCleansingNode<T> RemoveSpecialCharacters(Expression<Func<T, string?>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        Register(selector, value => value == null
            ? null
            : new string(value.Where(char.IsLetterOrDigit).ToArray()));

        return this;
    }

    /// <summary>
    ///     Removes all numeric characters from a string.
    /// </summary>
    public StringCleansingNode<T> RemoveDigits(Expression<Func<T, string?>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        Register(selector, value => value == null
            ? null
            : new string(value.Where(c => !char.IsDigit(c)).ToArray()));

        return this;
    }

    /// <summary>
    ///     Removes all non-ASCII characters from a string.
    /// </summary>
    public StringCleansingNode<T> RemoveNonAscii(Expression<Func<T, string?>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        Register(selector, value => value == null
            ? null
            : new string(value.Where(c => c < 128).ToArray()));

        return this;
    }

    /// <summary>
    ///     Truncates a string to a maximum length.
    /// </summary>
    public StringCleansingNode<T> Truncate(Expression<Func<T, string?>> selector, int maxLength)
    {
        ArgumentNullException.ThrowIfNull(selector);

        if (maxLength < 0)
            throw new ArgumentException("Maximum length cannot be negative.", nameof(maxLength));

        Register(selector, value => value == null
            ? null
            : value.Length > maxLength
                ? value.Substring(0, maxLength)
                : value);

        return this;
    }

    /// <summary>
    ///     Adds a prefix to a string if it doesn't already start with it.
    /// </summary>
    public StringCleansingNode<T> EnsurePrefix(Expression<Func<T, string?>> selector, string prefix)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(prefix);

        Register(selector, value => value == null
            ? null
            : value.StartsWith(prefix, StringComparison.Ordinal)
                ? value
                : prefix + value);

        return this;
    }

    /// <summary>
    ///     Adds a suffix to a string if it doesn't already end with it.
    /// </summary>
    public StringCleansingNode<T> EnsureSuffix(Expression<Func<T, string?>> selector, string suffix)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(suffix);

        Register(selector, value => value == null
            ? null
            : value.EndsWith(suffix, StringComparison.Ordinal)
                ? value
                : value + suffix);

        return this;
    }

    /// <summary>
    ///     Replaces all occurrences of a substring with another substring.
    /// </summary>
    public StringCleansingNode<T> Replace(Expression<Func<T, string?>> selector, string oldValue, string newValue)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(oldValue);

        Register(selector, value => value?.Replace(oldValue, newValue));
        return this;
    }

    /// <summary>
    ///     Provides a default value if a string is null or whitespace.
    /// </summary>
    public StringCleansingNode<T> DefaultIfNullOrWhitespace(Expression<Func<T, string?>> selector, string defaultValue)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(defaultValue);

        Register(selector, value => string.IsNullOrWhiteSpace(value)
            ? defaultValue
            : value);

        return this;
    }

    /// <summary>
    ///     Provides a default value if a string is null or empty.
    /// </summary>
    public StringCleansingNode<T> DefaultIfNullOrEmpty(Expression<Func<T, string?>> selector, string defaultValue)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(defaultValue);

        Register(selector, value => string.IsNullOrEmpty(value)
            ? defaultValue
            : value);

        return this;
    }

    /// <summary>
    ///     Returns null if a string is whitespace-only.
    /// </summary>
    public StringCleansingNode<T> NullIfWhitespace(Expression<Func<T, string?>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        Register(selector, value => string.IsNullOrWhiteSpace(value)
            ? null
            : value);

        return this;
    }
}
