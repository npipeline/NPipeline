namespace NPipeline.Connectors;

/// <summary>
///     Lightweight value object representing a storage URI scheme (e.g. "file", "s3").
///     - Case-insensitive, normalized to lowercase
///     - Open set: any valid scheme per RFC 3986 (alpha, followed by alpha|digit|+|-|.)
///     - Implicit conversions to/from string for ergonomics
///     - Provides well-known schemes via static properties
/// </summary>
public readonly record struct StorageScheme
{
    public StorageScheme(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new ArgumentException("Scheme cannot be null or whitespace.", nameof(value));

        var normalized = value.Trim().ToLowerInvariant();

        if (!IsValid(normalized))
        {
            throw new ArgumentException($"Invalid scheme '{value}'. A scheme must start with a letter and contain only letters, digits, '+', '-' or '.'.",
                nameof(value));
        }

        Value = normalized;
    }

    public string Value { get; }

    // Well-known schemes (non-exhaustive). These are conveniences only.
    public static StorageScheme File => new("file");

    public static bool TryParse(string? value, out StorageScheme scheme)
    {
        scheme = default;

        if (string.IsNullOrWhiteSpace(value))
            return false;

        var normalized = value.Trim().ToLowerInvariant();

        if (!IsValid(normalized))
            return false;

        scheme = new StorageScheme(normalized);
        return true;
    }

    public static bool IsValid(string value)
    {
        if (string.IsNullOrEmpty(value))
            return false;

        // First char: letter
        if (!char.IsLetter(value[0]))
            return false;

        // Subsequent: letter | digit | '+' | '-' | '.'
        for (var i = 1; i < value.Length; i++)
        {
            var ch = value[i];
            var ok = char.IsLetterOrDigit(ch) || ch is '+' or '-' or '.';

            if (!ok)
                return false;
        }

        return true;
    }

    public static implicit operator StorageScheme(string value)
    {
        return new StorageScheme(value);
    }

    public static implicit operator string(StorageScheme scheme)
    {
        return scheme.Value;
    }

    public override string ToString()
    {
        return Value ?? string.Empty;
    }
}
