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
    /// <summary>
    ///     Initializes a new instance of the <see cref="StorageScheme" /> struct.
    /// </summary>
    /// <param name="value">The scheme string to initialize with.</param>
    /// <exception cref="ArgumentException">Thrown when the scheme is null, whitespace, or invalid.</exception>
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

    /// <summary>
    ///     Gets the normalized lowercase value of the storage scheme.
    /// </summary>
    public string Value { get; }

    // Well-known schemes (non-exhaustive). These are conveniences only.
    /// <summary>
    ///     Gets a <see cref="StorageScheme" /> instance representing the "file" scheme.
    /// </summary>
    public static StorageScheme File => new("file");

    /// <summary>
    ///     Gets a <see cref="StorageScheme" /> instance representing the "postgres" scheme.
    /// </summary>
    public static StorageScheme Postgres => new("postgres");

    /// <summary>
    ///     Gets a <see cref="StorageScheme" /> instance representing the "postgresql" scheme.
    /// </summary>
    public static StorageScheme Postgresql => new("postgresql");

    /// <summary>
    ///     Gets a <see cref="StorageScheme" /> instance representing the "mssql" scheme.
    /// </summary>
    public static StorageScheme Mssql => new("mssql");

    /// <summary>
    ///     Gets a <see cref="StorageScheme" /> instance representing the "sqlserver" scheme.
    /// </summary>
    public static StorageScheme SqlServer => new("sqlserver");

    /// <summary>
    ///     Gets a <see cref="StorageScheme" /> instance representing the "s3" scheme.
    /// </summary>
    public static StorageScheme S3 => new("s3");

    /// <summary>
    ///     Attempts to parse the specified string into a <see cref="StorageScheme" /> instance.
    /// </summary>
    /// <param name="value">The string to parse.</param>
    /// <param name="scheme">
    ///     When this method returns, contains the parsed <see cref="StorageScheme" /> if the parsing succeeded, or the default value if parsing
    ///     failed.
    /// </param>
    /// <returns>true if the parsing was successful; otherwise, false.</returns>
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

    /// <summary>
    ///     Determines whether the specified string is a valid storage scheme according to RFC 3986.
    /// </summary>
    /// <param name="value">The string to validate.</param>
    /// <returns>true if the string is a valid scheme; otherwise, false.</returns>
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

    /// <summary>
    ///     Implicitly converts a string to a <see cref="StorageScheme" /> instance.
    /// </summary>
    /// <param name="value">The string to convert.</param>
    /// <returns>A new <see cref="StorageScheme" /> instance representing the specified string.</returns>
    public static implicit operator StorageScheme(string value)
    {
        return new StorageScheme(value);
    }

    /// <summary>
    ///     Implicitly converts a <see cref="StorageScheme" /> instance to a string.
    /// </summary>
    /// <param name="scheme">The <see cref="StorageScheme" /> to convert.</param>
    /// <returns>The string representation of the <see cref="StorageScheme" />.</returns>
    public static implicit operator string(StorageScheme scheme)
    {
        return scheme.Value;
    }

    /// <summary>
    ///     Returns the string representation of the <see cref="StorageScheme" />.
    /// </summary>
    /// <returns>The normalized scheme value, or an empty string if the value is null.</returns>
    public override string ToString()
    {
        return Value ?? string.Empty;
    }
}
