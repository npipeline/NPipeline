using System.Diagnostics.CodeAnalysis;

namespace NPipeline.Connectors;

/// <summary>
///     Represents a normalized storage location URI used by storage providers and data format connectors.
///     Supports both absolute URIs (e.g., "s3://bucket/key?region=ap-southeast-2") and local file paths
///     (e.g., "C:\path\to\file.csv" or "./relative/file.csv") via <see cref="FromFilePath(string)" /> or
///     <see cref="Parse(string)" /> fallback.
/// </summary>
public sealed record StorageUri
{
    private static readonly StringComparer KeyComparer = StringComparer.OrdinalIgnoreCase;

    [SetsRequiredMembers]
    private StorageUri(StorageScheme scheme, string? host, string path, IReadOnlyDictionary<string, string>? parameters = null)
    {
        Scheme = scheme;

        Host = string.IsNullOrWhiteSpace(host)
            ? null
            : host;

        Path = NormalizePath(path);

        Parameters = parameters is null
            ? new Dictionary<string, string>(KeyComparer)
            : new Dictionary<string, string>(parameters, KeyComparer);
    }

    /// <summary>The scheme identifying the storage system (e.g., "file", "s3", "azure").</summary>
    public required StorageScheme Scheme { get; init; }

    /// <summary>Optional authority/host component (e.g., bucket or container name).</summary>
    public string? Host { get; init; }

    /// <summary>Normalized, absolute-style path beginning with '/'.</summary>
    public required string Path { get; init; }

    /// <summary>Query parameters providing additional configuration.</summary>
    public IReadOnlyDictionary<string, string> Parameters { get; init; } =
        new Dictionary<string, string>(KeyComparer);

    /// <summary>
    ///     Parses a text representation into a <see cref="StorageUri" /> instance. Supports absolute URIs and local file paths.
    /// </summary>
    /// <exception cref="FormatException">Thrown when the text cannot be parsed into a valid StorageUri.</exception>
    public static StorageUri Parse(string text)
    {
        if (!TryParse(text, out var result, out var error))
            throw new FormatException($"Invalid storage URI. {error}");

        return result!;
    }

    /// <summary>Attempts to parse a <see cref="StorageUri" /> from the provided text.</summary>
    public static bool TryParse(string? text, out StorageUri? result, out string? error)
    {
        result = null;
        error = null;

        if (string.IsNullOrWhiteSpace(text))
        {
            error = "Value is null or whitespace.";
            return false;
        }

        // First, try standard absolute URI parsing
        if (Uri.TryCreate(text, UriKind.Absolute, out var uri) && !string.IsNullOrEmpty(uri.Scheme))
        {
            var scheme = new StorageScheme(uri.Scheme);

            var host = string.IsNullOrWhiteSpace(uri.Host)
                ? null
                : uri.Host;

            var path = NormalizePath(uri.AbsolutePath);
            var parameters = ParseQuery(uri.Query);

            result = new StorageUri(scheme, host, path, parameters);
            return true;
        }

        // Fallback: treat as file path (absolute or relative)
        try
        {
            result = FromFilePath(text);
            return true;
        }
        catch (Exception ex)
        {
            error = $"Failed to parse as absolute URI or file path. {ex.Message}";
            return false;
        }
    }

    /// <summary>
    ///     Constructs a <see cref="StorageUri" /> for the local file system from a file path (absolute or relative).
    /// </summary>
    public static StorageUri FromFilePath(string filePath)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("File path cannot be null or whitespace.", nameof(filePath));

        var full = System.IO.Path.GetFullPath(filePath);

        // Normalize Windows drive letters and separators into a leading-slash path
        var normalized = NormalizePath(full);

        // For UNC paths (\\server\share\path), GetFullPath preserves backslashes.
        // NormalizePath will convert to "/server/share/path". No host distinction is required for file scheme.
        return new StorageUri(StorageScheme.File, null, normalized);
    }

    /// <summary>Returns a URI-like string representation including query parameters.</summary>
    public override string ToString()
    {
        var authority = Host is { Length: > 0 }
            ? $"//{Host}"
            : "//";

        var query = SerializeQuery(Parameters);
        return $"{Scheme}:{authority}{Path}{query}";
    }

    private static string NormalizePath(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return "/";

        // Convert backslashes to forward slashes for consistency
        var p = path.Replace('\\', '/');

        // If Windows drive-rooted like "C:/folder/file", ensure leading slash
        if (p.Length >= 2 && char.IsLetter(p[0]) && p[1] == ':')
            p = "/" + p;

        // Ensure single leading slash
        if (!p.StartsWith("/", StringComparison.Ordinal))
            p = "/" + p;

        // Collapse multiple slashes (path context only)
        while (p.Contains("//", StringComparison.Ordinal))
        {
            p = p.Replace("//", "/", StringComparison.Ordinal);
        }

        return p;
    }

    private static IReadOnlyDictionary<string, string> ParseQuery(string query)
    {
        var dict = new Dictionary<string, string>(KeyComparer);

        if (string.IsNullOrEmpty(query))
            return dict;

        var q = query[0] == '?'
            ? query[1..]
            : query;

        if (q.Length == 0)
            return dict;

        var pairs = q.Split('&', StringSplitOptions.RemoveEmptyEntries);

        foreach (var pair in pairs)
        {
            var idx = pair.IndexOf('=', StringComparison.Ordinal);

            if (idx < 0)
            {
                var k = Uri.UnescapeDataString(pair);
                dict[k] = string.Empty;
            }
            else
            {
                var key = Uri.UnescapeDataString(pair[..idx]);
                var val = Uri.UnescapeDataString(pair[(idx + 1)..]);
                dict[key] = val;
            }
        }

        return dict;
    }

    private static string SerializeQuery(IReadOnlyDictionary<string, string> parameters)
    {
        if (parameters.Count == 0)
            return string.Empty;

        var parts = new List<string>(parameters.Count);

        foreach (var kvp in parameters)
        {
            var k = Uri.EscapeDataString(kvp.Key);
            var v = Uri.EscapeDataString(kvp.Value);
            parts.Add($"{k}={v}");
        }

        return parts.Count > 0
            ? "?" + string.Join("&", parts)
            : string.Empty;
    }
}
