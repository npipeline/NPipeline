using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace NPipeline.StorageProviders.Models;

/// <summary>
///     An immutable storage location: <c>scheme://[user[:password]@]host[:port]/path[?query]</c>, or a local file path.
/// </summary>
/// <remarks>
///     <para>
///         Percent-escapes in the path, user information and query are decoded exactly once, when the text is parsed.
///         <see cref="Path" /> is the literal object key or file path: <c>#</c>, <c>..</c> and <c>//</c> are ordinary
///         characters and are never rewritten. Text that does not start with <c>scheme://</c> (for example
///         <c>C:\data\x.csv</c>, <c>./x.csv</c> or <c>/tmp/x.csv</c>) is a local file path.
///     </para>
///     <para>
///         Instances have value semantics. <see cref="ToString" /> is canonical and redacts the password and secret
///         parameters (see <see cref="SecretParameterNames" />), so it is safe to log. <see cref="ToUnredactedString" />
///         keeps them, and <c>Parse(uri.ToUnredactedString())</c> returns an equal URI.
///     </para>
/// </remarks>
public sealed class StorageUri : IEquatable<StorageUri>
{
    private const string Redacted = "***";

    private static readonly FrozenDictionary<string, string> NoParameters =
        FrozenDictionary<string, string>.Empty.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);

    /// <summary>Parameter names whose values are replaced by <c>***</c> in <see cref="ToString" />. Matching ignores case.</summary>
    public static IReadOnlySet<string> SecretParameterNames { get; } = new[]
    {
        "password", "pwd", "secretKey", "sessionToken", "sasToken", "accountKey", "connectionString", "accessToken", "keyPassphrase",
        "accessKey", "key", "token", "apiKey", "credentialsPath",
    }.ToFrozenSet(StringComparer.OrdinalIgnoreCase);

    private StorageUri(
        StorageScheme scheme,
        string? host,
        int? port,
        string? userName,
        string? password,
        string path,
        FrozenDictionary<string, string> parameters)
    {
        Scheme = scheme;
        Host = string.IsNullOrWhiteSpace(host) ? null : host.ToLowerInvariant();
        Port = port;
        UserName = string.IsNullOrEmpty(userName) ? null : userName;
        Password = string.IsNullOrEmpty(password) ? null : password;
        Path = NormalizePath(path);
        Parameters = parameters;
    }

    /// <summary>The scheme identifying the storage system (for example <c>file</c>, <c>s3</c>, <c>azure</c>).</summary>
    public StorageScheme Scheme { get; }

    /// <summary>The host or authority (for example a bucket or container name), lower-cased. <see langword="null" /> for local files.</summary>
    public string? Host { get; }

    /// <summary>The explicit port, or <see langword="null" /> when none was given.</summary>
    public int? Port { get; }

    /// <summary>The decoded user name, or <see langword="null" />.</summary>
    public string? UserName { get; }

    /// <summary>The decoded password, or <see langword="null" />. <see cref="ToString" /> never prints it.</summary>
    public string? Password { get; }

    /// <summary>The decoded path. It always starts with <c>/</c> and is never rewritten.</summary>
    public string Path { get; }

    /// <summary>The decoded query parameters. The dictionary is immutable and its keys ignore case.</summary>
    public IReadOnlyDictionary<string, string> Parameters { get; }

    /// <summary><see langword="true" /> when <see cref="Path" /> ends with <c>/</c>.</summary>
    public bool IsDirectory => Path[^1] == '/';

    /// <summary>The last path segment, without a trailing <c>/</c>. Empty for the root.</summary>
    public string Name
    {
        get
        {
            var trimmed = Path.AsSpan().TrimEnd('/');
            return trimmed.Slice(trimmed.LastIndexOf('/') + 1).ToString();
        }
    }

    /// <summary>The containing directory (its path ends with <c>/</c>), or <see langword="null" /> for the root.</summary>
    public StorageUri? Parent
    {
        get
        {
            var trimmed = Path.AsSpan().TrimEnd('/');

            if (trimmed.IsEmpty)
                return null;

            return WithPath(trimmed[..(trimmed.LastIndexOf('/') + 1)].ToString());
        }
    }

    private FrozenDictionary<string, string> ParameterMap => (FrozenDictionary<string, string>)Parameters;

    /// <summary>Parses absolute URIs and local file paths.</summary>
    /// <exception cref="FormatException">The text cannot be parsed.</exception>
    public static StorageUri Parse(string text)
    {
        if (!TryParse(text, out var result, out var error))
            throw new FormatException($"Invalid storage URI. {error}");

        return result;
    }

    /// <summary>Attempts to parse <paramref name="text" />.</summary>
    public static bool TryParse(string? text, [NotNullWhen(true)] out StorageUri? uri) => TryParse(text, out uri, out _);

    /// <summary>Attempts to parse <paramref name="text" />, reporting why it failed.</summary>
    public static bool TryParse(string? text, [NotNullWhen(true)] out StorageUri? uri, [NotNullWhen(false)] out string? error)
    {
        uri = null;
        error = null;

        if (string.IsNullOrWhiteSpace(text))
        {
            error = "Value is null or whitespace.";
            return false;
        }

        text = text.Trim();
        var separator = text.IndexOf("://", StringComparison.Ordinal);

        // A one-letter "scheme" is a Windows drive ("C://x"), not a storage scheme.
        if (separator < 2 || !StorageScheme.TryParse(text[..separator], out var scheme))
            return TryFromFilePath(text, out uri, out error);

        var rest = text.AsSpan(separator + 3);
        var queryStart = rest.IndexOf('?');
        var query = queryStart < 0 ? default : rest[(queryStart + 1)..];
        var beforeQuery = queryStart < 0 ? rest : rest[..queryStart];

        var pathStart = beforeQuery.IndexOf('/');
        var authority = pathStart < 0 ? beforeQuery : beforeQuery[..pathStart];
        var path = pathStart < 0 ? "/" : Unescape(beforeQuery[pathStart..]);

        string? userName = null, password = null;
        var at = authority.LastIndexOf('@');

        if (at >= 0)
        {
            var userInfo = authority[..at];
            authority = authority[(at + 1)..];
            var colon = userInfo.IndexOf(':');
            userName = Unescape(colon < 0 ? userInfo : userInfo[..colon]);
            password = colon < 0 ? null : Unescape(userInfo[(colon + 1)..]);
        }

        if (!TrySplitHostAndPort(authority, out var host, out var port, out error))
            return false;

        uri = new StorageUri(scheme, host, port, userName, password, path, ParseQuery(query));
        return true;
    }

    /// <summary>Creates a <see cref="StorageScheme.File" /> URI from a file path, absolute or relative to the working directory.</summary>
    public static StorageUri FromFilePath(string filePath)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("File path cannot be null or whitespace.", nameof(filePath));

        var full = System.IO.Path.GetFullPath(filePath);
        string? host = null;

        if (System.IO.Path.DirectorySeparatorChar == '\\')
        {
            full = full.Replace('\\', '/');

            if (full.StartsWith("//", StringComparison.Ordinal))
            {
                // \\server\share\x is the UNC path of host "server".
                var hostEnd = full.IndexOf('/', 2);
                host = hostEnd < 0 ? full[2..] : full[2..hostEnd];
                full = hostEnd < 0 ? "/" : full[hostEnd..];
            }
        }

        return new StorageUri(StorageScheme.File, host, null, null, null, full, NoParameters);
    }

    /// <summary>Returns a copy with a different path. A missing leading <c>/</c> is added; nothing else is rewritten.</summary>
    public StorageUri WithPath(string path)
    {
        ArgumentNullException.ThrowIfNull(path);
        return new StorageUri(Scheme, Host, Port, UserName, Password, path, ParameterMap);
    }

    /// <summary>Returns a copy with <paramref name="key" /> set to <paramref name="value" />.</summary>
    public StorageUri WithParameter(string key, string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);

        var updated = new Dictionary<string, string>(ParameterMap, StringComparer.OrdinalIgnoreCase)
        {
            [key] = value ?? string.Empty,
        };

        return new StorageUri(Scheme, Host, Port, UserName, Password, Path, Freeze(updated));
    }

    /// <summary>Returns a copy without the parameter <paramref name="key" />.</summary>
    public StorageUri WithoutParameter(string key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);

        if (!ParameterMap.ContainsKey(key))
            return this;

        var updated = new Dictionary<string, string>(ParameterMap, StringComparer.OrdinalIgnoreCase);
        updated.Remove(key);
        return new StorageUri(Scheme, Host, Port, UserName, Password, Path, Freeze(updated));
    }

    /// <summary>Appends <paramref name="relativePath" /> to <see cref="Path" />, with exactly one <c>/</c> between them.</summary>
    public StorageUri Combine(string relativePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(relativePath);
        return WithPath($"{Path.TrimEnd('/')}/{relativePath.TrimStart('/')}");
    }

    /// <summary>The canonical, percent-encoded text with the password and every secret parameter replaced by <c>***</c>.</summary>
    public override string ToString() => Format(true);

    /// <summary>The canonical, percent-encoded text including the password and secret parameters. Do not log it.</summary>
    public string ToUnredactedString() => Format(false);

    /// <inheritdoc />
    public bool Equals(StorageUri? other)
    {
        if (other is null)
            return false;

        if (ReferenceEquals(this, other))
            return true;

        if (Scheme != other.Scheme
            || !string.Equals(Host, other.Host, StringComparison.OrdinalIgnoreCase)
            || Port != other.Port
            || !string.Equals(UserName, other.UserName, StringComparison.Ordinal)
            || !string.Equals(Password, other.Password, StringComparison.Ordinal)
            || !string.Equals(Path, other.Path, StringComparison.Ordinal)
            || ParameterMap.Count != other.ParameterMap.Count)
        {
            return false;
        }

        foreach (var (key, value) in ParameterMap)
        {
            if (!other.ParameterMap.TryGetValue(key, out var otherValue) || !string.Equals(value, otherValue, StringComparison.Ordinal))
                return false;
        }

        return true;
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as StorageUri);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        // Parameter order is irrelevant, so fold the pairs with a commutative operation.
        var parameters = 0;

        foreach (var (key, value) in ParameterMap)
        {
            parameters ^= HashCode.Combine(StringComparer.OrdinalIgnoreCase.GetHashCode(key), value);
        }

        var hash = new HashCode();
        hash.Add(Scheme);
        hash.Add(Host, StringComparer.OrdinalIgnoreCase);
        hash.Add(Port);
        hash.Add(UserName, StringComparer.Ordinal);
        hash.Add(Password, StringComparer.Ordinal);
        hash.Add(Path, StringComparer.Ordinal);
        hash.Add(parameters);
        return hash.ToHashCode();
    }

    /// <summary>Equality operator.</summary>
    public static bool operator ==(StorageUri? left, StorageUri? right) => left?.Equals(right) ?? right is null;

    /// <summary>Inequality operator.</summary>
    public static bool operator !=(StorageUri? left, StorageUri? right) => !(left == right);

    private static bool TryFromFilePath(string text, out StorageUri? uri, out string? error)
    {
        uri = null;
        error = null;

        try
        {
            uri = FromFilePath(text);
            return true;
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException or PathTooLongException)
        {
            error = $"Failed to parse as an absolute URI or a file path. {ex.Message}";
            return false;
        }
    }

    private static bool TrySplitHostAndPort(ReadOnlySpan<char> authority, out string? host, out int? port, [NotNullWhen(false)] out string? error)
    {
        host = null;
        port = null;
        error = null;

        // "[::1]:8080": the port follows the closing bracket. Otherwise it follows the last colon.
        var portSeparator = authority.Length > 0 && authority[0] == '['
            ? authority.IndexOf("]:".AsSpan()) is var close and >= 0 ? close + 1 : -1
            : authority.LastIndexOf(':');

        var hostPart = portSeparator < 0 ? authority : authority[..portSeparator];
        host = hostPart.IsEmpty ? null : hostPart.ToString();

        if (portSeparator < 0 || portSeparator == authority.Length - 1)
            return true;

        if (!ushort.TryParse(authority[(portSeparator + 1)..], out var parsed))
        {
            error = $"'{authority[(portSeparator + 1)..]}' is not a valid port.";
            return false;
        }

        port = parsed;
        return true;
    }

    private static FrozenDictionary<string, string> ParseQuery(ReadOnlySpan<char> query)
    {
        if (query.IsEmpty)
            return NoParameters;

        var parameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        while (!query.IsEmpty)
        {
            var end = query.IndexOf('&');
            var pair = end < 0 ? query : query[..end];
            query = end < 0 ? default : query[(end + 1)..];

            if (pair.IsEmpty)
                continue;

            var equals = pair.IndexOf('=');

            if (equals < 0)
                parameters[Unescape(pair)] = string.Empty;
            else
                parameters[Unescape(pair[..equals])] = Unescape(pair[(equals + 1)..]);
        }

        return Freeze(parameters);
    }

    private static FrozenDictionary<string, string> Freeze(Dictionary<string, string> parameters) =>
        parameters.Count == 0 ? NoParameters : parameters.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);

    private static string Unescape(ReadOnlySpan<char> value) =>
        value.Contains('%') ? Uri.UnescapeDataString(value.ToString()) : value.ToString();

    private static string NormalizePath(string path)
    {
        if (path.Length == 0)
            return "/";

        return path[0] == '/' ? path : "/" + path;
    }

    private string Format(bool redact)
    {
        var builder = new StringBuilder(Scheme.Value).Append("://");

        if (UserName is not null || Password is not null)
        {
            Escape(builder, UserName ?? string.Empty, UserInfoUnreserved);

            if (Password is not null)
            {
                builder.Append(':');

                if (redact)
                    builder.Append(Redacted);
                else
                    Escape(builder, Password, UserInfoUnreserved);
            }

            builder.Append('@');
        }

        builder.Append(Host);

        if (Port is { } port)
            builder.Append(':').Append(port);

        Escape(builder, Path, PathUnreserved);

        var separator = '?';

        foreach (var (key, value) in ParameterMap)
        {
            builder.Append(separator);
            separator = '&';
            Escape(builder, key, QueryUnreserved);
            builder.Append('=');

            if (redact && SecretParameterNames.Contains(key))
                builder.Append(Redacted);
            else
                Escape(builder, value, QueryUnreserved);
        }

        return builder.ToString();
    }

    private const string UserInfoUnreserved = "-._~!$&'()*+,;=";
    private const string PathUnreserved = "-._~!$&'()*+,;=:@/";
    private const string QueryUnreserved = "-._~!$'()*,;:@/";

    // Escapes ASCII outside [A-Za-z0-9] + allowed, and control characters. Non-ASCII text stays readable and round-trips.
    private static void Escape(StringBuilder builder, string value, string allowed)
    {
        foreach (var ch in value)
        {
            if (ch >= 0x80 || char.IsAsciiLetterOrDigit(ch) || allowed.Contains(ch))
            {
                builder.Append(ch);
                continue;
            }

            builder.Append('%').Append(((int)ch).ToString("X2", System.Globalization.CultureInfo.InvariantCulture));
        }
    }
}
