using System.Security.Cryptography;
using System.Text;

namespace NPipeline.StorageProviders.Sftp;

/// <summary>Compares a presented host key fingerprint with the configured ones.</summary>
internal static class HostKeyVerifier
{
    private const string Prefix = "SHA256:";

    /// <param name="presented">The SHA-256 fingerprint as SSH.NET reports it: unpadded base64, without a prefix.</param>
    /// <param name="trusted">The configured fingerprints, with or without the <c>SHA256:</c> prefix and padding.</param>
    public static bool IsTrusted(string? presented, IEnumerable<string> trusted)
    {
        if (string.IsNullOrEmpty(presented))
            return false;

        var presentedBytes = Encoding.ASCII.GetBytes(Normalize(presented));
        var match = false;

        // Base64 is case-sensitive, so the comparison is ordinal, and constant-time per candidate.
        foreach (var candidate in trusted)
        {
            if (string.IsNullOrWhiteSpace(candidate))
                continue;

            match |= CryptographicOperations.FixedTimeEquals(presentedBytes, Encoding.ASCII.GetBytes(Normalize(candidate)));
        }

        return match;
    }

    private static string Normalize(string fingerprint)
    {
        var value = fingerprint.Trim();

        if (value.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase))
            value = value[Prefix.Length..];

        return value.TrimEnd('=');
    }
}
