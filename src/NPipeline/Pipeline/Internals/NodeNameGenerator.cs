using System.Text.RegularExpressions;
using NPipeline.Graph;

namespace NPipeline.Pipeline.Internals;

/// <summary>
///     Internal utility for generating and validating node names / IDs.
///     Pure (no side-effects) except for throwing on invalid duplicate names when requested.
/// </summary>
/// <remarks>
///     Invariants:
///     - Sanitization is lowercase kebab-case limited to a-z0-9 and '-'.
///     - ID generation appends an incrementing suffix preserving first sanitized form.
///     - All methods are allocation-conscious (regex compiled via source generator).
/// </remarks>
internal static partial class NodeNameGenerator
{
    [GeneratedRegex("[^A-Za-z0-9]+")]
    private static partial Regex SanitizeNodeNameRegex();

    /// <summary>Sanitize a user provided node name into a safe identifier fragment (preserves case when possible).</summary>
    public static string Sanitize(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            return "node";

        var sanitized = SanitizeNodeNameRegex().Replace(name.Trim(), "-").Trim('-');

        if (string.IsNullOrEmpty(sanitized))
            return "node";

        // Normalize to lowercase kebab-case for stable IDs across the solution
        return sanitized.ToLowerInvariant();
    }

    /// <summary>Generate a unique node ID given a desired name and current ID set (stable deterministic suffixing).</summary>
    public static string GenerateIdFromName(string name, IReadOnlyDictionary<string, NodeDefinition> existing)
    {
        var sanitized = Sanitize(name);
        var counter = 1;
        var candidateId = sanitized;

        while (existing.ContainsKey(candidateId))
        {
            candidateId = $"{sanitized}-{counter++}";
        }

        return candidateId;
    }

    /// <summary>Generate a unique display name (case-insensitive uniqueness) without altering existing names.</summary>
    public static string GenerateUniqueNodeName(string baseName, IEnumerable<NodeDefinition> existing)
    {
        var list = existing as IList<NodeDefinition> ?? existing.ToList();
        var sanitized = Sanitize(baseName);
        var candidate = sanitized;
        var i = 1;

        while (list.Any(n => string.Equals(n.Name, candidate, StringComparison.OrdinalIgnoreCase)))
        {
            candidate = $"{sanitized}-{i++}";
        }

        return candidate;
    }

    /// <summary>Throws when the provided name already exists (case-insensitive).</summary>
    public static void EnsureUniqueName(string name, IEnumerable<NodeDefinition> existing)
    {
        var list = existing as IList<NodeDefinition> ?? existing.ToList();

        if (list.Any(n => string.Equals(n.Name, name, StringComparison.OrdinalIgnoreCase)))
            throw new ArgumentException(ErrorMessages.NodeNameNotUnique(name), nameof(name));
    }
}
