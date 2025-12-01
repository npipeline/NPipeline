using NPipeline.Connectors.Abstractions;

namespace NPipeline.Connectors;

/// <summary>
///     Result returned when creating a <see cref="StorageResolver" />.
/// </summary>
/// <param name="Resolver">Built resolver.</param>
/// <param name="Errors">Per-provider instantiation errors (empty when <c>CollectErrors</c> was false).</param>
public sealed record StorageResolverResult(
    IStorageResolver Resolver,
    IReadOnlyDictionary<string, IReadOnlyList<string>> Errors)
{
    /// <summary>
    ///     Convenience flag indicating whether any creation errors were captured.
    /// </summary>
    public bool HasErrors => Errors.Count > 0;

    /// <summary>
    ///     Allows tuple deconstruction: <c>var (resolver, errors) = StorageProviderFactory.CreateResolver();</c>
    /// </summary>
    public void Deconstruct(out IStorageResolver resolver, out IReadOnlyDictionary<string, IReadOnlyList<string>> errors)
    {
        resolver = Resolver;
        errors = Errors;
    }
}
