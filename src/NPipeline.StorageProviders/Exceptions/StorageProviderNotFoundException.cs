using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Exceptions;

/// <summary>
///     Thrown when no storage provider can handle a given <see cref="StorageUri" />.
///     Typically indicates that a connector package providing the required scheme is not referenced or registered.
/// </summary>
internal sealed class StorageProviderNotFoundException : ConnectorException
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="StorageProviderNotFoundException" /> class.
    /// </summary>
    /// <param name="uri">The storage URI for which no provider was found.</param>
    public StorageProviderNotFoundException(StorageUri uri)
        : base(BuildMessage(uri))
    {
        Scheme = uri.Scheme.ToString();
        Host = uri.Host;
        Path = uri.Path;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="StorageProviderNotFoundException" /> class.
    /// </summary>
    /// <param name="uri">The storage URI for which no provider was found.</param>
    /// <param name="innerException">The underlying exception that caused the provider lookup to fail.</param>
    public StorageProviderNotFoundException(StorageUri uri, Exception innerException)
        : base(BuildMessage(uri), innerException)
    {
        Scheme = uri.Scheme.ToString();
        Host = uri.Host;
        Path = uri.Path;
    }

    public string Scheme { get; }
    public string? Host { get; }
    public string Path { get; }

    private static string BuildMessage(StorageUri uri)
    {
        var scheme = uri.Scheme.ToString();

        return $"No storage provider found for scheme '{scheme}'. " +
               $"Ensure that the appropriate connector package for '{scheme}://' is referenced and registered.";
    }
}
