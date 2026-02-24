using Azure.Core;

namespace NPipeline.Connectors.Azure.Configuration;

/// <summary>
///     Azure service endpoint configuration with credential.
/// </summary>
public class AzureEndpointOptions
{
    /// <summary>
    ///     Gets or sets the service endpoint URI.
    /// </summary>
    public Uri? Endpoint { get; set; }

    /// <summary>
    ///     Gets or sets the token credential for authentication.
    /// </summary>
    public TokenCredential? Credential { get; set; }
}
