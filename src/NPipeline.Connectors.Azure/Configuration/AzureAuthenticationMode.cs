namespace NPipeline.Connectors.Azure.Configuration;

/// <summary>
///     Authentication modes for Azure services.
/// </summary>
public enum AzureAuthenticationMode
{
    /// <summary>
    ///     Use a connection string containing endpoint and key.
    ///     Format varies by service (e.g., AccountEndpoint=https://account.documents.azure.com:443/;AccountKey=xxx;).
    /// </summary>
    ConnectionString,

    /// <summary>
    ///     Use separate endpoint and key properties.
    ///     Useful when endpoint and key come from different configuration sources.
    /// </summary>
    EndpointWithKey,

    /// <summary>
    ///     Use Azure AD token-based authentication.
    ///     Recommended for production environments with managed identity or service principals.
    /// </summary>
    AzureAdCredential,
}
