using NPipeline.Connectors.Azure.Configuration;

namespace NPipeline.Connectors.Azure.CosmosDb.Configuration;

/// <summary>
///     Defines the authentication mode for Cosmos DB connections.
/// </summary>
/// <remarks>
///     <para>
///         This enum is maintained for backward compatibility. Consider using <see cref="AzureAuthenticationMode" /> from
///         NPipeline.Connectors.Azure for new development.
///     </para>
///     <para>
///         Migration guide:
///         <list type="bullet">
///             <item>
///                 <description>
///                     <see cref="ConnectionString" /> maps to
///                     <see cref="AzureAuthenticationMode.ConnectionString" />
///                 </description>
///             </item>
///             <item>
///                 <description>
///                     <see cref="AccountEndpointAndKey" /> maps to
///                     <see cref="AzureAuthenticationMode.EndpointWithKey" />
///                 </description>
///             </item>
///             <item>
///                 <description>
///                     <see cref="AzureAdCredential" /> maps to
///                     <see cref="AzureAuthenticationMode.AzureAdCredential" />
///                 </description>
///             </item>
///         </list>
///     </para>
/// </remarks>
public enum CosmosAuthenticationMode
{
    /// <summary>
    ///     Use a connection string containing endpoint and key.
    ///     Format: AccountEndpoint=https://account.documents.azure.com:443/;AccountKey=xxx;
    /// </summary>
    ConnectionString,

    /// <summary>
    ///     Use separate account endpoint and account key properties.
    ///     Useful when endpoint and key come from different configuration sources.
    /// </summary>
    AccountEndpointAndKey,

    /// <summary>
    ///     Use Azure AD token-based authentication.
    ///     Recommended for production environments with managed identity or service principals.
    /// </summary>
    AzureAdCredential,
}

/// <summary>
///     Extension methods for converting between <see cref="CosmosAuthenticationMode" /> and
///     <see cref="AzureAuthenticationMode" />.
/// </summary>
public static class CosmosAuthenticationModeExtensions
{
    /// <summary>
    ///     Converts a <see cref="CosmosAuthenticationMode" /> to an
    ///     <see cref="AzureAuthenticationMode" />.
    /// </summary>
    /// <param name="mode">The Cosmos authentication mode to convert.</param>
    /// <returns>The equivalent Azure authentication mode.</returns>
    public static AzureAuthenticationMode ToAzureAuthenticationMode(
        this CosmosAuthenticationMode mode)
    {
        return mode switch
        {
            CosmosAuthenticationMode.ConnectionString => AzureAuthenticationMode.ConnectionString,
            CosmosAuthenticationMode.AccountEndpointAndKey => AzureAuthenticationMode.EndpointWithKey,
            CosmosAuthenticationMode.AzureAdCredential => AzureAuthenticationMode.AzureAdCredential,
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
        };
    }

    /// <summary>
    ///     Converts an <see cref="AzureAuthenticationMode" /> to a
    ///     <see cref="CosmosAuthenticationMode" />.
    /// </summary>
    /// <param name="mode">The Azure authentication mode to convert.</param>
    /// <returns>The equivalent Cosmos authentication mode.</returns>
    public static CosmosAuthenticationMode ToCosmosAuthenticationMode(
        this AzureAuthenticationMode mode)
    {
        return mode switch
        {
            AzureAuthenticationMode.ConnectionString => CosmosAuthenticationMode.ConnectionString,
            AzureAuthenticationMode.EndpointWithKey => CosmosAuthenticationMode.AccountEndpointAndKey,
            AzureAuthenticationMode.AzureAdCredential => CosmosAuthenticationMode.AzureAdCredential,
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
        };
    }
}
