using Azure.Core;
using Azure.Identity;

namespace NPipeline.StorageProviders.Azure;

/// <summary>
///     Settings shared by the Azure Blob and ADLS Gen2 storage providers: how to authenticate, which endpoint to use, how
///     to retry, and how uploads are split into parts.
/// </summary>
/// <remarks>
///     Credentials belong here and not in the URI. A URI carries routing only (<c>accountName</c>, <c>serviceUrl</c>), so it
///     can be logged and compared safely and a client can be cached per endpoint. When more than one credential is set, the
///     first of these wins: connection string, SAS token, account key, <see cref="DefaultCredential" />, the default
///     credential chain.
/// </remarks>
public abstract class AzureAccountOptions
{
    private readonly Lazy<TokenCredential> _defaultCredentialChain = new(() => new DefaultAzureCredential());
    private int _clientCacheSizeLimit = 100;
    private int _maxConcurrency = 4;
    private int _partSizeBytes = 8 * 1024 * 1024;
    private AzureRetryOptions _retry = new();

    /// <summary>Gets or sets the default storage account name. A <c>accountName</c> URI parameter overrides it. Required with <see cref="DefaultAccountKey" />.</summary>
    public string? AccountName { get; set; }

    /// <summary>Gets or sets the connection string. It carries its own endpoint and credentials, so it cannot be combined with <see cref="ServiceUrl" />.</summary>
    public string? DefaultConnectionString { get; set; }

    /// <summary>Gets or sets a shared access signature token used to authenticate.</summary>
    public string? DefaultSasToken { get; set; }

    /// <summary>Gets or sets the storage account key used to authenticate. Requires <see cref="AccountName" /> or an <c>accountName</c> URI parameter.</summary>
    public string? DefaultAccountKey { get; set; }

    /// <summary>
    ///     Gets or sets the Azure credential used to authenticate. When it is not set and <see cref="UseDefaultCredentialChain" />
    ///     is true, the default Azure credential chain is used.
    /// </summary>
    public TokenCredential? DefaultCredential { get; set; }

    /// <summary>Gets or sets whether to use the default Azure credential chain when no other credential is set. Default is true.</summary>
    public bool UseDefaultCredentialChain { get; set; } = true;

    /// <summary>
    ///     Gets or sets a value indicating whether to connect without credentials when none are configured, for reading
    ///     public containers. Defaults to <see langword="false" />: a missing credential is a configuration error.
    /// </summary>
    public bool AllowAnonymousAccess { get; set; }

    /// <summary>Gets a cached instance of the default Azure credential chain.</summary>
    public TokenCredential DefaultCredentialChain => _defaultCredentialChain.Value;

    /// <summary>
    ///     Gets or sets the service URL for Azure Storage-compatible endpoints (for example the Azurite emulator). A
    ///     <c>serviceUrl</c> URI parameter overrides it. When it is not set, the public Azure endpoint for the account is used.
    /// </summary>
    public Uri? ServiceUrl { get; set; }

    /// <summary>Gets or sets the Azure SDK retry settings for the clients the provider creates.</summary>
    public AzureRetryOptions Retry
    {
        get => _retry;
        set => _retry = value ?? throw new ArgumentNullException(nameof(value));
    }

    /// <summary>
    ///     Gets or sets the size in bytes of each upload block. An object that fits in one block is sent in one request; a
    ///     larger one is staged block by block while it is being written. Memory use while writing is about
    ///     <c>PartSizeBytes × (MaxConcurrency + 1)</c>. Default is 8 MiB.
    /// </summary>
    public int PartSizeBytes
    {
        get => _partSizeBytes;
        set => _partSizeBytes = value > 0
            ? value
            : throw new ArgumentOutOfRangeException(nameof(value), "Part size must be positive.");
    }

    /// <summary>Gets or sets the most blocks of one object that upload at the same time. Default is 4.</summary>
    public int MaxConcurrency
    {
        get => _maxConcurrency;
        set => _maxConcurrency = value > 0
            ? value
            : throw new ArgumentOutOfRangeException(nameof(value), "Maximum concurrency must be positive.");
    }

    /// <summary>
    ///     Gets or sets whether a write creates its container first when it does not exist. Default is <see langword="false" />:
    ///     creating a container needs permissions a least-privilege token usually lacks, and costs a request. When true, each
    ///     container is created at most once per provider instance.
    /// </summary>
    public bool CreateContainerIfMissing { get; set; }

    /// <summary>Gets or sets the most endpoint clients kept in the cache. Default is 100.</summary>
    public int ClientCacheSizeLimit
    {
        get => _clientCacheSizeLimit;
        set => _clientCacheSizeLimit = value > 0
            ? value
            : throw new ArgumentOutOfRangeException(nameof(value), "Client cache size limit must be positive.");
    }
}
