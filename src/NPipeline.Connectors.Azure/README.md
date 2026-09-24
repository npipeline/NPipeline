# NPipeline.Connectors.Azure

Shared Azure authentication, connection management, and retry abstractions for NPipeline Azure connectors. This package is a dependency of `NPipeline.Connectors.Azure.CosmosDb` and `NPipeline.Connectors.Azure.ServiceBus`; it is not intended for direct use.

## Overview

`NPipeline.Connectors.Azure` provides the common building blocks used across all NPipeline Azure service connectors:

- **`AzureAuthenticationMode`** - Three authentication modes: `ConnectionString`, `EndpointWithKey`, and `AzureAdCredential`
- **`AzureConnectionOptions`** - Thread-safe registry of named connection strings and named `AzureEndpointOptions` instances
- **`AzureEndpointOptions`** - Pairs an Azure service `Uri` with an Azure.Identity `TokenCredential`
- **`AzureRetryConfiguration`** - Retry limits passed to Azure SDKs that retry natively (Cosmos DB's rate-limited retry)
- **`ITransientErrorDetector`** / **`AzureTransientErrorDetector`** - Classifies HTTP responses as transient (408, 410, 429, 449, 503) so retry logic knows when to retry

## Authentication Modes

| Mode | Description |
|------|-------------|
| `ConnectionString` | Single string containing endpoint and key - e.g., `AccountEndpoint=...;AccountKey=...` |
| `EndpointWithKey` | Separate `Endpoint` and `Credential` properties - useful when they come from different config sources |
| `AzureAdCredential` | Token-based via any Azure.Identity `TokenCredential` - recommended for production (managed identity, service principal) |

## AzureConnectionOptions

Maintains thread-safe named registries for both connection strings and endpoint+credential pairs.

```csharp
var connectionOptions = new AzureConnectionOptions
{
    DefaultConnectionString = "AccountEndpoint=...;AccountKey=..."
};

// Register named connections
connectionOptions.AddOrUpdateConnection("readonly", readonlyConnectionString);

// Register named endpoints (Azure AD)
connectionOptions.AddOrUpdateEndpoint("primary", new AzureEndpointOptions
{
    Endpoint = new Uri("https://myaccount.documents.azure.com:443/"),
    Credential = new DefaultAzureCredential()
});

// Retrieve later
var cs = connectionOptions.GetConnectionString("readonly");
var ep = connectionOptions.GetEndpoint("primary");
```

## AzureRetryConfiguration

Retry limits handed to the Azure SDK. The SDK owns the retry: for Cosmos DB these map to
`CosmosClientOptions.MaxRetryAttemptsOnRateLimitedRequests` and `MaxRetryWaitTimeOnRateLimitedRequests`, and the SDK
chooses the delays from the service's retry-after hints. NPipeline adds no retry layer on top.

| Property | Default | Description |
|----------|---------|-------------|
| `MaxRetryAttempts` | `9` | Maximum retry attempts for rate-limited (429) requests |
| `MaxRetryWaitTime` | `30s` | Maximum total wait time across all retries |

```csharp
var retryConfig = new AzureRetryConfiguration
{
    MaxRetryAttempts = 5,
    MaxRetryWaitTime = TimeSpan.FromSeconds(15)
};
```

## AzureTransientErrorDetector

`AzureTransientErrorDetector` implements `ITransientErrorDetector` and classifies Azure SDK exceptions as transient based on HTTP status code.

**Transient status codes:** `408` (Request Timeout), `410` (Gone), `429` (Too Many Requests), `449` (Retry With), `503` (Service Unavailable).

```csharp
ITransientErrorDetector detector = new AzureTransientErrorDetector();

bool shouldRetry = detector.IsTransient(exception);
```

Extend `AzureTransientErrorDetector` to add service-specific error codes:

```csharp
public class CosmosTransientErrorDetector : AzureTransientErrorDetector
{
    public override bool IsTransient(Exception exception)
    {
        if (base.IsTransient(exception)) return true;
        // Add Cosmos-specific checks
        return exception is CosmosException ce && ce.StatusCode == HttpStatusCode.TooManyRequests;
    }
}
```

## Dependencies

- `Azure.Core` - `TokenCredential` and Azure SDK pipeline primitives
- `Azure.Identity` - `DefaultAzureCredential` and other credential types

## Requirements

- .NET 8.0, 9.0, or 10.0

## Related Packages

- **[NPipeline.Connectors.Azure.CosmosDb](https://www.nuget.org/packages/NPipeline.Connectors.Azure.CosmosDb)** - Cosmos DB source and sink nodes
- **[NPipeline.Connectors.Azure.ServiceBus](https://www.nuget.org/packages/NPipeline.Connectors.Azure.ServiceBus)** - Azure Service Bus source and sink nodes

## License

This package is licensed under the [Business Source License 1.1](LICENSE.txt).

**Free for non-production use.** Production use is free for organizations with 4 or fewer developers and annual revenue of $5M AUD or less. Larger organizations require a [commercial license](https://npipeline.com). This license automatically converts to MIT two years after each release.
