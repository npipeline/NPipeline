# NPipeline.Connectors.Http

An HTTP/REST connector for [NPipeline](https://github.com/NPipeline/NPipeline) providing a fully-featured source node (paginated GET) and sink node (
POST/PUT/PATCH) for REST APIs.

## Features

- **`HttpSourceNode<T>`** — streams items from any paginated REST API with lazy, memory-efficient `IAsyncEnumerable<T>` delivery.
- **`HttpSinkNode<T>`** — batches items and writes them to a REST API via POST, PUT, or PATCH.
- **Pluggable pagination** — offset/page, cursor, RFC 5988 Link header, or bring your own strategy.
- **Pluggable auth** — Bearer token (static or factory), API key (header or query string), Basic, or bring your own.
- **Retry with exponential backoff** — honours `Retry-After` headers on 429 responses; configurable jitter.
- **Token-bucket rate limiting** — wraps the battle-tested .NET `System.Threading.RateLimiting.TokenBucketRateLimiter`.
- **OpenTelemetry** — an `ActivitySource` named `NPipeline.Connectors.Http` emits one span per page fetch and per sink flush.
- **`IHttpClientFactory` integration** — named clients for connection-pool reuse; raw `HttpClient` constructor for tests and simple scenarios.

## Installation

```xml
<PackageReference Include="NPipeline.Connectors.Http" Version="*" />
```

## Quick Start

### Source — reading from a paginated REST API

```csharp
using NPipeline.Connectors.Http.Auth;
using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Nodes;
using NPipeline.Connectors.Http.Pagination;

var config = new HttpSourceConfiguration
{
    BaseUri   = new Uri("https://api.github.com/repos/dotnet/runtime/releases"),
    Headers   = { ["User-Agent"] = "MyApp/1.0", ["Accept"] = "application/vnd.github+json" },
    Auth      = new BearerTokenAuthProvider(Environment.GetEnvironmentVariable("GITHUB_TOKEN")!),
    Pagination = new LinkHeaderPaginationStrategy(),
    MaxPages  = 5,
};

var source = new HttpSourceNode<GithubRelease>(config, httpClient);
```

### Sink — writing to a REST endpoint

```csharp
using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Nodes;

var config = new HttpSinkConfiguration
{
    Uri = new Uri("https://hooks.slack.com/services/…"),
    Method = SinkHttpMethod.Post,
};

var sink = new HttpSinkNode<SlackMessage>(config, httpClient);
```

### Using with dependency injection

```csharp
services.AddHttpClient();
services.AddSingleton(sourceConfig);
services.AddSingleton(sinkConfig);
services.AddNPipeline(Assembly.GetExecutingAssembly());
```

## Auth Providers

| Class                     | Description                                                              |
|---------------------------|--------------------------------------------------------------------------|
| `BearerTokenAuthProvider` | `Authorization: Bearer <token>` — accepts static string or async factory |
| `ApiKeyAuthProvider`      | API key in a named header or query-string parameter                      |
| `BasicAuthProvider`       | RFC 7617 Basic auth (Base64 UTF-8 `user:password`)                       |
| `NullAuthProvider`        | No-op; used when the API requires no authentication                      |

Implement `IHttpAuthProvider` to add custom schemes (OAuth2 PKCE, mTLS, AWS Signature V4, etc.).

## Pagination Strategies

| Class                          | Behaviour                                                                                                 |
|--------------------------------|-----------------------------------------------------------------------------------------------------------|
| `NoPaginationStrategy`         | Single request, no pagination                                                                             |
| `OffsetPaginationStrategy`     | Manages `page`/`pageSize` query parameters; stops when response is a short page or total count is reached |
| `CursorPaginationStrategy`     | Extracts cursor token from a JSON path and appends it to the next request                                 |
| `LinkHeaderPaginationStrategy` | Parses RFC 5988 `Link: <url>; rel="next"` headers (GitHub-compatible)                                     |

Implement `IPaginationStrategy` for custom pagination schemes.

## Rate Limiting

```csharp
// 10 requests per second
var rateLimiter = new TokenBucketRateLimiter(new TokenBucketRateLimiterOptions
{
    TokensPerPeriod = 10,
    Period = TimeSpan.FromSeconds(1),
    BucketCapacity = 10,
});
```

Implement `IRateLimiter` for custom strategies.

## Retry

```csharp
// Customise retry behaviour
var retryStrategy = new ExponentialBackoffHttpRetryStrategy
{
    MaxRetries  = 5,
    BaseDelayMs = 500,
    MaxDelayMs  = 60_000,
    JitterFactor = 0.3,
    RetryableStatusCodes = new HashSet<HttpStatusCode>
    {
        HttpStatusCode.TooManyRequests,
        HttpStatusCode.ServiceUnavailable,
    },
};

// Or use the built-in presets
var retryStrategy = ExponentialBackoffHttpRetryStrategy.Default;      // 3 retries, 200 ms base
var retryStrategy = ExponentialBackoffHttpRetryStrategy.Conservative; // 2 retries, 1 s base
```

## Sink: Batching and Idempotency

```csharp
var config = new HttpSinkConfiguration
{
    Uri = new Uri("https://api.example.com/products/bulk"),
    Method = SinkHttpMethod.Post,
    BatchSize = 50,
    BatchWrapperKey = "products",          // produces {"products":[...]}
    IdempotencyKeyFactory = item => Guid.NewGuid().ToString(),
    IdempotencyHeaderName = "Idempotency-Key",
};
```

## Configuration Reference

### `HttpSourceConfiguration`

| Property            | Type                                                      | Default                                       | Description                                   |
|---------------------|-----------------------------------------------------------|-----------------------------------------------|-----------------------------------------------|
| `BaseUri`           | `Uri`                                                     | **required**                                  | Absolute URI of the API endpoint              |
| `RequestMethod`     | `HttpMethod`                                              | `GET`                                         | HTTP method for source requests               |
| `Headers`           | `Dictionary<string,string>`                               | `{}`                                          | Fixed headers on every request                |
| `Auth`              | `IHttpAuthProvider`                                       | `NullAuthProvider`                            | Authentication provider                       |
| `Pagination`        | `IPaginationStrategy`                                     | `NoPaginationStrategy`                        | Pagination strategy                           |
| `RateLimiter`       | `IRateLimiter`                                            | `NullRateLimiter`                             | Rate limiter                                  |
| `RetryStrategy`     | `IHttpRetryStrategy`                                      | `ExponentialBackoffHttpRetryStrategy.Default` | Retry strategy                                |
| `ItemsJsonPath`     | `string?`                                                 | `null`                                        | Dot-separated JSON path to the array of items |
| `Timeout`           | `TimeSpan`                                                | 30 s                                          | Per-request timeout                           |
| `MaxPages`          | `int?`                                                    | `null`                                        | Safety limit on page count                    |
| `MaxResponseBytes`  | `long?`                                                   | `null`                                        | Safety limit on response body size            |
| `RequestCustomizer` | `Func<HttpRequestMessage, CancellationToken, ValueTask>?` | `null`                                        | Hook to mutate each request before send       |

### `HttpSinkConfiguration`

| Property                | Type                                                      | Default                                       | Description                                        |
|-------------------------|-----------------------------------------------------------|-----------------------------------------------|----------------------------------------------------|
| `Uri`                   | `Uri?`                                                    | —                                             | Static target URI                                  |
| `UriFactory`            | `Func<object, Uri>?`                                      | —                                             | Per-item URI factory (takes precedence over `Uri`) |
| `Method`                | `SinkHttpMethod`                                          | `Post`                                        | HTTP method (`Post`, `Put`, `Patch`)               |
| `BatchSize`             | `int`                                                     | `1`                                           | Items buffered before flushing                     |
| `BatchWrapperKey`       | `string?`                                                 | `null`                                        | JSON key wrapping the batch array                  |
| `Auth`                  | `IHttpAuthProvider`                                       | `NullAuthProvider`                            | Authentication provider                            |
| `RateLimiter`           | `IRateLimiter`                                            | `NullRateLimiter`                             | Rate limiter                                       |
| `RetryStrategy`         | `IHttpRetryStrategy`                                      | `ExponentialBackoffHttpRetryStrategy.Default` | Retry strategy                                     |
| `CaptureErrorResponses` | `bool`                                                    | `false`                                       | Capture non-2xx instead of throwing                |
| `IdempotencyKeyFactory` | `Func<object, string>?`                                   | `null`                                        | Idempotency key factory                            |
| `IdempotencyHeaderName` | `string`                                                  | `Idempotency-Key`                             | Header name for idempotency key                    |
| `RequestCustomizer`     | `Func<HttpRequestMessage, CancellationToken, ValueTask>?` | `null`                                        | Hook to mutate each request before send            |

## Sample Application

See [`samples/Sample_HttpConnector`](../../../samples/Sample_HttpConnector) for a complete example that fetches GitHub releases and posts summaries to a Slack
webhook.

```
GITHUB_TOKEN=ghp_...  SLACK_WEBHOOK=https://hooks.slack.com/...
dotnet run --project samples/Sample_HttpConnector
```
