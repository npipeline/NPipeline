---
title: "HTTP Connector"
description: "Read from and post to HTTP APIs with pagination, rate limiting, authentication, and retry."
order: 6
---

# HTTP Connector

The `NPipeline.Connectors.Http` package provides source and sink nodes for HTTP APIs. Supports pluggable authentication, pagination strategies (offset, cursor, link-header), rate limiting, retry with exponential backoff, batch posting, and per-item URI routing.

## Installation

```bash
dotnet add package NPipeline.Connectors.Http
```

**Dependencies:** [Microsoft.Extensions.Http](https://www.nuget.org/packages/Microsoft.Extensions.Http) 10.x

## Source Node - `HttpSourceNode<T>`

Fetches data from an HTTP endpoint and emits each item. Supports paginated APIs.

### Constructors

```csharp
// With IHttpClientFactory (recommended)
public HttpSourceNode(
    HttpSourceConfiguration configuration,
    IHttpClientFactory httpClientFactory)

// With metrics and logging
public HttpSourceNode(
    HttpSourceConfiguration configuration,
    IHttpClientFactory httpClientFactory,
    IHttpConnectorMetrics metrics,
    ILogger<HttpSourceNode<T>>? logger = null)

// With explicit HttpClient
public HttpSourceNode(
    HttpSourceConfiguration configuration,
    HttpClient httpClient,
    IHttpConnectorMetrics? metrics = null,
    ILogger<HttpSourceNode<T>>? logger = null)
```

### Example: Paginated API

```csharp
var config = new HttpSourceConfiguration
{
    BaseUri = new Uri("https://api.example.com/orders"),
    Auth = new BearerTokenAuthProvider("eyJ..."),
    Pagination = new OffsetPaginationStrategy(pageSize: 100),
    MaxPages = 50,
    ItemsJsonPath = "data.orders",
    Timeout = TimeSpan.FromSeconds(30)
};

var source = new HttpSourceNode<Order>(config, httpClientFactory);
```

## Sink Node - `HttpSinkNode<T>`

Posts items to an HTTP endpoint. Supports batch sending, per-item routing, and idempotency keys.

### Constructors

```csharp
// With IHttpClientFactory
public HttpSinkNode(
    HttpSinkConfiguration configuration,
    IHttpClientFactory httpClientFactory)

// Per-item URI routing
public HttpSinkNode(
    HttpSinkConfiguration configuration,
    Func<T, Uri> uriFactory,
    IHttpClientFactory httpClientFactory,
    IHttpConnectorMetrics? metrics = null,
    ILogger<HttpSinkNode<T>>? logger = null)
```

### Example: Batch POST with Idempotency

```csharp
var config = new HttpSinkConfiguration
{
    Uri = new Uri("https://api.example.com/orders"),
    Method = SinkHttpMethod.Post,
    Auth = new ApiKeyAuthProvider("X-Api-Key", "my-key"),
    BatchSize = 50,
    BatchWrapperKey = "orders",
    IdempotencyKeyFactory = item => ((Order)item).OrderId.ToString(),
    RateLimiter = new TokenBucketRateLimiter(permitsPerSecond: 100)
};

var sink = new HttpSinkNode<Order>(config, httpClientFactory);
```

## Authentication

| Provider | Usage |
|----------|-------|
| `NullAuthProvider` (default) | No authentication |
| `BasicAuthProvider(user, pass)` | HTTP Basic auth |
| `BearerTokenAuthProvider(token)` | Bearer token in Authorization header |
| `ApiKeyAuthProvider(header, key)` | API key in a custom header |

Implement `IHttpAuthProvider` for custom schemes (OAuth2, HMAC, etc.).

## Pagination

| Strategy | Description |
|----------|-------------|
| `NoPaginationStrategy` (default) | Single request, no pagination |
| `OffsetPaginationStrategy(pageSize)` | Offset/limit pagination |
| `CursorPaginationStrategy(cursorParam)` | Cursor-based (next token) pagination |
| `LinkHeaderPaginationStrategy()` | RFC 5988 Link header pagination |

Implement `IPaginationStrategy` for custom pagination patterns.

## Rate Limiting

```csharp
// Token bucket rate limiter
var config = new HttpSourceConfiguration
{
    RateLimiter = new TokenBucketRateLimiter(permitsPerSecond: 50)
};
```

Implement `IRateLimiter` for custom rate limiting (sliding window, per-endpoint, etc.).

## Configuration - Source

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `BaseUri` | `Uri` | (required) | Base URL (must be absolute) |
| `RequestMethod` | `HttpMethod` | `GET` | HTTP method |
| `Headers` | `Dictionary<string, string>` | `{}` | Default headers |
| `ItemsJsonPath` | `string?` | `null` | Dot-separated path to items array (e.g., `"data.orders"`) |
| `JsonOptions` | `JsonSerializerOptions?` | Web defaults | JSON deserialization options |
| `Auth` | `IHttpAuthProvider` | `NullAuthProvider` | Authentication provider |
| `Pagination` | `IPaginationStrategy` | `NoPaginationStrategy` | Pagination strategy |
| `RateLimiter` | `IRateLimiter` | `NullRateLimiter` | Rate limiter |
| `RetryStrategy` | `IHttpRetryStrategy` | Exponential backoff | Retry strategy |
| `Timeout` | `TimeSpan` | `30s` | Request timeout |
| `MaxPages` | `int?` | `null` | Safety guard for pagination loops |
| `MaxResponseBytes` | `long?` | `null` | Max response size |
| `RequestCustomizer` | `Func<HttpRequestMessage, CancellationToken, ValueTask>?` | `null` | Per-request hook |

## Configuration - Sink

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `Uri` | `Uri?` | `null` | Target URL |
| `UriFactory` | `Func<object, Uri>?` | `null` | Per-item URL (overrides `Uri`) |
| `Method` | `SinkHttpMethod` | `Post` | `Post`, `Put`, or `Patch` |
| `Headers` | `Dictionary<string, string>` | `{}` | Default headers |
| `BatchSize` | `int` | `1` | Items per request (1 = individual) |
| `BatchWrapperKey` | `string?` | `null` | JSON property name wrapping batch array |
| `Auth` | `IHttpAuthProvider` | `NullAuthProvider` | Authentication provider |
| `RateLimiter` | `IRateLimiter` | `NullRateLimiter` | Rate limiter |
| `RetryStrategy` | `IHttpRetryStrategy` | Exponential backoff | Retry strategy |
| `IdempotencyKeyFactory` | `Func<object, string>?` | `null` | Generate idempotency keys |
| `IdempotencyHeaderName` | `string` | `"Idempotency-Key"` | Header name for idempotency key |
| `CaptureErrorResponses` | `bool` | `false` | Capture error response bodies |

## Dependency Injection

```csharp
services.AddHttpConnector();
services.AddHttpConnectorClient("orders-api", client =>
{
    client.BaseAddress = new Uri("https://api.example.com/");
    client.DefaultRequestHeaders.Add("Accept", "application/json");
});
```

## Retry Strategy

The default retry strategy uses exponential backoff with jitter for transient HTTP errors (5xx, 408, 429):

```csharp
var config = new HttpSourceConfiguration
{
    RetryStrategy = new ExponentialBackoffHttpRetryStrategy
    {
        MaxRetries = 3,
        BaseDelayMs = 1000,
        MaxDelayMs = 30_000
    }
};
```

Implement `IHttpRetryStrategy` for custom retry logic (e.g., per-status-code behavior).

### 429 Too Many Requests

The default retry strategy respects `Retry-After` headers automatically.

## Idempotency

For sink operations, generate an idempotency key per item to prevent duplicate submissions:

```csharp
var sink = new HttpSinkNode<Order>(new HttpSinkConfiguration
{
    Uri = new Uri("https://api.example.com/orders"),
    Method = SinkHttpMethod.Post,
    IdempotencyKeyFactory = order => order.OrderId.ToString(),
    IdempotencyHeaderName = "Idempotency-Key"
});
```

## Best Practices

1. **Use pagination** for large result sets - never fetch unbounded data
2. **Set `MaxPages`** as a safety guard against infinite pagination loops
3. **Use rate limiting** to avoid overwhelming downstream APIs
4. **Use idempotency keys** for POST/PUT operations
5. **Register named `HttpClient`** via DI with `AddHttpConnectorClient` for testability and pooling
6. **Set `MaxResponseBytes`** to prevent memory exhaustion from unexpectedly large responses
7. **Implement `IHttpAuthProvider`** for OAuth2/OIDC flows

## Next Steps

- [Error Handling](../error-handling/index.md) - retry strategies for HTTP failures
- [Dependency Injection](../guides/dependency-injection.md) - HttpClient integration
