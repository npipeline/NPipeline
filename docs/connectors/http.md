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
| `Resilience` | `NResilience.Resilience` | `HttpConnectorResilience.Default` | Retries, backoff, and the per-request timeout |
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
| `Resilience` | `NResilience.Resilience` | `HttpConnectorResilience.Default` | Retries, backoff, and the per-request timeout |
| `IdempotencyKeyFactory` | `Func<object, string>?` | `null` | Generate idempotency keys |
| `IdempotencyHeaderName` | `string` | `"Idempotency-Key"` | Header name for idempotency key |
| `CaptureErrorResponses` | `bool` | `false` | Log a failed response and continue instead of throwing. Applies only after retries are spent |

## Dependency Injection

```csharp
services.AddHttpConnector();
services.AddHttpConnectorClient("orders-api", client =>
{
    client.BaseAddress = new Uri("https://api.example.com/");
    client.DefaultRequestHeaders.Add("Accept", "application/json");
});
```

## Resilience

Both nodes send every request through [NResilience](https://github.com/nresilience/NResilience)'s
`HttpResilienceHandler`. The `Resilience` property configures it. The default,
`HttpConnectorResilience.Default`, does the following:

- Makes up to four attempts (three retries).
- Retries network failures, client timeouts, 408, 429, and 5xx responses. A 404 or other 4xx response is not
  retried.
- Waits with exponential backoff and full jitter, from 200 milliseconds up to 30 seconds.
- Honors `Retry-After` on 429 and 503 responses.
- Times out each attempt after 30 seconds (`AttemptTimeout`). There is no overall deadline.
- Opens a circuit breaker per host when a dependency keeps failing, and spends retries from a per-host budget so a
  failing API is not flooded with retries.

`HttpConnectorResilience.Conservative` makes three attempts with backoff from 1 second up to 60 seconds. To change
a setting, derive a policy with a `with` expression:

```csharp
var config = new HttpSourceConfiguration
{
    BaseUri = new Uri("https://api.example.com/orders"),
    Resilience = HttpConnectorResilience.Default with
    {
        Attempts = 6,
        AttemptTimeout = TimeSpan.FromSeconds(10),
        Deadline = TimeSpan.FromMinutes(2),
    },
};
```

To turn retries off, use `Resilience.None`. It has no timeout, so set one:

```csharp
Resilience = Resilience.None with { AttemptTimeout = TimeSpan.FromSeconds(30) }
```

The nodes are the only layer that retries. Don't add a retry or resilience handler to the `HttpClient` you pass in or
register with `AddHttpConnectorClient`. Two retrying layers multiply attempts: four attempts at each layer make up
to 16 requests.

### Non-idempotent writes

The source retries every request, including POST requests that carry a query, because sources only read. The sink
retries PUT requests, but sends POST and PATCH requests **once** unless they carry an idempotency key.
Retrying a write that the server already applied would duplicate it. Set `IdempotencyKeyFactory` (see
[Idempotency](#idempotency)) to make POST and PATCH retryable. To retry a POST or PATCH that is safe to repeat for
another reason, mark the request in `RequestCustomizer`:

```csharp
RequestCustomizer = (request, _) =>
{
    request.MarkRepeatable();
    return ValueTask.CompletedTask;
}
```

## Idempotency

For sink operations, generate an idempotency key per item to prevent duplicate submissions. The key also makes POST
and PATCH requests retryable, and every attempt sends the same key:

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
4. **Use idempotency keys** for POST and PATCH operations, so they can be retried safely
5. **Register named `HttpClient`** via DI with `AddHttpConnectorClient` for testability and pooling
6. **Set `MaxResponseBytes`** to prevent memory exhaustion from unexpectedly large responses
7. **Implement `IHttpAuthProvider`** for OAuth2/OIDC flows

## Next Steps

- [Error Handling](../error-handling/index.md) - how pipeline-level retries relate to connector retries
- [Dependency Injection](../guides/dependency-injection.md) - HttpClient integration
