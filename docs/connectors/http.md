---
title: HTTP Connector
description: Read from and write to REST APIs with NPipeline using the HTTP connector with support for pagination, authentication, rate limiting, retry, and observability.
order: 7
---

## HTTP Connector

The `NPipeline.Connectors.Http` package provides specialized source and sink nodes for working with REST APIs. This allows you to easily integrate HTTP endpoints into your pipelines as an input source or an output destination.

This connector supports fully-featured REST API integration with pluggable pagination strategies, multiple authentication schemes, exponential backoff retry with `Retry-After` header support, token-bucket rate limiting, and OpenTelemetry observability.

## Installation

To use the HTTP connector, install the `NPipeline.Connectors.Http` NuGet package:

```bash
dotnet add package NPipeline.Connectors.Http
```

For the core NPipeline package and other available extensions, see the [Installation Guide](../getting-started/installation.md).

## Quick Start

### Reading from a REST API

```csharp
using NPipeline.Connectors.Http.Auth;
using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Nodes;
using NPipeline.Connectors.Http.Pagination;

// Define your model
public sealed record GithubRelease
{
    public string TagName { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string Body { get; set; } = string.Empty;
    public DateTime PublishedAt { get; set; }
}

// Create and configure the source node
var sourceConfig = new HttpSourceConfiguration
{
    BaseUri = new Uri("https://api.github.com/repos/dotnet/runtime/releases"),
    Headers = { ["User-Agent"] = "MyApp/1.0", ["Accept"] = "application/vnd.github+json" },
    Auth = new BearerTokenAuthProvider(Environment.GetEnvironmentVariable("GITHUB_TOKEN")!),
    Pagination = new LinkHeaderPaginationStrategy(),
    MaxPages = 5,
};

using var httpClient = new HttpClient();
var sourceNode = new HttpSourceNode<GithubRelease>(sourceConfig, httpClient);

// Use in a pipeline
var source = builder.AddSource(sourceNode, "github_source");
```

### Writing to a REST API

```csharp
public sealed record SlackMessage
{
    public string Text { get; set; } = string.Empty;
    public string Channel { get; set; } = string.Empty;
}

var sinkConfig = new HttpSinkConfiguration
{
    Uri = new Uri("https://hooks.slack.com/services/YOUR/WEBHOOK/URL"),
    Method = SinkHttpMethod.Post,
    BatchSize = 10,
};

using var httpClient = new HttpClient();
var sinkNode = new HttpSinkNode<SlackMessage>(sinkConfig, httpClient);

var sink = builder.AddSink(sinkNode, "slack_sink");
```

### Using with Dependency Injection

The HTTP connector integrates seamlessly with the NPipeline dependency injection system:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.Http.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;

var services = new ServiceCollection()
    .AddHttpClient()
    .AddHttpConnector()
    .AddNPipeline(Assembly.GetExecutingAssembly())
    .BuildServiceProvider();

// Configurations can be registered and injected
services.AddSingleton(sourceConfig);
services.AddSingleton(sinkConfig);

// Nodes will be automatically created with proper lifecycle management
var source = services.GetRequiredService<HttpSourceNode<GithubRelease>>();
var sink = services.GetRequiredService<HttpSinkNode<SlackMessage>>();
```

## Key Features

### Pagination Strategies

The HTTP connector supports multiple pagination strategies for consuming paginated REST APIs. Choose the strategy that matches your API:

| Strategy | Use Case |
|----------|----------|
| **`NoPaginationStrategy`** | Single request, no pagination. Suitable for non-paginated endpoints or when you only need one page. |
| **`OffsetPaginationStrategy`** | Offset/page-based pagination with `page` and `pageSize` query parameters. Stops when a short page or total count is reached. |
| **`CursorPaginationStrategy`** | Cursor-based pagination. Extracts a cursor token from a JSON path and appends it to subsequent requests. |
| **`LinkHeaderPaginationStrategy`** | RFC 5988 `Link` header pagination. Compatible with GitHub, GitLab, and other APIs that use standard link headers. |

Implement `IPaginationStrategy` to create custom pagination strategies for your API.

### Authentication Providers

Multiple authentication schemes are supported out of the box:

| Provider | Scheme | Use Case |
|----------|--------|----------|
| **`BearerTokenAuthProvider`** | `Authorization: Bearer <token>` | OAuth2 bearer tokens, API tokens. Supports both static and async token factories. |
| **`ApiKeyAuthProvider`** | Custom header or query parameter | APIs that use API keys in headers (e.g., `X-API-Key`) or query strings. |
| **`BasicAuthProvider`** | RFC 7617 Basic auth | Username/password authentication with Base64 encoding. |
| **`NullAuthProvider`** | None | Public APIs that require no authentication. |

Implement `IHttpAuthProvider` to add custom authentication schemes (OAuth2 PKCE, mTLS, AWS Signature V4, etc.).

### Rate Limiting

Control request throughput with token-bucket rate limiting:

```csharp
using System.Threading.RateLimiting;
using NPipeline.Connectors.Http.Configuration;

var config = new HttpSourceConfiguration
{
    BaseUri = new Uri("https://api.example.com/data"),
    // Limit to 10 requests per second
    RateLimiter = new TokenBucketRateLimiter(new TokenBucketRateLimiterOptions
    {
        TokensPerPeriod = 10,
        Period = TimeSpan.FromSeconds(1),
        BucketCapacity = 10,
    }),
};
```

### Retry Strategy

Automatic retry with exponential backoff and `Retry-After` header support:

```csharp
using NPipeline.Connectors.Http.Retry;

var config = new HttpSourceConfiguration
{
    BaseUri = new Uri("https://api.example.com/data"),
    RetryStrategy = new ExponentialBackoffHttpRetryStrategy
    {
        MaxRetries = 5,
        BaseDelayMs = 500,
        MaxDelayMs = 60_000,
        JitterFactor = 0.3,
        RetryableStatusCodes = new HashSet<HttpStatusCode>
        {
            HttpStatusCode.TooManyRequests,
            HttpStatusCode.ServiceUnavailable,
            HttpStatusCode.GatewayTimeout,
        },
    },
};

// Or use a built-in preset
var config = new HttpSourceConfiguration
{
    BaseUri = new Uri("https://api.example.com/data"),
    RetryStrategy = ExponentialBackoffHttpRetryStrategy.Default,      // 3 retries, 200 ms base
};
```

### OpenTelemetry Observability

The HTTP connector automatically emits OpenTelemetry spans for monitoring and tracing:

```csharp
using System.Diagnostics;

// An ActivitySource named "NPipeline.Connectors.Http" emits:
// - One span per page fetch (source)
// - One span per sink flush (sink)

var activitySource = new ActivitySource("NPipeline.Connectors.Http");
// Spans are automatically created and populated with timing and status information
```

## `HttpSourceNode<T>`

The `HttpSourceNode<T>` streams items from a paginated REST API with lazy, memory-efficient delivery.

### Source Configuration

```csharp
public sealed record HttpSourceConfiguration
{
    // Required: Base URI of the API endpoint
    public required Uri BaseUri { get; init; }

    // Optional: HTTP method (default: GET)
    public HttpMethod RequestMethod { get; init; } = HttpMethod.Get;

    // Optional: Fixed headers on every request
    public Dictionary<string, string> Headers { get; init; } = [];

    // Optional: Request body factory for POST sources
    public Func<Uri, HttpContent?>? RequestBodyFactory { get; init; }

    // Optional: Named HttpClient from IHttpClientFactory
    public string? HttpClientName { get; init; }

    // Optional: Per-request timeout (default: 30 seconds)
    public TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(30);

    // Optional: JSON path to items array in response
    public string? ItemsJsonPath { get; init; }

    // Optional: Custom JSON serialization options
    public JsonSerializerOptions? JsonOptions { get; init; }

    // Optional: Authentication provider (default: NullAuthProvider)
    public IHttpAuthProvider Auth { get; init; } = NullAuthProvider.Instance;

    // Optional: Pagination strategy (default: NoPaginationStrategy)
    public IPaginationStrategy Pagination { get; init; } = new NoPaginationStrategy();

    // Optional: Rate limiter (default: NullRateLimiter)
    public IRateLimiter RateLimiter { get; init; } = NullRateLimiter.Instance;

    // Optional: Retry strategy (default: ExponentialBackoffHttpRetryStrategy.Default)
    public IHttpRetryStrategy RetryStrategy { get; init; } = ExponentialBackoffHttpRetryStrategy.Default;

    // Optional: Request customizer hook
    public Func<HttpRequestMessage, CancellationToken, ValueTask>? RequestCustomizer { get; init; }

    // Optional: Maximum pages to fetch (safety guard)
    public int? MaxPages { get; init; }

    // Optional: Maximum response body size in bytes
    public long? MaxResponseBytes { get; init; }
}
```

### Constructor Overloads

```csharp
// Using HttpClient directly (for tests or simple scenarios)
public HttpSourceNode(HttpSourceConfiguration configuration, HttpClient httpClient)

// Using IHttpClientFactory (recommended for production)
public HttpSourceNode(HttpSourceConfiguration configuration, IHttpClientFactory httpClientFactory)

// With full dependency injection
public HttpSourceNode(
    HttpSourceConfiguration configuration,
    IHttpClientFactory httpClientFactory,
    IHttpConnectorMetrics metrics,
    ILogger<HttpSourceNode<T>>? logger = null)
```

### Example: Reading with Bearer Token and Link Header Pagination

```csharp
using NPipeline.Connectors.Http.Auth;
using NPipeline.Connectors.Http.Pagination;

public sealed record Repository
{
    public long Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public int Stars { get; set; }
}

var config = new HttpSourceConfiguration
{
    BaseUri = new Uri("https://api.github.com/user/repos"),
    Headers = { ["Accept"] = "application/vnd.github+json" },
    Auth = new BearerTokenAuthProvider(githubToken),
    Pagination = new LinkHeaderPaginationStrategy(),
    Timeout = TimeSpan.FromSeconds(30),
};

var source = new HttpSourceNode<Repository>(config, httpClientFactory);
```

### Example: Reading with Nested Items (ItemsJsonPath)

```csharp
public sealed record Product
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public decimal Price { get; set; }
}

var config = new HttpSourceConfiguration
{
    BaseUri = new Uri("https://api.example.com/products"),
    Auth = new ApiKeyAuthProvider("X-API-Key", apiKey),
    Pagination = new OffsetPaginationStrategy(),
    // API response is {"items": [{...}, {...}], "total": 100}
    ItemsJsonPath = "items",
};

var source = new HttpSourceNode<Product>(config, httpClientFactory);
```

### Example: Custom Request Headers and Body

```csharp
var config = new HttpSourceConfiguration
{
    BaseUri = new Uri("https://api.example.com/search"),
    RequestMethod = HttpMethod.Post,
    Headers = { ["X-Custom-Header"] = "custom-value" },
    RequestBodyFactory = uri => new StringContent(
        """{"query": "example"}""",
        Encoding.UTF8, "application/json"),
    RequestCustomizer = async (request, ct) =>
    {
        // Add correlation ID or other dynamic headers
        request.Headers.Add("X-Correlation-ID", Guid.NewGuid().ToString());
        await ValueTask.CompletedTask;
    },
};

var source = new HttpSourceNode<SearchResult>(config, httpClientFactory);
```

## `HttpSinkNode<T>`

The `HttpSinkNode<T>` writes items to a REST API via POST, PUT, or PATCH, with optional batching and idempotency support.

### Sink Configuration

```csharp
public sealed record HttpSinkConfiguration
{
    // Either Uri or UriFactory must be set
    public Uri? Uri { get; init; }
    public Func<object, Uri>? UriFactory { get; init; }

    // Optional: HTTP method (default: Post)
    public SinkHttpMethod Method { get; init; } = SinkHttpMethod.Post;

    // Optional: Fixed headers on every request
    public Dictionary<string, string> Headers { get; init; } = [];

    // Optional: Named HttpClient from IHttpClientFactory
    public string? HttpClientName { get; init; }

    // Optional: Per-request timeout (default: 30 seconds)
    public TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(30);

    // Optional: Items to buffer before flushing (default: 1)
    public int BatchSize { get; init; } = 1;

    // Optional: JSON key wrapping the batch array
    public string? BatchWrapperKey { get; init; }

    // Optional: Custom JSON serialization options
    public JsonSerializerOptions? JsonOptions { get; init; }

    // Optional: Capture non-2xx responses instead of throwing
    public bool CaptureErrorResponses { get; init; }

    // Optional: Authentication provider
    public IHttpAuthProvider Auth { get; init; } = NullAuthProvider.Instance;

    // Optional: Rate limiter
    public IRateLimiter RateLimiter { get; init; } = NullRateLimiter.Instance;

    // Optional: Retry strategy
    public IHttpRetryStrategy RetryStrategy { get; init; } = ExponentialBackoffHttpRetryStrategy.Default;

    // Optional: Request customizer hook
    public Func<HttpRequestMessage, CancellationToken, ValueTask>? RequestCustomizer { get; init; }

    // Optional: Idempotency key factory
    public Func<object, string>? IdempotencyKeyFactory { get; init; }

    // Optional: Header name for idempotency key (default: "Idempotency-Key")
    public string IdempotencyHeaderName { get; init; } = "Idempotency-Key";
}

public enum SinkHttpMethod
{
    Post = 0,
    Put = 1,
    Patch = 2,
}
```

### Constructor Overloads

```csharp
// Using HttpClient directly
public HttpSinkNode(HttpSinkConfiguration configuration, HttpClient httpClient)

// Using IHttpClientFactory (recommended)
public HttpSinkNode(HttpSinkConfiguration configuration, IHttpClientFactory httpClientFactory)

// With full dependency injection
public HttpSinkNode(
    HttpSinkConfiguration configuration,
    IHttpClientFactory httpClientFactory,
    IHttpConnectorMetrics? metrics = null,
    ILogger<HttpSinkNode<T>>? logger = null)
```

### Example: Single-Item POST

```csharp
public sealed record WebhookEvent
{
    public string EventType { get; set; } = string.Empty;
    public string Data { get; set; } = string.Empty;
}

var config = new HttpSinkConfiguration
{
    Uri = new Uri("https://webhook.example.com/events"),
    Method = SinkHttpMethod.Post,
    Headers = { ["X-API-Key"] = apiKey },
    IdempotencyKeyFactory = item => item.EventType + "_" + Guid.NewGuid(),
};

var sink = new HttpSinkNode<WebhookEvent>(config, httpClientFactory);
```

### Example: Batched Requests with Wrapper

```csharp
public sealed record OrderItem
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int Quantity { get; set; }
}

var config = new HttpSinkConfiguration
{
    Uri = new Uri("https://api.example.com/orders/bulk"),
    Method = SinkHttpMethod.Post,
    BatchSize = 50,
    BatchWrapperKey = "items",  // Produces {"items": [{...}, {...}]}
    Auth = new BearerTokenAuthProvider(token),
};

var sink = new HttpSinkNode<OrderItem>(config, httpClientFactory);
```

### Example: Per-Item URI (PUT)

```csharp
public sealed record UserUpdate
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
}

var config = new HttpSinkConfiguration
{
    UriFactory = item => new Uri($"https://api.example.com/users/{((UserUpdate)item).Id}"),
    Method = SinkHttpMethod.Put,
    Auth = new BasicAuthProvider("username", "password"),
};

var sink = new HttpSinkNode<UserUpdate>(config, httpClientFactory);
```

### Example: Error Handling with `CaptureErrorResponses`

```csharp
var config = new HttpSinkConfiguration
{
    Uri = new Uri("https://api.example.com/data"),
    Method = SinkHttpMethod.Post,
    CaptureErrorResponses = true,  // Don't throw on non-2xx responses
};

var sink = new HttpSinkNode<DataItem>(config, httpClientFactory);
// Non-2xx responses are captured in the result for inspection and processing
```

## Dependency Injection

Register the HTTP connector in your service collection for automatic lifecycle management:

```csharp
services.AddHttpClient();
services.AddHttpConnector();
```

### Configuring Named HttpClients

For advanced scenarios, configure named HTTP clients with custom handlers and policies:

```csharp
services.AddHttpClient()
    .AddHttpConnectorClient("github", client =>
    {
        client.DefaultRequestHeaders.Add("User-Agent", "MyApp/1.0");
        client.DefaultRequestHeaders.Add("Accept", "application/vnd.github+json");
    })
    .ConfigureHttpClient(client => 
    {
        client.Timeout = TimeSpan.FromSeconds(60);
    })
    .AddTransientHttpErrorPolicy(p => 
        p.WaitAndRetryAsync(3, count => TimeSpan.FromSeconds(Math.Pow(2, count))));

// Use in configuration
var config = new HttpSourceConfiguration
{
    BaseUri = new Uri("https://api.github.com/repos/dotnet/runtime/releases"),
    HttpClientName = "github",  // Use the named client
};
```

## Advanced Examples

### Complete Pipeline with GitHub API

```csharp
using NPipeline.Connectors.Http.Auth;
using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Nodes;
using NPipeline.Connectors.Http.Pagination;

public sealed record githubRelease
{
    public string TagName { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public int Downloads { get; set; }
}

public sealed record ReleaseReport
{
    public string RepositoryName { get; set; } = string.Empty;
    public int TotalReleases { get; set; }
    public int TotalDownloads { get; set; }
}

// Configure source
var sourceConfig = new HttpSourceConfiguration
{
    BaseUri = new Uri("https://api.github.com/repos/dotnet/runtime/releases"),
    Headers = { ["Accept"] = "application/vnd.github+json" },
    Auth = new BearerTokenAuthProvider(githubToken),
    Pagination = new LinkHeaderPaginationStrategy(),
    Timeout = TimeSpan.FromSeconds(30),
};

// Configure sink for reporting
var sinkConfig = new HttpSinkConfiguration
{
    Uri = new Uri("https://analytics.example.com/reports"),
    Method = SinkHttpMethod.Post,
    Auth = new ApiKeyAuthProvider("X-API-Key", reportingApiKey),
};

var pipeline = new PipelineBuilder()
    .AddSource(new HttpSourceNode<githubRelease>(sourceConfig, httpClientFactory), "github_source")
    .AddTransform("release_aggregation", async (IAsyncEnumerable<githubRelease> releases, CancellationToken ct) =>
    {
        var report = new ReleaseReport
        {
            RepositoryName = "dotnet/runtime",
            TotalReleases = 0,
            TotalDownloads = 0,
        };

        await foreach (var release in releases.WithCancellation(ct))
        {
            report.TotalReleases++;
            report.TotalDownloads += release.Downloads;
        }

        yield return report;
    })
    .AddSink(new HttpSinkNode<ReleaseReport>(sinkConfig, httpClientFactory), "report_sink")
    .Build();

await runner.RunAsync<MyPipelineDefinition>();
```

### Retry and Rate Limiting Example

```csharp
using NPipeline.Connectors.Http.Retry;
using System.Threading.RateLimiting;

var config = new HttpSourceConfiguration
{
    BaseUri = new Uri("https://api.example.com/data"),
    Auth = new BearerTokenAuthProvider(token),
    Pagination = new OffsetPaginationStrategy(),
    
    // Aggressive rate limiting: 5 requests per second
    RateLimiter = new TokenBucketRateLimiter(new TokenBucketRateLimiterOptions
    {
        TokensPerPeriod = 5,
        Period = TimeSpan.FromSeconds(1),
        BucketCapacity = 5,
    }),
    
    // Custom retry strategy
    RetryStrategy = new ExponentialBackoffHttpRetryStrategy
    {
        MaxRetries = 3,
        BaseDelayMs = 1000,
        MaxDelayMs = 30_000,
        JitterFactor = 0.1,
        RetryableStatusCodes = new HashSet<HttpStatusCode>
        {
            HttpStatusCode.TooManyRequests,
            HttpStatusCode.ServiceUnavailable,
            HttpStatusCode.BadGateway,
            HttpStatusCode.GatewayTimeout,
        },
    },
    
    MaxPages = 100,  // Safety limit
};
```

## Error Handling

The HTTP connector handles errors in several ways:

- **Retryable Errors** (429, 503, 504, etc.): Automatically retried with exponential backoff
- **`Retry-After` Headers**: Respected on 429 responses for rate-limited APIs
- **Non-2xx Responses**: Thrown as exceptions by default; captured in results if `CaptureErrorResponses` is true
- **Timeout**: Configurable per-request; defaults to 30 seconds
- **Size Limits**: Optional `MaxResponseBytes` to protect against oversized payloads

## Sample Application

See [Sample_HttpConnector](https://github.com/npipeline/NPipeline/blob/main/samples/Sample_HttpConnector) for a complete example that fetches GitHub releases and posts summaries to a Slack webhook.

To run the sample:

```bash
GITHUB_TOKEN=ghp_... SLACK_WEBHOOK=https://hooks.slack.com/... \
dotnet run --project samples/Sample_HttpConnector
```

## Performance Considerations

- **Pagination**: Use cursor-based pagination when available; offset pagination is less efficient at scale
- **Batching**: Larger `BatchSize` values reduce HTTP requests but increase memory usage
- **Rate Limiting**: Configure to match API rate limits to avoid expensive retries
- **Connection Reuse**: Use `IHttpClientFactory` with named clients for proper connection pooling
- **Timeouts**: Adjust `Timeout` based on expected API response times

## Troubleshooting

**Issue: 401 Unauthorized**

- Verify authentication credentials are correct
- Check that token hasn't expired
- Ensure auth provider is configured correctly

**Issue: 429 Too Many Requests**

- Enable rate limiting with `RateLimiter`
- Adjust `TokensPerPeriod` and `Period` to match API limits
- Increase `BatchSize` on sinks to reduce request frequency

**Issue: Large Memory Usage on Source**

- Reduce `MaxPages` to limit pagination depth
- Set `MaxResponseBytes` to prevent oversized responses
- Consider chunking the API response with `ItemsJsonPath`

**Issue: Retries Exhausted**

- Check `RetryStrategy.MaxRetries` and delay settings
- Verify API endpoint is responding
- Enable logging to see detailed retry attempts
