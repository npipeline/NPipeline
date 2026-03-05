# NPipeline.Connectors.Http

HTTP/REST connector for NPipeline. Provides source and sink nodes for consuming paginated REST APIs and writing to HTTP endpoints with support for multiple pagination strategies, authentication schemes, rate limiting, retry, and OpenTelemetry observability.

## Features

- **Source & Sink Nodes**: Read from paginated REST APIs and write to HTTP endpoints via POST, PUT, or PATCH
- **Multiple Pagination Strategies**: Offset/page, cursor-based, RFC 5988 Link headers, or custom
- **Authentication Providers**: Bearer token, API key, Basic auth, or custom schemes
- **Batching & Idempotency**: Buffer items before flush and prevent duplicate requests with idempotency keys
- **Retry with Exponential Backoff**: Automatic retry with `Retry-After` header support for rate-limited APIs
- **Token-Bucket Rate Limiting**: Builtin rate limiter for request throttling
- **Request Customization**: Hooks for dynamic headers, correlation IDs, and query parameters
- **OpenTelemetry Integration**: Activity source for distributed tracing and monitoring
- **IHttpClientFactory Integration**: Named clients for connection pooling and resource reuse

## Installation

```bash
dotnet add package NPipeline.Connectors.Http
```

## Quick Start

### Reading from a Paginated REST API

```csharp
using NPipeline.Connectors.Http.Auth;
using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Nodes;
using NPipeline.Connectors.Http.Pagination;

public record GithubRelease(string TagName, string Name, DateTime PublishedAt);

var sourceConfig = new HttpSourceConfiguration
{
    BaseUri = new Uri("https://api.github.com/repos/dotnet/runtime/releases"),
    Headers = { ["User-Agent"] = "MyApp/1.0", ["Accept"] = "application/vnd.github+json" },
    Auth = new BearerTokenAuthProvider(Environment.GetEnvironmentVariable("GITHUB_TOKEN")!),
    Pagination = new LinkHeaderPaginationStrategy(),
    MaxPages = 5,
};

using var httpClient = new HttpClient();
var source = new HttpSourceNode<GithubRelease>(sourceConfig, httpClient);

var pipeline = new PipelineBuilder()
    .AddSource(source, "github_source")
    .AddSink<ConsoleSinkNode<GithubRelease>, GithubRelease>("console_sink")
    .Build();

await pipeline.ExecuteAsync();
```

### Writing to a REST Endpoint

```csharp
using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Nodes;

public record SlackMessage(string Text, string Channel);

var sinkConfig = new HttpSinkConfiguration
{
    Uri = new Uri("https://hooks.slack.com/services/YOUR/WEBHOOK/URL"),
    Method = SinkHttpMethod.Post,
    BatchSize = 10,
};

using var httpClient = new HttpClient();
var sink = new HttpSinkNode<SlackMessage>(sinkConfig, httpClient);

var pipeline = new PipelineBuilder()
    .AddSource(sourceOfMessages, "message_source")
    .AddSink(sink, "slack_sink")
    .Build();

await pipeline.ExecuteAsync();
```

### Using with Dependency Injection

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.Http.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;

var services = new ServiceCollection()
    .AddHttpClient()
    .AddHttpConnector()
    .AddNPipeline(Assembly.GetExecutingAssembly())
    .BuildServiceProvider();

var source = services.GetRequiredService<HttpSourceNode<GithubRelease>>();
var sink = services.GetRequiredService<HttpSinkNode<SlackMessage>>();
```

## Authentication Providers

- **`BearerTokenAuthProvider`**: OAuth2 bearer tokens or API tokens (static or async factory)
- **`ApiKeyAuthProvider`**: API key in a named header or query-string parameter
- **`BasicAuthProvider`**: RFC 7617 Basic auth (username/password)
- **`NullAuthProvider`**: No authentication

Implement `IHttpAuthProvider` for custom schemes (OAuth2 PKCE, mTLS, AWS Signature V4, etc.).

## Pagination Strategies

- **`NoPaginationStrategy`**: Single request, no pagination
- **`OffsetPaginationStrategy`**: Manages `page`/`pageSize` query parameters
- **`CursorPaginationStrategy`**: Cursor-based pagination with JSON path token extraction
- **`LinkHeaderPaginationStrategy`**: RFC 5988 `Link` header pagination (GitHub-compatible)

Implement `IPaginationStrategy` for custom pagination schemes.

## Documentation

For detailed configuration reference, examples, and advanced usage, see the [HTTP Connector documentation](../../docs/connectors/http.md).

## Sample Application

See [`samples/Sample_HttpConnector`](../../samples/Sample_HttpConnector) for a complete example that fetches GitHub releases and posts summaries to a Slack webhook.

```bash
GITHUB_TOKEN=ghp_... SLACK_WEBHOOK=https://hooks.slack.com/... \
dotnet run --project samples/Sample_HttpConnector
```
