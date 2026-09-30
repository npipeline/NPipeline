---
title: "HTTP Connector"
description: "Read from and write to REST APIs with pagination, retries, rate limiting and safe telemetry."
order: 6
---

# HTTP Connector

The `NPipeline.Connectors.Http` package reads items from REST APIs and writes items to them. The source follows
pagination (page numbers, offsets, cursors, `Link` headers, next-page URLs, or your own rule) and reads each page's
body once. The sink posts items one at a time or in batches. Both retry through
[NResilience](https://github.com/nresilience/NResilience), take a rate-limiter lease for every attempt, and keep query
values (which can hold API keys) out of metrics, traces, logs and errors.

## Installation

```bash
dotnet add package NPipeline.Connectors.Http
```

## Reading

```csharp
public sealed record Order(int Id, string Customer, decimal Total);

var source = HttpConnector.Source<Order>(new Uri("https://api.example.com/orders"), httpClient, o => o with
{
    ItemsJsonPath = "data",
    Pagination = HttpPagination.Cursor(new CursorPaginationOptions { CursorJsonPath = "meta.next_cursor" }),
    Auth = new BearerTokenAuthProvider(token),
});

builder.AddSource(source, "orders");
```

Pass an `HttpClient` (you keep ownership of it) or an `IHttpClientFactory` (the node creates a client named by
`HttpClientName`). Items are deserialized with System.Text.Json's web defaults: camelCase names, matched
case-insensitively.

### Where the items are

When `ItemsJsonPath` is `null`, each response must be a JSON array. Otherwise the path names the array inside the
response (`data`, `result.items`; a leading `$.` is allowed), matching property names exactly first and then ignoring
case. A path that is missing, or that is not an array, fails the read with an `HttpSourceException` that lists the
properties the response does have. It never quietly returns no items.

A body that is not JSON (an HTML error page returned with 200, for example) fails with the URI, page number, content
type and the first 512 bytes of the body.

### Pagination

| Strategy | Requests | Stops when |
| --- | --- | --- |
| `HttpPagination.None` (default) | One | Always |
| `HttpPagination.PageNumber(new() { PageSize = 100 })` | `?page=1&pageSize=100`, `?page=2…` | A page is shorter than `PageSize`, or `TotalItemsJsonPath` says every item has been read |
| `HttpPagination.Offset(new() { Limit = 100 })` | `?offset=0&limit=100`, `?offset=100…` | A page is shorter than `Limit`, or the total is reached |
| `HttpPagination.Cursor(new() { CursorJsonPath = "meta.next" })` | `?cursor=<value>` | The cursor is missing, `null` or empty. It may be a string or a number |
| `HttpPagination.LinkHeader` | The `Link` header's `rel="next"` URL | There is no next link |
| `HttpPagination.NextUrl("links.next")` | The URL in the body, absolute or relative | The URL is missing, `null` or empty |
| `HttpPagination.Custom(page => …)` | Whatever the delegate returns | It returns `null` |

A strategy holds only configuration: each run pages independently, so one options instance can serve concurrent runs.
A custom strategy gets an `HttpPageContext` with the page's URI, number, status, headers, item count, the run's item
count so far, `TryGetValue(path)` for a value in the body, and `Body` for the whole body:

```csharp
Pagination = HttpPagination.Custom(page =>
    page.TryGetValue("paging.after", out var after) && after.ValueKind == JsonValueKind.String
        ? new Uri($"https://api.example.com/orders?after={after.GetString()}")
        : null),
```

`TryGetValue` parses only the value it finds, so it is cheaper than `Body`, which parses the whole page on first use.
A strategy that returns the page it just read fails the read instead of looping, and `MaxPages` caps a run.

### Items that do not convert

A single bad item doesn't fail the entire page; if a page doesn't convert as a whole, the source reads items individually and only reports the failed ones as errors. By default an error fails the read with a
`RecordMappingException` naming the item's position and JSON path (`$.total`). With `RowErrorHandler`, it can be
skipped or dead-lettered instead, as in the [file connectors](file-connectors.md#row-errors).

### Response size

`MaxResponseBytes` caps each response body. It is checked against `Content-Length` and enforced while the body is read,
before anything buffers it, so an oversized response cannot exhaust memory. A response over the limit fails with
`HttpResponseTooLargeException` and is never retried.

### Source options

| Option | Default | Description |
| --- | --- | --- |
| `BaseUri` | required | The endpoint, without pagination parameters |
| `RequestMethod` | GET | Use POST, with `RequestBodyFactory`, for APIs that take the query in a body |
| `Headers` | none | Headers sent with every request |
| `ItemsJsonPath` | `null` | The path to the items array |
| `Pagination` | `None` | How pages follow one another |
| `JsonOptions` | web defaults | Serializer options for items |
| `TypeInfo` | `null` | Source-generated metadata (`JsonTypeInfo<T>`), for trimming and Native AOT |
| `Auth` | none | See [Authentication](#authentication) |
| `RateLimiter` | `null` | A `System.Threading.RateLimiting.RateLimiter`; see [Rate limiting](#rate-limiting) |
| `Resilience` | `HttpConnectorResilience.Default` | See [Resilience](#resilience) |
| `RequestCustomizer` | `null` | Changes each request just before it is sent, including retries |
| `MaxPages` | `null` | The most pages per run |
| `MaxResponseBytes` | `null` | The largest body to accept |
| `RowErrorHandler` | `null` | What to do with items that do not convert |
| `RawExcerptLength` | 256 | The raw text a row error carries; `0` omits it |

## Writing

```csharp
var sink = HttpConnector.Sink<Order>(new Uri("https://api.example.com/orders"), httpClient, o => o with
{
    BatchSize = 100,
    BatchWrapperKey = "orders",
    IdempotencyKeyFactory = batch => $"orders-{batch[0].Id}-{batch.Count}",
});
```

With `BatchSize = 1` (the default) each item is sent as a JSON object; larger batches are sent as an array, or as
`{"orders":[…]}` with `BatchWrapperKey`. `UriFactory` sends each item to its own endpoint (`PUT /orders/{id}`): a batch
holds consecutive items for one URI, so an item is never sent to another item's endpoint.

### Failed requests

A request that still fails once retries are spent is handled as `FailedRequests` says:

| `FailedRequests` | Effect |
| --- | --- |
| `Fail` (default) | The write fails with an `HttpRequestException` carrying the status and the start of the response |
| `Skip` | A warning is logged and the sink carries on; the items are lost |
| `DeadLetter` | An `HttpRequestFailure<T>` (endpoint, method, status, response excerpt and the items) goes to the pipeline's dead-letter sink, attributed to the sink node, and the sink carries on |

### Sink options

| Option | Default | Description |
| --- | --- | --- |
| `Uri` | required unless `UriFactory` | The endpoint |
| `UriFactory` | `null` | The endpoint for each item |
| `Method` | `Post` | `Post`, `Put` or `Patch` |
| `Headers` | none | Headers sent with every request |
| `BatchSize` | 1 | The most items per request |
| `BatchLinger` | 1 s | A partial batch is sent once this long has passed since its first item |
| `BatchWrapperKey` | `null` | The property to wrap a batch in |
| `JsonOptions`, `TypeInfo` | web defaults | How items are serialized |
| `FailedRequests` | `Fail` | See above |
| `IdempotencyKeyFactory` | `null` | The key for a request, given its items; see [Idempotency](#idempotency) |
| `IdempotencyHeaderName` | `Idempotency-Key` | The header that carries it |
| `Auth`, `RateLimiter`, `Resilience`, `RequestCustomizer` | | As for the source |

## Authentication

| Provider | Usage |
| --- | --- |
| `NullAuthProvider` (default) | No authentication |
| `BasicAuthProvider(user, pass)` | HTTP Basic auth |
| `BearerTokenAuthProvider(token)` | A bearer token in the `Authorization` header |
| `ApiKeyAuthProvider(header, key)` | An API key in a header |
| `ApiKeyAuthProvider(name, key, ApiKeyLocation.QueryString)` | An API key in the query string; it is redacted from telemetry and errors |

Implement `IHttpAuthProvider` for other schemes, such as OAuth2 or HMAC signatures.

## Rate limiting

Pass any `System.Threading.RateLimiting.RateLimiter`. A lease is taken for every attempt, including retries, so a burst
of 429 or 503 retries cannot exceed the limit that caused it:

```csharp
var limiter = new TokenBucketRateLimiter(new TokenBucketRateLimiterOptions
{
    TokenLimit = 10,
    TokensPerPeriod = 10,
    ReplenishmentPeriod = TimeSpan.FromSeconds(1),
    QueueLimit = int.MaxValue,
});

var source = HttpConnector.Source<Order>(uri, httpClient, o => o with { RateLimiter = limiter });
```

A limiter shared between nodes limits them together. A lease the limiter rejects (its queue is full) fails the request.

## Resilience

Both nodes send every request through NResilience's `HttpResilienceHandler`. The `Resilience` option configures it.
The default, `HttpConnectorResilience.Default`, does the following:

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
var source = HttpConnector.Source<Order>(uri, httpClient, o => o with
{
    Resilience = HttpConnectorResilience.Default with
    {
        Attempts = 6,
        AttemptTimeout = TimeSpan.FromSeconds(10),
        Deadline = TimeSpan.FromMinutes(2),
    },
});
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
Retrying a write that the server already applied would duplicate it. Set `IdempotencyKeyFactory` to make POST and
PATCH retryable. To retry a POST or PATCH that is safe to repeat for another reason, mark the request in
`RequestCustomizer`:

```csharp
RequestCustomizer = (request, _) =>
{
    request.MarkRepeatable();
    return ValueTask.CompletedTask;
}
```

## Idempotency

`IdempotencyKeyFactory` gives each request a key, from the items it carries. The key makes POST and PATCH requests
retryable, and every attempt sends the same key, so a server that supports the header discards duplicates:

```csharp
IdempotencyKeyFactory = batch => string.Join(',', batch.Select(order => order.Id)),
```

## Observability

Traces come from the `NPipeline.Connectors.Http` activity source, one client span per attempt, with OpenTelemetry
names: `http.request.method`, `url.full`, `server.address`, `server.port`, `http.response.status_code` and
`error.type`. `url.full` keeps the query's names but replaces its values with `REDACTED`, as do log messages and
exception messages.

`IHttpConnectorMetrics` receives each request, response, retry, rate-limiter wait, page and write, labelled by the
endpoint without its query (`https://api.example.com/orders`), so page numbers and cursors never multiply the number
of series. Items read and written are also counted by the shared `npipeline.connector.rows_read` and `rows_written`
instruments, with `connector` = `http`.

## Dependency Injection

```csharp
services.AddHttpConnector();
services.AddHttpConnectorClient("orders-api", client =>
{
    client.BaseAddress = new Uri("https://api.example.com/");
});
```

`AddHttpConnector` registers `IHttpConnectorMetrics`. The nodes take per-node options, so create them with
`HttpConnector` (passing the container's `IHttpClientFactory`), or register an `HttpSourceOptions<T>` or
`HttpSinkOptions<T>` for each item type the container should build nodes for.

## Next Steps

- [Error Handling](../error-handling/index.md): how pipeline-level retries relate to connector retries
- [Dependency Injection](../guides/dependency-injection.md): `HttpClient` integration
- [JSON Connector](json.md): read and write JSON files
