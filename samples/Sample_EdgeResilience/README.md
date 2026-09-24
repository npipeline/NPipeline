# Sample: Resilience at the edges

This sample shows how to retry calls to an external API from inside a transform node. The node calls the API through
an `HttpClient` that NResilience makes resilient, and the pipeline's own item retry is turned off for that node. Each
call is then retried by exactly one layer: the one that knows HTTP.

## What the sample does

The pipeline reads five SKUs, looks up each one's stock level in an inventory service, and prints the results. Items
that still fail go to a dead-letter sink. A fake inventory service stands in for the real one, so the sample runs
without a network:

| SKU | The service's answer |
| --- | --- |
| `SKU-1`, `SKU-4` | `200 OK` at once |
| `SKU-2` | `503 Service Unavailable` twice, then `200 OK` |
| `SKU-3` | `404 Not Found` every time |
| `SKU-5` | `503 Service Unavailable` every time |

The sample runs the pipeline twice:

1. **Both layers retry.** The lookup node keeps the Default profile's item retry (three retries of transient
   failures). NResilience makes four attempts at `SKU-5`, gives up, and the node throws. The pipeline's classifier
   treats a 503 as transient, so it retries the item three times, and each retry makes four more attempts. The
   service receives 16 requests for one item.
2. **Only the edge retries.** Item retry is off for the lookup node. The service receives four requests for `SKU-5`,
   and the item goes to the dead-letter sink.

In both runs, `SKU-2` recovers inside the HTTP client, and `SKU-3` is dead-lettered after one request, because a 404 is
an answer, not a transient failure.

## Run the sample

To run the sample, use the .NET CLI:

```bash
cd samples/Sample_EdgeResilience
dotnet run
```

The output is similar to the following:

```text
Run 1: HTTP retries, and pipeline item retry on the same node (not recommended)
...
  Requests the inventory service received:
    SKU-1: 1
    SKU-2: 3
    SKU-3: 1
    SKU-4: 1
    SKU-5: 16
...
Run 2: HTTP retries only, pipeline item retry off for the node (recommended)
...
    SKU-5: 4
```

## How it works

[`Program.cs`](Program.cs) builds one long-lived client from NResilience's HTTP preset. The preset retries 408, 429,
5xx, and network failures, honors `Retry-After`, and doesn't retry POST or PATCH without an idempotency key:

```csharp
var policy = Resilience.Http with { Name = "inventory", Attempts = 4 };

using var inventory = HttpResilience.CreateClient(policy, innerHandler: api);
```

[`StockLookupTransform.cs`](StockLookupTransform.cs) makes one call for each item and judges the final response. It
doesn't retry.

[`StockPipeline.cs`](StockPipeline.cs) turns item retry off for the lookup node, and dead-letters the items that still
fail:

```csharp
builder.WithResilience(lookup, options => options with
{
    ItemRetry = ItemRetryOptions.None,
    OnItemFailure = ItemFailureAction.DeadLetter,
});
```

The sample also turns off NResilience's per-host circuit breaker (`BreakerPerHost = false`), so that both runs send the
same calls. Keep it on in production: it stops calls to a host that keeps failing.

## Why the pipeline doesn't catch this for you

When NResilience gives up by throwing, it marks the exception, and the pipeline's default classifier never retries a
marked exception. This sample's failure is different: NResilience returns the last `503` response, and the node's
`EnsureSuccessStatusCode()` throws a new `HttpRequestException` that carries no mark. The classifier sees an ordinary
503 and treats it as transient. So turn item retry off for a node that calls a resilient client, or classify its
exceptions as permanent:

```csharp
ItemRetry = options.ItemRetry with
{
    Classifier = options.ItemRetry.Classifier.Permanent<HttpRequestException>(),
},
```

## Related documentation

- [The three resilience layers](../../docs/error-handling/three-layers.md)
- [Retry strategies](../../docs/error-handling/retry-strategies.md)
- [Dead-letter queues](../../docs/error-handling/dead-letter-queues.md)
