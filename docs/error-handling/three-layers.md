---
title: "The three resilience layers"
description: "What each resilience layer retries, why sinks aren't run again, and how connector retries and pipeline retries compose."
order: 2
---

# The three resilience layers

NPipeline can repeat work at three layers. Each layer repeats a different unit of work, has its own options, and
waits on its own backoff. Outside the pipeline, the connectors retry their calls to external systems with
[NResilience](https://github.com/nresilience/NResilience). This page explains what each layer covers, and how to
combine them so that each failure is retried by exactly one layer.

## The layers at a glance

The following table summarizes the three layers:

| Layer | Option | Applies to | What it repeats | Off by default? |
| --- | --- | --- | --- | --- |
| L1: item retry | `ItemRetry` | Transform nodes | One item's `TransformAsync` | No. The Default profile retries transient failures three times |
| L2: node restart | `NodeRestart` | Transform nodes | The node's output stream, resumed from its checkpoint | Yes |
| L3: node retry | `NodeRetry` | Any node | The node's setup, before it reads any input | Yes |

All three options are properties of `PipelineResilienceOptions`, in the `NPipeline.Reliability` namespace. You set
them for the whole pipeline, or for one node:

```csharp
builder.WithResilience(options => options with
{
    ItemRetry = ItemRetryOptions.Default with { MaxRetries = 5 },
});

builder.WithResilience(enrich, options => options with
{
    NodeRestart = new NodeRestartOptions { MaxRestarts = 3 },
});
```

Setting `ItemRetry`, `NodeRestart`, or `CircuitBreaker` on a source, sink, aggregate, or join node is a build error,
because none of them can apply there. The [NP9204 analyzer](../analyzers/index.md) reports the same mistake at compile
time.

A [resilience policy](resilience-policies.md) makes the decision at each layer: `DecideItemFailureAsync` for L1,
`DecideRestartAsync` for L2, and `DecideNodeFailureAsync` for L3. With no policy registered,
`DefaultResiliencePolicy` follows the node's options exactly.

## L1: item retry

Item retry runs one item's transform again after it fails. It's the cheapest layer: only the failed item is
processed again, and the rest of the stream carries on.

Only transient failures are retried. The node's `ItemRetry.Classifier` decides which failures are transient, and by
default it treats timeouts, I/O and socket errors, and HTTP 408, 429, and 5xx responses as transient. A programming
error or a malformed record fails at once. When an item isn't retried, or runs out of retries, `OnItemFailure` decides
what happens to it: `Fail` (the default), `Skip`, or `DeadLetter`.

A [circuit breaker](circuit-breakers.md), when you configure one, guards every L1 attempt. It isn't a layer of its
own: it decides whether an attempt is made at all.

For more information about backoff and the classifier, see [Retry strategies](retry-strategies.md).

## L2: node restart

Node restart recovers a transform whose output stream fails mid-stream. The node runs again from its *checkpoint*,
the first input item whose outcome it hasn't delivered, so items it has already delivered aren't processed or
delivered again.

An item failure that L1 doesn't resolve fails the node's stream, and that's what L2 restarts. So the layers escalate:
an item is retried first, and if it still fails, the node restarts from the checkpoint. A failure of the node's input
itself, such as an upstream source losing its connection, isn't restarted, because the input can't be read again.

For more information about the checkpoint and what a restart guarantees, see
[Node restart and the replay window](materialization.md).

## L3: node retry

Node retry executes a whole node again, but only if it failed before it read any input. For a source, that means
before its first item was pulled. This covers setup failures, such as a connection that can't be opened or a resource
that can't be resolved.

Once a node has read input, L3 doesn't execute it again, whatever the policy answers. The policy sees this as
`NodeFailure.InputConsumed`, and `NodeFailure.CanRetry` is `false`.

### Why sinks and sources aren't run again

A node's input is a stream that can be read only once. If a sink failed after it had written 5,000 of 10,000 items,
running it again would need those 10,000 items a second time, and they're gone. Running it on what's left would lose
the item that failed. Running it from the start, if the input could somehow be replayed, would write the first 5,000
items twice.

So a failure after data has flowed is recovered by a layer that knows what's safe to repeat:

- **A transform** recovers through L2, which resumes at the checkpoint instead of starting again.
- **A sink or a source** recovers through its connector, which retries the individual read or write that failed. The
  connector knows the protocol: for example, the HTTP sink doesn't retry a POST without an idempotency key, because
  sending it twice could create two records.

## Connector retries at the edges

The connectors make the calls to external systems, and they retry those calls themselves, below the pipeline's layers.
Each connector's configuration has a single `Resilience` property, which defaults to a preset for that connector.
Depending on the system, the connector does one of the following:

- **The connector owns the protocol**, for example HTTP, the SQL writers, MongoDB, RabbitMQ, and Google Cloud Storage.
  NResilience retries the call, and the client SDK's own retry is off.
- **The SDK has protocol-aware retry**, for example Azure Service Bus, Cosmos DB, the AWS SDK, and the Kafka producer.
  The connector configures the SDK's retry, and NResilience doesn't retry on top of it.

Either way, exactly one layer retries each call. Each connector's page describes its defaults. For example, see
[HTTP](../connectors/http.md#resilience) and [Kafka](../connectors/kafka.md#resilience).

## How connector and pipeline retries compose

When NResilience gives up on a call, it marks the exception it throws with the attempts it made. The pipeline's
default classifier, `RetryClassifier.Default`, treats a marked exception as permanent. This means a connector's
failure isn't retried again by L1 or L3: a connector that made four attempts fails after four attempts, not after
sixteen.

The mark is on the exception NResilience throws. It isn't on an exception your own code throws after NResilience
returns. For example, after the HTTP handler exhausts its retries on a `503 Service Unavailable`, it returns the last
response. If your node then calls `EnsureSuccessStatusCode()`, the new `HttpRequestException` carries no mark, and the
default classifier treats a 503 as transient. The next section explains how to handle this.

Similarly, L2 isn't affected by the mark. A restart runs the node's stream again from its checkpoint, and doesn't depend
on the classifier.

## Call external APIs from a transform

When a transform calls an external API, make the call resilient inside the node, with NResilience, and turn L1 off
for that node. NResilience knows things the pipeline can't know about a call:

- Which HTTP methods are safe to repeat.
- What `Retry-After` and quota headers ask for.
- That a request message must be rebuilt for each attempt.
- That one host being down shouldn't stop calls to the others. NResilience gives each host its own circuit breaker.

The following example creates one long-lived client from NResilience's HTTP preset, and turns item retry off for the
node that uses it:

```csharp
using NPipeline.Reliability;
using NResilience;

// Create the client once and reuse it: the handler keeps per-host state, such as each host's circuit breaker.
var inventory = HttpResilience.CreateClient(Resilience.Http with { Name = "inventory", Attempts = 4 });
inventory.BaseAddress = new Uri("https://inventory.example.com/");

// In the pipeline definition:
builder.WithResilience(lookup, options => options with
{
    ItemRetry = ItemRetryOptions.None,               // The HTTP client already retried.
    OnItemFailure = ItemFailureAction.DeadLetter,    // What still fails goes to the dead-letter sink.
});
```

If you want L1 to keep retrying other failures in that node, classify the client's exceptions as permanent instead:

```csharp
builder.WithResilience(lookup, options => options with
{
    ItemRetry = options.ItemRetry with
    {
        Classifier = options.ItemRetry.Classifier.Permanent<HttpRequestException>(),
    },
});
```

The same approach applies to any resilience library you already use inside a node, such as Polly: let one layer
retry each call.

For a runnable version of this example, see
[`Sample_EdgeResilience`](https://github.com/NPipeline/NPipeline/tree/main/samples/Sample_EdgeResilience). It runs the
same pipeline with and without L1 on the node, and counts the requests the service receives.

## Choose a layer

The following table suggests a layer for common failures:

| Failure | Layer |
| --- | --- |
| One item's call to a service times out | L1, or NResilience inside the node when the call is HTTP |
| One record is malformed | None. It's permanent: set `OnItemFailure` to `Skip` or `DeadLetter` |
| A transform's stream fails mid-stream, for example a lost connection it holds | L2 |
| A node can't open its connection at startup | L3 |
| A sink's write fails mid-stream | The sink's connector |
| A dependency is down for minutes | A [circuit breaker](circuit-breakers.md) on the transform, with `WhenOpen = Pause` if waiting is better than failing |

## Next steps

- [Retry strategies](retry-strategies.md): choose the backoff and the classifier for each layer.
- [Resilience policies](resilience-policies.md): make the decision at each layer in code.
- [Node restart and the replay window](materialization.md): what a restart guarantees.
- [Circuit breakers](circuit-breakers.md): stop calling a dependency that keeps failing.
