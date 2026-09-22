---
title: "AI decisions"
description: "Classify and route pipeline items with typed labels, confidence, and probabilities."
order: 13
---

The `NPipeline.Extensions.AI.Decisions` package provides provider-neutral classification and confidence-aware routing. It separates the decision from the domain item, so downstream nodes receive the original input type without temporary classification properties.

## Install the package

```bash
dotnet add package NPipeline.Extensions.AI.Decisions
```

Implement `IAIClassifier<TInput,TLabel>` directly, or install a provider adapter such as `NPipeline.Extensions.AI.Decisions.Jev`.

## Understand the classification contract

A classifier receives one input and returns a typed decision:

```csharp
public interface IAIClassifier<in TInput, TLabel>
    where TLabel : notnull
{
    ValueTask<AIClassification<TLabel>> ClassifyAsync(
        TInput input,
        CancellationToken cancellationToken = default);
}
```

`AIClassification<TLabel>` contains the complete decision result:

| Property | Description |
| --- | --- |
| `Label` | The selected typed label. |
| `Confidence` | Provider-reported confidence for the selected decision. |
| `Probabilities` | The probability distribution keyed by typed label. |
| `Metadata` | Provider, model, request ID, and optional token usage. |

Invocation metadata uses these shared contracts:

```csharp
public sealed record AIInvocationMetadata(
    string Provider,
    string? Model = null,
    string? RequestId = null,
    AIUsage? Usage = null);

public sealed record AIUsage(
    long? InputTokens = null,
    long? OutputTokens = null);
```

## Implement a classifier

Map the provider response to the typed contract and pass cancellation through to the provider:

```csharp
public sealed class TicketClassifier(IDecisionClient client)
    : IAIClassifier<Ticket, TicketRoute>
{
    public async ValueTask<AIClassification<TicketRoute>> ClassifyAsync(
        Ticket input,
        CancellationToken cancellationToken = default)
    {
        var response = await client.ClassifyAsync(input.Message, cancellationToken);

        return new AIClassification<TicketRoute>(
            response.Label,
            response.Confidence,
            response.Probabilities,
            new AIInvocationMetadata(
                response.Provider,
                response.Model,
                response.RequestId,
                new AIUsage(response.InputTokens, response.OutputTokens)));
    }
}
```

NPipeline doesn't impose a universal confidence threshold. Calibrate thresholds against representative data from your domain.

## Route decisions

```csharp
var route = builder.AddAIRoute<Ticket, TicketRoute>(classifier, "ticket-route")
    .WhenLabel(TicketRoute.Billing, billingSink, minimumConfidence: 0.75)
    .WhenLabel(TicketRoute.Technical, technicalSink, minimumConfidence: 0.65)
    .Otherwise(reviewSink);

builder.Connect(source, route);
```

The returned `AIRouteBuilder<TInput,TLabel>` implements `IInputNodeHandle<TInput>`, so you connect the upstream node directly to it.

Internally, `AIClassificationNode<TInput,TLabel>` creates an `AIClassifiedItem<TInput,TLabel>` envelope containing the original item and its classification. Each route branch passes through an internal unwrap node before reaching your target. For reference types, the target receives the same object instance that entered the classifier.

## Configure route branches

The route builder provides the following methods:

| Method | Behavior |
| --- | --- |
| `WhenLabel(label, target, minimumConfidence)` | Matches the selected label when confidence meets the optional threshold. |
| `WhenProbability(label, minimumProbability, target)` | Matches any label whose probability meets the threshold, including a nonwinning label. |
| `When(predicate, target)` | Evaluates a predicate against the complete `AIClassification<TLabel>`. |
| `Otherwise(target)` | Receives items that matched no branch. Only one fallback can be configured. |
| `WithMatchMode(mode)` | Selects first-match or all-match routing. |

Confidence and probability thresholds must be finite values in the inclusive range `0`-`1`. Invalid values raise `ArgumentOutOfRangeException` while the graph is being built.

### Route by a custom policy

Use `When` when the policy depends on metadata or several probabilities:

```csharp
route.When(
    classification =>
        classification.Metadata.Model == "PINNED_MODEL"
        && classification.Confidence >= 0.8
        && classification.Probabilities[TicketRoute.Billing] >= 0.7,
    billingSink);
```

Replace `PINNED_MODEL` with the model identifier required by your provider policy.

## Match probabilities

Use `WhenProbability` and all-match mode when secondary labels should receive a copy:

```csharp
route
    .WithMatchMode(RouteMatchMode.AllMatches)
    .WhenLabel(TicketRoute.Billing, billingSink, minimumConfidence: 0.6)
    .WhenProbability(TicketRoute.Technical, 0.25, technicalReviewSink)
    .Otherwise(manualReviewSink);
```

For example, Billing can be the selected label while the same item also reaches technical review because the Technical probability exceeds `0.25`.

## Understand branch semantics

Route behavior follows these rules:

| Behavior | Detail |
| --- | --- |
| First match | The first matching branch receives the item. This is the default. |
| All matches | Every matching branch receives the original item. |
| Otherwise | The fallback receives an item only when no ordinary branch matches. |
| No match without `Otherwise` | The route drops the item. |
| Branch order | In first-match mode, configure more specific predicates before broader predicates. |
| Predicate input | Predicates receive `AIClassification<TLabel>`, not the domain item. |

`Otherwise` isn't a low-confidence threshold by itself. An item reaches it when all configured branch predicates return `false`.

## Use advanced handles

`AIRouteBuilder<TInput,TLabel>` exposes two handles for advanced graph construction:

| Property | Type and purpose |
| --- | --- |
| `ClassificationHandle` | Sources `AIClassifiedItem<TInput,TLabel>` immediately after classification. Use it when another node intentionally consumes the item and decision together. |
| `RouteHandle` | Sources the classified envelope from the internal route node. Use it for custom envelope-level graph connections. |

Ordinary route targets should use `WhenLabel`, `WhenProbability`, `When`, or `Otherwise`. These methods add unwrap nodes so targets remain typed as `TInput`.

When you pass `name: "ticket-route"`, the main nodes use `ticket-route_classify` and `ticket-route_route`. Branch unwrap nodes use `ticket-route_unwrap_N`, and conditional outputs use `ticket-route_decision_N`.

## Handle errors and cancellation

`AIClassificationNode` doesn't wrap classifier failures. Provider-specific exceptions and cancellation propagate unchanged so NPipeline resilience policies can apply the correct retry, fallback, or dead-letter behavior.

Avoid retrying low-confidence results as transport failures. Route them to review or apply a domain-specific policy instead.

## Related documentation

- [AI package overview](ai.md)
- [TypeSafe AI Jev decisions](ai-decisions-jev.md)
- [Routing with route nodes](../guides/routing-with-route-node.md)
- [Resilience policies](../error-handling/resilience-policies.md)
- [Parallel execution](parallelism.md)
