# NPipeline.Extensions.AI.Decisions

Provider-neutral typed classification and confidence-aware routing for NPipeline.

## Install the package

```bash
dotnet add package NPipeline.Extensions.AI.Decisions
```

Implement `IAIClassifier<TInput, TLabel>` or install a provider adapter such as `NPipeline.Extensions.AI.Decisions.Jev`.

## Route a classification

```csharp
using NPipeline.Extensions.AI.Decisions;

var route = builder.AddAIRoute<Ticket, TicketRoute>(classifier, "ticket-route")
    .WhenLabel(TicketRoute.Billing, billingSink, minimumConfidence: 0.75)
    .WhenLabel(TicketRoute.Technical, technicalSink, minimumConfidence: 0.65)
    .Otherwise(reviewSink);

builder.Connect(source, route);
```

`AIClassification<TLabel>` contains the selected label, confidence, full probability distribution, and invocation metadata. The route keeps the original item in
an internal envelope and unwraps it before each target. Downstream nodes receive `TInput` without requiring classification fields on your domain model.

## Configure branches

- `WhenLabel` matches the selected label and an optional minimum confidence.
- `WhenProbability` matches any label whose returned probability meets your threshold.
- `When` evaluates the complete `AIClassification<TLabel>`.
- `Otherwise` receives unmatched or low-confidence items.
- `WithMatchMode(RouteMatchMode.AllMatches)` sends an item to every matching branch.

Thresholds must be finite values from `0` through `1`. Choose thresholds from evaluations against your own data; the package doesn't apply a universal default.

## License

This package is licensed under the [Business Source License 1.1](LICENSE.txt).

Production use is free for organizations with four or fewer developers and annual revenue of AUD 5 million or less. Larger organizations require
a [commercial license](https://npipeline.com). Each release converts to the MIT License on its change date.
