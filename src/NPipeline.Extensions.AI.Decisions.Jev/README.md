# NPipeline.Extensions.AI.Decisions.Jev

Native TypeSafe AI System One integration for typed, confidence-aware NPipeline decisions.

## Install the package

```bash
dotnet add package NPipeline.Extensions.AI.Decisions.Jev
```

## Create a client

Reuse an application-managed `HttpClient` and keep the API key outside source code:

```csharp
var apiKey = Environment.GetEnvironmentVariable("TYPESAFE_API_KEY")
    ?? throw new InvalidOperationException("TYPESAFE_API_KEY is required.");

var jevClient = new JevClient(httpClient, new JevClientOptions
{
    ApiKey = apiKey,
});
```

The client calls `POST /v1/systemone` and supports mixed Choice, Score, and Noul questions. It uses `jev-latest` by default, applies a 10-second timeout to each
attempt, and retries connection failures, timeouts, HTTP `408`, `429`, and `5xx` responses. Set `Retry.MaxRetries` to `0` when an outer resilience policy owns
retries.

## Route with Jev Choice

```csharp
var route = builder.AddJevRoute<Ticket, TicketRoute>(jevClient, options => options
        .WithState(ticket => new
        {
            ticket.Subject,
            ticket.Message,
            ticket.CustomerTier,
        })
        .WithInstructions("Which team should handle this ticket?")
        .AddChoice(TicketRoute.Billing, "billing", "Charges, invoices, and refunds")
        .AddChoice(TicketRoute.Technical, "technical", "Bugs, outages, and integrations")
        .AddChoice(TicketRoute.Other, "other", "None of the other routes apply"))
    .WhenLabel(TicketRoute.Billing, billingSink, minimumConfidence: 0.75)
    .WhenLabel(TicketRoute.Technical, technicalSink, minimumConfidence: 0.65)
    .Otherwise(reviewSink);

builder.Connect(source, route);
```

The thresholds are examples. Calibrate them against representative data. Include an explicit `other` choice when the listed options aren't exhaustive, and use
`Otherwise` for low-confidence or unmatched results.

`jev-latest` can resolve to a different model version over time. Pin a model with `WithModel` when a calibrated threshold must remain tied to one model.
Validate model quality for your domain and languages before production use.

## Work with metadata

Jev classifications preserve the concrete response model, TypeSafe request ID, token usage, confidence, and every option probability. Use the advanced
classification handle when downstream code needs this envelope.

TypeSafe rate limits can change. Bound pipeline concurrency for your account limits, and place independent questions about one item in a single System One
request.

## License

This package is licensed under the [Business Source License 1.1](LICENSE.txt).

Production use is free for organizations with four or fewer developers and annual revenue of AUD 5 million or less. Larger organizations require
a [commercial license](https://npipeline.com). Each release converts to the MIT License on its change date.
