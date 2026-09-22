---
title: "TypeSafe AI Jev decisions"
description: "Make typed, confidence-aware routing decisions with TypeSafe AI System One and Jev."
order: 14
---

The `NPipeline.Extensions.AI.Decisions.Jev` package calls TypeSafe AI's native System One API. Choice questions integrate with NPipeline's typed routing API. Choice, Score, and Noul questions are also available through the lower-level `IJevClient` interface.

## Install the package

```bash
dotnet add package NPipeline.Extensions.AI.Decisions.Jev
```

## Create a Jev client

Read credentials from a secure configuration source and reuse an application-managed `HttpClient`:

```csharp
var apiKey = Environment.GetEnvironmentVariable("TYPESAFE_API_KEY")
    ?? throw new InvalidOperationException("TYPESAFE_API_KEY is required.");

var jevClient = new JevClient(httpClient, new JevClientOptions
{
    ApiKey = apiKey,
});
```

The caller owns `HttpClient`. `JevClient` doesn't modify its default headers or dispose it. Authorization is attached to each request.

## Add a Choice route

`AddJevRoute` creates a typed Choice classifier and delegates graph construction to the provider-neutral `AddAIRoute` implementation:

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
    .WhenLabel(TicketRoute.Other, reviewSink)
    .Otherwise(reviewSink);

builder.Connect(source, route);
```

Include an explicit `other` option when the listed choices aren't exhaustive. `Otherwise` handles low-confidence decisions or route policies that don't match; it doesn't add a model option.

The example thresholds are illustrative. Evaluate representative data and languages before selecting thresholds.

## Configure the Choice classifier

The builder validates its configuration before graph execution:

| Method | Required | Description |
| --- | --- | --- |
| `WithState(Func<TInput,object?>)` | Yes | Projects the pipeline item into string or structured JSON state. Return `JsonNode` for direct control of the JSON value. |
| `WithInstructions(string)` | Yes | Sets text instructions for the Choice question. |
| `WithInstructions(JsonNode)` | Yes, alternative | Sets structured JSON instructions. |
| `AddChoice(label, wireName, description)` | At least two | Maps a typed label to the provider wire name and optional text description. |
| `AddChoice(label, wireName, JsonNode?)` | At least two, alternative | Maps a label to structured JSON criteria. |
| `WithModel(string)` | No | Overrides the client's default model for this classifier. |
| `WithQuestionId(string)` | No | Sets the answer correlation key. The default is `route`. |

Labels and wire names must be unique. A Choice question accepts at most 255 configured options.

The classifier builds and freezes the label mappings and question map once. Per item, it serializes only the projected state, sends one System One request, and maps the returned wire names back to typed labels.

## Inspect classification metadata

`JevChoiceClassifier` returns `AIClassification<TLabel>` with the following data:

- The selected typed label.
- Jev's confidence value.
- A probability for every configured choice.
- Provider metadata with `Provider` set to `typesafe`.
- The concrete response model, TypeSafe request ID, and input/output token usage.

The classifier rejects responses that omit the configured answer, return the wrong answer type, select an unknown wire name, or omit a configured probability.

Use the Decisions package's `ClassificationHandle` when another pipeline node needs the complete classification and metadata. Ordinary route targets receive the original pipeline item.

## Evaluate System One questions directly

Use `IJevClient.EvaluateAsync` when you need several independent judgments about one state or need Score or Noul answers. Place the questions in one request so the model evaluates the shared state once:

```csharp
using System.Text.Json;
using System.Text.Json.Nodes;

var state = JsonSerializer.SerializeToNode(new
{
    ticket.Subject,
    ticket.Message,
    ticket.CustomerTier,
});

var questions = new Dictionary<string, JevQuestion>
{
    ["route"] = new JevChoiceQuestion(
        "Which team should handle this ticket?",
        new Dictionary<string, string?>
        {
            ["billing"] = "Charges, invoices, and refunds",
            ["technical"] = "Bugs, outages, and integrations",
            ["other"] = "None of the other routes apply",
        }),
    ["urgency"] = new JevScoreQuestion(
        "How urgent is the ticket?",
        ["low", "medium", "high", "critical"]),
    ["needs_human"] = new JevNoulQuestion(
        "Does this ticket require human review?"),
};

var response = await jevClient.EvaluateAsync(
    state,
    questions,
    cancellationToken: cancellationToken);

var routeAnswer = (JevChoiceAnswer)response.Answers["route"];
var urgencyAnswer = (JevScoreAnswer)response.Answers["urgency"];
var reviewAnswer = (JevNoulAnswer)response.Answers["needs_human"];
```

The question and answer types are:

| Question | Answer | Result |
| --- | --- | --- |
| `JevChoiceQuestion` | `JevChoiceAnswer` | Selected option, confidence, and full probability distribution. |
| `JevScoreQuestion` | `JevScoreAnswer` | Weighted score, confidence, level legend, and level probabilities. |
| `JevNoulQuestion` | `JevNoulAnswer` | Probability that the yes-or-no statement is true. |

String constructors cover common text instructions and criteria. Constructors that accept `JsonNode` support structured instructions and criteria.

## Configure the HTTP client

`JevClientOptions` provides these settings:

| Property | Default | Description |
| --- | --- | --- |
| `ApiKey` | Required | TypeSafe API key. It is never included in exceptions. |
| `BaseUri` | `https://api.typesafe.ai` | TypeSafe API root. It must be absolute. |
| `DefaultModel` | `jev-latest` | Model used when a request or classifier doesn't override it. |
| `AttemptTimeout` | 10 seconds | Timeout applied to each complete HTTP attempt, including response deserialization. |
| `Retry` | `JevRetryPolicy` defaults | Client retry policy. |
| `TimeProvider` | `TimeProvider.System` | Time provider used for retry delays and date-based retry headers. |

`jev-latest` is a moving alias. Use `WithModel` on a classifier, or the `model` argument to `EvaluateAsync`, to pin a concrete model when calibrated thresholds must remain tied to one version. The response always reports the concrete model that answered.

## Configure reliability

The default retry policy is:

| Property | Default | Description |
| --- | --- | --- |
| `MaxRetries` | `2` | Maximum retries after the initial attempt. Set to `0` to disable client retries. |
| `InitialDelay` | 500 milliseconds | First exponential-backoff delay. |
| `MaxDelay` | 5 seconds | Maximum client-calculated backoff delay. |
| `MaxRetryAfter` | 1 minute | Maximum accepted server-specified delay. |
| `JitterFactor` | `0.25` | Fraction of calculated delay that random jitter can subtract. |

The client retries connection failures, per-attempt timeouts, HTTP `408`, `429`, and `5xx` responses. It honors nonnegative `retry-after-ms` and `Retry-After` values that don't exceed `MaxRetryAfter`; otherwise it uses bounded exponential backoff with jitter.

Set `MaxRetries` to `0` when an NPipeline resilience policy owns retries. This avoids multiplying provider-level and pipeline-level attempts:

```csharp
var options = new JevClientOptions
{
    ApiKey = apiKey,
    Retry = new JevRetryPolicy { MaxRetries = 0 },
};
```

Caller cancellation stops HTTP attempts and retry delays. A final per-attempt timeout raises `TimeoutException`.

## Handle API errors

Nonretryable responses and exhausted retryable responses raise `JevApiException`, which derives from `AIInvocationException`.

| Property | Description |
| --- | --- |
| `StatusCode` | HTTP status returned by TypeSafe. |
| `ResponseBody` | Unmodified error body, when present. |
| `Headers` | Response and content headers for diagnostics. |
| `Provider` | Always `typesafe`. |
| `Model` | Model requested for the failed invocation. |
| `RequestId` | `x-typesafe-request-id`, when returned. |

The exception doesn't include the API key. Cancellation initiated by the caller propagates unchanged.

## Tune reliability and decision quality

Apply these guidelines to Jev workloads:

- Send one request per pipeline item unless your application explicitly batches unrelated records outside this package.
- Put independent questions about the same state in one request.
- Remove state fields that aren't relevant to the judgment.
- Keep arithmetic, date comparison, deterministic policy, and text generation in application code.
- Include an explicit no-match Choice option when the listed choices aren't exhaustive.
- Calibrate confidence and probability thresholds against representative production-like data.
- Re-evaluate thresholds when changing a pinned model or moving with `jev-latest`.
- Validate non-English workloads separately.
- Bound pipeline concurrency to the current TypeSafe request and token limits for your account.

## Related documentation

- [AI package overview](ai.md)
- [Typed AI decisions and routing](ai-decisions.md)
- [Resilience policies](../error-handling/resilience-policies.md)
- [Parallel execution](parallelism.md)
