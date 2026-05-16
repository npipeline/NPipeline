---
title: "AI"
description: "Transform and enrich pipeline data using any LLM provider through the IChatClient abstraction from Microsoft.Extensions.AI."
order: 11
---

# AI

The `NPipeline.Extensions.AI` package adds AI-powered transform and enrichment nodes to NPipeline. Each node sends data to a language model using `IChatClient` from `Microsoft.Extensions.AI.Abstractions`, deserializes the response into a strongly typed .NET object, and routes the result downstream.

```bash
dotnet add package NPipeline.Extensions.AI
```

Install a provider package alongside it:

```bash
# OpenAI / Azure OpenAI
dotnet add package Microsoft.Extensions.AI.OpenAI

# Ollama (local models)
dotnet add package OllamaSharp
```

> 📝 **Provider-agnostic by design.** The extension depends only on the `IChatClient` interface. Any implementation works — OpenAI, Azure OpenAI, Anthropic, Ollama, OpenRouter, LM Studio, or your own.

## Quick Start

```csharp
using Microsoft.Extensions.AI;
using NPipeline.Extensions.AI;

public record Comment(string Text, string Author);
public record ClassificationResult(string Category, float Confidence);

public class ClassificationPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var chatClient = /* resolve your IChatClient */;

        var source  = builder.AddSource<CommentSource, Comment>("source");
        var classify = builder.AddAITransform<Comment, ClassificationResult>(chatClient, options => options
            .WithSystemPrompt("Classify the following text into one of: Greeting, Question, Complaint, Spam. Return JSON with 'category' and 'confidence' fields.")
            .WithItemTemplate(comment => $"Text: {comment.Text}")
            .WithNativeStructuredOutput()
            .WithTemperature(0.1f));
        var sink    = builder.AddSink<ResultSink, ClassificationResult>("sink");

        builder.Connect(source, classify);
        builder.Connect(classify, sink);
    }
}
```

## How It Works

Every AI node follows the same execution model:

1. A template delegate formats the input into a user message string.
2. The node sends a two-message exchange to the LLM: `[system prompt, user message]`.
3. The raw response text is deserialized with `System.Text.Json` into the target type.
4. On success the typed result flows downstream. On failure an `AITransformException` is thrown.

Each call is **stateless** — no conversation history is maintained between items. Every item or batch starts a fresh `[system, user]` session.

## Node Reference

There are six nodes, split across two families.

### Transform Family

Transform nodes replace the input type with a new output type.

#### `AITransformNode<TIn, TOut>`

One LLM call per item. Use for per-record classification, extraction, summarization, or format conversion.

```csharp
builder.AddAITransform<RawReview, SentimentScore>(chatClient, options => options
    .WithSystemPrompt("You are a sentiment analyser. Return JSON with 'score' (0.0–1.0) and 'label' (Positive/Negative/Neutral).")
    .WithItemTemplate(r => $"Review: {r.Body}")
    .WithNativeStructuredOutput()
    .WithMaxOutputTokens(64));
```

Returns a `TransformNodeHandle<TIn, TOut>`.

#### `AIBatchedTransformNode<TIn, TOut>`

One LLM call for the whole batch. The model receives all items in a single user message and must return an array of results in the same order.

The node enforces a 1:1 count between input and output — if the model returns a different number of results, it throws `AITransformException` with a count mismatch message.

```csharp
builder.AddAIBatchedTransform<Comment, ClassificationResult>(chatClient, options => options
    .WithSystemPrompt("Classify each comment below. Return a JSON array, one object per comment, with 'category' and 'confidence'.")
    .WithBatchTemplate(batch =>
    {
        var lines = batch.Select((c, i) => $"{i + 1}. {c.Text}");
        return string.Join("\n", lines);
    }));
```

Returns a `TransformNodeHandle<IReadOnlyCollection<TIn>, IReadOnlyCollection<TOut>>`. Pair this with `AddBatcher` and `AddUnbatcher` to buffer items before sending and fan results back out individually.

#### `AIBatchedStreamTransformNode<TIn, TOut>`

Handles the full buffering and fan-out internally. Items arrive as a stream, are collected into batches of `BatchSize`, each batch is sent to the LLM, and the results are yielded back as individual items. A `BatchTimeout` flushes incomplete batches when items arrive slowly.

```csharp
builder.AddAIBatchedStreamTransform<Comment, ClassificationResult>(chatClient, options => options
    .WithSystemPrompt("Classify each comment. Return a JSON array, one object per comment, with 'category' and 'confidence'.")
    .WithBatchTemplate(batch => string.Join("\n", batch.Select((c, i) => $"{i + 1}. {c.Text}")))
    .WithBatchSize(32)
    .WithBatchTimeout(TimeSpan.FromSeconds(2)));
```

Returns a `TransformNodeHandle<TIn, TOut>` — the batching is invisible to the rest of the pipeline. Use this when you want the throughput benefits of batching without changing the upstream/downstream node types.

### Enrich Family

Enrich nodes keep the original item type. The LLM produces a separate field type (`TField`), and a `ResultMapper` delegate splices it back onto the input item.

#### `AIEnrichNode<TIn, TField>`

One LLM call per item, result merged via `ResultMapper`.

```csharp
public record Article(string Title, string Body, string? Summary);
public record SummaryResult(string Summary);

builder.AddAIEnrich<Article, SummaryResult>(chatClient, options => options
    .WithSystemPrompt("Summarise the article in one sentence. Return JSON with a 'summary' field.")
    .WithItemTemplate(a => $"Title: {a.Title}\n\n{a.Body}")
    .WithResultMapper((article, result) => article with { Summary = result.Summary })
    .WithMaxOutputTokens(128));
```

Returns a `TransformNodeHandle<TIn, TIn>`.

#### `AIBatchedEnrichNode<TIn, TField>`

One LLM call for the whole batch. The model returns one `TField` per input item; the `ResultMapper` is called once per pair to produce the enriched item. Count parity is enforced — a mismatch throws `AITransformException`.

```csharp
builder.AddAIBatchedEnrich<Article, SummaryResult>(chatClient, options => options
    .WithSystemPrompt("Summarise each article in one sentence. Return a JSON array, one object per article, with a 'summary' field.")
    .WithBatchTemplate(batch => string.Join("\n\n", batch.Select((a, i) => $"Article {i + 1}: {a.Title}\n{a.Body}")))
    .WithResultMapper((article, result) => article with { Summary = result.Summary }));
```

Returns a `TransformNodeHandle<IReadOnlyCollection<TIn>, IReadOnlyCollection<TIn>>`.

#### `AIBatchedStreamEnrichNode<TIn, TField>`

Stream-level enrichment with internal batching. Combines the automatic buffering of `AIBatchedStreamTransformNode` with the in-place field merging of `AIBatchedEnrichNode`.

```csharp
builder.AddAIBatchedStreamEnrich<Article, SummaryResult>(chatClient, options => options
    .WithSystemPrompt("Summarise each article in one sentence. Return a JSON array with a 'summary' field per article.")
    .WithBatchTemplate(batch => string.Join("\n\n", batch.Select((a, i) => $"Article {i + 1}: {a.Title}\n{a.Body}")))
    .WithResultMapper((article, result) => article with { Summary = result.Summary })
    .WithBatchSize(16)
    .WithBatchTimeout(TimeSpan.FromSeconds(5)));
```

Returns a `TransformNodeHandle<TIn, TIn>`.

## AI Routing

AI routing combines an enrich node with a conditional route node. The item is enriched by the LLM first, then dispatched to a downstream node based on predicates that test the enriched item. This lets the model make the branching decision rather than hard-coded logic.

### How AI Routing Works

Internally `AddAIRoute` registers two nodes and wires them together:

1. An `AIEnrichNode<TIn, TField>` that calls the LLM and splices the result onto the item via `ResultMapper`.
2. A `RouteNode<TIn>` that evaluates `When` predicates against the enriched item and dispatches to the first matching branch.

The `ResultMapper` runs **before** any predicate is tested, so your predicates always see the AI-classified item. Connect your upstream node to `AIRouteBuilder.EnrichHandle`.

### `AddAIRoute<TIn, TField>`

One LLM call per item. Use when throughput demands are modest or items arrive individually.

```csharp
public record Comment(string Text, string Author, string? Sentiment = null);
public record SentimentResult(string Label, float Score);

var route = builder.AddAIRoute<Comment, SentimentResult>(chatClient, opts => opts
    .WithSystemPrompt("Classify the sentiment of each comment. Return JSON with 'label' (Positive/Negative/Neutral) and 'score' (0.0–1.0).")
    .WithItemTemplate(c => $"Comment: {c.Text}")
    .WithResultMapper((c, r) => c with { Sentiment = r.Label }));

// Define branches — predicates test the enriched item (Sentiment is already set)
var positiveHandle = builder.AddSink<PositiveSink, Comment>("positive");
var negativeSink   = builder.AddSink<NegativeSink, Comment>("negative");
var reviewHandle   = builder.AddSink<ReviewSink, Comment>("review");

route
    .When(c => c.Sentiment == "Positive", positiveHandle)
    .When(c => c.Sentiment == "Negative", negativeSink)
    .Otherwise(reviewHandle);

// Connect your upstream node to EnrichHandle — this is the entry point of the composite
builder.Connect(source, route.EnrichHandle);
```

### `AddAIBatchedStreamRoute<TIn, TField>`

Stream-level version. Items are buffered into batches, each batch is sent to the LLM in a single call, and the enriched items are dispatched through the route node. Use for high-throughput pipelines where per-item LLM calls are too expensive.

```csharp
var route = builder.AddAIBatchedStreamRoute<Comment, SentimentResult>(chatClient, opts => opts
    .WithSystemPrompt("Classify the sentiment of each comment. Return a JSON array, one object per comment, with 'label' and 'score'.")
    .WithBatchTemplate(batch => string.Join("\n", batch.Select((c, i) => $"{i + 1}. {c.Text}")))
    .WithResultMapper((c, r) => c with { Sentiment = r.Label })
    .WithBatchSize(32)
    .WithBatchTimeout(TimeSpan.FromSeconds(2)));

route
    .When(c => c.Sentiment == "Positive", positiveHandle)
    .When(c => c.Sentiment == "Negative", negativeHandle)
    .Otherwise(reviewHandle);

builder.Connect(source, route.EnrichHandle);
```

### `AIRouteBuilder<T>` API

`AddAIRoute` and `AddAIBatchedStreamRoute` both return an `AIRouteBuilder<T>`. It has a fluent API for wiring branches and two handle properties for pipeline connections.

#### `.When(predicate, target)`

Routes items where `predicate` returns `true` to `target`. Evaluated in declaration order — the item goes to the **first** matching branch only.

```csharp
route
    .When(c => c.Sentiment == "Positive", positiveHandle)
    .When(c => c.Sentiment == "Negative", negativeHandle);
```

#### `.Otherwise(target)`

Routes items that did not match any `When` predicate. If omitted, unmatched items are dropped (standard route node behaviour).

```csharp
route.Otherwise(reviewHandle);
```

#### `.EnrichHandle`

The handle of the internal enrich node. **This is the upstream connection point** — pass it to `builder.Connect(upstream, route.EnrichHandle)`.

#### `.RouteHandle`

The handle of the internal route node. Use this when you need to attach the route node as a source (for example, to inspect or replace the outgoing connections manually) rather than connecting downstream of it.

### Routing Branch Semantics

| Behaviour | Detail |
|-----------|--------|
| **First-match** | Only the first `When` predicate that returns `true` receives the item |
| **Otherwise** | Catches all items that matched no `When`; optional |
| **No match, no `Otherwise`** | Item is dropped — standard route node behaviour |
| **Predicate input** | Predicates always receive the enriched item, after `ResultMapper` has run |

### Named Route Nodes

Pass `name` to control the internal node names (useful for observability and debugging). The enrich and route nodes are registered as `{name}_enrich` and `{name}_route`:

```csharp
builder.AddAIRoute<Comment, SentimentResult>(chatClient, opts => opts
    .WithSystemPrompt("...")
    .WithItemTemplate(c => c.Text)
    .WithResultMapper((c, r) => c with { Sentiment = r.Label }),
    name: "sentiment-route");
// Internal nodes: "sentiment-route_enrich", "sentiment-route_route"
```

## Choosing the Right Node

| Scenario | Node |
|----------|------|
| One item at a time, change type | `AddAITransform` |
| Pre-batched items arrive as a collection, change type | `AddAIBatchedTransform` |
| Stream of items, batch internally, change type | `AddAIBatchedStreamTransform` |
| One item at a time, add a field | `AddAIEnrich` |
| Pre-batched items arrive as a collection, add a field | `AddAIBatchedEnrich` |
| Stream of items, batch internally, add a field | `AddAIBatchedStreamEnrich` |
| LLM classifies each item, route to different branches | `AddAIRoute` |
| LLM classifies a stream in batches, route to different branches | `AddAIBatchedStreamRoute` |

## Configuration Reference

All six extension methods accept an options builder delegate. The options share a common set of properties.

### Common Options (all nodes)

| Builder Method | Type | Required | Description |
|----------------|------|----------|-------------|
| `WithSystemPrompt(string)` | `string` | **Yes** | System prompt sent as the first message |
| `WithItemTemplate(Func<TIn, string>)` | `Func<TIn, string>` | **Yes** *(per-item)* | Formats one item into the user message |
| `WithBatchTemplate(Func<IReadOnlyCollection<TIn>, string>)` | `Func<IReadOnlyCollection<TIn>, string>` | **Yes** *(batch)* | Formats a whole batch into the user message |
| `WithTemperature(float)` | `float?` | No | LLM temperature (not set by default — provider default applies) |
| `WithMaxOutputTokens(int)` | `int?` | No | Maximum tokens in the model's response; must be positive |
| `WithNativeStructuredOutput(bool)` | `bool` | No | Sets `ChatOptions.ResponseFormat = ChatResponseFormat.Json`; defaults to `false` |
| `WithConfigureOptions(Action<ChatOptions>)` | `Action<ChatOptions>?` | No | Advanced callback applied **after** all other options; use for anything not covered above |

### Enrich-Only Options

| Builder Method | Type | Required | Description |
|----------------|------|----------|-------------|
| `WithResultMapper(ResultMapper<TIn, TField>)` | `ResultMapper<TIn, TField>` | **Yes** | Maps the AI-generated `TField` back onto the `TIn` item |

### Stream Batching Options

| Builder Method | Type | Required | Description |
|----------------|------|----------|-------------|
| `WithBatchSize(int)` | `int` | **Yes** | Number of items to buffer before sending; must be positive |
| `WithBatchTimeout(TimeSpan)` | `TimeSpan?` | No | Flush an incomplete batch after this interval; must be positive; defaults to 5 seconds when not set |

All required properties are validated at `Build()` time — misconfigured nodes throw `InvalidOperationException` before the pipeline runs.

### Template Delegates

Templates are plain C# lambdas — no template syntax to learn and no runtime reflection.

```csharp
// Per-item template: include as much context as the model needs
.WithItemTemplate(comment => $"""
    Author: {comment.Author}
    Text: {comment.Text}
    """)

// Batch template: number items so the model can return them in order
.WithBatchTemplate(batch =>
{
    var sb = new StringBuilder();
    int i = 1;
    foreach (var c in batch)
        sb.AppendLine($"{i++}. [{c.Author}] {c.Text}");
    return sb.ToString();
})
```

### ResultMapper

`ResultMapper<TIn, TField>` is a delegate with the signature `TIn(TIn input, TField aiResult)`. The mapper receives the original item and the deserialized AI result and returns the enriched item.

```csharp
// Record with-expression
.WithResultMapper((comment, result) => comment with { Sentiment = result.Label, Score = result.Score })

// Mutable class
.WithResultMapper((order, tags) => { order.Tags = tags.Values; return order; })
```

### Advanced ChatOptions

Use `WithConfigureOptions` to set anything not exposed directly — model identifiers, stop sequences, top-P, and so on. This callback fires **last**, after temperature, max tokens, and response format have been applied, so it can override any of them.

```csharp
.WithConfigureOptions(opts =>
{
    opts.ModelId = "gpt-4o";
    opts.AdditionalProperties ??= new();
    opts.AdditionalProperties["top_p"] = 0.95f;
})
```

## Error Handling

### AITransformException

`AITransformException` is thrown for failures that are caused by the model's response, not by infrastructure. It inherits from `PipelineException` and integrates with NPipeline's resilience and dead-letter systems.

| Property | Type | Description |
|----------|------|-------------|
| `ErrorCode` | `string` | Always `"AI_TRANSFORM_ERROR"` |
| `Message` | `string` | Human-readable description of the failure |
| `InnerException` | `Exception` | The underlying cause |
| `OriginalItem` | `object?` | The item or batch being processed when the failure occurred |
| `PromptSent` | `string?` | The user message sent to the model |
| `ModelUsed` | `string?` | `ChatResponse.ModelId`, if the provider returned one |
| `RawResponse` | `string?` | The raw response text, when deserialization failed |

### Exception Wrapping Policy

The nodes apply different behaviour depending on the exception type:

| Situation | Behaviour |
|-----------|-----------|
| Model returns null or whitespace | Wrapped in `AITransformException` |
| Model returns JSON that deserializes to `null` | Wrapped in `AITransformException` |
| Model returns malformed JSON | `JsonException` wrapped in `AITransformException` |
| Batch and enrich count mismatch | Wrapped in `AITransformException` |
| `ItemTemplate` or `BatchTemplate` delegate throws | Wrapped in `AITransformException` with `OriginalItem` set |
| `ResultMapper` delegate throws | Wrapped in `AITransformException` with `OriginalItem` set |
| `ConfigureOptions` callback throws | Wrapped in `AITransformException` with `PromptSent` set |
| `HttpRequestException`, `TimeoutException` | **Propagated as-is** — handle via resilience policy |
| `OperationCanceledException` | **Propagated as-is** |
| Any other unexpected exception from the client | Wrapped in `AITransformException` |

Infrastructure exceptions propagate unchanged so they can be caught by NPipeline's retry, circuit breaker, and dead-letter mechanisms without losing fidelity.

### Resilience Integration

`AITransformException` is a `PipelineException`, so it participates in the standard resilience pipeline:

```csharp
var policy = ResiliencePolicyBuilder
    .ForNode<AITransformNode<Comment, ClassificationResult>, Comment>()
    .On<HttpRequestException>().Retry(maxRetries: 3)
    .On<TimeoutException>().Retry(maxRetries: 2)
    .On<AITransformException>().DeadLetter()  // bad response → dead-letter, don't retry
    .OnAny().Fail()
    .Build();

builder.AddResiliencePolicy(policy);
```

For per-item error details, catch `AITransformException` in the policy and inspect `OriginalItem`, `PromptSent`, and `RawResponse` for diagnostics.

## Provider Setup

The extension accepts any `IChatClient`. Credentials and model selection live entirely in how you construct the client — never in the extension itself.

### OpenAI

```csharp
using Microsoft.Extensions.AI;
using OpenAI;

IChatClient chatClient = new OpenAIChatClient(
    new OpenAIClient("sk-..."),
    "gpt-4o-mini");
```

### Azure OpenAI

```csharp
IChatClient chatClient = new AzureOpenAIChatClient(
    new AzureOpenAIClient(
        new Uri("https://my-resource.openai.azure.com/"),
        new AzureKeyCredential("...")),
    "my-gpt4o-deployment");
```

### Ollama (local models)

```csharp
using OllamaSharp;

IChatClient chatClient = new OllamaApiClient(
    new Uri("http://localhost:11434"),
    "llama3.2");
```

### OpenAI-Compatible Endpoints

Any endpoint that implements the OpenAI API (OpenRouter, Groq, LM Studio, Together.ai, local vLLM) works via a custom base URI:

```csharp
IChatClient chatClient = new OpenAIChatClient(
    new OpenAIClient(
        new ApiKeyCredential("..."),
        new OpenAIClientOptions { Endpoint = new Uri("https://openrouter.ai/api/v1") }),
    "anthropic/claude-3-5-sonnet");
```

### Dependency Injection

When using `NPipeline.Extensions.DependencyInjection`, register the `IChatClient` with the service container and inject it into your pipeline definition:

```csharp
// Registration (Program.cs)
services.AddChatClient(
    new OpenAIChatClient(new OpenAIClient("sk-..."), "gpt-4o-mini"));

services.AddNPipeline(builder =>
{
    builder.AddPipeline<ClassificationPipeline>();
});

// Pipeline definition
public class ClassificationPipeline : IPipelineDefinition
{
    private readonly IChatClient _chatClient;

    public ClassificationPipeline(IChatClient chatClient)
    {
        _chatClient = chatClient;
    }

    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        builder.AddAITransform<Comment, ClassificationResult>(_chatClient, options => options
            .WithSystemPrompt("Classify.")
            .WithItemTemplate(c => c.Text));
    }
}
```

## Observability

The AI nodes themselves emit no metrics or traces. Observability is delegated to the `IChatClient` middleware pipeline, which supports `UseOpenTelemetry()`, `UseLogging()`, and `UseDistributedCache()` from `Microsoft.Extensions.AI`:

```csharp
IChatClient chatClient = new OpenAIChatClient(...)
    .AsBuilder()
    .UseOpenTelemetry()        // traces, token counts, model metadata
    .UseLogging(loggerFactory) // structured request/response logging
    .UseDistributedCache(cache) // response caching
    .Build();
```

This approach gives you token usage, latency histograms, and request traces for every LLM call without any coupling to NPipeline's observability extension.

## Structured Output and JSON Deserialization

### UseNativeStructuredOutput

Calling `.WithNativeStructuredOutput()` sets `ChatOptions.ResponseFormat = ChatResponseFormat.Json` on every request. This tells the model to return valid JSON. Whether the model constrains the schema to match your output type depends on the provider:

- **OpenAI** uses it for JSON mode; combine with a schema in `ConfigureOptions` for constrained generation.
- **Ollama** passes the flag to the model; support varies by model.
- **Other providers** may ignore it.

When `UseNativeStructuredOutput` is `false` (default), the model can still return valid JSON — just instruct it in the system prompt.

### Deserialization

Responses are deserialized with `System.Text.Json` using `PropertyNameCaseInsensitive = true`. Your output types must be JSON-deserializable. Record types, POCOs, and types decorated with `[JsonPropertyName]` all work.

```csharp
// All of these work as TOut or TField
public record ClassificationResult(string Category, float Confidence);

public class SentimentScore
{
    [JsonPropertyName("score")]
    public float Score { get; init; }
    [JsonPropertyName("label")]
    public string Label { get; init; } = "";
}
```

For batch nodes, the LLM must return a JSON array (`[...]`) with one element per input item.

## Best Practices

### Prompts

- Include the expected JSON schema or a concrete example in the system prompt — most models produce better results when the schema is explicit.
- Number batch items in the template (`1. ...\n2. ...`) so the model maps its responses to the correct input positions.
- Keep prompts concise. Long prompts consume context window and increase cost; shorter prompts typically yield lower latency.

### Batch Size

- Start in the 16–32 range for `AIBatchedStreamTransformNode` and `AIBatchedStreamEnrichNode`.
- Larger batches reduce API call overhead but increase context window usage and the probability of a count mismatch response.
- If a model consistently returns mismatched counts, reduce the batch size.

### Temperature

- Use low temperature (0.0–0.2) for deterministic classification and extraction tasks.
- Leave temperature unset when using providers that set a sensible default, or when using structured output mode.

### MaxOutputTokens

- Set `WithMaxOutputTokens` when you know the expected response size — for example, 64 for a short classification JSON, 256 for a summary sentence. This reduces latency and cost.
- Do not set it lower than the JSON overhead for your output type.

## Next Steps

- [Error Handling](../error-handling/resilience-policies.md) — configure retry, skip, and dead-letter policies
- [Batching and Windowing](../guides/batching-and-windowing.md) — use `AddBatcher` with `AddAIBatchedTransform` for explicit batch control
- [Routing](../guides/routing.md) — understand first-match, multi-match, and otherwise route node semantics
- [Parallelism](parallelism.md) — run multiple AI nodes in parallel for higher throughput
- [Observability](observability.md) — add pipeline-level metrics alongside LLM-level tracing
