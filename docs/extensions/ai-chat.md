---
title: "AI chat"
description: "Transform and enrich pipeline data through Microsoft.Extensions.AI.IChatClient."
order: 12
---

The `NPipeline.Extensions.AI.Chat` package transforms and enriches pipeline data through any `Microsoft.Extensions.AI.IChatClient` implementation. Each node formats input as a chat request, calls the model, and deserializes the response into a strongly typed .NET value.

## Install the package

```bash
dotnet add package NPipeline.Extensions.AI.Chat
```

Install and configure a provider package that supplies `IChatClient`. The NPipeline package depends only on the provider-neutral abstraction.

## Transform data

Use `AddChatTransform` when the model response becomes a different pipeline type:

```csharp
using NPipeline.Extensions.AI.Chat;

public record Comment(string Text, string Author);
public record ClassificationResult(string Category, double Confidence);

var classify = builder.AddChatTransform<Comment, ClassificationResult>(chatClient, options => options
    .WithSystemPrompt("Classify the comment as Greeting, Question, Complaint, or Spam.")
    .WithItemTemplate(comment => $"Text: {comment.Text}")
    .WithNativeStructuredOutput()
    .WithTemperature(0.1f)
    .WithMaxOutputTokens(128));
```

The method returns a `TransformNodeHandle<TIn,TOut>`.

## Enrich data

Use `AddChatEnrichment` to map model output back to the original type:

```csharp
var enrich = builder.AddChatEnrichment<Article, SummaryResult>(chatClient, options => options
    .WithSystemPrompt("Summarize the article in one sentence.")
    .WithItemTemplate(article => article.Body)
    .WithResultMapper((article, result) => article with { Summary = result.Summary })
    .WithMaxOutputTokens(128));
```

`ResultMapper<TIn,TField>` has the signature `TIn(TIn input, TField result)`. It receives the original input and the deserialized model result, then returns the enriched item. `AddChatEnrichment` returns a `TransformNodeHandle<TIn,TIn>`.

## Understand the execution model

Every Chat node follows the same request flow:

1. A template delegate formats an item or batch as the user message.
2. The node sends a system message and the formatted user message through `IChatClient`.
3. The node removes a surrounding Markdown code fence when the model includes one.
4. `System.Text.Json` deserializes the response into the configured output type.
5. The typed result flows to the next node, or an enrichment mapper merges it into the input.

Calls are stateless. The nodes don't retain conversation history between items or batches.

## Choose a node

The package provides per-item, pre-batched, and stream-batched variants:

| Scenario | Method | Input and output |
| --- | --- | --- |
| Transform one item per request | `AddChatTransform<TIn,TOut>` | `TIn -> TOut` |
| Transform a collection in one request | `AddChatBatchedTransform<TIn,TOut>` | `IReadOnlyCollection<TIn> -> IReadOnlyCollection<TOut>` |
| Buffer a stream, transform each batch, and emit individual results | `AddChatBatchedStreamTransform<TIn,TOut>` | `TIn -> TOut` |
| Enrich one item per request | `AddChatEnrichment<TIn,TField>` | `TIn -> TIn` |
| Enrich a collection in one request | `AddChatBatchedEnrichment<TIn,TField>` | `IReadOnlyCollection<TIn> -> IReadOnlyCollection<TIn>` |
| Buffer a stream, enrich each batch, and emit individual items | `AddChatBatchedStreamEnrichment<TIn,TField>` | `TIn -> TIn` |
| Batch, enrich, and unbatch as one chain | `AddChatBatchedEnrichmentWithUnbatch<T,TField>` | `T -> T` connection handles |

### Transform a pre-batched collection

Use `AddChatBatchedTransform` when an upstream node already produces collections:

```csharp
var classifyBatch = builder.AddChatBatchedTransform<Comment, ClassificationResult>(
    chatClient,
    options => options
        .WithSystemPrompt("Classify every comment. Return an Items array in input order.")
        .WithBatchTemplate(batch => string.Join(
            "\n",
            batch.Select((comment, index) => $"{index + 1}. {comment.Text}")))
        .WithNativeStructuredOutput());
```

The model must return one result for each input item. The expected JSON shape is an object with an `Items` array.

### Transform or enrich a stream in batches

Stream-batched nodes keep individual item types at the surrounding pipeline boundary. They buffer up to `BatchSize`, flush an incomplete batch after `BatchTimeout`, make one model request, and emit individual results:

```csharp
var classifyStream = builder.AddChatBatchedStreamTransform<Comment, ClassificationResult>(
    chatClient,
    options => options
        .WithSystemPrompt("Classify every comment. Return an Items array in input order.")
        .WithBatchTemplate(batch => string.Join(
            "\n",
            batch.Select((comment, index) => $"{index + 1}. {comment.Text}")))
        .WithBatchSize(32)
        .WithBatchTimeout(TimeSpan.FromSeconds(2)));
```

Use a pre-batched node when batching is part of the upstream contract. Use a stream-batched node when batching is an internal throughput optimization that downstream nodes shouldn't observe.

### Enrich and unbatch a collection

`AddChatBatchedEnrichmentWithUnbatch` creates a batcher, batched enrichment node, and unbatcher. It returns handles for the beginning and end of the chain:

```csharp
var (input, output) = builder.AddChatBatchedEnrichmentWithUnbatch<Article, SummaryResult>(
    chatClient,
    batchSize: 16,
    batchTimeout: TimeSpan.FromSeconds(5),
    configure: options => options
        .WithSystemPrompt("Summarize every article. Return an Items array in input order.")
        .WithBatchTemplate(batch => string.Join(
            "\n\n",
            batch.Select((article, index) => $"Article {index + 1}: {article.Body}")))
        .WithResultMapper((article, result) => article with { Summary = result.Summary }));

builder.Connect(source, input);
builder.Connect(output, sink);
```

The generated node names use the suffixes `_batch`, `_enrich`, and `_unbatch`.

## Configure Chat nodes

All options builders share the following methods:

| Builder method | Required | Description |
| --- | --- | --- |
| `WithSystemPrompt(string)` | Yes | Sets the system message. |
| `WithItemTemplate(Func<TIn,string>)` | Per-item nodes | Formats one input as the user message. |
| `WithBatchTemplate(Func<IReadOnlyCollection<TIn>,string>)` | Batch nodes | Formats a collection as the user message. |
| `WithTemperature(float)` | No | Sets `ChatOptions.Temperature`. If omitted, the provider default applies. |
| `WithMaxOutputTokens(int)` | No | Sets the maximum response tokens. The value must be positive. |
| `WithNativeStructuredOutput(bool)` | No | Requests a native JSON response format. The default is `false`. |
| `WithConfigureOptions(Action<ChatOptions>)` | No | Applies advanced `ChatOptions` changes after the other settings. |

Enrichment builders also require `WithResultMapper`. Stream-batched builders require `WithBatchSize`; `WithBatchTimeout` is optional and defaults to 5 seconds.

Required settings are validated when the builder creates its options, before pipeline execution.

### Format prompts with delegates

Templates are ordinary C# delegates. Include only the state the model needs, and number batched items when order matters:

```csharp
.WithItemTemplate(comment => $"""
    Author: {comment.Author}
    Text: {comment.Text}
    """)

.WithBatchTemplate(batch => string.Join(
    "\n",
    batch.Select((comment, index) => $"{index + 1}. [{comment.Author}] {comment.Text}")))
```

A template that throws or returns null, empty, or whitespace text causes a `ChatTransformException`.

### Map enrichment results

The mapper can return a record copy or update a mutable object:

```csharp
// Record copy
.WithResultMapper((comment, result) => comment with
{
    Sentiment = result.Label,
    Score = result.Score,
})

// Mutable object
.WithResultMapper((order, result) =>
{
    order.Tags = result.Values;
    return order;
})
```

### Configure advanced chat options

`WithConfigureOptions` runs after temperature, maximum tokens, and response format are set. Use it for provider-specific settings or to override an earlier value:

```csharp
.WithConfigureOptions(options =>
{
    options.ModelId = "MODEL_ID";
    options.AdditionalProperties ??= new();
    options.AdditionalProperties["top_p"] = 0.95f;
})
```

Replace `MODEL_ID` with the model identifier accepted by your `IChatClient` provider.

## Use structured output

`WithNativeStructuredOutput()` configures `ChatOptions.ResponseFormat`:

- Per-item transform and enrichment nodes use `ChatResponseFormat.Json`.
- Batch and stream-batched nodes use a JSON schema for an object with an `Items` array.

Provider support varies. A provider can enforce the format, treat it as guidance, or ignore it. Even when native structured output is disabled, your system prompt can instruct the model to return JSON.

Responses use `System.Text.Json` with case-insensitive property matching and support for numbers encoded as JSON strings. Records, plain classes, and `[JsonPropertyName]` attributes are supported:

```csharp
public sealed class SentimentScore
{
    [JsonPropertyName("score")]
    public double Score { get; init; }

    [JsonPropertyName("label")]
    public string Label { get; init; } = string.Empty;
}
```

The deserializer handles these common model response shapes:

- A surrounding `json` Markdown code fence is removed.
- A bare array is wrapped as `{ "Items": [...] }` when a batch wrapper is expected.
- A single object is wrapped in an array when the requested output type is an `IReadOnlyCollection<T>`.

Batch nodes still require one result per input item. If the first response count differs, the node sends one corrective request that states the expected count. A second mismatch raises `ChatTransformException`.

## Handle model output errors

`ChatTransformException` derives from `AIInvocationException`, which derives from `PipelineException`. It identifies invalid model output and application callback failures separately from transport failures.

The exception exposes the following diagnostics:

| Property | Description |
| --- | --- |
| `ErrorCode` | Always `CHAT_TRANSFORM_ERROR`. |
| `Provider` | Always `chat`. |
| `OriginalItem` | The item or batch being processed. |
| `PromptSent` | The user message sent to the model, when available. |
| `ModelUsed` | `ChatResponse.ModelId`, when the provider returns one. |
| `RawResponse` | The unmodified model response, when available. |
| `InnerException` | The underlying callback, client, or deserialization exception. |

The wrapping policy is:

| Situation | Behavior |
| --- | --- |
| Empty response or JSON `null` | Raises `ChatTransformException`. |
| Malformed or unsupported JSON | Wraps the deserialization failure in `ChatTransformException`. |
| Batch count mismatch | Retries once, then raises `ChatTransformException` if the count still differs. |
| Template, mapper, or `ConfigureOptions` callback failure | Wraps the callback failure in `ChatTransformException`. |
| `HttpRequestException` or `TimeoutException` | Propagates unchanged. |
| `OperationCanceledException` | Propagates unchanged. |
| Another unexpected client exception | Wraps the exception in `ChatTransformException`. |

Use NPipeline resilience policies for transport retries and dead-letter behavior. Avoid retrying malformed model output without changing the prompt, model, or input.

## Configure providers and observability

Credentials, endpoints, and model selection belong to the `IChatClient` construction code. Keep them outside pipeline definitions and source control. You can register the client with dependency injection and pass it to Chat builder methods:

```csharp
services.AddSingleton<IChatClient>(_ => CreateChatClientFromConfiguration());

public sealed class ClassificationPipeline(IChatClient chatClient) : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var classify = builder.AddChatTransform<Comment, ClassificationResult>(
            chatClient,
            options => options
                .WithSystemPrompt("Classify the comment.")
                .WithItemTemplate(comment => comment.Text));
    }
}
```

Microsoft.Extensions.AI middleware can add logging, caching, and OpenTelemetry around the underlying client:

```csharp
IChatClient observedClient = providerClient
    .AsBuilder()
    .UseOpenTelemetry()
    .UseLogging(loggerFactory)
    .UseDistributedCache(cache)
    .Build();
```

The required middleware packages and available methods depend on your Microsoft.Extensions.AI setup. This instrumentation can report model latency, token usage, and request traces independently of NPipeline observability.

## Tune performance and quality

Use the following starting points, then measure against your provider and workload:

- Keep prompts concise and include the required JSON shape or a representative example.
- Number batch inputs so the model can preserve result order.
- Start stream batches around 16-32 items, then tune for latency, token limits, and mismatch frequency.
- Reduce the batch size if count mismatches persist after the corrective retry.
- Use a low temperature for classification and extraction workloads that benefit from repeatability.
- Set `WithMaxOutputTokens` when the expected response size is bounded, but leave enough room for JSON structure.
- Bound pipeline concurrency against provider request and token limits.

## Related documentation

- [AI package overview](ai.md)
- [Typed AI decisions and routing](ai-decisions.md)
- [Resilience policies](../error-handling/resilience-policies.md)
- [Batching and windowing](../guides/batching-and-windowing.md)
- [Parallel execution](parallelism.md)
- [Observability](observability.md)
