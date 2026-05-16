# NPipeline.Extensions.AI

AI-powered transform and enrichment nodes for [NPipeline](https://github.com/NPipeline/NPipeline), built on
`Microsoft.Extensions.AI.Abstractions`.

## What You Get

- **6 node types** across per-item, batch, and stream-batched processing.
- **Provider-agnostic integration** through `IChatClient`.
- **Typed structured outputs** using `System.Text.Json` deserialization.
- **Resilience-friendly errors** via `AITransformException` when model output is malformed.

## Node Matrix

### Replace Family (input becomes new output type)

- `AITransformNode<TIn, TOut>`: 1 item -> 1 LLM call -> 1 output
- `AIBatchedTransformNode<TIn, TOut>`: batch in -> single LLM call -> batch out
- `AIBatchedStreamTransformNode<TIn, TOut>`: stream in -> internal batching -> fan-out outputs

### Enrich Family (input type is preserved)

- `AIEnrichNode<TIn, TField>`: 1 item -> AI field -> mapped back to `TIn`
- `AIBatchedEnrichNode<TIn, TField>`: batch enrich with per-item result mapping
- `AIBatchedStreamEnrichNode<TIn, TField>`: stream-level enrichment with internal batching

## Installation

```bash
dotnet add package NPipeline.Extensions.AI
```

Install your provider package of choice (examples):

```bash
# OpenAI / Azure OpenAI integrations
dotnet add package Microsoft.Extensions.AI.OpenAI

# Ollama native client (local models)
dotnet add package OllamaSharp
```

## Requirements

- .NET 8.0, 9.0, or 10.0
- NPipeline core package
- Any `IChatClient` implementation

## Quick Start

```csharp
using Microsoft.Extensions.AI;
using NPipeline;
using NPipeline.Extensions.AI;

public record Comment(string Text, string Author);
public record ClassificationResult(string Category, float Confidence);

var builder = new PipelineBuilder();
var chatClient = ResolveChatClient();

builder.AddAITransform<Comment, ClassificationResult>(chatClient, options => options
 .WithSystemPrompt("Classify text into: Greeting, Question, Complaint, Spam")
 .WithItemTemplate(comment => $"Classify: {comment.Text}")
 .WithNativeStructuredOutput()
 .WithTemperature(0.1f)
 .WithMaxOutputTokens(128));
```

## Enrichment Example

```csharp
public record SentimentResult(string Label, float Score);
public record CommentWithSentiment(string Text, string Author, string? Sentiment, float? Score);

builder.AddAIEnrich<CommentWithSentiment, SentimentResult>(chatClient, options => options
 .WithSystemPrompt("Analyze sentiment and return JSON with Label and Score.")
 .WithItemTemplate(comment => $"Analyze sentiment: {comment.Text}")
 .WithResultMapper((comment, result) => comment with
 {
  Sentiment = result.Label,
  Score = result.Score
 }));
```

## Stream Batching Example

```csharp
builder.AddAIBatchedStreamTransform<Comment, ClassificationResult>(chatClient, options => options
 .WithSystemPrompt("Classify each comment.")
 .WithBatchTemplate(batch => string.Join("\n", batch.Select(x => x.Text)))
 .WithBatchSize(32)
 .WithBatchTimeout(TimeSpan.FromSeconds(2)));
```

## Error Handling Behavior

- Malformed or null model responses are wrapped in `AITransformException` with diagnostic fields.
- Transport and cancellation failures (for example `HttpRequestException`, `TimeoutException`, and cancellation) propagate unchanged.
- Batched transform and enrich nodes enforce **1 input -> 1 output** count parity.

## Performance Tips

- Use stream-batched nodes to amortize LLM latency across many records.
- Start with `BatchSize` in the 16-64 range, then tune based on token budgets and latency targets.
- Keep prompt templates concise to reduce token usage and improve throughput.
- Use strongly typed, minimal output DTOs to cut parsing cost and reduce failure surface.

## License

MIT License - see LICENSE file for details.
