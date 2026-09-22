# NPipeline.Extensions.AI.Chat

Transform and enrich NPipeline data with any `Microsoft.Extensions.AI.IChatClient` implementation.

## Install the package

```bash
dotnet add package NPipeline.Extensions.AI.Chat
```

Install the package that provides your `IChatClient`, such as an OpenAI, Azure OpenAI, or Ollama integration.

## Transform an item

Use `AddChatTransform` when the model response replaces the input type:

```csharp
using NPipeline.Extensions.AI.Chat;

public record Comment(string Text);
public record Classification(string Category, double Confidence);

var classify = builder.AddChatTransform<Comment, Classification>(chatClient, options => options
    .WithSystemPrompt("Classify the comment as Greeting, Question, Complaint, or Spam.")
    .WithItemTemplate(comment => comment.Text)
    .WithNativeStructuredOutput()
    .WithTemperature(0.1f));
```

Use `AddChatEnrichment` when the model produces a field that you map back to the original type:

```csharp
public record Article(string Body, string? Summary = null);
public record SummaryResult(string Summary);

var enrich = builder.AddChatEnrichment<Article, SummaryResult>(chatClient, options => options
    .WithSystemPrompt("Summarize the article in one sentence.")
    .WithItemTemplate(article => article.Body)
    .WithResultMapper((article, result) => article with { Summary = result.Summary }));
```

## Choose a processing mode

| Input shape                     | Transform                       | Enrichment                            |
|---------------------------------|---------------------------------|---------------------------------------|
| One item per request            | `AddChatTransform`              | `AddChatEnrichment`                   |
| A collection per request        | `AddChatBatchedTransform`       | `AddChatBatchedEnrichment`            |
| A stream with internal batching | `AddChatBatchedStreamTransform` | `AddChatBatchedStreamEnrichment`      |
| Batch, enrich, and unbatch      | Not applicable                  | `AddChatBatchedEnrichmentWithUnbatch` |

Batch nodes require one output for every input and retry one count mismatch with a corrective prompt. Stream-batched nodes amortize model latency without
changing the surrounding pipeline item types.

## Handle failures

Malformed or empty model output raises `ChatTransformException`. The exception includes the original item, prompt, model, and raw response when available.
Transport failures, timeouts, and cancellation propagate unchanged so NPipeline resilience policies can handle them.

## License

This package is licensed under the [Business Source License 1.1](LICENSE.txt).

Production use is free for organizations with four or fewer developers and annual revenue of AUD 5 million or less. Larger organizations require
a [commercial license](https://npipeline.com). Each release converts to the MIT License on its change date.
