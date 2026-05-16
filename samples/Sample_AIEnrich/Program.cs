using Microsoft.Extensions.AI;
using NPipeline.Extensions.AI;
using NPipeline.Pipeline;

namespace Sample_AIEnrich;

public record Comment(string Text, string Author, string? Sentiment = null, float? Confidence = null);

public record SentimentResult(string Label, float Score);

public sealed class FakeChatClient : IChatClient
{
    public Task<ChatResponse> GetResponseAsync(
        IEnumerable<ChatMessage> messages,
        ChatOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var response = """{"label":"Positive","score":0.92}""";
        return Task.FromResult(new ChatResponse(new ChatMessage(ChatRole.Assistant, response)));
    }

    public IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
        IEnumerable<ChatMessage> messages,
        ChatOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    void IDisposable.Dispose()
    {
    }

    object? IChatClient.GetService(Type serviceType, object? serviceKey)
    {
        return null;
    }
}

public sealed class SentimentEnrichPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var chatClient = new FakeChatClient();

        builder
            .AddAIEnrich<Comment, SentimentResult>(chatClient, options => options
                .WithSystemPrompt("Analyze sentiment. Return JSON with Label and Score.")
                .WithItemTemplate(comment => $"Analyze: {comment.Text}")
                .WithResultMapper((comment, result) => comment with
                {
                    Sentiment = result.Label,
                    Confidence = result.Score,
                }));
    }
}

public static class Program
{
    public static void Main(string[] args)
    {
        Console.WriteLine("NPipeline.Extensions.AI — AI Enrichment Sample");
        Console.WriteLine("  SentimentEnrichPipeline: AIEnrichNode<Comment, SentimentResult>");
        Console.WriteLine("  Sends each comment to an LLM and splices the sentiment result back into the original record.");
    }
}
