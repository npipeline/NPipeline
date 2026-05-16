using Microsoft.Extensions.AI;
using NPipeline.Extensions.AI;
using NPipeline.Pipeline;

namespace Sample_AITransform;

public record Comment(string Text, string Author);

public record ClassificationResult(string Category, float Confidence);

public record CommentWithSentiment(string Text, string Author, string? Sentiment, float? Score);

public record SentimentResult(string Label, float Score);

public sealed class FakeChatClient : IChatClient
{
    public Task<ChatResponse> GetResponseAsync(
        IEnumerable<ChatMessage> messages,
        ChatOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var response = """{"category":"Greeting","confidence":0.95}""";
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

public sealed class ClassificationPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var chatClient = new FakeChatClient();

        builder
            .AddAITransform<Comment, ClassificationResult>(chatClient, options => options
                .WithSystemPrompt("Classify text into: Greeting, Question, Complaint, Spam")
                .WithItemTemplate(comment => $"Classify: {comment.Text}")
                .WithTemperature(0.1f));
    }
}

public sealed class EnrichPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var chatClient = new FakeChatClient();

        builder
            .AddAIEnrich<CommentWithSentiment, SentimentResult>(chatClient, options => options
                .WithSystemPrompt("Analyze sentiment. Return JSON with Label and Score.")
                .WithItemTemplate(comment => $"Analyze: {comment.Text}")
                .WithResultMapper((comment, result) => comment with { Sentiment = result.Label, Score = result.Score }));
    }
}

public static class Program
{
    public static void Main(string[] args)
    {
        Console.WriteLine("NPipeline.Extensions.AI sample loaded successfully.");
        Console.WriteLine("Available pipeline definitions:");
        Console.WriteLine("  - ClassificationPipeline: AITransformNode<Comment, ClassificationResult>");
        Console.WriteLine("  - EnrichPipeline: AIEnrichNode<CommentWithSentiment, SentimentResult>");
    }
}
