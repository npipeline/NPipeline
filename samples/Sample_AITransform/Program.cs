using Microsoft.Extensions.AI;
using NPipeline.Extensions.AI;
using NPipeline.Pipeline;

namespace Sample_AITransform;

public record Comment(string Text, string Author);

public record ClassificationResult(string Category, float Confidence);

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
        => throw new NotSupportedException();

    void IDisposable.Dispose() { }
    object? IChatClient.GetService(Type serviceType, object? serviceKey) => null;
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

public static class Program
{
    public static void Main(string[] args)
    {
        Console.WriteLine("NPipeline.Extensions.AI — AI Transform Sample");
        Console.WriteLine("  ClassificationPipeline: AITransformNode<Comment, ClassificationResult>");
        Console.WriteLine("  Sends each comment's Text to an LLM for classification.");
    }
}
