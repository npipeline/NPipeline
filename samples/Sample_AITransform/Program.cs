using System.Reflection;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Hosting;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Extensions.AI;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Nodes;
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

public sealed class CommentSource : SourceNode<Comment>
{
    public override IDataStream<Comment> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        var comments = new List<Comment>
        {
            new("Hey everyone, great work on the release!", "alice"),
            new("I need a refund for this broken product immediately.", "bob"),
            new("Has anyone seen the meeting agenda for today?", "carol"),
            new("BUY NOW!!! Limited time offer!!! Click here!!!", "spammer42"),
            new("Thanks for the quick response, really appreciate it!", "dave"),
        };

        return new InMemoryDataStream<Comment>(comments, "CommentSource");
    }
}

public sealed class ClassificationSink : SinkNode<ClassificationResult>
{
    public override async Task ConsumeAsync(
        IDataStream<ClassificationResult> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        Console.WriteLine();
        Console.WriteLine("=== Classification Results ===");
        Console.WriteLine();

        await foreach (var result in input.WithCancellation(cancellationToken))
        {
            Console.BackgroundColor = result.Category switch
            {
                "Greeting" => ConsoleColor.Green,
                "Complaint" => ConsoleColor.Red,
                "Question" => ConsoleColor.Blue,
                "Spam" => ConsoleColor.DarkRed,
                _ => ConsoleColor.Gray,
            };

            Console.Write($" [{result.Category,-12}]");
            Console.ResetColor();
            Console.WriteLine($" (confidence: {result.Confidence:P0})");
        }

        Console.WriteLine();
    }
}

public sealed class ClassificationPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var chatClient = new FakeChatClient();

        var source = builder.AddSource<CommentSource, Comment>("comment-source");

        var aiTransform = builder.AddAITransform<Comment, ClassificationResult>(chatClient, options => options
            .WithSystemPrompt("Classify text into: Greeting, Question, Complaint, Spam")
            .WithItemTemplate(comment => $"Classify: {comment.Text}")
            .WithTemperature(0.1f),
            "ai-classification");

        var sink = builder.AddSink<ClassificationSink, ClassificationResult>("classification-sink");

        builder.Connect(source, aiTransform);
        builder.Connect(aiTransform, sink);
    }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline AI Transform Sample ===");
        Console.WriteLine("  ClassificationPipeline: CommentSource -> AITransform -> ClassificationSink");
        Console.WriteLine("  Sends each comment to an LLM for classification and displays results.");
        Console.WriteLine();

        try
        {
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                {
                    services.AddNPipeline(Assembly.GetExecutingAssembly());
                })
                .Build();

            await host.Services.RunPipelineAsync<ClassificationPipeline>();

            Console.WriteLine();
            Console.WriteLine("Pipeline execution completed successfully.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error executing pipeline: {ex.Message}");
            Console.WriteLine(ex.ToString());
            Environment.ExitCode = 1;
        }
    }
}
