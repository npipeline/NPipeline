using System.Reflection;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Hosting;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Extensions.AI;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Nodes;
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

public sealed class CommentSource : SourceNode<Comment>
{
    public override IDataStream<Comment> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        var comments = new List<Comment>
        {
            new("I absolutely love this product, it changed my life!", "alice"),
            new("This is the worst experience I have ever had.", "bob"),
            new("The delivery was on time, packaging was fine, nothing special.", "carol"),
            new("Terrible customer support, nobody answers the phone.", "dave"),
            new("Outstanding quality, will definitely recommend to friends!", "emma"),
        };

        return new InMemoryDataStream<Comment>(comments, "CommentSource");
    }
}

public sealed class EnrichedCommentSink : SinkNode<Comment>
{
    public override async Task ConsumeAsync(
        IDataStream<Comment> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        Console.WriteLine();
        Console.WriteLine("=== Sentiment Analysis Results ===");
        Console.WriteLine();
        Console.WriteLine($"{"Author",-12} {"Sentiment",-12} {"Confidence",-10} Text");
        Console.WriteLine(new string('-', 80));

        await foreach (var comment in input.WithCancellation(cancellationToken))
        {
            var sentimentColor = comment.Sentiment switch
            {
                "Positive" => ConsoleColor.Green,
                "Negative" => ConsoleColor.Red,
                _ => ConsoleColor.Yellow,
            };

            Console.Write($" {comment.Author,-12} ");

            Console.ForegroundColor = sentimentColor;
            Console.Write($"{comment.Sentiment,-12} ");
            Console.ResetColor();

            Console.Write($"{comment.Confidence:P0}".PadRight(10));

            Console.WriteLine(comment.Text.Length > 48 ? comment.Text[..45] + "..." : comment.Text);
        }

        Console.WriteLine();
    }
}

public sealed class SentimentEnrichPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var chatClient = new FakeChatClient();

        var source = builder.AddSource<CommentSource, Comment>("comment-source");

        var aiEnrich = builder.AddAIEnrich<Comment, SentimentResult>(chatClient, options => options
            .WithSystemPrompt("Analyze sentiment. Return JSON with Label (Positive/Negative/Neutral) and Score (0-1).")
            .WithItemTemplate(comment => $"Analyze: {comment.Text}")
            .WithResultMapper((comment, result) => comment with
            {
                Sentiment = result.Label,
                Confidence = result.Score,
            }),
            "ai-sentiment-enrich");

        var sink = builder.AddSink<EnrichedCommentSink, Comment>("enriched-comment-sink");

        builder.Connect(source, aiEnrich);
        builder.Connect(aiEnrich, sink);
    }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline AI Enrichment Sample ===");
        Console.WriteLine("  SentimentEnrichPipeline: CommentSource -> AIEnrich -> EnrichedCommentSink");
        Console.WriteLine("  Sends each comment to an LLM for sentiment analysis and splices results back.");
        Console.WriteLine();

        try
        {
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                {
                    services.AddNPipeline(Assembly.GetExecutingAssembly());
                })
                .Build();

            await host.Services.RunPipelineAsync<SentimentEnrichPipeline>();

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
