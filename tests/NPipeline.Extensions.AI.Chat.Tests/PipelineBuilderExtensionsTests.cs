using Microsoft.Extensions.AI;
using NPipeline.Execution;
using NPipeline.Extensions.AI.Chat.Configuration;
using NPipeline.Extensions.Testing;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Chat.Tests;

public class PipelineBuilderExtensionsTests
{
    private const string ClientContextKey = "chatClient";
    private const string SinkContextKey = "sink";

    [Fact]
    public void AddChatTransform_RegistersNode()
    {
        var builder = new PipelineBuilder();
        var client = FakeChatClient.ThatReturns("""{"category":"X","confidence":0.5}""");

        var handle = builder.AddChatTransform<TestDomain.Comment, TestDomain.ClassificationResult>(client, options => options
            .WithSystemPrompt("Classify.")
            .WithItemTemplate(c => c.Text));

        Assert.NotNull(handle);
    }

    [Fact]
    public void AddChatTransform_CustomName_IsUsed()
    {
        var builder = new PipelineBuilder();
        var client = FakeChatClient.ThatReturns("""{"category":"X","confidence":0.5}""");

        var handle = builder.AddChatTransform<TestDomain.Comment, TestDomain.ClassificationResult>(
            client, options => options
                .WithSystemPrompt("Classify.")
                .WithItemTemplate(c => c.Text),
            "my-classifier");

        Assert.NotNull(handle);
    }

    [Fact]
    public void AddChatEnrichment_RegistersNode()
    {
        var builder = new PipelineBuilder();
        var client = FakeChatClient.ThatReturns("""{"label":"X","score":0.5}""");

        var handle = builder.AddChatEnrichment<TestDomain.Comment, TestDomain.SentimentResult>(client, options => options
            .WithSystemPrompt("Analyze.")
            .WithItemTemplate(c => c.Text)
            .WithResultMapper((item, _) => item));

        Assert.NotNull(handle);
    }

    [Fact]
    public void AddChatBatchedTransform_RegistersNode()
    {
        var builder = new PipelineBuilder();
        var client = FakeChatClient.ThatReturns("[]");

        var handle = builder.AddChatBatchedTransform<TestDomain.Comment, TestDomain.ClassificationResult>(client, options => options
            .WithSystemPrompt("Classify.")
            .WithBatchTemplate(batch => "classify"));

        Assert.NotNull(handle);
    }

    [Fact]
    public void AddChatBatchedStreamTransform_RegistersNode()
    {
        var builder = new PipelineBuilder();
        var client = FakeChatClient.ThatReturns("[]");

        var handle = builder.AddChatBatchedStreamTransform<TestDomain.Comment, TestDomain.ClassificationResult>(client, options => options
            .WithSystemPrompt("Classify.")
            .WithBatchTemplate(batch => "classify")
            .WithBatchSize(10));

        Assert.NotNull(handle);
    }

    [Fact]
    public void AddChatBatchedEnrichment_RegistersNode()
    {
        var builder = new PipelineBuilder();
        var client = FakeChatClient.ThatReturns("[]");

        var handle = builder.AddChatBatchedEnrichment<TestDomain.Comment, TestDomain.SentimentResult>(client, options => options
            .WithSystemPrompt("Analyze.")
            .WithBatchTemplate(batch => "analyze")
            .WithResultMapper((item, _) => item));

        Assert.NotNull(handle);
    }

    [Fact]
    public void AddChatBatchedStreamEnrichment_RegistersNode()
    {
        var builder = new PipelineBuilder();
        var client = FakeChatClient.ThatReturns("[]");

        var handle = builder.AddChatBatchedStreamEnrichment<TestDomain.Comment, TestDomain.SentimentResult>(client, options => options
            .WithSystemPrompt("Analyze.")
            .WithBatchTemplate(batch => "analyze")
            .WithResultMapper((item, _) => item)
            .WithBatchSize(10));

        Assert.NotNull(handle);
    }

    [Fact]
    public async Task AddChatBatchedStreamEnrichment_ExecutesInPipeline()
    {
        var client = FakeChatClient.ThatReturns("""[{"label":"positive","score":0.9}]""");
        var sink = new InMemorySinkNode<TestDomain.Comment>();

        var context = PipelineContext.CreateDefault();
        context.Items[ClientContextKey] = client;
        context.Items[SinkContextKey] = sink;

        await PipelineRunner.Create().RunAsync<ChatBatchedStreamEnrichmentPipelineDefinition>(context);

        Assert.Single(sink.Items);
        Assert.Equal("positive", sink.Items[0].Author);
    }

    [Fact]
    public async Task AddChatBatchedEnrichmentWithUnbatch_ExecutesBatchChain()
    {
        var client = FakeChatClient.ThatReturns("""[{"label":"positive","score":0.9},{"label":"negative","score":0.1}]""");
        var sink = new InMemorySinkNode<TestDomain.Comment>();

        var context = PipelineContext.CreateDefault();
        context.Items[ClientContextKey] = client;
        context.Items[SinkContextKey] = sink;

        await PipelineRunner.Create().RunAsync<ChatBatchedEnrichmentWithUnbatchPipelineDefinition>(context);

        Assert.Equal(2, sink.Items.Count);
        var authors = sink.Items.Select(item => item.Author).ToHashSet(StringComparer.OrdinalIgnoreCase);
        Assert.Contains("positive", authors);
        Assert.Contains("negative", authors);
    }

    [Fact]
    public void AddChatBatchedEnrichmentWithUnbatch_WithNonPositiveBatchTimeout_Throws()
    {
        var builder = new PipelineBuilder();
        var client = FakeChatClient.ThatReturns("[]");

        static void Configure(ChatBatchedEnrichmentOptionsBuilder<TestDomain.Comment, TestDomain.SentimentResult> options)
        {
            options
                .WithSystemPrompt("Analyze.")
                .WithBatchTemplate(batch => "analyze")
                .WithResultMapper((item, result) => item with { Author = result.Label });
        }

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            builder.AddChatBatchedEnrichmentWithUnbatch<TestDomain.Comment, TestDomain.SentimentResult>(
                client,
                2,
                TimeSpan.Zero,
                Configure));

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            builder.AddChatBatchedEnrichmentWithUnbatch<TestDomain.Comment, TestDomain.SentimentResult>(
                client,
                2,
                TimeSpan.FromMilliseconds(-1),
                Configure));
    }

    [Fact]
    public void NullBuilder_ThrowsArgumentNullException()
    {
        var client = FakeChatClient.ThatReturns("{}");

        Assert.Throws<ArgumentNullException>(() =>
            ((PipelineBuilder)null!).AddChatTransform<string, string>(client, _ => { }));
    }

    private sealed class ChatBatchedStreamEnrichmentPipelineDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var client = (IChatClient)context.Items[ClientContextKey];
            var sink = (InMemorySinkNode<TestDomain.Comment>)context.Items[SinkContextKey];

            var source = builder.AddInMemorySource("src", [new TestDomain.Comment("hello", "alice")]);

            var enrich = builder.AddChatBatchedStreamEnrichment<TestDomain.Comment, TestDomain.SentimentResult>(client, options => options
                .WithSystemPrompt("Analyze.")
                .WithBatchTemplate(batch => "analyze")
                .WithResultMapper((item, result) => item with { Author = result.Label })
                .WithBatchSize(10));

            var sinkHandle = builder.AddSink<InMemorySinkNode<TestDomain.Comment>, TestDomain.Comment>("sink");
            builder.AddPreconfiguredNodeInstance(sinkHandle.Id, sink);

            builder.Connect(source, enrich)
                .Connect(enrich, sinkHandle);
        }
    }

    private sealed class ChatBatchedEnrichmentWithUnbatchPipelineDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var client = (IChatClient)context.Items[ClientContextKey];
            var sink = (InMemorySinkNode<TestDomain.Comment>)context.Items[SinkContextKey];

            var source = builder.AddInMemorySource("src", [
                new TestDomain.Comment("love it", "alice"),
                new TestDomain.Comment("hate it", "bob"),
            ]);

            var (inputHandle, outputHandle) = builder.AddChatBatchedEnrichmentWithUnbatch<TestDomain.Comment, TestDomain.SentimentResult>(
                client,
                2,
                TimeSpan.FromSeconds(1),
                options => options
                    .WithSystemPrompt("Analyze.")
                    .WithBatchTemplate(batch => "analyze")
                    .WithResultMapper((item, result) => item with { Author = result.Label }),
                "batched-enrich");

            var sinkHandle = builder.AddSink<InMemorySinkNode<TestDomain.Comment>, TestDomain.Comment>("sink");
            builder.AddPreconfiguredNodeInstance(sinkHandle.Id, sink);

            builder.Connect(source, inputHandle)
                .Connect(outputHandle, sinkHandle);
        }
    }
}
