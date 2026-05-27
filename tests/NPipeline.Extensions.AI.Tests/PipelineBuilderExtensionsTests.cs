using Microsoft.Extensions.AI;
using NPipeline.Execution;
using NPipeline.Extensions.AI.Configuration;
using NPipeline.Extensions.Testing;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Tests;

public class PipelineBuilderExtensionsTests
{
    private const string ClientContextKey = "chatClient";
    private const string SinkContextKey = "sink";

    [Fact]
    public void AddAITransform_RegistersNode()
    {
        var builder = new PipelineBuilder();
        var client = FakeChatClient.ThatReturns("""{"category":"X","confidence":0.5}""");

        var handle = builder.AddAITransform<TestDomain.Comment, TestDomain.ClassificationResult>(client, options => options
            .WithSystemPrompt("Classify.")
            .WithItemTemplate(c => c.Text));

        Assert.NotNull(handle);
    }

    [Fact]
    public void AddAITransform_CustomName_IsUsed()
    {
        var builder = new PipelineBuilder();
        var client = FakeChatClient.ThatReturns("""{"category":"X","confidence":0.5}""");

        var handle = builder.AddAITransform<TestDomain.Comment, TestDomain.ClassificationResult>(
            client, options => options
                .WithSystemPrompt("Classify.")
                .WithItemTemplate(c => c.Text),
            "my-classifier");

        Assert.NotNull(handle);
    }

    [Fact]
    public void AddAIEnrich_RegistersNode()
    {
        var builder = new PipelineBuilder();
        var client = FakeChatClient.ThatReturns("""{"label":"X","score":0.5}""");

        var handle = builder.AddAIEnrich<TestDomain.Comment, TestDomain.SentimentResult>(client, options => options
            .WithSystemPrompt("Analyze.")
            .WithItemTemplate(c => c.Text)
            .WithResultMapper((item, _) => item));

        Assert.NotNull(handle);
    }

    [Fact]
    public void AddAIBatchedTransform_RegistersNode()
    {
        var builder = new PipelineBuilder();
        var client = FakeChatClient.ThatReturns("[]");

        var handle = builder.AddAIBatchedTransform<TestDomain.Comment, TestDomain.ClassificationResult>(client, options => options
            .WithSystemPrompt("Classify.")
            .WithBatchTemplate(batch => "classify"));

        Assert.NotNull(handle);
    }

    [Fact]
    public void AddAIBatchedStreamTransform_RegistersNode()
    {
        var builder = new PipelineBuilder();
        var client = FakeChatClient.ThatReturns("[]");

        var handle = builder.AddAIBatchedStreamTransform<TestDomain.Comment, TestDomain.ClassificationResult>(client, options => options
            .WithSystemPrompt("Classify.")
            .WithBatchTemplate(batch => "classify")
            .WithBatchSize(10));

        Assert.NotNull(handle);
    }

    [Fact]
    public void AddAIBatchedEnrich_RegistersNode()
    {
        var builder = new PipelineBuilder();
        var client = FakeChatClient.ThatReturns("[]");

        var handle = builder.AddAIBatchedEnrich<TestDomain.Comment, TestDomain.SentimentResult>(client, options => options
            .WithSystemPrompt("Analyze.")
            .WithBatchTemplate(batch => "analyze")
            .WithResultMapper((item, _) => item));

        Assert.NotNull(handle);
    }

    [Fact]
    public void AddAIBatchedStreamEnrich_RegistersNode()
    {
        var builder = new PipelineBuilder();
        var client = FakeChatClient.ThatReturns("[]");

        var handle = builder.AddAIBatchedStreamEnrich<TestDomain.Comment, TestDomain.SentimentResult>(client, options => options
            .WithSystemPrompt("Analyze.")
            .WithBatchTemplate(batch => "analyze")
            .WithResultMapper((item, _) => item)
            .WithBatchSize(10));

        Assert.NotNull(handle);
    }

    [Fact]
    public async Task AddAIBatchedStreamEnrich_ExecutesInPipeline()
    {
        var client = FakeChatClient.ThatReturns("""[{"label":"positive","score":0.9}]""");
        var sink = new InMemorySinkNode<TestDomain.Comment>();

        var context = PipelineContext.Default;
        context.Items[ClientContextKey] = client;
        context.Items[SinkContextKey] = sink;

        await PipelineRunner.Create().RunAsync<AIBatchedStreamEnrichPipelineDefinition>(context);

        Assert.Single(sink.Items);
        Assert.Equal("positive", sink.Items[0].Author);
    }

    [Fact]
    public async Task AddAIBatchedEnrichWithUnbatch_ExecutesBatchChain()
    {
        var client = FakeChatClient.ThatReturns("""[{"label":"positive","score":0.9},{"label":"negative","score":0.1}]""");
        var sink = new InMemorySinkNode<TestDomain.Comment>();

        var context = PipelineContext.Default;
        context.Items[ClientContextKey] = client;
        context.Items[SinkContextKey] = sink;

        await PipelineRunner.Create().RunAsync<AIBatchedEnrichWithUnbatchPipelineDefinition>(context);

        Assert.Equal(2, sink.Items.Count);
        var authors = sink.Items.Select(item => item.Author).ToHashSet(StringComparer.OrdinalIgnoreCase);
        Assert.Contains("positive", authors);
        Assert.Contains("negative", authors);
    }

    [Fact]
    public void AddAIBatchedEnrichWithUnbatch_WithNonPositiveBatchTimeout_Throws()
    {
        var builder = new PipelineBuilder();
        var client = FakeChatClient.ThatReturns("[]");

        static void Configure(AIBatchedEnrichOptionsBuilder<TestDomain.Comment, TestDomain.SentimentResult> options)
        {
            options
                .WithSystemPrompt("Analyze.")
                .WithBatchTemplate(batch => "analyze")
                .WithResultMapper((item, result) => item with { Author = result.Label });
        }

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            builder.AddAIBatchedEnrichWithUnbatch<TestDomain.Comment, TestDomain.SentimentResult>(
                client,
                batchSize: 2,
                batchTimeout: TimeSpan.Zero,
                configure: Configure));

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            builder.AddAIBatchedEnrichWithUnbatch<TestDomain.Comment, TestDomain.SentimentResult>(
                client,
                batchSize: 2,
                batchTimeout: TimeSpan.FromMilliseconds(-1),
                configure: Configure));
    }

    [Fact]
    public void NullBuilder_ThrowsArgumentNullException()
    {
        var client = FakeChatClient.ThatReturns("{}");

        Assert.Throws<ArgumentNullException>(() =>
            ((PipelineBuilder)null!).AddAITransform<string, string>(client, _ => { }));
    }

    private sealed class AIBatchedStreamEnrichPipelineDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var client = (IChatClient)context.Items[ClientContextKey];
            var sink = (InMemorySinkNode<TestDomain.Comment>)context.Items[SinkContextKey];

            var source = builder.AddInMemorySource("src", [new TestDomain.Comment("hello", "alice")]);
            var enrich = builder.AddAIBatchedStreamEnrich<TestDomain.Comment, TestDomain.SentimentResult>(client, options => options
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

    private sealed class AIBatchedEnrichWithUnbatchPipelineDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var client = (IChatClient)context.Items[ClientContextKey];
            var sink = (InMemorySinkNode<TestDomain.Comment>)context.Items[SinkContextKey];

            var source = builder.AddInMemorySource("src", [
                new TestDomain.Comment("love it", "alice"),
                new TestDomain.Comment("hate it", "bob"),
            ]);

            var (inputHandle, outputHandle) = builder.AddAIBatchedEnrichWithUnbatch<TestDomain.Comment, TestDomain.SentimentResult>(
                chatClient: client,
                batchSize: 2,
                batchTimeout: TimeSpan.FromSeconds(1),
                configure: options => options
                    .WithSystemPrompt("Analyze.")
                    .WithBatchTemplate(batch => "analyze")
                    .WithResultMapper((item, result) => item with { Author = result.Label }),
                name: "batched-enrich");

            var sinkHandle = builder.AddSink<InMemorySinkNode<TestDomain.Comment>, TestDomain.Comment>("sink");
            builder.AddPreconfiguredNodeInstance(sinkHandle.Id, sink);

            builder.Connect(source, inputHandle)
                .Connect(outputHandle, sinkHandle);
        }
    }
}
