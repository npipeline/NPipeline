using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Tests;

public class PipelineBuilderExtensionsTests
{
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
    public void NullBuilder_ThrowsArgumentNullException()
    {
        var client = FakeChatClient.ThatReturns("{}");

        Assert.Throws<ArgumentNullException>(() =>
            ((PipelineBuilder)null!).AddAITransform<string, string>(client, _ => { }));
    }
}
