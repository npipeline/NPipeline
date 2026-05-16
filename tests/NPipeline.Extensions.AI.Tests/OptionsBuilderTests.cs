using NPipeline.Extensions.AI.Configuration;

namespace NPipeline.Extensions.AI.Tests;

public class OptionsBuilderTests
{
    [Fact]
    public void AITransformOptionsBuilder_ValidatesRequiredFields()
    {
        var builder = new AITransformOptionsBuilder<SampleItem, SampleOutput>();
        Assert.Throws<InvalidOperationException>(() => builder.Build());

        builder.WithSystemPrompt("System prompt");
        Assert.Throws<InvalidOperationException>(() => builder.Build());

        builder.WithItemTemplate(x => x.Text);
        var options = builder.Build();
        Assert.Equal("System prompt", options.SystemPrompt);
    }

    [Fact]
    public void AITransformOptionsBuilder_AllOptionalFields()
    {
        var builder = new AITransformOptionsBuilder<SampleItem, SampleOutput>();

        var options = builder
            .WithSystemPrompt("SP")
            .WithItemTemplate(x => x.Text)
            .WithTemperature(0.5f)
            .WithMaxOutputTokens(200)
            .WithNativeStructuredOutput()
            .Build();

        Assert.Equal(0.5f, options.Temperature);
        Assert.Equal(200, options.MaxOutputTokens);
        Assert.True(options.UseNativeStructuredOutput);
    }

    [Fact]
    public void AIEnrichOptionsBuilder_ValidatesResultMapper()
    {
        var builder = new AIEnrichOptionsBuilder<SampleItem, string>();
        builder.WithSystemPrompt("SP").WithItemTemplate(x => x.Text);
        Assert.Throws<InvalidOperationException>(() => builder.Build());

        builder.WithResultMapper((input, field) => input);
        var options = builder.Build();
        Assert.NotNull(options.ResultMapper);
    }

    [Fact]
    public void AIBatchedStreamTransformOptionsBuilder_ValidatesBatchSize()
    {
        var builder = new AIBatchedStreamTransformOptionsBuilder<SampleItem, SampleOutput>();
        builder.WithSystemPrompt("SP").WithBatchTemplate(batch => string.Join(",", batch.Select(x => x.Text)));
        Assert.Throws<InvalidOperationException>(() => builder.Build());

        builder.WithBatchSize(10);
        var options = builder.Build();
        Assert.Equal(10, options.BatchSize);
    }

    [Fact]
    public void AIBatchedStreamTransformOptionsBuilder_RejectsZeroBatchSize()
    {
        var builder = new AIBatchedStreamTransformOptionsBuilder<SampleItem, SampleOutput>();
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithBatchSize(0));
    }

    [Fact]
    public void AIBatchedStreamTransformOptionsBuilder_RejectsNegativeBatchSize()
    {
        var builder = new AIBatchedStreamTransformOptionsBuilder<SampleItem, SampleOutput>();
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithBatchSize(-1));
    }

    [Fact]
    public void AITransformOptionsBuilder_SystemPromptWhitespace_Rejected()
    {
        var builder = new AITransformOptionsBuilder<SampleItem, SampleOutput>();
        builder.WithSystemPrompt("   ");
        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    [Fact]
    public void AITransformOptionsBuilder_RejectsNonPositiveMaxOutputTokens()
    {
        var builder = new AITransformOptionsBuilder<SampleItem, SampleOutput>();

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithMaxOutputTokens(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithMaxOutputTokens(-1));
    }

    [Fact]
    public void AIBatchedTransformOptionsBuilder_ValidatesRequiredFields()
    {
        var builder = new AIBatchedTransformOptionsBuilder<SampleItem, SampleOutput>();
        Assert.Throws<InvalidOperationException>(() => builder.Build());
        builder.WithSystemPrompt("SP");
        Assert.Throws<InvalidOperationException>(() => builder.Build());
        builder.WithBatchTemplate(batch => "batch");
        Assert.NotNull(builder.Build());
    }

    [Fact]
    public void AIBatchedStreamEnrichOptionsBuilder_ValidatesAllRequiredFields()
    {
        var builder = new AIBatchedStreamEnrichOptionsBuilder<SampleItem, SampleOutput>();
        Assert.Throws<InvalidOperationException>(() => builder.Build());
        builder.WithSystemPrompt("SP");
        Assert.Throws<InvalidOperationException>(() => builder.Build());
        builder.WithBatchTemplate(batch => "batch");
        Assert.Throws<InvalidOperationException>(() => builder.Build());
        builder.WithResultMapper((item, field) => item);
        Assert.Throws<InvalidOperationException>(() => builder.Build());
        builder.WithBatchSize(5);
        Assert.NotNull(builder.Build());
    }

    [Fact]
    public void AIBatchedStreamTransformOptionsBuilder_RejectsNonPositiveBatchTimeout()
    {
        var builder = new AIBatchedStreamTransformOptionsBuilder<SampleItem, SampleOutput>();

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithBatchTimeout(TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithBatchTimeout(TimeSpan.FromMilliseconds(-1)));
    }

    [Fact]
    public void AIBatchedStreamEnrichOptionsBuilder_RejectsNonPositiveBatchTimeout()
    {
        var builder = new AIBatchedStreamEnrichOptionsBuilder<SampleItem, SampleOutput>();

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithBatchTimeout(TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithBatchTimeout(TimeSpan.FromMilliseconds(-1)));
    }

    public record SampleItem(string Text);

    public record SampleOutput(string Result);
}
