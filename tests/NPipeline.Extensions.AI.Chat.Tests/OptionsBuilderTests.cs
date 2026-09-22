using NPipeline.Extensions.AI.Chat.Configuration;

namespace NPipeline.Extensions.AI.Chat.Tests;

public class OptionsBuilderTests
{
    [Fact]
    public void ChatTransformOptionsBuilder_ValidatesRequiredFields()
    {
        var builder = new ChatTransformOptionsBuilder<SampleItem, SampleOutput>();
        Assert.Throws<InvalidOperationException>(() => builder.Build());

        builder.WithSystemPrompt("System prompt");
        Assert.Throws<InvalidOperationException>(() => builder.Build());

        builder.WithItemTemplate(x => x.Text);
        var options = builder.Build();
        Assert.Equal("System prompt", options.SystemPrompt);
    }

    [Fact]
    public void ChatTransformOptionsBuilder_AllOptionalFields()
    {
        var builder = new ChatTransformOptionsBuilder<SampleItem, SampleOutput>();

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
    public void ChatEnrichmentOptionsBuilder_ValidatesResultMapper()
    {
        var builder = new ChatEnrichmentOptionsBuilder<SampleItem, string>();
        builder.WithSystemPrompt("SP").WithItemTemplate(x => x.Text);
        Assert.Throws<InvalidOperationException>(() => builder.Build());

        builder.WithResultMapper((input, field) => input);
        var options = builder.Build();
        Assert.NotNull(options.ResultMapper);
    }

    [Fact]
    public void ChatBatchedStreamTransformOptionsBuilder_ValidatesBatchSize()
    {
        var builder = new ChatBatchedStreamTransformOptionsBuilder<SampleItem, SampleOutput>();
        builder.WithSystemPrompt("SP").WithBatchTemplate(batch => string.Join(",", batch.Select(x => x.Text)));
        Assert.Throws<InvalidOperationException>(() => builder.Build());

        builder.WithBatchSize(10);
        var options = builder.Build();
        Assert.Equal(10, options.BatchSize);
    }

    [Fact]
    public void ChatBatchedStreamTransformOptionsBuilder_RejectsZeroBatchSize()
    {
        var builder = new ChatBatchedStreamTransformOptionsBuilder<SampleItem, SampleOutput>();
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithBatchSize(0));
    }

    [Fact]
    public void ChatBatchedStreamTransformOptionsBuilder_RejectsNegativeBatchSize()
    {
        var builder = new ChatBatchedStreamTransformOptionsBuilder<SampleItem, SampleOutput>();
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithBatchSize(-1));
    }

    [Fact]
    public void ChatTransformOptionsBuilder_SystemPromptWhitespace_Rejected()
    {
        var builder = new ChatTransformOptionsBuilder<SampleItem, SampleOutput>();
        builder.WithSystemPrompt("   ");
        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    [Fact]
    public void ChatTransformOptionsBuilder_RejectsNonPositiveMaxOutputTokens()
    {
        var builder = new ChatTransformOptionsBuilder<SampleItem, SampleOutput>();

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithMaxOutputTokens(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithMaxOutputTokens(-1));
    }

    [Fact]
    public void ChatBatchedTransformOptionsBuilder_ValidatesRequiredFields()
    {
        var builder = new ChatBatchedTransformOptionsBuilder<SampleItem, SampleOutput>();
        Assert.Throws<InvalidOperationException>(() => builder.Build());
        builder.WithSystemPrompt("SP");
        Assert.Throws<InvalidOperationException>(() => builder.Build());
        builder.WithBatchTemplate(batch => "batch");
        Assert.NotNull(builder.Build());
    }

    [Fact]
    public void ChatBatchedStreamEnrichmentOptionsBuilder_ValidatesAllRequiredFields()
    {
        var builder = new ChatBatchedStreamEnrichmentOptionsBuilder<SampleItem, SampleOutput>();
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
    public void ChatBatchedStreamTransformOptionsBuilder_RejectsNonPositiveBatchTimeout()
    {
        var builder = new ChatBatchedStreamTransformOptionsBuilder<SampleItem, SampleOutput>();

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithBatchTimeout(TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithBatchTimeout(TimeSpan.FromMilliseconds(-1)));
    }

    [Fact]
    public void ChatBatchedStreamEnrichmentOptionsBuilder_RejectsNonPositiveBatchTimeout()
    {
        var builder = new ChatBatchedStreamEnrichmentOptionsBuilder<SampleItem, SampleOutput>();

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithBatchTimeout(TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithBatchTimeout(TimeSpan.FromMilliseconds(-1)));
    }

    public record SampleItem(string Text);

    public record SampleOutput(string Result);
}
