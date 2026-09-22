using NPipeline.ErrorHandling;
using NPipeline.Extensions.AI.Chat.Exceptions;

namespace NPipeline.Extensions.AI.Chat.Tests;

public class ChatTransformExceptionTests
{
    [Fact]
    public void Constructor_SetsProperties()
    {
        var inner = new Exception("inner");

        var ex = new ChatTransformException("Something went wrong", inner)
        {
            OriginalItem = new { Text = "hello" },
            PromptSent = "Classify: hello",
            ModelUsed = "gpt-4o",
            RawResponse = "bad json response",
        };

        Assert.Equal("Something went wrong", ex.Message);
        Assert.Equal(inner, ex.InnerException);
        Assert.Equal("CHAT_TRANSFORM_ERROR", ex.ErrorCode);
        Assert.Equal("chat", ex.Provider);
        Assert.NotNull(ex.OriginalItem);
        Assert.Equal("Classify: hello", ex.PromptSent);
        Assert.Equal("gpt-4o", ex.ModelUsed);
        Assert.Equal("bad json response", ex.RawResponse);
    }

    [Fact]
    public void Constructor_NullProperties_AreAllowed()
    {
        var ex = new ChatTransformException("failed", new Exception("inner"));

        Assert.Null(ex.OriginalItem);
        Assert.Null(ex.PromptSent);
        Assert.Null(ex.ModelUsed);
        Assert.Null(ex.RawResponse);
    }

    [Fact]
    public void InheritsFromPipelineException()
    {
        var ex = new ChatTransformException("test", new Exception("inner"));
        Assert.IsType<PipelineException>(ex, false);
    }
}
