using NPipeline.Extensions.Nodes.Core;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes.Tests.Core;

public class StringCleansingNodeTests
{
    #region TrimStart Tests

    [Fact]
    public async Task TrimStart_WithLeadingWhitespace_Removes()
    {
        var node = new StringCleansingNode<TestObject>();
        node.TrimStart(x => x.StringValue);

        var item = new TestObject { StringValue = "  test  " };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("test  ", result.StringValue);
    }

    #endregion

    #region TrimEnd Tests

    [Fact]
    public async Task TrimEnd_WithTrailingWhitespace_Removes()
    {
        var node = new StringCleansingNode<TestObject>();
        node.TrimEnd(x => x.StringValue);

        var item = new TestObject { StringValue = "  test  " };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("  test", result.StringValue);
    }

    #endregion

    #region CollapseWhitespace Tests

    [Fact]
    public async Task CollapseWhitespace_WithMultipleSpaces_Collapses()
    {
        var node = new StringCleansingNode<TestObject>();
        node.CollapseWhitespace(x => x.StringValue);

        var item = new TestObject { StringValue = "test  with   multiple    spaces" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("test with multiple spaces", result.StringValue);
    }

    #endregion

    #region RemoveWhitespace Tests

    [Fact]
    public async Task RemoveWhitespace_RemovesAllWhitespace()
    {
        var node = new StringCleansingNode<TestObject>();
        node.RemoveWhitespace(x => x.StringValue);

        var item = new TestObject { StringValue = "test with spaces" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("testwithspaces", result.StringValue);
    }

    #endregion

    #region ToLower Tests

    [Fact]
    public async Task ToLower_ConvertsToLowercase()
    {
        var node = new StringCleansingNode<TestObject>();
        node.ToLower(x => x.StringValue);

        var item = new TestObject { StringValue = "TeSt" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("test", result.StringValue);
    }

    #endregion

    #region ToUpper Tests

    [Fact]
    public async Task ToUpper_ConvertsToUppercase()
    {
        var node = new StringCleansingNode<TestObject>();
        node.ToUpper(x => x.StringValue);

        var item = new TestObject { StringValue = "TeSt" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("TEST", result.StringValue);
    }

    #endregion

    #region ToTitleCase Tests

    [Fact]
    public async Task ToTitleCase_CapitalizeWords()
    {
        var node = new StringCleansingNode<TestObject>();
        node.ToTitleCase(x => x.StringValue);

        var item = new TestObject { StringValue = "hello world" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("Hello World", result.StringValue);
    }

    #endregion

    #region RemoveSpecialCharacters Tests

    [Fact]
    public async Task RemoveSpecialCharacters_RemovesNonAlphanumeric()
    {
        var node = new StringCleansingNode<TestObject>();
        node.RemoveSpecialCharacters(x => x.StringValue);

        var item = new TestObject { StringValue = "test@#$%123" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("test123", result.StringValue);
    }

    #endregion

    #region RemoveDigits Tests

    [Fact]
    public async Task RemoveDigits_RemovesAllNumbers()
    {
        var node = new StringCleansingNode<TestObject>();
        node.RemoveDigits(x => x.StringValue);

        var item = new TestObject { StringValue = "test123abc456" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("testabc", result.StringValue);
    }

    #endregion

    #region RemoveNonAscii Tests

    [Fact]
    public async Task RemoveNonAscii_RemovesUnicode()
    {
        var node = new StringCleansingNode<TestObject>();
        node.RemoveNonAscii(x => x.StringValue);

        var item = new TestObject { StringValue = "test™café" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.DoesNotContain("™", result.StringValue);
    }

    #endregion

    #region Replace Tests

    [Fact]
    public async Task Replace_Replaces()
    {
        var node = new StringCleansingNode<TestObject>();
        node.Replace(x => x.StringValue, "test", "demo");

        var item = new TestObject { StringValue = "test string" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("demo string", result.StringValue);
    }

    #endregion

    private sealed class TestObject
    {
        public string? StringValue { get; set; }
    }

    #region Trim Tests

    [Fact]
    public async Task Trim_WithWhitespace_Removes()
    {
        var node = new StringCleansingNode<TestObject>();
        node.Trim(x => x.StringValue);

        var item = new TestObject { StringValue = "  test  " };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("test", result.StringValue);
    }

    [Fact]
    public async Task Trim_WithNull_Stays()
    {
        var node = new StringCleansingNode<TestObject>();
        node.Trim(x => x.StringValue);

        var item = new TestObject { StringValue = null };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Null(result.StringValue);
    }

    #endregion

    #region Truncate Tests

    [Fact]
    public async Task Truncate_WithLongString_Truncates()
    {
        var node = new StringCleansingNode<TestObject>();
        node.Truncate(x => x.StringValue, 4);

        var item = new TestObject { StringValue = "testing" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("test", result.StringValue);
    }

    [Fact]
    public async Task Truncate_WithShortString_NoChange()
    {
        var node = new StringCleansingNode<TestObject>();
        node.Truncate(x => x.StringValue, 10);

        var item = new TestObject { StringValue = "test" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("test", result.StringValue);
    }

    #endregion

    #region EnsurePrefix Tests

    [Fact]
    public async Task EnsurePrefix_WithoutPrefix_Adds()
    {
        var node = new StringCleansingNode<TestObject>();
        node.EnsurePrefix(x => x.StringValue, "pre_");

        var item = new TestObject { StringValue = "test" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("pre_test", result.StringValue);
    }

    [Fact]
    public async Task EnsurePrefix_WithPrefix_NoChange()
    {
        var node = new StringCleansingNode<TestObject>();
        node.EnsurePrefix(x => x.StringValue, "pre_");

        var item = new TestObject { StringValue = "pre_test" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("pre_test", result.StringValue);
    }

    #endregion

    #region EnsureSuffix Tests

    [Fact]
    public async Task EnsureSuffix_WithoutSuffix_Adds()
    {
        var node = new StringCleansingNode<TestObject>();
        node.EnsureSuffix(x => x.StringValue, "_suf");

        var item = new TestObject { StringValue = "test" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("test_suf", result.StringValue);
    }

    [Fact]
    public async Task EnsureSuffix_WithSuffix_NoChange()
    {
        var node = new StringCleansingNode<TestObject>();
        node.EnsureSuffix(x => x.StringValue, "_suf");

        var item = new TestObject { StringValue = "test_suf" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("test_suf", result.StringValue);
    }

    #endregion

    #region DefaultIfNullOrWhitespace Tests

    [Fact]
    public async Task DefaultIfNullOrWhitespace_WithNull_ReplacesWithDefault()
    {
        var node = new StringCleansingNode<TestObject>();
        node.DefaultIfNullOrWhitespace(x => x.StringValue, "default");

        var item = new TestObject { StringValue = null };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("default", result.StringValue);
    }

    [Fact]
    public async Task DefaultIfNullOrWhitespace_WithWhitespace_ReplacesWithDefault()
    {
        var node = new StringCleansingNode<TestObject>();
        node.DefaultIfNullOrWhitespace(x => x.StringValue, "default");

        var item = new TestObject { StringValue = "   " };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("default", result.StringValue);
    }

    [Fact]
    public async Task DefaultIfNullOrWhitespace_WithValue_NoChange()
    {
        var node = new StringCleansingNode<TestObject>();
        node.DefaultIfNullOrWhitespace(x => x.StringValue, "default");

        var item = new TestObject { StringValue = "test" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("test", result.StringValue);
    }

    #endregion

    #region DefaultIfNullOrEmpty Tests

    [Fact]
    public async Task DefaultIfNullOrEmpty_WithNull_ReplacesWithDefault()
    {
        var node = new StringCleansingNode<TestObject>();
        node.DefaultIfNullOrEmpty(x => x.StringValue, "default");

        var item = new TestObject { StringValue = null };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("default", result.StringValue);
    }

    [Fact]
    public async Task DefaultIfNullOrEmpty_WithEmpty_ReplacesWithDefault()
    {
        var node = new StringCleansingNode<TestObject>();
        node.DefaultIfNullOrEmpty(x => x.StringValue, "default");

        var item = new TestObject { StringValue = "" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("default", result.StringValue);
    }

    [Fact]
    public async Task DefaultIfNullOrEmpty_WithValue_NoChange()
    {
        var node = new StringCleansingNode<TestObject>();
        node.DefaultIfNullOrEmpty(x => x.StringValue, "default");

        var item = new TestObject { StringValue = "test" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("test", result.StringValue);
    }

    #endregion

    #region NullIfWhitespace Tests

    [Fact]
    public async Task NullIfWhitespace_WithWhitespace_BecomesNull()
    {
        var node = new StringCleansingNode<TestObject>();
        node.NullIfWhitespace(x => x.StringValue);

        var item = new TestObject { StringValue = "   " };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Null(result.StringValue);
    }

    [Fact]
    public async Task NullIfWhitespace_WithValue_NoChange()
    {
        var node = new StringCleansingNode<TestObject>();
        node.NullIfWhitespace(x => x.StringValue);

        var item = new TestObject { StringValue = "test" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("test", result.StringValue);
    }

    #endregion
}
