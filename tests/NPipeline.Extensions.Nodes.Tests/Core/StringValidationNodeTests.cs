using NPipeline.Extensions.Nodes.Core;
using NPipeline.Extensions.Nodes.Core.Exceptions;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes.Tests.Core;

public class StringValidationNodeTests
{
    private sealed class TestObject
    {
        public string? StringValue { get; set; }
    }

    #region IsNotEmpty Tests

    [Fact]
    public async Task IsNotEmpty_WithNonEmptyString_Passes()
    {
        var node = new StringValidationNode<TestObject>();
        node.IsNotEmpty(x => x.StringValue);

        var item = new TestObject { StringValue = "test" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsNotEmpty_WithEmptyString_Throws()
    {
        var node = new StringValidationNode<TestObject>();
        node.IsNotEmpty(x => x.StringValue);

        var item = new TestObject { StringValue = "" };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    [Fact]
    public async Task IsNotEmpty_WithNull_Throws()
    {
        var node = new StringValidationNode<TestObject>();
        node.IsNotEmpty(x => x.StringValue);

        var item = new TestObject { StringValue = null };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsNotWhitespace Tests

    [Fact]
    public async Task IsNotWhitespace_WithNonWhitespaceString_Passes()
    {
        var node = new StringValidationNode<TestObject>();
        node.IsNotWhitespace(x => x.StringValue);

        var item = new TestObject { StringValue = "test" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsNotWhitespace_WithWhitespaceString_Throws()
    {
        var node = new StringValidationNode<TestObject>();
        node.IsNotWhitespace(x => x.StringValue);

        var item = new TestObject { StringValue = "   " };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    [Fact]
    public async Task IsNotWhitespace_WithNull_Throws()
    {
        var node = new StringValidationNode<TestObject>();
        node.IsNotWhitespace(x => x.StringValue);

        var item = new TestObject { StringValue = null };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region HasMinLength Tests

    [Fact]
    public async Task HasMinLength_WithSufficientLength_Passes()
    {
        var node = new StringValidationNode<TestObject>();
        node.HasMinLength(x => x.StringValue, 3);

        var item = new TestObject { StringValue = "test" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task HasMinLength_WithInsufficientLength_Throws()
    {
        var node = new StringValidationNode<TestObject>();
        node.HasMinLength(x => x.StringValue, 5);

        var item = new TestObject { StringValue = "test" };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    [Fact]
    public async Task HasMinLength_WithNull_Passes()
    {
        var node = new StringValidationNode<TestObject>();
        node.HasMinLength(x => x.StringValue, 3);

        var item = new TestObject { StringValue = null };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    #endregion

    #region HasMaxLength Tests

    [Fact]
    public async Task HasMaxLength_WithSufficientLength_Passes()
    {
        var node = new StringValidationNode<TestObject>();
        node.HasMaxLength(x => x.StringValue, 5);

        var item = new TestObject { StringValue = "test" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task HasMaxLength_WithExcessiveLength_Throws()
    {
        var node = new StringValidationNode<TestObject>();
        node.HasMaxLength(x => x.StringValue, 3);

        var item = new TestObject { StringValue = "test" };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    [Fact]
    public async Task HasMaxLength_WithNull_Passes()
    {
        var node = new StringValidationNode<TestObject>();
        node.HasMaxLength(x => x.StringValue, 3);

        var item = new TestObject { StringValue = null };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    #endregion

    #region HasLengthBetween Tests

    [Fact]
    public async Task HasLengthBetween_WithLengthInRange_Passes()
    {
        var node = new StringValidationNode<TestObject>();
        node.HasLengthBetween(x => x.StringValue, 3, 5);

        var item = new TestObject { StringValue = "test" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task HasLengthBetween_WithLengthTooShort_Throws()
    {
        var node = new StringValidationNode<TestObject>();
        node.HasLengthBetween(x => x.StringValue, 5, 10);

        var item = new TestObject { StringValue = "test" };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    [Fact]
    public async Task HasLengthBetween_WithLengthTooLong_Throws()
    {
        var node = new StringValidationNode<TestObject>();
        node.HasLengthBetween(x => x.StringValue, 1, 3);

        var item = new TestObject { StringValue = "test" };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsEmail Tests

    [Fact]
    public async Task IsEmail_WithValidEmail_Passes()
    {
        var node = new StringValidationNode<TestObject>();
        node.IsEmail(x => x.StringValue);

        var item = new TestObject { StringValue = "test@example.com" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsEmail_WithInvalidEmail_Throws()
    {
        var node = new StringValidationNode<TestObject>();
        node.IsEmail(x => x.StringValue);

        var item = new TestObject { StringValue = "notanemail" };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    [Fact]
    public async Task IsEmail_WithNull_Passes()
    {
        var node = new StringValidationNode<TestObject>();
        node.IsEmail(x => x.StringValue);

        var item = new TestObject { StringValue = null };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    #endregion

    #region IsAlphanumeric Tests

    [Fact]
    public async Task IsAlphanumeric_WithAlphanumericString_Passes()
    {
        var node = new StringValidationNode<TestObject>();
        node.IsAlphanumeric(x => x.StringValue);

        var item = new TestObject { StringValue = "test123" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsAlphanumeric_WithSpecialCharacters_Throws()
    {
        var node = new StringValidationNode<TestObject>();
        node.IsAlphanumeric(x => x.StringValue);

        var item = new TestObject { StringValue = "test@123" };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsAlphabetic Tests

    [Fact]
    public async Task IsAlphabetic_WithAlphabeticString_Passes()
    {
        var node = new StringValidationNode<TestObject>();
        node.IsAlphabetic(x => x.StringValue);

        var item = new TestObject { StringValue = "test" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsAlphabetic_WithDigits_Throws()
    {
        var node = new StringValidationNode<TestObject>();
        node.IsAlphabetic(x => x.StringValue);

        var item = new TestObject { StringValue = "test123" };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsDigitsOnly Tests

    [Fact]
    public async Task IsDigitsOnly_WithOnlyDigits_Passes()
    {
        var node = new StringValidationNode<TestObject>();
        node.IsDigitsOnly(x => x.StringValue);

        var item = new TestObject { StringValue = "12345" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsDigitsOnly_WithLetters_Throws()
    {
        var node = new StringValidationNode<TestObject>();
        node.IsDigitsOnly(x => x.StringValue);

        var item = new TestObject { StringValue = "123a45" };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsNumeric Tests

    [Fact]
    public async Task IsNumeric_WithValidNumber_Passes()
    {
        var node = new StringValidationNode<TestObject>();
        node.IsNumeric(x => x.StringValue);

        var item = new TestObject { StringValue = "123.45" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsNumeric_WithNonNumber_Throws()
    {
        var node = new StringValidationNode<TestObject>();
        node.IsNumeric(x => x.StringValue);

        var item = new TestObject { StringValue = "abc" };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region Contains Tests

    [Fact]
    public async Task Contains_WithContainingString_Passes()
    {
        var node = new StringValidationNode<TestObject>();
        node.Contains(x => x.StringValue, "est");

        var item = new TestObject { StringValue = "test" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task Contains_WithoutContainingString_Throws()
    {
        var node = new StringValidationNode<TestObject>();
        node.Contains(x => x.StringValue, "xyz");

        var item = new TestObject { StringValue = "test" };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region StartsWith Tests

    [Fact]
    public async Task StartsWith_WithMatchingPrefix_Passes()
    {
        var node = new StringValidationNode<TestObject>();
        node.StartsWith(x => x.StringValue, "te");

        var item = new TestObject { StringValue = "test" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task StartsWith_WithoutMatchingPrefix_Throws()
    {
        var node = new StringValidationNode<TestObject>();
        node.StartsWith(x => x.StringValue, "ab");

        var item = new TestObject { StringValue = "test" };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region EndsWith Tests

    [Fact]
    public async Task EndsWith_WithMatchingSuffix_Passes()
    {
        var node = new StringValidationNode<TestObject>();
        node.EndsWith(x => x.StringValue, "st");

        var item = new TestObject { StringValue = "test" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task EndsWith_WithoutMatchingSuffix_Throws()
    {
        var node = new StringValidationNode<TestObject>();
        node.EndsWith(x => x.StringValue, "xy");

        var item = new TestObject { StringValue = "test" };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsInList Tests

    [Fact]
    public async Task IsInList_WithContainedValue_Passes()
    {
        var node = new StringValidationNode<TestObject>();
        node.IsInList(x => x.StringValue, ["test", "demo", "sample"]);

        var item = new TestObject { StringValue = "test" };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsInList_WithoutContainedValue_Throws()
    {
        var node = new StringValidationNode<TestObject>();
        node.IsInList(x => x.StringValue, ["test", "demo", "sample"]);

        var item = new TestObject { StringValue = "other" };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    [Fact]
    public async Task IsInList_WithNull_Passes()
    {
        var node = new StringValidationNode<TestObject>();
        node.IsInList(x => x.StringValue, ["test", "demo", "sample"]);

        var item = new TestObject { StringValue = null };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    #endregion
}
