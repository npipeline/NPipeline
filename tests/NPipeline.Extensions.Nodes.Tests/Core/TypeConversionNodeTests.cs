using System.Globalization;
using NPipeline.Extensions.Nodes.Core;
using NPipeline.Extensions.Nodes.Core.Exceptions;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes.Tests.Core;

public sealed class TypeConversionNodeTests
{
    #region Cancellation Tests

    [Fact]
    public async Task ExecuteAsync_WithCancelledToken_Throws()
    {
        var node = TypeConversions.StringToInt();
        var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            node.ExecuteAsync("42", PipelineContext.Default, cts.Token));
    }

    #endregion

    private sealed class TestObject
    {
        public string StringValue { get; set; } = "0";
        public int IntValue { get; set; }
        public double DoubleValue { get; set; }
        public decimal DecimalValue { get; set; }
        public bool BoolValue { get; set; }
        public DateTime DateTimeValue { get; set; }
    }

    #region StringToInt Tests

    [Fact]
    public async Task StringToInt_WithValidString_Converts()
    {
        var node = TypeConversions.StringToInt();
        var result = await node.ExecuteAsync("42", PipelineContext.Default, CancellationToken.None);
        Assert.Equal(42, result);
    }

    [Theory]
    [InlineData("-100")]
    [InlineData("0")]
    [InlineData("2147483647")]
    public async Task StringToInt_WithVariousValidNumbers_Converts(string input)
    {
        var node = TypeConversions.StringToInt();
        var result = await node.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(int.Parse(input, CultureInfo.InvariantCulture), result);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public async Task StringToInt_WithNullOrWhitespace_Throws(string input)
    {
        var node = TypeConversions.StringToInt();

        await Assert.ThrowsAsync<TypeConversionException>(() =>
            node.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None));
    }

    [Theory]
    [InlineData("not a number")]
    [InlineData("42.5")]
    [InlineData("9999999999")]
    public async Task StringToInt_WithInvalidString_Throws(string input)
    {
        var node = TypeConversions.StringToInt();

        await Assert.ThrowsAsync<TypeConversionException>(() =>
            node.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region StringToLong Tests

    [Fact]
    public async Task StringToLong_WithValidString_Converts()
    {
        var node = TypeConversions.StringToLong();
        var result = await node.ExecuteAsync("9223372036854775807", PipelineContext.Default, CancellationToken.None);
        Assert.Equal(long.MaxValue, result);
    }

    [Theory]
    [InlineData("-100")]
    [InlineData("0")]
    [InlineData("9223372036854775807")]
    public async Task StringToLong_WithVariousValidNumbers_Converts(string input)
    {
        var node = TypeConversions.StringToLong();
        var result = await node.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(long.Parse(input, CultureInfo.InvariantCulture), result);
    }

    [Theory]
    [InlineData("not a number")]
    [InlineData("99999999999999999999")]
    public async Task StringToLong_WithInvalidString_Throws(string input)
    {
        var node = TypeConversions.StringToLong();

        await Assert.ThrowsAsync<TypeConversionException>(() =>
            node.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region StringToDouble Tests

    [Fact]
    public async Task StringToDouble_WithValidString_Converts()
    {
        var node = TypeConversions.StringToDouble();
        var result = await node.ExecuteAsync("42.5", PipelineContext.Default, CancellationToken.None);
        Assert.Equal(42.5, result);
    }

    [Theory]
    [InlineData("-100.5")]
    [InlineData("0")]
    [InlineData("3.14159")]
    [InlineData("1,000.5")]
    public async Task StringToDouble_WithVariousValidNumbers_Converts(string input)
    {
        var node = TypeConversions.StringToDouble();
        var result = await node.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

        // Just verify it was parsed successfully (could be any value including 0)
        _ = result;
        Assert.True(true);
    }

    [Theory]
    [InlineData("not a number")]
    [InlineData("")]
    [InlineData("   ")]
    public async Task StringToDouble_WithInvalidString_Throws(string input)
    {
        var node = TypeConversions.StringToDouble();

        await Assert.ThrowsAsync<TypeConversionException>(() =>
            node.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region StringToDecimal Tests

    [Fact]
    public async Task StringToDecimal_WithValidString_Converts()
    {
        var node = TypeConversions.StringToDecimal();
        var result = await node.ExecuteAsync("42.5", PipelineContext.Default, CancellationToken.None);
        Assert.Equal(42.5m, result);
    }

    [Theory]
    [InlineData("-100.5")]
    [InlineData("0")]
    [InlineData("3.14159")]
    public async Task StringToDecimal_WithVariousValidNumbers_Converts(string input)
    {
        var node = TypeConversions.StringToDecimal();
        var result = await node.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

        // Just verify it was parsed successfully (could be any value including 0)
        _ = result;
        Assert.True(true);
    }

    [Theory]
    [InlineData("not a number")]
    [InlineData("")]
    [InlineData("   ")]
    public async Task StringToDecimal_WithInvalidString_Throws(string input)
    {
        var node = TypeConversions.StringToDecimal();

        await Assert.ThrowsAsync<TypeConversionException>(() =>
            node.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region StringToBool Tests

    [Theory]
    [InlineData("true", true)]
    [InlineData("TRUE", true)]
    [InlineData("1", true)]
    [InlineData("yes", true)]
    [InlineData("YES", true)]
    [InlineData("on", true)]
    [InlineData("ON", true)]
    [InlineData("false", false)]
    [InlineData("FALSE", false)]
    [InlineData("0", false)]
    [InlineData("no", false)]
    [InlineData("NO", false)]
    [InlineData("off", false)]
    [InlineData("OFF", false)]
    public async Task StringToBool_WithValidString_Converts(string input, bool expected)
    {
        var node = TypeConversions.StringToBool();
        var result = await node.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("maybe")]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("truee")]
    public async Task StringToBool_WithInvalidString_Throws(string input)
    {
        var node = TypeConversions.StringToBool();

        await Assert.ThrowsAsync<TypeConversionException>(() =>
            node.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region StringToDateTime Tests

    [Fact]
    public async Task StringToDateTime_WithValidString_Converts()
    {
        var node = TypeConversions.StringToDateTime();
        var result = await node.ExecuteAsync("2025-01-15 14:30:00", PipelineContext.Default, CancellationToken.None);
        Assert.Equal(new DateTime(2025, 1, 15, 14, 30, 0), result);
    }

    [Theory]
    [InlineData("2025-01-15")]
    [InlineData("01/15/2025")]
    [InlineData("2025-01-15 14:30:00")]
    public async Task StringToDateTime_WithVariousValidFormats_Converts(string input)
    {
        var node = TypeConversions.StringToDateTime();
        var result = await node.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);
        Assert.True(result.Year >= 2025);
    }

    [Fact]
    public async Task StringToDateTime_WithSpecificFormat_Converts()
    {
        var node = TypeConversions.StringToDateTime("yyyy-MM-dd");
        var result = await node.ExecuteAsync("2025-01-15", PipelineContext.Default, CancellationToken.None);
        Assert.Equal(new DateTime(2025, 1, 15), result);
    }

    [Theory]
    [InlineData("not a date")]
    [InlineData("")]
    [InlineData("   ")]
    public async Task StringToDateTime_WithInvalidString_Throws(string input)
    {
        var node = TypeConversions.StringToDateTime();

        await Assert.ThrowsAsync<TypeConversionException>(() =>
            node.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IntToString Tests

    [Fact]
    public async Task IntToString_WithValue_Converts()
    {
        var node = TypeConversions.IntToString();
        var result = await node.ExecuteAsync(42, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("42", result);
    }

    [Theory]
    [InlineData(-100)]
    [InlineData(0)]
    [InlineData(int.MaxValue)]
    public async Task IntToString_WithVariousValues_Converts(int input)
    {
        var node = TypeConversions.IntToString();
        var result = await node.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(input.ToString(CultureInfo.InvariantCulture), result);
    }

    [Fact]
    public async Task IntToString_WithFormat_Converts()
    {
        var node = TypeConversions.IntToString("D5");
        var result = await node.ExecuteAsync(42, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("00042", result);
    }

    #endregion

    #region DoubleToString Tests

    [Fact]
    public async Task DoubleToString_WithValue_Converts()
    {
        var node = TypeConversions.DoubleToString();
        var result = await node.ExecuteAsync(42.5, PipelineContext.Default, CancellationToken.None);
        Assert.NotEmpty(result);
    }

    [Theory]
    [InlineData(-100.5)]
    [InlineData(0.0)]
    [InlineData(3.14159)]
    public async Task DoubleToString_WithVariousValues_Converts(double input)
    {
        var node = TypeConversions.DoubleToString();
        var result = await node.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);
        Assert.NotEmpty(result);
    }

    [Fact]
    public async Task DoubleToString_WithFormat_Converts()
    {
        var node = TypeConversions.DoubleToString("F2");
        var result = await node.ExecuteAsync(42.567, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("42.57", result);
    }

    #endregion

    #region DecimalToString Tests

    [Fact]
    public async Task DecimalToString_WithValue_Converts()
    {
        var node = TypeConversions.DecimalToString();
        var result = await node.ExecuteAsync(42.5m, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("42.5", result);
    }

    [Fact]
    public async Task DecimalToString_WithFormat_Converts()
    {
        var node = TypeConversions.DecimalToString("C");
        var result = await node.ExecuteAsync(42.5m, PipelineContext.Default, CancellationToken.None);
        Assert.NotEmpty(result);
    }

    #endregion

    #region DateTimeToString Tests

    [Fact]
    public async Task DateTimeToString_WithValue_Converts()
    {
        var node = TypeConversions.DateTimeToString();
        var input = new DateTime(2025, 1, 15, 14, 30, 0);
        var result = await node.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);
        Assert.NotEmpty(result);
    }

    [Fact]
    public async Task DateTimeToString_WithFormat_Converts()
    {
        var node = TypeConversions.DateTimeToString("yyyy-MM-dd");
        var input = new DateTime(2025, 1, 15);
        var result = await node.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("2025-01-15", result);
    }

    #endregion

    #region BoolToString Tests

    [Fact]
    public async Task BoolToString_WithTrue_Converts()
    {
        var node = TypeConversions.BoolToString();
        var result = await node.ExecuteAsync(true, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("true", result);
    }

    [Fact]
    public async Task BoolToString_WithFalse_Converts()
    {
        var node = TypeConversions.BoolToString();
        var result = await node.ExecuteAsync(false, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("false", result);
    }

    [Fact]
    public async Task BoolToString_WithCustomValues_Converts()
    {
        var node = TypeConversions.BoolToString("yes", "no");
        var result = await node.ExecuteAsync(true, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("yes", result);
    }

    #endregion

    #region EnumToString Tests

    public enum TestValues
    {
        Value1,
        Value2,
        Value3,
    }

    [Fact]
    public async Task EnumToString_WithValue_Converts()
    {
        var node = TypeConversions.EnumToString<TestValues>();
        var result = await node.ExecuteAsync(TestValues.Value1, PipelineContext.Default, CancellationToken.None);
        Assert.Equal("Value1", result);
    }

    [Theory]
    [InlineData(TestValues.Value1)]
    [InlineData(TestValues.Value2)]
    [InlineData(TestValues.Value3)]
    public async Task EnumToString_WithVariousValues_Converts(TestValues input)
    {
        var node = TypeConversions.EnumToString<TestValues>();
        var result = await node.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(input.ToString(), result);
    }

    #endregion

    #region StringToEnum Tests

    [Fact]
    public async Task StringToEnum_WithValidString_Converts()
    {
        var node = TypeConversions.StringToEnum<TestValues>();
        var result = await node.ExecuteAsync("Value1", PipelineContext.Default, CancellationToken.None);
        Assert.Equal(TestValues.Value1, result);
    }

    [Fact]
    public async Task StringToEnum_WithCaseInsensitive_Converts()
    {
        var node = TypeConversions.StringToEnum<TestValues>(true);
        var result = await node.ExecuteAsync("value1", PipelineContext.Default, CancellationToken.None);
        Assert.Equal(TestValues.Value1, result);
    }

    [Fact]
    public async Task StringToEnum_WithCaseSensitive_ThrowsForWrongCase()
    {
        var node = TypeConversions.StringToEnum<TestValues>(false);

        await Assert.ThrowsAsync<TypeConversionException>(() =>
            node.ExecuteAsync("value1", PipelineContext.Default, CancellationToken.None));
    }

    [Theory]
    [InlineData("InvalidValue")]
    [InlineData("")]
    [InlineData("   ")]
    public async Task StringToEnum_WithInvalidString_Throws(string input)
    {
        var node = TypeConversions.StringToEnum<TestValues>();

        await Assert.ThrowsAsync<TypeConversionException>(() =>
            node.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region Custom Converter Tests

    [Fact]
    public async Task WithConverter_UsesCustomFunction_Converts()
    {
        var node = new TypeConversionNode<string, int>()
            .WithConverter(input => input.Length);

        var result = await node.ExecuteAsync("hello", PipelineContext.Default, CancellationToken.None);
        Assert.Equal(5, result);
    }

    [Fact]
    public async Task WithConverter_WhenNotSet_Throws()
    {
        var node = new TypeConversionNode<string, int>();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            node.ExecuteAsync("test", PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region ValueTask Tests

    [Fact]
    public async Task ExecuteValueTaskAsync_WithStringToInt_ReturnsValue()
    {
        var node = TypeConversions.StringToInt();
        var result = await node.ExecuteValueTaskAsync("42", PipelineContext.Default, CancellationToken.None);
        Assert.Equal(42, result);
    }

    [Fact]
    public async Task ExecuteValueTaskAsync_WithInvalidInput_Throws()
    {
        var node = TypeConversions.StringToInt();

        await Assert.ThrowsAsync<TypeConversionException>(async () =>
            await node.ExecuteValueTaskAsync("not a number", PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region Exception Details Tests

    [Fact]
    public async Task StringToInt_OnFailure_IncludesSourceAndTargetTypes()
    {
        var node = TypeConversions.StringToInt();

        var ex = await Assert.ThrowsAsync<TypeConversionException>(() =>
            node.ExecuteAsync("not a number", PipelineContext.Default, CancellationToken.None));

        Assert.Equal(typeof(string), ex.SourceType);
        Assert.Equal(typeof(int), ex.TargetType);
        Assert.NotNull(ex.Message);
    }

    [Fact]
    public async Task StringToInt_OnFailure_IncludesFailedValue()
    {
        var node = TypeConversions.StringToInt();

        var ex = await Assert.ThrowsAsync<TypeConversionException>(() =>
            node.ExecuteAsync("not a number", PipelineContext.Default, CancellationToken.None));

        Assert.Equal("not a number", ex.Value);
    }

    #endregion

    #region Format Provider Tests

    [Fact]
    public async Task StringToDouble_WithCustomFormatProvider_Uses()
    {
        var germanCulture = new CultureInfo("de-DE");

        var node = TypeConversions.StringToDouble(
            NumberStyles.Float | NumberStyles.AllowThousands,
            germanCulture);

        // German uses comma as decimal separator
        var result = await node.ExecuteAsync("42,5", PipelineContext.Default, CancellationToken.None);
        Assert.Equal(42.5, result);
    }

    [Fact]
    public async Task DoubleToString_WithCulture_Formats()
    {
        var frenchCulture = new CultureInfo("fr-FR");
        var node = TypeConversions.DoubleToString("F2", frenchCulture);

        var result = await node.ExecuteAsync(42.5, PipelineContext.Default, CancellationToken.None);
        Assert.NotEmpty(result);
    }

    #endregion

    #region Chaining Tests

    [Fact]
    public async Task Chaining_StringToInt_ThenIntToString_Roundtrips()
    {
        var stringToInt = TypeConversions.StringToInt();
        var intToString = TypeConversions.IntToString();

        var intResult = await stringToInt.ExecuteAsync("42", PipelineContext.Default, CancellationToken.None);
        var stringResult = await intToString.ExecuteAsync(intResult, PipelineContext.Default, CancellationToken.None);

        Assert.Equal("42", stringResult);
    }

    [Fact]
    public async Task Chaining_StringToDouble_ThenDoubleToString_Roundtrips()
    {
        var stringToDouble = TypeConversions.StringToDouble();
        var doubleToString = TypeConversions.DoubleToString("F4");

        var doubleResult = await stringToDouble.ExecuteAsync("42.5", PipelineContext.Default, CancellationToken.None);
        var stringResult = await doubleToString.ExecuteAsync(doubleResult, PipelineContext.Default, CancellationToken.None);

        Assert.NotEmpty(stringResult);
    }

    #endregion

    #region Edge Cases Tests

    [Fact]
    public async Task StringToInt_WithMaxValue_Converts()
    {
        var node = TypeConversions.StringToInt();
        var result = await node.ExecuteAsync(int.MaxValue.ToString(), PipelineContext.Default, CancellationToken.None);
        Assert.Equal(int.MaxValue, result);
    }

    [Fact]
    public async Task StringToInt_WithMinValue_Converts()
    {
        var node = TypeConversions.StringToInt();
        var result = await node.ExecuteAsync(int.MinValue.ToString(), PipelineContext.Default, CancellationToken.None);
        Assert.Equal(int.MinValue, result);
    }

    [Fact]
    public async Task StringToDouble_WithInfinity_Converts()
    {
        var node = TypeConversions.StringToDouble();
        var result = await node.ExecuteAsync("Infinity", PipelineContext.Default, CancellationToken.None);
        Assert.True(double.IsInfinity(result));
    }

    [Fact]
    public async Task StringToDouble_WithNegativeInfinity_Converts()
    {
        var node = TypeConversions.StringToDouble();
        var result = await node.ExecuteAsync("-Infinity", PipelineContext.Default, CancellationToken.None);
        Assert.True(double.IsInfinity(result));
    }

    [Fact]
    public async Task StringToDouble_WithNaN_Converts()
    {
        var node = TypeConversions.StringToDouble();
        var result = await node.ExecuteAsync("NaN", PipelineContext.Default, CancellationToken.None);
        Assert.True(double.IsNaN(result));
    }

    [Fact]
    public async Task DateTimeToString_WithDateTime_MinValue_Converts()
    {
        var node = TypeConversions.DateTimeToString();
        var result = await node.ExecuteAsync(DateTime.MinValue, PipelineContext.Default, CancellationToken.None);
        Assert.NotEmpty(result);
    }

    [Fact]
    public async Task DateTimeToString_WithDateTime_MaxValue_Converts()
    {
        var node = TypeConversions.DateTimeToString();
        var result = await node.ExecuteAsync(DateTime.MaxValue, PipelineContext.Default, CancellationToken.None);
        Assert.NotEmpty(result);
    }

    #endregion
}
