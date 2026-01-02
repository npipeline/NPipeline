using NPipeline.Extensions.Nodes.Core;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes.Tests.Core;

public class NumericCleansingNodeTests
{
    #region Floor Tests

    [Fact]
    public async Task Floor_RemovesDecimal_Floors()
    {
        var node = new NumericCleansingNode<TestObject>();
        node.Floor(x => x.DoubleValue);

        var item = new TestObject { DoubleValue = 5.9 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(5.0, result.DoubleValue);
    }

    #endregion

    #region Ceiling Tests

    [Fact]
    public async Task Ceiling_RoundsUp_Ceils()
    {
        var node = new NumericCleansingNode<TestObject>();
        node.Ceiling(x => x.DoubleValue);

        var item = new TestObject { DoubleValue = 5.1 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(6.0, result.DoubleValue);
    }

    #endregion

    private sealed class TestObject
    {
        public int IntValue { get; set; }
        public double DoubleValue { get; set; }
        public decimal DecimalValue { get; set; }
        public double? NullableDoubleValue { get; set; }
        public decimal? NullableDecimalValue { get; set; }
    }

    #region Clamp Tests

    [Fact]
    public async Task Clamp_WithValueWithinBounds_ReturnsValue()
    {
        var node = new NumericCleansingNode<TestObject>();
        node.Clamp(x => x.IntValue, 0, 10);

        var item = new TestObject { IntValue = 5 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(5, result.IntValue);
    }

    [Fact]
    public async Task Clamp_WithValueBelowBounds_Clamps()
    {
        var node = new NumericCleansingNode<TestObject>();
        node.Clamp(x => x.IntValue, 0, 10);

        var item = new TestObject { IntValue = -5 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(0, result.IntValue);
    }

    [Fact]
    public async Task Clamp_WithValueAboveBounds_Clamps()
    {
        var node = new NumericCleansingNode<TestObject>();
        node.Clamp(x => x.IntValue, 0, 10);

        var item = new TestObject { IntValue = 15 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(10, result.IntValue);
    }

    #endregion

    #region Round Tests

    [Fact]
    public async Task Round_WithPrecision_Rounds()
    {
        var node = new NumericCleansingNode<TestObject>();
        node.Round(x => x.DoubleValue);

        var item = new TestObject { DoubleValue = 5.556 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(5.56, result.DoubleValue);
    }

    [Fact]
    public async Task Round_WithNullableValue_Rounds()
    {
        var node = new NumericCleansingNode<TestObject>();
        node.Round(x => x.NullableDoubleValue);

        var item = new TestObject { NullableDoubleValue = 5.556 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(5.56, result.NullableDoubleValue);
    }

    #endregion

    #region AbsoluteValue Tests

    [Fact]
    public async Task AbsoluteValue_WithNegativeInt_ReturnsPositive()
    {
        var node = new NumericCleansingNode<TestObject>();
        node.AbsoluteValue(x => x.IntValue);

        var item = new TestObject { IntValue = -5 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(5, result.IntValue);
    }

    [Fact]
    public async Task AbsoluteValue_WithPositiveInt_ReturnsPositive()
    {
        var node = new NumericCleansingNode<TestObject>();
        node.AbsoluteValue(x => x.IntValue);

        var item = new TestObject { IntValue = 5 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(5, result.IntValue);
    }

    [Fact]
    public async Task AbsoluteValue_WithNegativeDouble_ReturnsPositive()
    {
        var node = new NumericCleansingNode<TestObject>();
        node.AbsoluteValue(x => x.DoubleValue);

        var item = new TestObject { DoubleValue = -5.5 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(5.5, result.DoubleValue);
    }

    [Fact]
    public async Task AbsoluteValue_WithNegativeDecimal_ReturnsPositive()
    {
        var node = new NumericCleansingNode<TestObject>();
        node.AbsoluteValue(x => x.DecimalValue);

        var item = new TestObject { DecimalValue = -5.5m };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(5.5m, result.DecimalValue);
    }

    #endregion

    #region Scale Tests

    [Fact]
    public async Task Scale_MultipliesDouble_Scales()
    {
        var node = new NumericCleansingNode<TestObject>();
        node.Scale(x => x.DecimalValue, 2);

        var item = new TestObject { DecimalValue = 5m };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(10m, result.DecimalValue);
    }

    [Fact]
    public async Task Scale_MultipliesDecimal_Scales()
    {
        var node = new NumericCleansingNode<TestObject>();
        node.Scale(x => x.DecimalValue, 2.5m);

        var item = new TestObject { DecimalValue = 5m };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(12.5m, result.DecimalValue);
    }

    #endregion

    #region DefaultIfNull Tests

    [Fact]
    public async Task DefaultIfNull_WithNullValue_ReturnsDefault()
    {
        var node = new NumericCleansingNode<TestObject>();
        node.DefaultIfNull(x => x.NullableDoubleValue, 0.0);

        var item = new TestObject { NullableDoubleValue = null };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(0.0, result.NullableDoubleValue);
    }

    [Fact]
    public async Task DefaultIfNull_WithValue_ReturnsValue()
    {
        var node = new NumericCleansingNode<TestObject>();
        node.DefaultIfNull(x => x.NullableDoubleValue, 0.0);

        var item = new TestObject { NullableDoubleValue = 5.5 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(5.5, result.NullableDoubleValue);
    }

    #endregion

    #region ToZeroIfNegative Tests

    [Fact]
    public async Task ToZeroIfNegative_WithNegativeInt_ReturnsZero()
    {
        var node = new NumericCleansingNode<TestObject>();
        node.ToZeroIfNegative(x => x.IntValue);

        var item = new TestObject { IntValue = -5 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(0, result.IntValue);
    }

    [Fact]
    public async Task ToZeroIfNegative_WithPositiveInt_ReturnsValue()
    {
        var node = new NumericCleansingNode<TestObject>();
        node.ToZeroIfNegative(x => x.IntValue);

        var item = new TestObject { IntValue = 5 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(5, result.IntValue);
    }

    [Fact]
    public async Task ToZeroIfNegative_WithNegativeDouble_ReturnsZero()
    {
        var node = new NumericCleansingNode<TestObject>();
        node.ToZeroIfNegative(x => x.DoubleValue);

        var item = new TestObject { DoubleValue = -5.5 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(0.0, result.DoubleValue);
    }

    [Fact]
    public async Task ToZeroIfNegative_WithNegativeDecimal_ReturnsZero()
    {
        var node = new NumericCleansingNode<TestObject>();
        node.ToZeroIfNegative(x => x.DecimalValue);

        var item = new TestObject { DecimalValue = -5.5m };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(0m, result.DecimalValue);
    }

    #endregion

    #region Round Decimal Tests

    [Fact]
    public async Task Round_WithDecimalPrecision_Rounds()
    {
        var node = new NumericCleansingNode<TestObject>();
        node.Round(x => x.DecimalValue);

        var item = new TestObject { DecimalValue = 5.556m };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(5.56m, result.DecimalValue);
    }

    [Fact]
    public async Task Round_WithNullableDecimalValue_Rounds()
    {
        var node = new NumericCleansingNode<TestObject>();
        node.Round(x => x.NullableDecimalValue);

        var item = new TestObject { NullableDecimalValue = 5.556m };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(5.56m, result.NullableDecimalValue);
    }

    #endregion

    #region Chaining Tests

    [Fact]
    public async Task MultipleCleansingOperations_AppliesAll_Cleanses()
    {
        var node = new NumericCleansingNode<TestObject>();

        node.Clamp(x => x.IntValue, 0, 10)
            .ToZeroIfNegative(x => x.IntValue);

        var item = new TestObject { IntValue = -5 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(0, result.IntValue);
    }

    [Fact]
    public async Task RoundAndScaleOperations_AppliesAll_Cleanses()
    {
        var node = new NumericCleansingNode<TestObject>();

        node.Round(x => x.DoubleValue)
            .Scale(x => x.DecimalValue, 2);

        var item = new TestObject { DoubleValue = 5.556, DecimalValue = 5m };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(5.56, result.DoubleValue);
        Assert.Equal(10m, result.DecimalValue);
    }

    #endregion
}
