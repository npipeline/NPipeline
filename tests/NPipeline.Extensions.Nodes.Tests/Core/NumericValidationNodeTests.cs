using NPipeline.Extensions.Nodes.Core;
using NPipeline.Extensions.Nodes.Core.Exceptions;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes.Tests.Core;

public class NumericValidationNodeTests
{
    #region Error Message Tests

    [Fact]
    public async Task ValidationError_Contains_PropertyPath()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsPositive(x => x.IntValue);

        var item = new TestObject { IntValue = -5 };
        var exception = await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
        Assert.Contains(nameof(TestObject.IntValue), exception.PropertyPath);
    }

    #endregion

    private sealed class TestObject
    {
        public int IntValue { get; set; }
        public double DoubleValue { get; set; }
        public decimal DecimalValue { get; set; }
    }

    #region IsPositive Tests

    [Fact]
    public async Task IsPositive_WithPositiveInt_Passes()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsPositive(x => x.IntValue);

        var item = new TestObject { IntValue = 5 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsPositive_WithZeroInt_Throws()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsPositive(x => x.IntValue);

        var item = new TestObject { IntValue = 0 };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    [Fact]
    public async Task IsPositive_WithPositiveDouble_Passes()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsPositive(x => x.DoubleValue);

        var item = new TestObject { DoubleValue = 5.5 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsPositive_WithZeroDouble_Throws()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsPositive(x => x.DoubleValue);

        var item = new TestObject { DoubleValue = 0.0 };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    [Fact]
    public async Task IsPositive_WithPositiveDecimal_Passes()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsPositive(x => x.DecimalValue);

        var item = new TestObject { DecimalValue = 5.5m };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    #endregion

    #region IsNegative Tests

    [Fact]
    public async Task IsNegative_WithNegativeInt_Passes()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsNegative(x => x.IntValue);

        var item = new TestObject { IntValue = -5 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsNegative_WithZeroInt_Throws()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsNegative(x => x.IntValue);

        var item = new TestObject { IntValue = 0 };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    [Fact]
    public async Task IsNegative_WithNegativeDouble_Passes()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsNegative(x => x.DoubleValue);

        var item = new TestObject { DoubleValue = -5.5 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    #endregion

    #region IsZeroOrPositive Tests

    [Fact]
    public async Task IsZeroOrPositive_WithZeroInt_Passes()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsZeroOrPositive(x => x.IntValue);

        var item = new TestObject { IntValue = 0 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsZeroOrPositive_WithNegativeInt_Throws()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsZeroOrPositive(x => x.IntValue);

        var item = new TestObject { IntValue = -5 };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsNonZero Tests

    [Fact]
    public async Task IsNonZero_WithPositiveInt_Passes()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsNonZero(x => x.IntValue);

        var item = new TestObject { IntValue = 5 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsNonZero_WithZeroInt_Throws()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsNonZero(x => x.IntValue);

        var item = new TestObject { IntValue = 0 };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsBetween Tests

    [Fact]
    public async Task IsBetween_WithValueInRange_Passes()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsBetween(x => x.IntValue, 1, 10);

        var item = new TestObject { IntValue = 5 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsBetween_WithValueBelowRange_Throws()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsBetween(x => x.IntValue, 1, 10);

        var item = new TestObject { IntValue = 0 };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    [Fact]
    public async Task IsBetween_WithValueAboveRange_Throws()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsBetween(x => x.IntValue, 1, 10);

        var item = new TestObject { IntValue = 11 };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsEven Tests

    [Fact]
    public async Task IsEven_WithEvenInt_Passes()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsEven(x => x.IntValue);

        var item = new TestObject { IntValue = 4 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsEven_WithOddInt_Throws()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsEven(x => x.IntValue);

        var item = new TestObject { IntValue = 5 };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsOdd Tests

    [Fact]
    public async Task IsOdd_WithOddInt_Passes()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsOdd(x => x.IntValue);

        var item = new TestObject { IntValue = 5 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsOdd_WithEvenInt_Throws()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsOdd(x => x.IntValue);

        var item = new TestObject { IntValue = 4 };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsFinite Tests

    [Fact]
    public async Task IsFinite_WithFiniteDouble_Passes()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsFinite(x => x.DoubleValue);

        var item = new TestObject { DoubleValue = 5.5 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsFinite_WithInfinityDouble_Throws()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsFinite(x => x.DoubleValue);

        var item = new TestObject { DoubleValue = double.PositiveInfinity };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsIntegerValue Tests

    [Fact]
    public async Task IsIntegerValue_WithIntegerDouble_Passes()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsIntegerValue(x => x.DoubleValue);

        var item = new TestObject { DoubleValue = 5.0 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsIntegerValue_WithFractionalDouble_Throws()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsIntegerValue(x => x.DoubleValue);

        var item = new TestObject { DoubleValue = 5.5 };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsGreaterThan Tests

    [Fact]
    public async Task IsGreaterThan_WithValueGreater_Passes()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsGreaterThan(x => x.IntValue, 5);

        var item = new TestObject { IntValue = 10 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsGreaterThan_WithValueEqual_Throws()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsGreaterThan(x => x.IntValue, 5);

        var item = new TestObject { IntValue = 5 };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsLessThan Tests

    [Fact]
    public async Task IsLessThan_WithValueLess_Passes()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsLessThan(x => x.IntValue, 10);

        var item = new TestObject { IntValue = 5 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsLessThan_WithValueEqual_Throws()
    {
        var node = new NumericValidationNode<TestObject>();
        node.IsLessThan(x => x.IntValue, 10);

        var item = new TestObject { IntValue = 10 };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region Chaining Tests

    [Fact]
    public async Task MultipleValidations_AllPass_Passes()
    {
        var node = new NumericValidationNode<TestObject>();

        node.IsPositive(x => x.IntValue)
            .IsBetween(x => x.IntValue, 1, 10);

        var item = new TestObject { IntValue = 5 };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task MultipleValidations_FirstFails_Throws()
    {
        var node = new NumericValidationNode<TestObject>();

        node.IsPositive(x => x.IntValue)
            .IsBetween(x => x.IntValue, 1, 10);

        var item = new TestObject { IntValue = -5 };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion
}
