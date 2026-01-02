using NPipeline.Extensions.Nodes.Core;
using NPipeline.Extensions.Nodes.Core.Exceptions;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes.Tests.Core;

public class DateTimeValidationNodeTests
{
    private sealed class TestObject
    {
        public DateTime DateTime { get; set; }
        public DateTime? NullableDateTime { get; set; }
    }

    #region IsInFuture Tests

    [Fact]
    public async Task IsInFuture_WithFutureDate_Passes()
    {
        var node = new DateTimeValidationNode<TestObject>();
        node.IsInFuture(x => x.DateTime);

        var item = new TestObject { DateTime = DateTime.UtcNow.AddDays(1) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsInFuture_WithPastDate_Throws()
    {
        var node = new DateTimeValidationNode<TestObject>();
        node.IsInFuture(x => x.DateTime);

        var item = new TestObject { DateTime = DateTime.UtcNow.AddDays(-1) };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    [Fact]
    public async Task IsInFuture_WithNullableDate_Passes()
    {
        var node = new DateTimeValidationNode<TestObject>();
        node.IsInFuture(x => x.NullableDateTime);

        var item = new TestObject { NullableDateTime = DateTime.UtcNow.AddDays(1) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    #endregion

    #region IsInPast Tests

    [Fact]
    public async Task IsInPast_WithPastDate_Passes()
    {
        var node = new DateTimeValidationNode<TestObject>();
        node.IsInPast(x => x.DateTime);

        var item = new TestObject { DateTime = DateTime.UtcNow.AddDays(-1) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsInPast_WithFutureDate_Throws()
    {
        var node = new DateTimeValidationNode<TestObject>();
        node.IsInPast(x => x.DateTime);

        var item = new TestObject { DateTime = DateTime.UtcNow.AddDays(1) };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsToday Tests

    [Fact]
    public async Task IsToday_WithTodayDate_Passes()
    {
        var node = new DateTimeValidationNode<TestObject>();
        node.IsToday(x => x.DateTime);

        var item = new TestObject { DateTime = DateTime.Today };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsToday_WithPastDate_Throws()
    {
        var node = new DateTimeValidationNode<TestObject>();
        node.IsToday(x => x.DateTime);

        var item = new TestObject { DateTime = DateTime.Today.AddDays(-1) };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsWeekday Tests

    [Fact]
    public async Task IsWeekday_WithMondayDate_Passes()
    {
        var node = new DateTimeValidationNode<TestObject>();
        node.IsWeekday(x => x.DateTime);

        // Monday
        var monday = DateTime.Now;

        while (monday.DayOfWeek != DayOfWeek.Monday)
        {
            monday = monday.AddDays(1);
        }

        var item = new TestObject { DateTime = monday };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsWeekday_WithSundayDate_Throws()
    {
        var node = new DateTimeValidationNode<TestObject>();
        node.IsWeekday(x => x.DateTime);

        // Sunday
        var sunday = DateTime.Now;

        while (sunday.DayOfWeek != DayOfWeek.Sunday)
        {
            sunday = sunday.AddDays(1);
        }

        var item = new TestObject { DateTime = sunday };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsWeekend Tests

    [Fact]
    public async Task IsWeekend_WithSaturdayDate_Passes()
    {
        var node = new DateTimeValidationNode<TestObject>();
        node.IsWeekend(x => x.DateTime);

        // Saturday
        var saturday = DateTime.Now;

        while (saturday.DayOfWeek != DayOfWeek.Saturday)
        {
            saturday = saturday.AddDays(1);
        }

        var item = new TestObject { DateTime = saturday };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsWeekend_WithMondayDate_Throws()
    {
        var node = new DateTimeValidationNode<TestObject>();
        node.IsWeekend(x => x.DateTime);

        // Monday
        var monday = DateTime.Now;

        while (monday.DayOfWeek != DayOfWeek.Monday)
        {
            monday = monday.AddDays(1);
        }

        var item = new TestObject { DateTime = monday };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsUtc Tests

    [Fact]
    public async Task IsUtc_WithUtcDate_Passes()
    {
        var node = new DateTimeValidationNode<TestObject>();
        node.IsUtc(x => x.DateTime);

        var item = new TestObject { DateTime = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsUtc_WithLocalDate_Throws()
    {
        var node = new DateTimeValidationNode<TestObject>();
        node.IsUtc(x => x.DateTime);

        var item = new TestObject { DateTime = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Local) };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsLocal Tests

    [Fact]
    public async Task IsLocal_WithLocalDate_Passes()
    {
        var node = new DateTimeValidationNode<TestObject>();
        node.IsLocal(x => x.DateTime);

        var item = new TestObject { DateTime = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Local) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsLocal_WithUtcDate_Throws()
    {
        var node = new DateTimeValidationNode<TestObject>();
        node.IsLocal(x => x.DateTime);

        var item = new TestObject { DateTime = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc) };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsNotMinValue Tests

    [Fact]
    public async Task IsNotMinValue_WithValidDate_Passes()
    {
        var node = new DateTimeValidationNode<TestObject>();
        node.IsNotMinValue(x => x.DateTime);

        var item = new TestObject { DateTime = new DateTime(2025, 1, 1) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsNotMinValue_WithMinValue_Throws()
    {
        var node = new DateTimeValidationNode<TestObject>();
        node.IsNotMinValue(x => x.DateTime);

        var item = new TestObject { DateTime = DateTime.MinValue };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsNotMaxValue Tests

    [Fact]
    public async Task IsNotMaxValue_WithValidDate_Passes()
    {
        var node = new DateTimeValidationNode<TestObject>();
        node.IsNotMaxValue(x => x.DateTime);

        var item = new TestObject { DateTime = new DateTime(2025, 1, 1) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsNotMaxValue_WithMaxValue_Throws()
    {
        var node = new DateTimeValidationNode<TestObject>();
        node.IsNotMaxValue(x => x.DateTime);

        var item = new TestObject { DateTime = DateTime.MaxValue };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsBefore Tests

    [Fact]
    public async Task IsBefore_WithEarlierDate_Passes()
    {
        var node = new DateTimeValidationNode<TestObject>();
        var cutoff = new DateTime(2025, 6, 1);
        node.IsBefore(x => x.DateTime, cutoff);

        var item = new TestObject { DateTime = new DateTime(2025, 1, 1) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsBefore_WithLaterDate_Throws()
    {
        var node = new DateTimeValidationNode<TestObject>();
        var cutoff = new DateTime(2025, 6, 1);
        node.IsBefore(x => x.DateTime, cutoff);

        var item = new TestObject { DateTime = new DateTime(2025, 12, 1) };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsAfter Tests

    [Fact]
    public async Task IsAfter_WithLaterDate_Passes()
    {
        var node = new DateTimeValidationNode<TestObject>();
        var cutoff = new DateTime(2025, 6, 1);
        node.IsAfter(x => x.DateTime, cutoff);

        var item = new TestObject { DateTime = new DateTime(2025, 12, 1) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsAfter_WithEarlierDate_Throws()
    {
        var node = new DateTimeValidationNode<TestObject>();
        var cutoff = new DateTime(2025, 6, 1);
        node.IsAfter(x => x.DateTime, cutoff);

        var item = new TestObject { DateTime = new DateTime(2025, 1, 1) };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsBetween Tests

    [Fact]
    public async Task IsBetween_WithDateInRange_Passes()
    {
        var node = new DateTimeValidationNode<TestObject>();
        var from = new DateTime(2025, 1, 1);
        var to = new DateTime(2025, 12, 31);
        node.IsBetween(x => x.DateTime, from, to);

        var item = new TestObject { DateTime = new DateTime(2025, 6, 1) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsBetween_WithDateBeforeRange_Throws()
    {
        var node = new DateTimeValidationNode<TestObject>();
        var from = new DateTime(2025, 1, 1);
        var to = new DateTime(2025, 12, 31);
        node.IsBetween(x => x.DateTime, from, to);

        var item = new TestObject { DateTime = new DateTime(2024, 6, 1) };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsDayOfWeek Tests

    [Fact]
    public async Task IsDayOfWeek_WithCorrectDay_Passes()
    {
        var node = new DateTimeValidationNode<TestObject>();

        // Monday
        var monday = DateTime.Now;

        while (monday.DayOfWeek != DayOfWeek.Monday)
        {
            monday = monday.AddDays(1);
        }

        node.IsDayOfWeek(x => x.DateTime, DayOfWeek.Monday);
        var item = new TestObject { DateTime = monday };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsDayOfWeek_WithWrongDay_Throws()
    {
        var node = new DateTimeValidationNode<TestObject>();

        // Monday
        var monday = DateTime.Now;

        while (monday.DayOfWeek != DayOfWeek.Monday)
        {
            monday = monday.AddDays(1);
        }

        node.IsDayOfWeek(x => x.DateTime, DayOfWeek.Tuesday);
        var item = new TestObject { DateTime = monday };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsInYear Tests

    [Fact]
    public async Task IsInYear_WithCorrectYear_Passes()
    {
        var node = new DateTimeValidationNode<TestObject>();
        node.IsInYear(x => x.DateTime, 2025);

        var item = new TestObject { DateTime = new DateTime(2025, 6, 1) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsInYear_WithWrongYear_Throws()
    {
        var node = new DateTimeValidationNode<TestObject>();
        node.IsInYear(x => x.DateTime, 2025);

        var item = new TestObject { DateTime = new DateTime(2024, 6, 1) };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsInMonth Tests

    [Fact]
    public async Task IsInMonth_WithCorrectMonth_Passes()
    {
        var node = new DateTimeValidationNode<TestObject>();
        node.IsInMonth(x => x.DateTime, 6);

        var item = new TestObject { DateTime = new DateTime(2025, 6, 1) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsInMonth_WithWrongMonth_Throws()
    {
        var node = new DateTimeValidationNode<TestObject>();
        node.IsInMonth(x => x.DateTime, 6);

        var item = new TestObject { DateTime = new DateTime(2025, 5, 1) };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region Chaining Tests

    [Fact]
    public async Task MultipleValidations_AllPass_Passes()
    {
        var node = new DateTimeValidationNode<TestObject>();

        node.IsInFuture(x => x.DateTime)
            .IsUtc(x => x.DateTime);

        var item = new TestObject { DateTime = new DateTime(2100, 1, 1, 0, 0, 0, DateTimeKind.Utc) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task MultipleValidations_FirstFails_Throws()
    {
        var node = new DateTimeValidationNode<TestObject>();

        node.IsInPast(x => x.DateTime)
            .IsUtc(x => x.DateTime);

        var item = new TestObject { DateTime = new DateTime(2100, 1, 1, 0, 0, 0, DateTimeKind.Utc) };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion
}
