using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes.Tests;

public class DateTimeCleansingNodeTests
{
    #region SpecifyKind Tests

    [Fact]
    public async Task SpecifyKind_SetsDateTimeKind_SpecifiesKind()
    {
        var node = new DateTimeCleansingNode<TestObject>();
        node.SpecifyKind(x => x.DateTime, DateTimeKind.Utc);

        var unspecifiedDt = new DateTime(2025, 1, 1, 12, 0, 0);
        var item = new TestObject { DateTime = unspecifiedDt };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(DateTimeKind.Utc, result.DateTime.Kind);
    }

    #endregion

    private sealed class TestObject
    {
        public DateTime DateTime { get; set; }
        public DateTime? NullableDateTime { get; set; }
    }

    #region ToUtc Tests

    [Fact]
    public async Task ToUtc_ConvertToUtc_Converts()
    {
        var node = new DateTimeCleansingNode<TestObject>();
        node.ToUtc(x => x.DateTime);

        var localDt = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Local);
        var item = new TestObject { DateTime = localDt };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(DateTimeKind.Utc, result.DateTime.Kind);
    }

    [Fact]
    public async Task ToUtc_WithNullableValue_Converts()
    {
        var node = new DateTimeCleansingNode<TestObject>();
        node.ToUtc(x => x.NullableDateTime);

        var localDt = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Local);
        var item = new TestObject { NullableDateTime = localDt };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(DateTimeKind.Utc, result.NullableDateTime!.Value.Kind);
    }

    #endregion

    #region ToLocal Tests

    [Fact]
    public async Task ToLocal_ConvertToLocal_Converts()
    {
        var node = new DateTimeCleansingNode<TestObject>();
        node.ToLocal(x => x.DateTime);

        var utcDt = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        var item = new TestObject { DateTime = utcDt };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(DateTimeKind.Local, result.DateTime.Kind);
    }

    [Fact]
    public async Task ToLocal_WithNullableValue_Converts()
    {
        var node = new DateTimeCleansingNode<TestObject>();
        node.ToLocal(x => x.NullableDateTime);

        var utcDt = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        var item = new TestObject { NullableDateTime = utcDt };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(DateTimeKind.Local, result.NullableDateTime!.Value.Kind);
    }

    #endregion

    #region StripTime Tests

    [Fact]
    public async Task StripTime_RemovesTime_Strips()
    {
        var node = new DateTimeCleansingNode<TestObject>();
        node.StripTime(x => x.DateTime);

        var item = new TestObject { DateTime = new DateTime(2025, 1, 1, 14, 30, 45) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(0, result.DateTime.Hour);
        Assert.Equal(0, result.DateTime.Minute);
        Assert.Equal(0, result.DateTime.Second);
    }

    [Fact]
    public async Task StripTime_WithNullableValue_Strips()
    {
        var node = new DateTimeCleansingNode<TestObject>();
        node.StripTime(x => x.NullableDateTime);

        var item = new TestObject { NullableDateTime = new DateTime(2025, 1, 1, 14, 30, 45) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(0, result.NullableDateTime!.Value.Hour);
        Assert.Equal(0, result.NullableDateTime!.Value.Minute);
        Assert.Equal(0, result.NullableDateTime!.Value.Second);
    }

    #endregion

    #region Truncate Tests

    [Fact]
    public async Task Truncate_ToSeconds_Truncates()
    {
        var node = new DateTimeCleansingNode<TestObject>();
        node.Truncate(x => x.DateTime, TimeSpan.FromSeconds(1));

        var item = new TestObject { DateTime = new DateTime(2025, 1, 1, 12, 0, 0, 500) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(0, result.DateTime.Millisecond);
    }

    [Fact]
    public async Task Truncate_WithNullableValue_Truncates()
    {
        var node = new DateTimeCleansingNode<TestObject>();
        node.Truncate(x => x.NullableDateTime, TimeSpan.FromSeconds(1));

        var item = new TestObject { NullableDateTime = new DateTime(2025, 1, 1, 12, 0, 0, 500) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(0, result.NullableDateTime!.Value.Millisecond);
    }

    #endregion

    #region RoundToMinute Tests

    [Fact]
    public async Task RoundToMinute_RoundsToMinute_Rounds()
    {
        var node = new DateTimeCleansingNode<TestObject>();
        node.RoundToMinute(x => x.DateTime);

        var item = new TestObject { DateTime = new DateTime(2025, 1, 1, 12, 0, 30) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(1, result.DateTime.Minute);
    }

    [Fact]
    public async Task RoundToMinute_WithLateSeconds_Rounds()
    {
        var node = new DateTimeCleansingNode<TestObject>();
        node.RoundToMinute(x => x.DateTime);

        var item = new TestObject { DateTime = new DateTime(2025, 1, 1, 12, 0, 45) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(1, result.DateTime.Minute);
    }

    #endregion

    #region RoundToHour Tests

    [Fact]
    public async Task RoundToHour_RoundsToHour_Rounds()
    {
        var node = new DateTimeCleansingNode<TestObject>();
        node.RoundToHour(x => x.DateTime);

        var item = new TestObject { DateTime = new DateTime(2025, 1, 1, 12, 30, 0) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(13, result.DateTime.Hour);
    }

    [Fact]
    public async Task RoundToHour_WithMidDay_Rounds()
    {
        var node = new DateTimeCleansingNode<TestObject>();
        node.RoundToHour(x => x.DateTime);

        var item = new TestObject { DateTime = new DateTime(2025, 1, 1, 12, 45, 0) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(13, result.DateTime.Hour);
    }

    #endregion

    #region RoundToDay Tests

    [Fact]
    public async Task RoundToDay_RoundsToDay_Rounds()
    {
        var node = new DateTimeCleansingNode<TestObject>();
        node.RoundToDay(x => x.DateTime);

        var item = new TestObject { DateTime = new DateTime(2025, 1, 1, 14, 0, 0) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(2, result.DateTime.Day);
    }

    [Fact]
    public async Task RoundToDay_WithMidDay_Rounds()
    {
        var node = new DateTimeCleansingNode<TestObject>();
        node.RoundToDay(x => x.DateTime);

        var item = new TestObject { DateTime = new DateTime(2025, 1, 1, 14, 0, 0) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(2, result.DateTime.Day);
    }

    #endregion

    #region DefaultIfMinValue Tests

    [Fact]
    public async Task DefaultIfMinValue_WithMinValue_ReturnsDefault()
    {
        var node = new DateTimeCleansingNode<TestObject>();
        var defaultDate = new DateTime(2025, 1, 1);
        node.DefaultIfMinValue(x => x.DateTime, defaultDate);

        var item = new TestObject { DateTime = DateTime.MinValue };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(defaultDate, result.DateTime);
    }

    [Fact]
    public async Task DefaultIfMinValue_WithValidValue_ReturnsValue()
    {
        var node = new DateTimeCleansingNode<TestObject>();
        var defaultDate = new DateTime(2025, 1, 1);
        var originalDate = new DateTime(2025, 6, 1);
        node.DefaultIfMinValue(x => x.DateTime, defaultDate);

        var item = new TestObject { DateTime = originalDate };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(originalDate, result.DateTime);
    }

    #endregion

    #region DefaultIfMaxValue Tests

    [Fact]
    public async Task DefaultIfMaxValue_WithMaxValue_ReturnsDefault()
    {
        var node = new DateTimeCleansingNode<TestObject>();
        var defaultDate = new DateTime(2025, 1, 1);
        node.DefaultIfMaxValue(x => x.DateTime, defaultDate);

        var item = new TestObject { DateTime = DateTime.MaxValue };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(defaultDate, result.DateTime);
    }

    [Fact]
    public async Task DefaultIfMaxValue_WithValidValue_ReturnsValue()
    {
        var node = new DateTimeCleansingNode<TestObject>();
        var defaultDate = new DateTime(2025, 1, 1);
        var originalDate = new DateTime(2025, 6, 1);
        node.DefaultIfMaxValue(x => x.DateTime, defaultDate);

        var item = new TestObject { DateTime = originalDate };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(originalDate, result.DateTime);
    }

    #endregion

    #region DefaultIfNull Tests

    [Fact]
    public async Task DefaultIfNull_WithNullValue_ReturnsDefault()
    {
        var node = new DateTimeCleansingNode<TestObject>();
        var defaultDate = new DateTime(2025, 1, 1);
        node.DefaultIfNull(x => x.NullableDateTime, defaultDate);

        var item = new TestObject { NullableDateTime = null };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(defaultDate, result.NullableDateTime);
    }

    [Fact]
    public async Task DefaultIfNull_WithValue_ReturnsValue()
    {
        var node = new DateTimeCleansingNode<TestObject>();
        var defaultDate = new DateTime(2025, 1, 1);
        var originalDate = new DateTime(2025, 6, 1);
        node.DefaultIfNull(x => x.NullableDateTime, originalDate);

        var item = new TestObject { NullableDateTime = originalDate };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(originalDate, result.NullableDateTime);
    }

    #endregion

    #region Chaining Tests

    [Fact]
    public async Task MultipleCleansingOperations_AppliesAll_Cleanses()
    {
        var node = new DateTimeCleansingNode<TestObject>();

        node.SpecifyKind(x => x.DateTime, DateTimeKind.Utc)
            .StripTime(x => x.DateTime);

        var item = new TestObject { DateTime = new DateTime(2025, 1, 1, 14, 30, 45) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.Equal(DateTimeKind.Utc, result.DateTime.Kind);
        Assert.Equal(0, result.DateTime.Hour);
    }

    [Fact]
    public async Task RoundingOperations_AppliesAll_Cleanses()
    {
        var node = new DateTimeCleansingNode<TestObject>();

        node.RoundToMinute(x => x.DateTime)
            .RoundToHour(x => x.DateTime);

        var item = new TestObject { DateTime = new DateTime(2025, 1, 1, 12, 30, 45) };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);

        // After both roundings
        Assert.True(result.DateTime.Hour >= 12);
    }

    #endregion
}
