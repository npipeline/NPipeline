using AwesomeAssertions;
using NPipeline.DataFlow.Watermarks;

namespace NPipeline.Tests.DataFlow.Watermarks;

public sealed class WatermarksTests
{
    #region Cross-Watermark Comparison Tests

    [Fact]
    public void Watermark_MultipleComparisons_ConsistentOrdering()
    {
        // Arrange
        DateTimeOffset time1 = new(2024, 1, 15, 9, 0, 0, TimeSpan.Zero);
        DateTimeOffset time2 = new(2024, 1, 15, 10, 0, 0, TimeSpan.Zero);
        DateTimeOffset time3 = new(2024, 1, 15, 11, 0, 0, TimeSpan.Zero);

        Watermark w2 = new(time2); // Watermark at middle time

        // Act & Assert - verify ordering is consistent
        // IsEarlierThan checks if parameter is earlier than watermark
        _ = w2.IsEarlierThan(time1).Should().BeTrue(); // time1 < time2, so true
        _ = w2.IsEarlierThan(time3).Should().BeFalse(); // time3 > time2, so false
        _ = w2.IsEarlierThan(time2).Should().BeFalse(); // time2 == time2, so false

        // IsLaterThanOrEqual checks if parameter is >= watermark
        _ = w2.IsLaterThanOrEqual(time3).Should().BeTrue(); // time3 >= time2, so true
        _ = w2.IsLaterThanOrEqual(time1).Should().BeFalse(); // time1 < time2, so false
        _ = w2.IsLaterThanOrEqual(time2).Should().BeTrue(); // time2 >= time2, so true
    }

    #endregion

    #region Watermark Record Tests

    [Fact]
    public void Watermark_Create_WithSpecificTimestamp_StoresTimestamp()
    {
        // Arrange
        DateTimeOffset timestamp = new(2024, 1, 15, 10, 30, 45, TimeSpan.Zero);

        // Act
        var watermark = Watermark.Create(timestamp);

        // Assert
        _ = watermark.Timestamp.Should().Be(timestamp);
    }

    [Fact]
    public void Watermark_Now_CreatesWatermarkWithCurrentTime()
    {
        // Arrange
        var beforeCreation = DateTimeOffset.UtcNow.AddSeconds(-1);
        var afterCreation = DateTimeOffset.UtcNow.AddSeconds(1);

        // Act
        var watermark = Watermark.Now();

        // Assert - timestamp should be within expected range
        _ = (watermark.Timestamp >= beforeCreation && watermark.Timestamp <= afterCreation).Should().BeTrue();
    }

    [Fact]
    public void Watermark_IsEarlierThan_WithEarlierTimestamp_ReturnsTrue()
    {
        // Arrange - watermark at baseTime, check if an earlier timestamp is earlier than it
        DateTimeOffset baseTime = new(2024, 1, 15, 10, 0, 0, TimeSpan.Zero);
        Watermark watermark = new(baseTime);
        var earlierTime = baseTime.AddMinutes(-5);

        // Act
        var result = watermark.IsEarlierThan(earlierTime);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void Watermark_IsEarlierThan_WithLaterTimestamp_ReturnsFalse()
    {
        // Arrange - watermark at baseTime, check if a later timestamp is earlier than it
        DateTimeOffset baseTime = new(2024, 1, 15, 10, 0, 0, TimeSpan.Zero);
        Watermark watermark = new(baseTime);
        var laterTime = baseTime.AddMinutes(5);

        // Act
        var result = watermark.IsEarlierThan(laterTime);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void Watermark_IsEarlierThan_WithEqualTimestamp_ReturnsFalse()
    {
        // Arrange
        DateTimeOffset baseTime = new(2024, 1, 15, 10, 0, 0, TimeSpan.Zero);
        Watermark watermark = new(baseTime);

        // Act
        var result = watermark.IsEarlierThan(baseTime);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void Watermark_IsLaterThanOrEqual_WithLaterTimestamp_ReturnsTrue()
    {
        // Arrange - watermark at baseTime, check if a later timestamp is >= it
        DateTimeOffset baseTime = new(2024, 1, 15, 10, 0, 0, TimeSpan.Zero);
        Watermark watermark = new(baseTime);
        var laterTime = baseTime.AddMinutes(5);

        // Act
        var result = watermark.IsLaterThanOrEqual(laterTime);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void Watermark_IsLaterThanOrEqual_WithEarlierTimestamp_ReturnsFalse()
    {
        // Arrange - watermark at baseTime, check if an earlier timestamp is >= it
        DateTimeOffset baseTime = new(2024, 1, 15, 10, 0, 0, TimeSpan.Zero);
        Watermark watermark = new(baseTime);
        var earlierTime = baseTime.AddMinutes(-5);

        // Act
        var result = watermark.IsLaterThanOrEqual(earlierTime);

        // Assert
        _ = result.Should().BeFalse();
    }

    [Fact]
    public void Watermark_IsLaterThanOrEqual_WithEqualTimestamp_ReturnsTrue()
    {
        // Arrange
        DateTimeOffset baseTime = new(2024, 1, 15, 10, 0, 0, TimeSpan.Zero);
        Watermark watermark = new(baseTime);

        // Act
        var result = watermark.IsLaterThanOrEqual(baseTime);

        // Assert
        _ = result.Should().BeTrue();
    }

    [Fact]
    public void Watermark_ToString_ReturnsFormattedString()
    {
        // Arrange
        DateTimeOffset timestamp = new(2024, 1, 15, 10, 30, 45, 500, TimeSpan.Zero);
        Watermark watermark = new(timestamp);

        // Act
        var result = watermark.ToString();

        // Assert
        _ = result.Should().Contain("Watermark(");
        _ = result.Should().Contain("2024");
    }

    [Fact]
    public void Watermark_RecordEquality_WithSameTimestamp_AreEqual()
    {
        // Arrange
        DateTimeOffset timestamp = new(2024, 1, 15, 10, 30, 45, TimeSpan.Zero);
        Watermark watermark1 = new(timestamp);
        Watermark watermark2 = new(timestamp);

        // Act & Assert
        _ = watermark1.Should().Be(watermark2);
    }

    [Fact]
    public void Watermark_RecordEquality_WithDifferentTimestamp_AreNotEqual()
    {
        // Arrange
        DateTimeOffset timestamp1 = new(2024, 1, 15, 10, 30, 45, TimeSpan.Zero);
        DateTimeOffset timestamp2 = new(2024, 1, 15, 10, 30, 46, TimeSpan.Zero);
        Watermark watermark1 = new(timestamp1);
        Watermark watermark2 = new(timestamp2);

        // Act & Assert
        _ = watermark1.Should().NotBe(watermark2);
    }

    #endregion

    #region BoundedOutOfOrdernessWatermarkGenerator Tests

    [Fact]
    public void BoundedOutOfOrdernessGenerator_WhenCreated_InitializesWithMinValue()
    {
        // Arrange & Act
        BoundedOutOfOrdernessWatermarkGenerator<int> generator = new(TimeSpan.FromSeconds(5));

        // Assert
        var watermark = generator.GetCurrentWatermark();
        _ = watermark.Timestamp.Should().Be(DateTimeOffset.MinValue);
    }

    [Fact]
    public void BoundedOutOfOrdernessGenerator_Update_WithSingleEvent_UpdatesMaxTimestamp()
    {
        // Arrange
        var maxOutOfOrderness = TimeSpan.FromSeconds(5);
        BoundedOutOfOrdernessWatermarkGenerator<int> generator = new(maxOutOfOrderness);
        DateTimeOffset timestamp = new(2024, 1, 15, 10, 30, 45, TimeSpan.Zero);

        // Act
        generator.Update(timestamp);
        var watermark = generator.GetCurrentWatermark();

        // Assert - watermark should be timestamp minus maxOutOfOrderness
        var expectedWatermark = timestamp.AddSeconds(-5);
        _ = watermark.Timestamp.Should().Be(expectedWatermark);
    }

    [Fact]
    public void BoundedOutOfOrdernessGenerator_Update_WithMultipleEvents_TracksMaxTimestamp()
    {
        // Arrange
        var maxOutOfOrderness = TimeSpan.FromSeconds(10);
        BoundedOutOfOrdernessWatermarkGenerator<int> generator = new(maxOutOfOrderness);
        DateTimeOffset time1 = new(2024, 1, 15, 10, 30, 0, TimeSpan.Zero);
        DateTimeOffset time2 = new(2024, 1, 15, 10, 30, 20, TimeSpan.Zero);
        DateTimeOffset time3 = new(2024, 1, 15, 10, 30, 15, TimeSpan.Zero); // Out of order

        // Act
        generator.Update(time1);
        generator.Update(time2);
        generator.Update(time3);
        var watermark = generator.GetCurrentWatermark();

        // Assert - should use max (time2)
        var expectedWatermark = time2.AddSeconds(-10);
        _ = watermark.Timestamp.Should().Be(expectedWatermark);
    }

    [Fact]
    public void BoundedOutOfOrdernessGenerator_Update_WithLargeOutOfOrderness_SubtractsCorrectly()
    {
        // Arrange
        var largeOutOfOrderness = TimeSpan.FromDays(100);
        BoundedOutOfOrdernessWatermarkGenerator<int> generator = new(largeOutOfOrderness);
        DateTimeOffset timestamp = new(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        generator.Update(timestamp);
        var watermark = generator.GetCurrentWatermark();

        // Assert - should subtract the outoforder delta
        var expectedWatermark = timestamp.AddDays(-100);
        _ = watermark.Timestamp.Should().Be(expectedWatermark);
    }

    #endregion

    #region PeriodicWatermarkGenerator Tests

    [Fact]
    public void PeriodicGenerator_WhenCreated_InitializesWithMinValue()
    {
        // Arrange & Act
        PeriodicWatermarkGenerator<int> generator = new(
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(5));

        // Assert
        var watermark = generator.GetCurrentWatermark();
        _ = watermark.Timestamp.Should().Be(DateTimeOffset.MinValue);
    }

    [Fact]
    public void PeriodicGenerator_Update_WithSingleEvent_GeneratesWatermark()
    {
        // Arrange
        var interval = TimeSpan.FromMilliseconds(100);
        var maxOutOfOrderness = TimeSpan.FromSeconds(5);
        PeriodicWatermarkGenerator<int> generator = new(interval, maxOutOfOrderness);
        DateTimeOffset timestamp = new(2024, 1, 15, 10, 30, 45, TimeSpan.Zero);

        // Act
        generator.Update(timestamp);
        var watermark = generator.GetCurrentWatermark();

        // Assert
        var expectedWatermark = timestamp.AddSeconds(-5);
        _ = watermark.Timestamp.Should().Be(expectedWatermark);
    }

    [Fact]
    public void PeriodicGenerator_Update_WithNoEvents_ReturnsMinValue()
    {
        // Arrange
        PeriodicWatermarkGenerator<int> generator = new(
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(5));

        // Act
        var watermark = generator.GetCurrentWatermark();

        // Assert
        _ = watermark.Timestamp.Should().Be(DateTimeOffset.MinValue);
    }

    [Fact]
    public void PeriodicGenerator_Update_TracksMaxTimestamp()
    {
        // Arrange
        var interval = TimeSpan.FromSeconds(1);
        var maxOutOfOrderness = TimeSpan.FromSeconds(10);
        PeriodicWatermarkGenerator<int> generator = new(interval, maxOutOfOrderness);
        DateTimeOffset time1 = new(2024, 1, 15, 10, 30, 0, TimeSpan.Zero);
        DateTimeOffset time2 = new(2024, 1, 15, 10, 30, 30, TimeSpan.Zero);

        // Act
        generator.Update(time1);
        generator.Update(time2);
        var watermark = generator.GetCurrentWatermark();

        // Assert - should use max (time2)
        var expectedWatermark = time2.AddSeconds(-10);
        _ = watermark.Timestamp.Should().Be(expectedWatermark);
    }

    #endregion

    #region WatermarkGenerators Factory Tests

    [Fact]
    public void WatermarkGenerators_BoundedOutOfOrderness_CreatesGenerator()
    {
        // Arrange
        var maxOutOfOrderness = TimeSpan.FromSeconds(5);

        // Act
        var generator = WatermarkGenerators.BoundedOutOfOrderness<int>(maxOutOfOrderness);

        // Assert
        _ = generator.Should().NotBeNull();
        _ = generator.Should().BeOfType<BoundedOutOfOrdernessWatermarkGenerator<int>>();
    }

    [Fact]
    public void WatermarkGenerators_Periodic_CreatesGenerator()
    {
        // Arrange
        var interval = TimeSpan.FromSeconds(1);
        var maxOutOfOrderness = TimeSpan.FromSeconds(5);

        // Act
        var generator = WatermarkGenerators.Periodic<int>(interval, maxOutOfOrderness);

        // Assert
        _ = generator.Should().NotBeNull();
        _ = generator.Should().BeOfType<PeriodicWatermarkGenerator<int>>();
    }

    #endregion

    #region SafeSubtract Behavior Tests (through WatermarkGenerator)

    [Fact]
    public void BoundedOutOfOrdernessWatermarkGenerator_WithValidTimestamps_SubtractsCorrectly()
    {
        // Arrange - test that the watermark subtracts out-of-orderness correctly
        DateTimeOffset timestamp = new(2024, 1, 15, 10, 30, 45, TimeSpan.Zero);
        var maxOutOfOrderness = TimeSpan.FromSeconds(10);
        var generator = new BoundedOutOfOrdernessWatermarkGenerator<int>(maxOutOfOrderness);

        // Act
        generator.Update(timestamp);
        var watermark = generator.GetCurrentWatermark();

        // Assert - watermark should be timestamp minus maxOutOfOrderness
        var expected = timestamp.AddSeconds(-10);
        _ = watermark.Timestamp.Should().Be(expected);
    }

    [Fact]
    public void BoundedOutOfOrdernessWatermarkGenerator_WithMinValueTimestamp_ReturnsMinValue()
    {
        // Arrange - test underflow protection when no events have been processed
        var maxOutOfOrderness = TimeSpan.FromSeconds(10);
        var generator = new BoundedOutOfOrdernessWatermarkGenerator<int>(maxOutOfOrderness);

        // Act - don't update, just get watermark (should be MinValue)
        var watermark = generator.GetCurrentWatermark();

        // Assert
        _ = watermark.Timestamp.Should().Be(DateTimeOffset.MinValue);
    }

    [Fact]
    public void BoundedOutOfOrdernessWatermarkGenerator_WithVeryLargeDelta_ReturnsMinValue()
    {
        // Arrange - create a timestamp close to MinValue and use large out-of-orderness
        var timestamp = DateTimeOffset.MinValue.AddDays(10); // Only 10 days from MinValue
        var massiveMaxOutOfOrderness = TimeSpan.FromDays(365 * 100); // 100 years, way more than 10 days
        var generator = new BoundedOutOfOrdernessWatermarkGenerator<int>(massiveMaxOutOfOrderness);

        // Act
        generator.Update(timestamp);
        var watermark = generator.GetCurrentWatermark();

        // Assert - should prevent underflow by returning MinValue since delta exceeds distance to min
        _ = watermark.Timestamp.Should().Be(DateTimeOffset.MinValue);
    }

    [Fact]
    public void BoundedOutOfOrdernessWatermarkGenerator_WithZeroMaxOutOfOrderness_ReturnsOriginalTimestamp()
    {
        // Arrange
        DateTimeOffset timestamp = new(2024, 1, 15, 10, 30, 45, TimeSpan.Zero);
        var generator = new BoundedOutOfOrdernessWatermarkGenerator<int>(TimeSpan.Zero);

        // Act
        generator.Update(timestamp);
        var watermark = generator.GetCurrentWatermark();

        // Assert - with zero delta, watermark should equal the timestamp
        _ = watermark.Timestamp.Should().Be(timestamp);
    }

    [Fact]
    public void BoundedOutOfOrdernessWatermarkGenerator_WithNegativeMaxOutOfOrderness_AddsToTimestamp()
    {
        // Arrange - negative out-of-orderness means we're looking ahead
        DateTimeOffset timestamp = new(2024, 1, 15, 10, 30, 45, TimeSpan.Zero);
        var negativeDelta = TimeSpan.FromSeconds(-10);
        var generator = new BoundedOutOfOrdernessWatermarkGenerator<int>(negativeDelta);

        // Act
        generator.Update(timestamp);
        var watermark = generator.GetCurrentWatermark();

        // Assert - subtracting negative = adding
        var expected = timestamp.AddSeconds(10);
        _ = watermark.Timestamp.Should().Be(expected);
    }

    #endregion
}
