using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.Utils;

namespace NPipeline.Tests.Utils;

public sealed class TimestampUtilsTests
{
    #region Generic Type Parameter Tests

    [Fact]
    public void ExtractTimestamp_WithDifferentGenericTypes()
    {
        // Arrange
        var now = DateTimeOffset.UtcNow;

        // Act & Assert - should work with different types
        var result1 = TimestampUtils.ExtractTimestamp(
            new TimestampedItem(now));

        result1.Should().Be(now);

        var result2 = TimestampUtils.ExtractTimestamp(
            new TestData { EventTime = now },
            x => x.EventTime);

        result2.Should().Be(now);
    }

    #endregion

    #region Extraction from ITimestamped Tests

    [Fact]
    public void ExtractTimestamp_WithITimestampedItem_ReturnsTimestamp()
    {
        // Arrange
        var timestamp = DateTimeOffset.UtcNow;
        var item = new TimestampedItem(timestamp);

        // Act
        var result = TimestampUtils.ExtractTimestamp(item);

        // Assert
        result.Should().Be(timestamp);
    }

    [Fact]
    public void ExtractTimestamp_WithITimestampedItem_IgnoresExtractor()
    {
        // Arrange
        var timestamp = DateTimeOffset.UtcNow;
        var item = new TimestampedItem(timestamp);
        var extractorCalled = false;

        // Act
        var result = TimestampUtils.ExtractTimestamp(item, i =>
        {
            extractorCalled = true;
            return DateTimeOffset.UtcNow;
        });

        // Assert
        result.Should().Be(timestamp);
        extractorCalled.Should().BeFalse();
    }

    #endregion

    #region Extraction with Custom Extractor Tests

    [Fact]
    public void ExtractTimestamp_WithCustomExtractor_ReturnsExtractedTimestamp()
    {
        // Arrange
        var timestamp = DateTimeOffset.UtcNow.AddHours(-1);
        var item = new { CreatedAt = timestamp };

        // Act
        var result = TimestampUtils.ExtractTimestamp(item, x => x.CreatedAt);

        // Assert
        result.Should().Be(timestamp);
    }

    [Fact]
    public void ExtractTimestamp_WithComplexExtractor_Works()
    {
        // Arrange
        var timestamp = new DateTimeOffset(2025, 10, 22, 12, 0, 0, TimeSpan.Zero);
        var item = new TestData { EventTime = timestamp };

        // Act
        var result = TimestampUtils.ExtractTimestamp(item, x => x.EventTime);

        // Assert
        result.Should().Be(timestamp);
    }

    #endregion

    #region Error Handling Tests

    [Fact]
    public void ExtractTimestamp_WithoutExtractorAndNoInterface_Throws()
    {
        // Arrange
        var item = "plain string";

        // Act
        var act = () => TimestampUtils.ExtractTimestamp(item);

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*Cannot extract timestamp*");
    }

    [Fact]
    public void ExtractTimestamp_WithoutExtractorAndNoInterface_ContainsTypeName()
    {
        // Arrange
        var item = 42;

        // Act
        var act = () => TimestampUtils.ExtractTimestamp(item);

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*Int32*");
    }

    [Fact]
    public void ExtractTimestamp_ErrorMessage_SuggetstsITimestampedOrExtractor()
    {
        // Arrange
        var item = new object();

        // Act
        var act = () => TimestampUtils.ExtractTimestamp(item);

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*ITimestamped*");
    }

    #endregion

    #region TryExtractTimestamp Success Cases Tests

    [Fact]
    public void TryExtractTimestamp_WithITimestampedItem_ReturnsTrueAndTimestamp()
    {
        // Arrange
        var timestamp = DateTimeOffset.UtcNow;
        var item = new TimestampedItem(timestamp);

        // Act
        var result = TimestampUtils.TryExtractTimestamp(item, out var extractedTimestamp);

        // Assert
        result.Should().BeTrue();
        extractedTimestamp.Should().Be(timestamp);
    }

    [Fact]
    public void TryExtractTimestamp_WithCustomExtractor_ReturnsTrueAndTimestamp()
    {
        // Arrange
        var timestamp = DateTimeOffset.UtcNow;
        var item = new TestData { EventTime = timestamp };

        // Act
        var result = TimestampUtils.TryExtractTimestamp(item, out var extractedTimestamp, x => x.EventTime);

        // Assert
        result.Should().BeTrue();
        extractedTimestamp.Should().Be(timestamp);
    }

    [Fact]
    public void TryExtractTimestamp_WithExtractor_IgnoresITimestamped()
    {
        // Arrange
        var originalTimestamp = DateTimeOffset.UtcNow;
        var item = new TimestampedItem(originalTimestamp);
        var extractorTimestamp = DateTimeOffset.UtcNow.AddHours(1);

        // Act
        var result = TimestampUtils.TryExtractTimestamp(
            item,
            out var extractedTimestamp,
            x => extractorTimestamp);

        // Assert
        result.Should().BeTrue();
        extractedTimestamp.Should().Be(originalTimestamp); // ITimestamped takes priority
    }

    #endregion

    #region TryExtractTimestamp Failure Cases Tests

    [Fact]
    public void TryExtractTimestamp_WithoutExtractorAndNoInterface_ReturnsFalse()
    {
        // Arrange
        var item = "plain string";

        // Act
        var result = TimestampUtils.TryExtractTimestamp(item, out var extractedTimestamp);

        // Assert
        result.Should().BeFalse();
        extractedTimestamp.Should().BeNull();
    }

    [Fact]
    public void TryExtractTimestamp_WithoutExtractorAndNoInterface_OutVariableIsNull()
    {
        // Arrange
        var item = new object();

        // Act
        var result = TimestampUtils.TryExtractTimestamp(item, out var extractedTimestamp);

        // Assert
        result.Should().BeFalse();
        extractedTimestamp.Should().BeNull();
    }

    #endregion

    #region Edge Cases Tests

    [Fact]
    public void ExtractTimestamp_WithMinDateTimeOffset()
    {
        // Arrange
        var timestamp = DateTimeOffset.MinValue;
        var item = new TimestampedItem(timestamp);

        // Act
        var result = TimestampUtils.ExtractTimestamp(item);

        // Assert
        result.Should().Be(timestamp);
    }

    [Fact]
    public void ExtractTimestamp_WithMaxDateTimeOffset()
    {
        // Arrange
        var timestamp = DateTimeOffset.MaxValue;
        var item = new TimestampedItem(timestamp);

        // Act
        var result = TimestampUtils.ExtractTimestamp(item);

        // Assert
        result.Should().Be(timestamp);
    }

    [Fact]
    public void ExtractTimestamp_WithNegativeOffset()
    {
        // Arrange
        var timestamp = new DateTimeOffset(2025, 10, 22, 12, 0, 0, TimeSpan.FromHours(-5));
        var item = new TimestampedItem(timestamp);

        // Act
        var result = TimestampUtils.ExtractTimestamp(item);

        // Assert
        result.Should().Be(timestamp);
    }

    [Fact]
    public void ExtractTimestamp_WithPositiveOffset()
    {
        // Arrange
        var timestamp = new DateTimeOffset(2025, 10, 22, 12, 0, 0, TimeSpan.FromHours(5));
        var item = new TimestampedItem(timestamp);

        // Act
        var result = TimestampUtils.ExtractTimestamp(item);

        // Assert
        result.Should().Be(timestamp);
    }

    [Fact]
    public void ExtractTimestamp_WithNullableType()
    {
        // Arrange
        var timestamp = DateTimeOffset.UtcNow;
        int? item = 42;

        // Act
        var act = () => TimestampUtils.ExtractTimestamp(item);

        // Assert
        act.Should().Throw<InvalidOperationException>();
    }

    #endregion

    #region Test Helpers

    private sealed class TimestampedItem(DateTimeOffset timestamp) : ITimestamped
    {
        public DateTimeOffset Timestamp { get; } = timestamp;
    }

    private sealed class TestData
    {
        public DateTimeOffset EventTime { get; set; }
    }

    #endregion
}
