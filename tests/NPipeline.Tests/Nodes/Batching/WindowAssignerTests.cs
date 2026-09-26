using AwesomeAssertions;
using NPipeline.DataFlow.Windowing;
using NPipeline.Nodes;

namespace NPipeline.Tests.Nodes.Batching;

/// <summary>
///     Tests for window assigner creation in both AggregateWindows and TimeWindowedJoinWindows.
///     Both utilities support identical windowing strategies (Tumbling and Sliding).
///     This consolidated test suite validates both implementations.
/// </summary>
public class WindowAssignerTests
{
    #region AggregateWindows Tests

    [Fact]
    public void AggregateWindows_Tumbling_WithValidWindowSize_ReturnsTumblingWindowAssigner()
    {
        var windowSize = TimeSpan.FromSeconds(10);

        var result = AggregateWindows.Tumbling(windowSize);

        _ = result.Should().NotBeNull();
        _ = result.Should().BeOfType<TumblingWindowAssigner>();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(60)]
    public void AggregateWindows_Tumbling_WithDifferentWindowSizes_CreatesCorrectAssigner(int seconds)
    {
        var windowSize = TimeSpan.FromSeconds(seconds);

        var result = AggregateWindows.Tumbling(windowSize);

        _ = result.Should().NotBeNull();
    }

    [Fact]
    public void AggregateWindows_Sliding_WithValidWindowSizeAndSlide_ReturnsSlidingWindowAssigner()
    {
        var windowSize = TimeSpan.FromSeconds(10);
        var slide = TimeSpan.FromSeconds(5);

        var result = AggregateWindows.Sliding(windowSize, slide);

        _ = result.Should().NotBeNull();
        _ = result.Should().BeOfType<SlidingWindowAssigner>();
    }

    [Theory]
    [InlineData(10, 5)]
    [InlineData(60, 30)]
    [InlineData(20, 10)]
    public void AggregateWindows_Sliding_WithDifferentWindowSizesAndSlides_CreatesCorrectAssigner(int windowSeconds, int slideSeconds)
    {
        var windowSize = TimeSpan.FromSeconds(windowSeconds);
        var slide = TimeSpan.FromSeconds(slideSeconds);

        var result = AggregateWindows.Sliding(windowSize, slide);

        _ = result.Should().NotBeNull();
    }

    #endregion

    #region TimeWindowedJoinWindows Tests

    [Fact]
    public void TimeWindowedJoinWindows_Tumbling_WithValidWindowSize_ReturnsTumblingWindowAssigner()
    {
        var windowSize = TimeSpan.FromSeconds(10);

        var result = TimeWindowedJoinWindows.Tumbling(windowSize);

        _ = result.Should().NotBeNull();
        _ = result.Should().BeOfType<TumblingWindowAssigner>();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(60)]
    public void TimeWindowedJoinWindows_Tumbling_WithDifferentWindowSizes_CreatesCorrectAssigner(int seconds)
    {
        var windowSize = TimeSpan.FromSeconds(seconds);

        var result = TimeWindowedJoinWindows.Tumbling(windowSize);

        _ = result.Should().NotBeNull();
    }

    [Fact]
    public void TimeWindowedJoinWindows_Sliding_WithValidWindowSizeAndSlide_ReturnsSlidingWindowAssigner()
    {
        var windowSize = TimeSpan.FromSeconds(10);
        var slide = TimeSpan.FromSeconds(5);

        var result = TimeWindowedJoinWindows.Sliding(windowSize, slide);

        _ = result.Should().NotBeNull();
        _ = result.Should().BeOfType<SlidingWindowAssigner>();
    }

    [Theory]
    [InlineData(10, 5)]
    [InlineData(60, 30)]
    [InlineData(20, 10)]
    public void TimeWindowedJoinWindows_Sliding_WithDifferentWindowSizesAndSlides_CreatesCorrectAssigner(int windowSeconds, int slideSeconds)
    {
        var windowSize = TimeSpan.FromSeconds(windowSeconds);
        var slide = TimeSpan.FromSeconds(slideSeconds);

        var result = TimeWindowedJoinWindows.Sliding(windowSize, slide);

        _ = result.Should().NotBeNull();
    }

    #endregion

    #region Validation Tests (C26)

    [Fact]
    public void WindowStart_IsIndependentOfOffset()
    {
        var a = TimeWindow.ForTimestamp(new DateTimeOffset(2024, 1, 1, 10, 15, 0, TimeSpan.FromHours(5.5)), TimeSpan.FromHours(1));
        var b = TimeWindow.ForTimestamp(new DateTimeOffset(2024, 1, 1, 4, 45, 0, TimeSpan.Zero), TimeSpan.FromHours(1));
        a.Start.UtcDateTime.Should().Be(b.Start.UtcDateTime);
    }

    [Fact]
    public void WindowStart_ForNonUtcTimestamp_CoversTheTimestamp()
    {
        var timestamp = new DateTimeOffset(2024, 1, 1, 10, 15, 0, TimeSpan.FromHours(5.5));
        var window = TimeWindow.ForTimestamp(timestamp, TimeSpan.FromHours(1));
        window.Contains(timestamp).Should().BeTrue();
        window.Start.Offset.Should().Be(timestamp.Offset);
    }

    [Fact]
    public void WindowStart_SameInstantSameWindow_AcrossOffsets()
    {
        var utc = new DateTimeOffset(2024, 1, 1, 4, 45, 0, TimeSpan.Zero);
        var shifted = utc.ToOffset(TimeSpan.FromHours(5.5));
        var windowUtc = TimeWindow.ForTimestamp(utc, TimeSpan.FromHours(1));
        var windowShifted = TimeWindow.ForTimestamp(shifted, TimeSpan.FromHours(1));
        windowUtc.Should().Be(windowShifted);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Tumbling_WithNonPositiveWindowSize_ThrowsArgumentOutOfRange(int seconds)
    {
        var act = () => WindowAssigner.Tumbling(TimeSpan.FromSeconds(seconds));
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Sliding_WithNegativeSlide_ThrowsArgumentOutOfRange()
    {
        var act = () => WindowAssigner.Sliding(TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(-1));
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Sliding_WithZeroSlide_ThrowsArgumentOutOfRange()
    {
        var act = () => WindowAssigner.Sliding(TimeSpan.FromMinutes(5), TimeSpan.Zero);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Sliding_WithNonPositiveWindowSize_ThrowsArgumentOutOfRange(int seconds)
    {
        var act = () => WindowAssigner.Sliding(TimeSpan.FromSeconds(seconds), TimeSpan.FromSeconds(1));
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void AggregateWindows_Tumbling_WithZeroWindowSize_ThrowsArgumentOutOfRange()
    {
        var act = () => AggregateWindows.Tumbling(TimeSpan.Zero);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void AggregateWindows_Sliding_WithNegativeSlide_ThrowsArgumentOutOfRange()
    {
        var act = () => AggregateWindows.Sliding(TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(-1));
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void TimeWindowedJoinWindows_Tumbling_WithZeroWindowSize_ThrowsArgumentOutOfRange()
    {
        var act = () => TimeWindowedJoinWindows.Tumbling(TimeSpan.Zero);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void TimeWindowedJoinWindows_Sliding_WithNegativeSlide_ThrowsArgumentOutOfRange()
    {
        var act = () => TimeWindowedJoinWindows.Sliding(TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(-1));
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    #endregion

    #region Assignment Tests (P08)

    [Fact]
    public void Tumbling_TryGetSingleWindow_ReturnsTheContainingWindow()
    {
        var assigner = WindowAssigner.Tumbling(TimeSpan.FromMinutes(1));
        var timestamp = new DateTimeOffset(2024, 1, 1, 0, 0, 30, TimeSpan.Zero);

        assigner.TryGetSingleWindow(timestamp, out var window).Should().BeTrue();
        window!.Contains(timestamp).Should().BeTrue();
        window.Should().Be(TimeWindow.ForTimestamp(timestamp, TimeSpan.FromMinutes(1)));
    }

    [Theory]
    [InlineData(10, 5)]
    [InlineData(10, 10)]
    [InlineData(60, 30)]
    [InlineData(20, 7)]
    [InlineData(5, 20)]
    public void Sliding_AssignWindows_ReturnsEveryWindowContainingTheTimestamp(int windowSeconds, int slideSeconds)
    {
        var assigner = WindowAssigner.Sliding(TimeSpan.FromSeconds(windowSeconds), TimeSpan.FromSeconds(slideSeconds));
        var timestamp = new DateTimeOffset(2024, 1, 1, 3, 17, 23, TimeSpan.Zero).AddTicks(123);

        var windows = assigner.AssignWindows(0, timestamp).ToList();

        windows.Should().OnlyContain(w => w.Contains(timestamp));
        windows.Should().BeEquivalentTo(ReferenceWindows(timestamp, windowSeconds, slideSeconds));
    }

    private static IEnumerable<IWindow> ReferenceWindows(DateTimeOffset timestamp, int windowSeconds, int slideSeconds)
    {
        var windowSize = TimeSpan.FromSeconds(windowSeconds);
        var slide = TimeSpan.FromSeconds(slideSeconds);
        var windowStart = TimeWindow.GetWindowStart(timestamp, slide);

        var currentStart = windowStart;

        while (currentStart + windowSize > timestamp)
        {
            if (currentStart <= timestamp)
                yield return new TimeWindow(currentStart, windowSize);

            currentStart -= slide;
        }
    }

    #endregion
}
