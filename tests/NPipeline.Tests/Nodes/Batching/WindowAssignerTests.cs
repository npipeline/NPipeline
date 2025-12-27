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
}
