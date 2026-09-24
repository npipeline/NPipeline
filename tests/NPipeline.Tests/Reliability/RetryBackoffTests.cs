using AwesomeAssertions;
using NPipeline.Reliability;

namespace NPipeline.Tests.Reliability;

public sealed class RetryBackoffTests
{
    [Fact]
    public void None_NeverWaits()
    {
        RetryBackoff.None.DelayFor(1).Should().Be(TimeSpan.Zero);
        RetryBackoff.None.DelayFor(50).Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void Constant_WaitsTheSameBeforeEveryRetry()
    {
        var backoff = RetryBackoff.Constant(TimeSpan.FromMilliseconds(250));

        Enumerable.Range(1, 5).Select(backoff.DelayFor).Should().AllBeEquivalentTo(TimeSpan.FromMilliseconds(250));
    }

    [Fact]
    public void Linear_GrowsByTheStep_UpToTheCap()
    {
        var backoff = RetryBackoff.Linear(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(250), RetryJitter.None);

        Enumerable.Range(1, 4).Select(backoff.DelayFor).Should().Equal(
            TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(200), TimeSpan.FromMilliseconds(250), TimeSpan.FromMilliseconds(250));
    }

    [Fact]
    public void Exponential_MultipliesByTheFactor_UpToTheCap()
    {
        var backoff = RetryBackoff.Exponential(TimeSpan.FromMilliseconds(100), 3, TimeSpan.FromSeconds(1), RetryJitter.None);

        Enumerable.Range(1, 4).Select(backoff.DelayFor).Should().Equal(
            TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(300), TimeSpan.FromMilliseconds(900), TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void Exponential_WithoutACap_StaysWithinWhatTaskDelayAccepts()
    {
        var backoff = RetryBackoff.Exponential(TimeSpan.FromSeconds(1), jitter: RetryJitter.None);

        backoff.DelayFor(1_000).Should().Be(TimeSpan.FromMilliseconds(int.MaxValue));
    }

    [Fact]
    public void FullJitter_StaysBetweenZeroAndTheComputedDelay()
    {
        var backoff = RetryBackoff.Constant(TimeSpan.FromMilliseconds(100), RetryJitter.Full);

        var max = TimeSpan.FromMilliseconds(100);

        Enumerable.Range(0, 200).Select(_ => backoff.DelayFor(1)).Should().OnlyContain(d => d >= TimeSpan.Zero && d <= max);
    }

    [Fact]
    public void EqualJitter_StaysBetweenHalfAndAllOfTheComputedDelay()
    {
        var backoff = RetryBackoff.Constant(TimeSpan.FromMilliseconds(100), RetryJitter.Equal);

        var (min, max) = (TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(100));

        Enumerable.Range(0, 200).Select(_ => backoff.DelayFor(1)).Should().OnlyContain(d => d >= min && d <= max);
    }

    [Fact]
    public void Jitter_NeverExceedsTheCap()
    {
        // H4 in the defect register: jitter added after the cap overshot it by up to 20%.
        var backoff = RetryBackoff.Exponential(TimeSpan.FromMilliseconds(100), maxDelay: TimeSpan.FromMilliseconds(150), jitter: RetryJitter.Equal);

        var cap = TimeSpan.FromMilliseconds(150);

        Enumerable.Range(0, 200).Select(_ => backoff.DelayFor(5)).Should().OnlyContain(d => d <= cap);
    }

    [Fact]
    public void DelayFor_DependsOnlyOnTheRetryNumber()
    {
        // C7: the old decorrelated jitter shared state across every caller of a strategy.
        var backoff = RetryBackoff.Exponential(TimeSpan.FromMilliseconds(10), jitter: RetryJitter.None);

        _ = backoff.DelayFor(8);
        backoff.DelayFor(1).Should().Be(TimeSpan.FromMilliseconds(10));
    }

    [Fact]
    public void Custom_UsesTheFunction_AndTreatsANegativeResultAsZero()
    {
        var backoff = RetryBackoff.Custom(retry => TimeSpan.FromMilliseconds(retry == 1
            ? -5
            : retry * 7));

        backoff.DelayFor(1).Should().Be(TimeSpan.Zero);
        backoff.DelayFor(3).Should().Be(TimeSpan.FromMilliseconds(21));
    }

    [Fact]
    public void DelayFor_RejectsARetryNumberBelowOne()
    {
        var act = () => RetryBackoff.None.DelayFor(0);

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Factories_RejectInvalidValues()
    {
        ((Action)(() => RetryBackoff.Constant(TimeSpan.FromMilliseconds(-1)))).Should().Throw<ArgumentOutOfRangeException>();
        ((Action)(() => RetryBackoff.Exponential(TimeSpan.FromSeconds(1), 0.5))).Should().Throw<ArgumentOutOfRangeException>();
        ((Action)(() => RetryBackoff.Linear(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(-1)))).Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Validate_RejectsACustomBackoffWithoutAFunction()
    {
        var act = () => new RetryBackoff { Kind = RetryBackoffKind.Custom }.Validate();

        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void With_RejectsAnInvalidValueWhereItIsWritten()
    {
        var backoff = RetryBackoff.Exponential(TimeSpan.FromMilliseconds(100));

        ((Action)(() => _ = backoff with { Factor = 0.5 })).Should().Throw<ArgumentOutOfRangeException>().WithParameterName("Factor");
        ((Action)(() => _ = backoff with { Factor = double.NaN })).Should().Throw<ArgumentOutOfRangeException>();

        ((Action)(() => _ = backoff with { BaseDelay = TimeSpan.FromMilliseconds(-1) })).Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName("BaseDelay");

        ((Action)(() => _ = backoff with { MaxDelay = TimeSpan.FromMilliseconds(-1) })).Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName("MaxDelay");

        ((Action)(() => _ = backoff with { Kind = (RetryBackoffKind)99 })).Should().Throw<ArgumentOutOfRangeException>();
        ((Action)(() => _ = backoff with { Jitter = (RetryJitter)99 })).Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void With_AcceptsValidValues()
    {
        var slower = RetryBackoff.Exponential(TimeSpan.FromMilliseconds(100)) with { Factor = 3, MaxDelay = TimeSpan.Zero };

        slower.Factor.Should().Be(3);
        slower.DelayFor(2).Should().BeLessThanOrEqualTo(TimeSpan.FromMilliseconds(300));
    }
}
