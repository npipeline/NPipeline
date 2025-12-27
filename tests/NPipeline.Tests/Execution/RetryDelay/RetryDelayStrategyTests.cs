using System.Diagnostics;
using System.Reflection;
using AwesomeAssertions;
using NPipeline.Configuration.RetryDelay;
using NPipeline.Execution.RetryDelay;

namespace NPipeline.Tests.Execution.RetryDelay;

/// <summary>
///     Comprehensive unit tests for all retry delay strategy implementations.
///     Tests backoff strategies, jitter strategies, composite strategies, and factory creation.
/// </summary>
public sealed class RetryDelayStrategyTests
{
    #region Performance Tests

    [Fact]
    public async Task NoOpStrategy_GetDelayAsync_WithHighThroughput_ShouldHandleEfficiently()
    {
        // Arrange
        var strategy = NoOpRetryDelayStrategy.Instance;
        var stopwatch = Stopwatch.StartNew();

        // Act
        var tasks = new List<Task<TimeSpan>>();

        for (var i = 0; i < 10000; i++)
        {
            tasks.Add(strategy.GetDelayAsync(i).AsTask());
        }

        var results = await Task.WhenAll(tasks);
        stopwatch.Stop();

        // Assert
        results.Should().HaveCount(10000);
        results.Should().AllBeEquivalentTo(TimeSpan.Zero);
        stopwatch.ElapsedMilliseconds.Should().BeLessThan(1000);
    }

    #endregion

    #region NoOp Strategy Tests

    [Fact]
    public void NoOpStrategy_Instance_ShouldReturnSingleton()
    {
        // Act
        var instance1 = NoOpRetryDelayStrategy.Instance;
        var instance2 = NoOpRetryDelayStrategy.Instance;

        // Assert
        instance1.Should().BeSameAs(instance2);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(10)]
    [InlineData(100)]
    [InlineData(int.MaxValue)]
    [InlineData(int.MinValue)]
    public async Task NoOpStrategy_GetDelayAsync_WithAnyAttemptNumber_ShouldReturnZero(int attemptNumber)
    {
        // Arrange
        var strategy = NoOpRetryDelayStrategy.Instance;

        // Act
        var result = await strategy.GetDelayAsync(attemptNumber);

        // Assert
        result.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public async Task NoOpStrategy_GetDelayAsync_WithCancelledToken_ShouldThrowTaskCanceledException()
    {
        // Arrange
        var strategy = NoOpRetryDelayStrategy.Instance;
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act & Assert
        await Assert.ThrowsAsync<TaskCanceledException>(() => strategy.GetDelayAsync(5, cts.Token).AsTask());
    }

    [Fact]
    public async Task NoOpStrategy_GetDelayAsync_ConcurrentCalls_ShouldBeThreadSafe()
    {
        // Arrange
        var strategy = NoOpRetryDelayStrategy.Instance;
        var tasks = new List<Task<TimeSpan>>();

        // Act
        for (var i = 0; i < 100; i++)
        {
            tasks.Add(strategy.GetDelayAsync(i).AsTask());
        }

        var results = await Task.WhenAll(tasks);

        // Assert
        results.Should().HaveCount(100);
        results.Should().AllBeEquivalentTo(TimeSpan.Zero);
    }

    [Fact]
    public void NoOpStrategy_Type_ShouldBeSealedWithPrivateConstructor()
    {
        // Assert - Type should be sealed
        typeof(NoOpRetryDelayStrategy).IsSealed.Should().BeTrue();

        // Assert - No public constructors
        typeof(NoOpRetryDelayStrategy)
            .GetConstructors(BindingFlags.Public | BindingFlags.Instance)
            .Should().BeEmpty();

        // Assert - Implements IRetryDelayStrategy
        NoOpRetryDelayStrategy.Instance.Should().BeAssignableTo<IRetryDelayStrategy>();
    }

    #endregion

    #region Fixed Delay Strategy Tests

    [Theory]
    [InlineData(-1)]
    [InlineData(-10)]
    public void FixedDelayStrategy_CalculateDelay_WithNegativeAttempt_ReturnsZero(int attempt)
    {
        var delay = TimeSpan.FromMilliseconds(25);
        var strategy = BackoffStrategies.FixedDelay(delay);

        strategy(attempt).Should().Be(TimeSpan.Zero);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(100)]
    public void FixedDelayStrategy_CalculateDelay_WithValidAttempt_ReturnsConfiguredDelay(int attempt)
    {
        var delay = TimeSpan.FromMilliseconds(25);
        var strategy = BackoffStrategies.FixedDelay(delay);

        strategy(attempt).Should().Be(delay);
    }

    [Fact]
    public void FixedDelayStrategy_CalculateDelay_WithZeroDelay_ThrowsArgumentException()
    {
        // FixedDelay requires a positive delay
        Assert.Throws<ArgumentException>(() => BackoffStrategies.FixedDelay(TimeSpan.Zero));
    }

    #endregion

    #region Exponential Backoff Strategy Tests

    [Fact]
    public void ExponentialBackoffStrategy_CalculateDelay_ShouldIncreaseExponentially()
    {
        // Arrange
        var initialDelay = TimeSpan.FromMilliseconds(100);
        var multiplier = 2.0;
        var maxDelay = TimeSpan.FromSeconds(10);
        var strategy = BackoffStrategies.ExponentialBackoff(initialDelay, multiplier, maxDelay);

        // Act & Assert
        strategy(0).Should().Be(TimeSpan.FromMilliseconds(100));
        strategy(1).Should().Be(TimeSpan.FromMilliseconds(200));
        strategy(2).Should().Be(TimeSpan.FromMilliseconds(400));
        strategy(3).Should().Be(TimeSpan.FromMilliseconds(800));
    }

    [Fact]
    public void ExponentialBackoffStrategy_CalculateDelay_ShouldRespectMaxDelay()
    {
        // Arrange
        var initialDelay = TimeSpan.FromMilliseconds(100);
        var multiplier = 10.0; // Aggressive multiplier
        var maxDelay = TimeSpan.FromMilliseconds(500);
        var strategy = BackoffStrategies.ExponentialBackoff(initialDelay, multiplier, maxDelay);

        // Act & Assert - Should never exceed max delay
        strategy(0).Should().Be(TimeSpan.FromMilliseconds(100));
        strategy(1).Should().Be(maxDelay); // 100 * 10 = 1000, capped to 500
        strategy(10).Should().Be(maxDelay); // Still capped
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(-10)]
    public void ExponentialBackoffStrategy_CalculateDelay_WithNegativeAttempt_ReturnsZero(int attempt)
    {
        var strategy = BackoffStrategies.ExponentialBackoff(TimeSpan.FromMilliseconds(100));

        strategy(attempt).Should().Be(TimeSpan.Zero);
    }

    #endregion

    #region Linear Backoff Strategy Tests

    [Fact]
    public void LinearBackoffStrategy_CalculateDelay_ShouldIncreaseLinearly()
    {
        // Arrange
        var initialDelay = TimeSpan.FromMilliseconds(100);
        var increment = TimeSpan.FromMilliseconds(50);
        var maxDelay = TimeSpan.FromSeconds(10);
        var strategy = BackoffStrategies.LinearBackoff(initialDelay, increment, maxDelay);

        // Act & Assert
        strategy(0).Should().Be(TimeSpan.FromMilliseconds(100));
        strategy(1).Should().Be(TimeSpan.FromMilliseconds(150));
        strategy(2).Should().Be(TimeSpan.FromMilliseconds(200));
        strategy(3).Should().Be(TimeSpan.FromMilliseconds(250));
    }

    [Fact]
    public void LinearBackoffStrategy_CalculateDelay_ShouldRespectMaxDelay()
    {
        // Arrange
        var initialDelay = TimeSpan.FromMilliseconds(100);
        var increment = TimeSpan.FromMilliseconds(100);
        var maxDelay = TimeSpan.FromMilliseconds(300);
        var strategy = BackoffStrategies.LinearBackoff(initialDelay, increment, maxDelay);

        // Act & Assert - Should cap at max delay
        strategy(0).Should().Be(TimeSpan.FromMilliseconds(100));
        strategy(1).Should().Be(TimeSpan.FromMilliseconds(200));
        strategy(2).Should().Be(TimeSpan.FromMilliseconds(300));
        strategy(3).Should().Be(TimeSpan.FromMilliseconds(300)); // Capped
        strategy(100).Should().Be(TimeSpan.FromMilliseconds(300)); // Still capped
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(-10)]
    public void LinearBackoffStrategy_CalculateDelay_WithNegativeAttempt_ReturnsZero(int attempt)
    {
        var strategy = BackoffStrategies.LinearBackoff(
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromMilliseconds(50));

        strategy(attempt).Should().Be(TimeSpan.Zero);
    }

    #endregion

    #region Composite Strategy Tests

    [Fact]
    public void CompositeStrategy_Constructor_WithNullBackoffStrategy_ShouldThrowArgumentNullException()
    {
        var jitterStrategy = JitterStrategies.FullJitter();
        var random = new Random(42);

        Assert.Throws<ArgumentNullException>(() =>
            new CompositeRetryDelayStrategy(null!, jitterStrategy, random));
    }

    [Fact]
    public void CompositeStrategy_Constructor_WithNullRandom_ShouldThrowArgumentNullException()
    {
        var backoffStrategy = BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1));
        var jitterStrategy = JitterStrategies.FullJitter();

        Assert.Throws<ArgumentNullException>(() =>
            new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, null!));
    }

    [Fact]
    public async Task CompositeStrategy_GetDelayAsync_WithJitter_ShouldApplyJitter()
    {
        // Arrange
        var backoffStrategy = BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1));
        var jitterStrategy = JitterStrategies.FullJitter();
        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random);

        // Act
        var delay = await strategy.GetDelayAsync(0);

        // Assert - With full jitter, delay should be between 0 and base delay
        delay.Should().BeGreaterThan(TimeSpan.Zero);
        delay.Should().BeLessThan(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task CompositeStrategy_GetDelayAsync_WithoutJitter_ShouldReturnBackoffDelay()
    {
        // Arrange
        var backoffStrategy = BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1));
        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, null, random);

        // Act
        var delay = await strategy.GetDelayAsync(0);

        // Assert
        delay.Should().Be(TimeSpan.FromSeconds(1));
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(5)]
    public async Task CompositeStrategy_GetDelayAsync_WithVariousAttemptNumbers_ShouldReturnValidDelay(int attemptNumber)
    {
        // Arrange
        var backoffStrategy = BackoffStrategies.ExponentialBackoff(
            TimeSpan.FromMilliseconds(100),
            2.0,
            TimeSpan.FromSeconds(10));

        var jitterStrategy = JitterStrategies.FullJitter();
        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random);

        // Act
        var delay = await strategy.GetDelayAsync(attemptNumber);

        // Assert
        delay.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        delay.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task CompositeStrategy_GetDelayAsync_WithCancellation_ShouldRespectCancellationToken()
    {
        // Arrange
        var backoffStrategy = BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1));
        var jitterStrategy = JitterStrategies.FullJitter();
        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act & Assert
        await Assert.ThrowsAsync<TaskCanceledException>(() => strategy.GetDelayAsync(0, cts.Token).AsTask());
    }

    [Fact]
    public async Task CompositeStrategy_WithExponentialBackoffAndFullJitter_ShouldCalculateCorrectly()
    {
        // Arrange
        var backoffStrategy = BackoffStrategies.ExponentialBackoff(
            TimeSpan.FromMilliseconds(100),
            2.0,
            TimeSpan.FromSeconds(5));

        var jitterStrategy = JitterStrategies.FullJitter();
        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random);

        // Act
        var delays = new List<TimeSpan>();

        for (var i = 0; i < 5; i++)
        {
            delays.Add(await strategy.GetDelayAsync(i));
        }

        // Assert - With full jitter, each delay should be between 0 and base delay
        // Base delays: 100ms, 200ms, 400ms, 800ms, 1600ms
        delays[0].Should().BeLessThan(TimeSpan.FromMilliseconds(100));
        delays[1].Should().BeLessThan(TimeSpan.FromMilliseconds(200));
        delays[2].Should().BeLessThan(TimeSpan.FromMilliseconds(400));
        delays[3].Should().BeLessThan(TimeSpan.FromMilliseconds(800));
        delays[4].Should().BeLessThan(TimeSpan.FromMilliseconds(1600));
    }

    [Fact]
    public async Task CompositeStrategy_WithLinearBackoffAndEqualJitter_ShouldCalculateCorrectly()
    {
        // Arrange
        var backoffStrategy = BackoffStrategies.LinearBackoff(
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromMilliseconds(50),
            TimeSpan.FromSeconds(5));

        var jitterStrategy = JitterStrategies.EqualJitter();
        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random);

        // Act
        var delays = new List<TimeSpan>();

        for (var i = 0; i < 5; i++)
        {
            delays.Add(await strategy.GetDelayAsync(i));
        }

        // Assert - With equal jitter, each delay should be between base/2 and base
        // Base delays: 100ms, 150ms, 200ms, 250ms, 300ms
        delays[0].Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(50));
        delays[0].Should().BeLessThan(TimeSpan.FromMilliseconds(100));
        delays[1].Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(75));
        delays[1].Should().BeLessThan(TimeSpan.FromMilliseconds(150));
    }

    [Fact]
    public async Task CompositeStrategy_WithMaxDelayCapping_ShouldRespectMaxDelay()
    {
        // Arrange
        var backoffStrategy = BackoffStrategies.ExponentialBackoff(
            TimeSpan.FromMilliseconds(1),
            10.0,
            TimeSpan.FromMilliseconds(100));

        var jitterStrategy = JitterStrategies.FullJitter();
        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random);

        // Act
        var delays = new List<TimeSpan>();

        for (var i = 0; i < 10; i++)
        {
            delays.Add(await strategy.GetDelayAsync(i));
        }

        // Assert - All delays should be capped at max delay
        foreach (var delay in delays)
        {
            delay.Should().BeLessThan(TimeSpan.FromMilliseconds(100));
        }
    }

    #endregion

    #region Factory Tests

    [Fact]
    public async Task Factory_CreateStrategy_WithExponentialBackoff_ShouldCreateStrategy()
    {
        // Arrange
        var factory = new DefaultRetryDelayStrategyFactory();

        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromSeconds(1),
                2.0,
                TimeSpan.FromMinutes(1)),
            JitterStrategies.NoJitter());

        // Act
        var strategy = factory.CreateStrategy(configuration);

        // Assert
        var compositeStrategy = strategy as CompositeRetryDelayStrategy;
        compositeStrategy.Should().NotBeNull();

        var delay = await strategy.GetDelayAsync(0);
        delay.Should().Be(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task Factory_CreateStrategy_WithLinearBackoff_ShouldCreateStrategy()
    {
        // Arrange
        var factory = new DefaultRetryDelayStrategyFactory();

        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.LinearBackoff(
                TimeSpan.FromSeconds(1),
                TimeSpan.FromMilliseconds(500),
                TimeSpan.FromMinutes(2)),
            JitterStrategies.NoJitter());

        // Act
        var strategy = factory.CreateStrategy(configuration);

        // Assert
        var compositeStrategy = strategy as CompositeRetryDelayStrategy;
        compositeStrategy.Should().NotBeNull();

        var delay = await strategy.GetDelayAsync(0);
        delay.Should().Be(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task Factory_CreateStrategy_WithFixedDelay_ShouldReturnStrategy()
    {
        // Arrange
        var factory = new DefaultRetryDelayStrategyFactory();

        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.FixedDelay(TimeSpan.FromMilliseconds(200)),
            JitterStrategies.NoJitter());

        // Act
        var strategy = factory.CreateStrategy(configuration);

        // Assert
        strategy.Should().NotBeNull();
        strategy.Should().BeOfType<CompositeRetryDelayStrategy>();

        var delay = await strategy.GetDelayAsync(0);
        delay.Should().Be(TimeSpan.FromMilliseconds(200));
    }

    [Fact]
    public void Factory_CreateStrategy_WithJitterStrategy_ShouldReturnCompositeStrategy()
    {
        // Arrange
        var factory = new DefaultRetryDelayStrategyFactory();

        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                2.0,
                TimeSpan.FromSeconds(30)),
            JitterStrategies.FullJitter());

        // Act
        var strategy = factory.CreateStrategy(configuration);

        // Assert
        strategy.Should().NotBeNull();
        strategy.Should().BeOfType<CompositeRetryDelayStrategy>();
    }

    [Fact]
    public void Factory_CreateStrategy_WithNullConfiguration_ShouldThrowArgumentNullException()
    {
        // Arrange
        var factory = new DefaultRetryDelayStrategyFactory();

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => factory.CreateStrategy(null!));
    }

    [Fact]
    public void Factory_CreateNoOp_ShouldReturnSingletonInstance()
    {
        // Arrange
        var factory = new DefaultRetryDelayStrategyFactory();

        // Act
        var strategy1 = factory.CreateNoOp();
        var strategy2 = factory.CreateNoOp();

        // Assert
        strategy1.Should().NotBeNull();
        strategy2.Should().NotBeNull();
        strategy1.Should().BeSameAs(strategy2);
        strategy1.Should().BeOfType<NoOpRetryDelayStrategy>();
    }

    [Fact]
    public void Factory_CreateFullJitter_ShouldReturnStrategy()
    {
        // Arrange
        var factory = new DefaultRetryDelayStrategyFactory();

        // Act
        var strategy = factory.CreateFullJitter();

        // Assert
        strategy.Should().NotBeNull();
        strategy.Should().BeOfType<JitterStrategy>();
    }

    [Fact]
    public void Factory_CreateDecorrelatedJitter_WithValidParameters_ShouldReturnStrategy()
    {
        // Arrange
        var factory = new DefaultRetryDelayStrategyFactory();
        var maxDelay = TimeSpan.FromSeconds(30);

        // Act
        var strategy = factory.CreateDecorrelatedJitter(maxDelay);

        // Assert
        strategy.Should().NotBeNull();
        strategy.Should().BeOfType<JitterStrategy>();
    }

    [Fact]
    public void Factory_CreateDecorrelatedJitter_WithInvalidMaxDelay_ShouldThrowArgumentException()
    {
        // Arrange
        var factory = new DefaultRetryDelayStrategyFactory();
        var invalidMaxDelay = TimeSpan.Zero;

        // Act & Assert
        Assert.Throws<ArgumentException>(() => factory.CreateDecorrelatedJitter(invalidMaxDelay));
    }

    [Fact]
    public void Factory_CreateDecorrelatedJitter_WithInvalidMultiplier_ShouldThrowArgumentException()
    {
        // Arrange
        var factory = new DefaultRetryDelayStrategyFactory();
        var maxDelay = TimeSpan.FromSeconds(30);
        const double invalidMultiplier = 0.5; // Invalid - less than 1.0

        // Act & Assert
        Assert.Throws<ArgumentException>(() => factory.CreateDecorrelatedJitter(maxDelay, invalidMultiplier));
    }

    [Fact]
    public void Factory_CreateEqualJitter_ShouldReturnStrategy()
    {
        // Arrange
        var factory = new DefaultRetryDelayStrategyFactory();

        // Act
        var strategy = factory.CreateEqualJitter();

        // Assert
        strategy.Should().NotBeNull();
        strategy.Should().BeOfType<JitterStrategy>();
    }

    [Fact]
    public void Factory_CreateNoJitter_ShouldReturnStrategy()
    {
        // Arrange
        var factory = new DefaultRetryDelayStrategyFactory();

        // Act
        var strategy = factory.CreateNoJitter();

        // Assert
        strategy.Should().NotBeNull();
        strategy.Should().BeOfType<JitterStrategy>();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(10)]
    public async Task Factory_CreateStrategy_WithExponentialBackoffDifferentMultipliers_ShouldCalculateCorrectly(double multiplier)
    {
        // Arrange
        var factory = new DefaultRetryDelayStrategyFactory();

        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                multiplier,
                TimeSpan.FromSeconds(10)),
            JitterStrategies.NoJitter());

        // Act
        var strategy = factory.CreateStrategy(configuration);

        // Assert
        strategy.Should().NotBeNull();

        var delay1 = await strategy.GetDelayAsync(1);
        var delay2 = await strategy.GetDelayAsync(2);

        delay1.Should().BeGreaterThan(TimeSpan.Zero);
        delay2.Should().BeGreaterThanOrEqualTo(delay1);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(10)]
    public async Task Factory_CreateStrategy_WithLinearBackoffDifferentIncrements_ShouldCalculateCorrectly(int incrementSeconds)
    {
        // Arrange
        var factory = new DefaultRetryDelayStrategyFactory();

        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.LinearBackoff(
                TimeSpan.FromMilliseconds(100),
                TimeSpan.FromSeconds(incrementSeconds),
                TimeSpan.FromSeconds(30)),
            JitterStrategies.NoJitter());

        // Act
        var strategy = factory.CreateStrategy(configuration);

        // Assert
        strategy.Should().NotBeNull();

        var delay1 = await strategy.GetDelayAsync(1);
        var delay2 = await strategy.GetDelayAsync(2);

        delay1.Should().BeGreaterThan(TimeSpan.Zero);
        delay2.Should().BeGreaterThanOrEqualTo(delay1);
    }

    [Fact]
    public async Task Factory_CreateStrategies_WithConcurrentAccess_ShouldBeThreadSafe()
    {
        // Arrange
        var factory = new DefaultRetryDelayStrategyFactory();
        var tasks = new Task[10];

        // Act
        for (var i = 0; i < tasks.Length; i++)
        {
            tasks[i] = Task.Run(async () =>
            {
                var configuration = new RetryDelayStrategyConfiguration(
                    BackoffStrategies.ExponentialBackoff(
                        TimeSpan.FromMilliseconds(100),
                        2.0,
                        TimeSpan.FromSeconds(30)),
                    JitterStrategies.FullJitter());

                var strategy = factory.CreateStrategy(configuration);
                strategy.Should().NotBeNull();

                var delay = await strategy.GetDelayAsync(1);
                delay.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
            });
        }

        // Assert
        await Task.WhenAll(tasks);
    }

    [Fact]
    public void Factory_CreateAllStrategyTypes_ShouldReturnValidInstances()
    {
        // Arrange
        var factory = new DefaultRetryDelayStrategyFactory();

        var exponentialConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                2.0,
                TimeSpan.FromSeconds(30)),
            JitterStrategies.NoJitter());

        var linearConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.LinearBackoff(
                TimeSpan.FromMilliseconds(100),
                TimeSpan.FromMilliseconds(50),
                TimeSpan.FromSeconds(30)),
            JitterStrategies.NoJitter());

        var fixedConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.FixedDelay(TimeSpan.FromMilliseconds(200)),
            JitterStrategies.NoJitter());

        // Act
        var exponentialStrategy = factory.CreateStrategy(exponentialConfig);
        var linearStrategy = factory.CreateStrategy(linearConfig);
        var fixedStrategy = factory.CreateStrategy(fixedConfig);
        var noOpStrategy = factory.CreateNoOp();
        var fullJitterStrategy = factory.CreateFullJitter();
        var decorrelatedJitterStrategy = factory.CreateDecorrelatedJitter(TimeSpan.FromSeconds(30));
        var equalJitterStrategy = factory.CreateEqualJitter();
        var noJitterStrategy = factory.CreateNoJitter();

        // Assert
        exponentialStrategy.Should().NotBeNull();
        linearStrategy.Should().NotBeNull();
        fixedStrategy.Should().NotBeNull();
        noOpStrategy.Should().NotBeNull();
        fullJitterStrategy.Should().NotBeNull();
        decorrelatedJitterStrategy.Should().NotBeNull();
        equalJitterStrategy.Should().NotBeNull();
        noJitterStrategy.Should().NotBeNull();
    }

    #endregion
}
