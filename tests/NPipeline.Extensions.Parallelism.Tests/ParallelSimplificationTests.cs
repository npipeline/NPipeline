using AwesomeAssertions;

namespace NPipeline.Extensions.Parallelism.Tests;

/// <summary>
///     Tests for the simplified parallel execution API (ParallelWorkloadType and ParallelOptionsBuilder).
/// </summary>
public class ParallelSimplificationTests
{
    #region ParallelWorkloadType Tests

    [Fact]
    public void ParallelWorkloadType_General_ShouldHaveSensibleDefaults()
    {
        // Arrange & Act
        var options = ParallelOptionsPresets.GetForWorkloadType(ParallelWorkloadType.General);

        // Assert
        var expectedDop = Environment.ProcessorCount * 2;
        var expectedQueueLength = Environment.ProcessorCount * 4;
        var expectedBufferCapacity = Environment.ProcessorCount * 8;

        options.MaxDegreeOfParallelism.Should().Be(expectedDop);
        options.MaxQueueLength.Should().Be(expectedQueueLength);
        options.QueuePolicy.Should().Be(BoundedQueuePolicy.Block);
        options.OutputBufferCapacity.Should().Be(expectedBufferCapacity);
    }

    [Fact]
    public void ParallelWorkloadType_CpuBound_ShouldAvoidOversubscription()
    {
        // Arrange & Act
        var options = ParallelOptionsPresets.GetForWorkloadType(ParallelWorkloadType.CpuBound);

        // Assert
        var expectedDop = Environment.ProcessorCount;
        var expectedQueueLength = Environment.ProcessorCount * 2;
        var expectedBufferCapacity = Environment.ProcessorCount * 4;

        options.MaxDegreeOfParallelism.Should().Be(expectedDop);
        options.MaxQueueLength.Should().Be(expectedQueueLength);
        options.QueuePolicy.Should().Be(BoundedQueuePolicy.Block);
        options.OutputBufferCapacity.Should().Be(expectedBufferCapacity);
    }

    [Fact]
    public void ParallelWorkloadType_IoBound_ShouldHideLatency()
    {
        // Arrange & Act
        var options = ParallelOptionsPresets.GetForWorkloadType(ParallelWorkloadType.IoBound);

        // Assert
        var expectedDop = Environment.ProcessorCount * 4;
        var expectedQueueLength = Environment.ProcessorCount * 8;
        var expectedBufferCapacity = Environment.ProcessorCount * 16;

        options.MaxDegreeOfParallelism.Should().Be(expectedDop);
        options.MaxQueueLength.Should().Be(expectedQueueLength);
        options.QueuePolicy.Should().Be(BoundedQueuePolicy.Block);
        options.OutputBufferCapacity.Should().Be(expectedBufferCapacity);
    }

    [Fact]
    public void ParallelWorkloadType_NetworkBound_ShouldMaximizeUnderthroughput()
    {
        // Arrange & Act
        var options = ParallelOptionsPresets.GetForWorkloadType(ParallelWorkloadType.NetworkBound);

        // Assert
        var expectedDop = Math.Min(Environment.ProcessorCount * 8, 100);
        const int expectedQueueLength = 200;
        const int expectedBufferCapacity = 400;

        options.MaxDegreeOfParallelism.Should().Be(expectedDop);
        options.MaxQueueLength.Should().Be(expectedQueueLength);
        options.QueuePolicy.Should().Be(BoundedQueuePolicy.Block);
        options.OutputBufferCapacity.Should().Be(expectedBufferCapacity);
    }

    [Fact]
    public void ParallelWorkloadType_InvalidValue_ShouldThrow()
    {
        // Arrange
        const ParallelWorkloadType invalidWorkloadType = (ParallelWorkloadType)999;

        // Act & Assert
        Assert.Throws<ArgumentException>(() =>
            ParallelOptionsPresets.GetForWorkloadType(invalidWorkloadType));
    }

    #endregion

    #region ParallelOptionsBuilder Tests

    [Fact]
    public void ParallelOptionsBuilder_MaxDegreeOfParallelism_ShouldSetValue()
    {
        // Arrange & Act
        var options = new ParallelOptionsBuilder()
            .MaxDegreeOfParallelism(8)
            .Build();

        // Assert
        options.MaxDegreeOfParallelism.Should().Be(8);
    }

    [Fact]
    public void ParallelOptionsBuilder_MaxDegreeOfParallelism_InvalidValue_ShouldThrow()
    {
        // Arrange & Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ParallelOptionsBuilder().MaxDegreeOfParallelism(0));

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ParallelOptionsBuilder().MaxDegreeOfParallelism(-1));
    }

    [Fact]
    public void ParallelOptionsBuilder_MaxQueueLength_ShouldSetValue()
    {
        // Arrange & Act
        var options = new ParallelOptionsBuilder()
            .MaxQueueLength(50)
            .Build();

        // Assert
        options.MaxQueueLength.Should().Be(50);
    }

    [Fact]
    public void ParallelOptionsBuilder_MaxQueueLength_InvalidValue_ShouldThrow()
    {
        // Arrange & Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ParallelOptionsBuilder().MaxQueueLength(0));

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ParallelOptionsBuilder().MaxQueueLength(-1));
    }

    [Fact]
    public void ParallelOptionsBuilder_DropOldestOnBackpressure_ShouldSetQueuePolicy()
    {
        // Arrange & Act
        var options = new ParallelOptionsBuilder()
            .DropOldestOnBackpressure()
            .Build();

        // Assert
        options.QueuePolicy.Should().Be(BoundedQueuePolicy.DropOldest);
    }

    [Fact]
    public void ParallelOptionsBuilder_DropNewestOnBackpressure_ShouldSetQueuePolicy()
    {
        // Arrange & Act
        var options = new ParallelOptionsBuilder()
            .DropNewestOnBackpressure()
            .Build();

        // Assert
        options.QueuePolicy.Should().Be(BoundedQueuePolicy.DropNewest);
    }

    [Fact]
    public void ParallelOptionsBuilder_BlockOnBackpressure_ShouldSetQueuePolicy()
    {
        // Arrange & Act
        var options = new ParallelOptionsBuilder()
            .BlockOnBackpressure()
            .Build();

        // Assert
        options.QueuePolicy.Should().Be(BoundedQueuePolicy.Block);
    }

    [Fact]
    public void ParallelOptionsBuilder_QueuePolicies_ShouldBeOverrideable()
    {
        // Arrange & Act
        var options = new ParallelOptionsBuilder()
            .DropOldestOnBackpressure()
            .DropNewestOnBackpressure()
            .BlockOnBackpressure()
            .Build();

        // Assert
        options.QueuePolicy.Should().Be(BoundedQueuePolicy.Block);
    }

    [Fact]
    public void ParallelOptionsBuilder_OutputBufferCapacity_ShouldSetValue()
    {
        // Arrange & Act
        var options = new ParallelOptionsBuilder()
            .OutputBufferCapacity(200)
            .Build();

        // Assert
        options.OutputBufferCapacity.Should().Be(200);
    }

    [Fact]
    public void ParallelOptionsBuilder_OutputBufferCapacity_InvalidValue_ShouldThrow()
    {
        // Arrange & Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ParallelOptionsBuilder().OutputBufferCapacity(0));

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ParallelOptionsBuilder().OutputBufferCapacity(-1));
    }

    [Fact]
    public void ParallelOptionsBuilder_AllowUnorderedOutput_ShouldDisableOrdering()
    {
        // Arrange & Act
        var options = new ParallelOptionsBuilder()
            .AllowUnorderedOutput()
            .Build();

        // Assert
        options.PreserveOrdering.Should().BeFalse();
    }

    [Fact]
    public void ParallelOptionsBuilder_DefaultShouldPreserveOrdering()
    {
        // Arrange & Act
        var options = new ParallelOptionsBuilder()
            .Build();

        // Assert
        options.PreserveOrdering.Should().BeTrue();
    }

    [Fact]
    public void ParallelOptionsBuilder_MetricsInterval_ShouldSetValue()
    {
        // Arrange
        var interval = TimeSpan.FromSeconds(2);

        // Act
        var options = new ParallelOptionsBuilder()
            .MetricsInterval(interval)
            .Build();

        // Assert
        options.MetricsInterval.Should().Be(interval);
    }

    [Fact]
    public void ParallelOptionsBuilder_MetricsInterval_InvalidValue_ShouldThrow()
    {
        // Arrange & Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ParallelOptionsBuilder().MetricsInterval(TimeSpan.Zero));

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ParallelOptionsBuilder().MetricsInterval(TimeSpan.FromSeconds(-1)));
    }

    [Fact]
    public void ParallelOptionsBuilder_FluentChaining_ShouldWork()
    {
        // Arrange & Act
        var options = new ParallelOptionsBuilder()
            .MaxDegreeOfParallelism(4)
            .MaxQueueLength(100)
            .DropOldestOnBackpressure()
            .OutputBufferCapacity(50)
            .AllowUnorderedOutput()
            .MetricsInterval(TimeSpan.FromMilliseconds(500))
            .Build();

        // Assert
        options.MaxDegreeOfParallelism.Should().Be(4);
        options.MaxQueueLength.Should().Be(100);
        options.QueuePolicy.Should().Be(BoundedQueuePolicy.DropOldest);
        options.OutputBufferCapacity.Should().Be(50);
        options.PreserveOrdering.Should().BeFalse();
        options.MetricsInterval.Should().Be(TimeSpan.FromMilliseconds(500));
    }

    [Fact]
    public void ParallelOptionsBuilder_Build_ShouldReturnNewInstanceEachTime()
    {
        // Arrange
        var builder = new ParallelOptionsBuilder().MaxDegreeOfParallelism(5);

        // Act
        var options1 = builder.Build();
        var options2 = builder.Build();

        // Assert
        options1.Should().Be(options2); // Same configuration

        // Both should have the set value
        options1.MaxDegreeOfParallelism.Should().Be(5);
        options2.MaxDegreeOfParallelism.Should().Be(5);
    }

    #endregion

    #region ParallelOptionsPresets Helper Tests

    [Fact]
    public void ParallelOptionsPresets_AllWorkloadTypes_ShouldUseBlockPolicy()
    {
        // Arrange
        var workloadTypes = new[]
        {
            ParallelWorkloadType.General,
            ParallelWorkloadType.CpuBound,
            ParallelWorkloadType.IoBound,
            ParallelWorkloadType.NetworkBound,
        };

        // Act & Assert
        foreach (var workloadType in workloadTypes)
        {
            var options = ParallelOptionsPresets.GetForWorkloadType(workloadType);

            options.QueuePolicy.Should().Be(BoundedQueuePolicy.Block,
                $"Workload type {workloadType} should use Block queue policy");
        }
    }

    [Fact]
    public void ParallelOptionsPresets_AllWorkloadTypes_ShouldHaveOutputBuffering()
    {
        // Arrange
        var workloadTypes = new[]
        {
            ParallelWorkloadType.General,
            ParallelWorkloadType.CpuBound,
            ParallelWorkloadType.IoBound,
            ParallelWorkloadType.NetworkBound,
        };

        // Act & Assert
        foreach (var workloadType in workloadTypes)
        {
            var options = ParallelOptionsPresets.GetForWorkloadType(workloadType);

            options.OutputBufferCapacity.Should().NotBeNull(
                $"Workload type {workloadType} should have output buffer capacity configured");
        }
    }

    [Fact]
    public void ParallelOptionsPresets_CpuBoundDop_ShouldNotExceedProcessorCount()
    {
        // Arrange & Act
        var options = ParallelOptionsPresets.GetForWorkloadType(ParallelWorkloadType.CpuBound);

        // Assert
        options.MaxDegreeOfParallelism.Should().BeLessThanOrEqualTo(Environment.ProcessorCount);
    }

    [Fact]
    public void ParallelOptionsPresets_NetworkBoundDop_ShouldHaveCap()
    {
        // Arrange & Act
        var options = ParallelOptionsPresets.GetForWorkloadType(ParallelWorkloadType.NetworkBound);

        // Assert
        options.MaxDegreeOfParallelism.Should().BeLessThanOrEqualTo(100);
    }

    #endregion
}
