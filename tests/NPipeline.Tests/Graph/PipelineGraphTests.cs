using System.Collections.Immutable;
using AwesomeAssertions;
using NPipeline.Configuration;

namespace NPipeline.Tests.Graph;

/// <summary>
///     Tests for PipelineGraph configuration records.
/// </summary>
public class PipelineGraphTests
{
    #region ErrorHandlingConfiguration Tests

    [Fact]
    public void ErrorHandlingConfiguration_Default_ReturnsNewInstance()
    {
        var result = ErrorHandlingConfiguration.Default;

        _ = result.Should().NotBeNull();
        _ = result.PipelineErrorHandler.Should().BeNull();
        _ = result.DeadLetterSink.Should().BeNull();
        _ = result.PipelineErrorHandlerType.Should().BeNull();
        _ = result.DeadLetterSinkType.Should().BeNull();
        _ = result.RetryOptions.Should().BeNull();
        _ = result.NodeRetryOverrides.Should().BeNull();
        _ = result.CircuitBreakerOptions.Should().BeNull();
    }

    [Fact]
    public void ErrorHandlingConfiguration_WithValues_StoresProperties()
    {
        PipelineRetryOptions retryOptions = new(1, MaxNodeRestartAttempts: 3, MaxSequentialNodeAttempts: 5);

        ErrorHandlingConfiguration config = new()
        {
            RetryOptions = retryOptions,
        };

        _ = config.RetryOptions.Should().NotBeNull();
        _ = config.RetryOptions?.MaxItemRetries.Should().Be(1);
    }

    [Fact]
    public void ErrorHandlingConfiguration_Default_MultipleCalls_ReturnEqual()
    {
        var default1 = ErrorHandlingConfiguration.Default;
        var default2 = ErrorHandlingConfiguration.Default;

        _ = default1.Should().Be(default2);
    }

    [Fact]
    public void ErrorHandlingConfiguration_IsRecord_SupportsEquality()
    {
        ErrorHandlingConfiguration config1 = new();
        ErrorHandlingConfiguration config2 = new();

        _ = config1.Should().Be(config2);
    }

    [Fact]
    public void ErrorHandlingConfiguration_CanBeModified()
    {
        var original = ErrorHandlingConfiguration.Default;

        var modified = original with
        {
            PipelineErrorHandlerType = typeof(object),
        };

        _ = original.PipelineErrorHandlerType.Should().BeNull();
        _ = modified.PipelineErrorHandlerType.Should().NotBeNull();
    }

    #endregion

    #region LineageConfiguration Tests

    [Fact]
    public void LineageConfiguration_Default_ReturnsNewInstance()
    {
        var result = LineageConfiguration.Default;

        _ = result.Should().NotBeNull();
        _ = result.ItemLevelLineageEnabled.Should().BeFalse();
        _ = result.LineageSink.Should().BeNull();
        _ = result.LineageSinkType.Should().BeNull();
        _ = result.PipelineLineageSink.Should().BeNull();
        _ = result.PipelineLineageSinkType.Should().BeNull();
        _ = result.LineageOptions.Should().BeNull();
    }

    [Fact]
    public void LineageConfiguration_WithItemLevelLineageEnabled_StoresValue()
    {
        LineageConfiguration config = new()
        {
            ItemLevelLineageEnabled = true,
        };

        _ = config.ItemLevelLineageEnabled.Should().BeTrue();
    }

    [Fact]
    public void LineageConfiguration_Default_MultipleCalls_ReturnEqual()
    {
        var default1 = LineageConfiguration.Default;
        var default2 = LineageConfiguration.Default;

        _ = default1.Should().Be(default2);
    }

    [Fact]
    public void LineageConfiguration_CanBeModified()
    {
        var original = LineageConfiguration.Default;

        var modified = original with
        {
            ItemLevelLineageEnabled = true,
        };

        _ = original.ItemLevelLineageEnabled.Should().BeFalse();
        _ = modified.ItemLevelLineageEnabled.Should().BeTrue();
    }

    #endregion

    #region ExecutionOptionsConfiguration Tests

    [Fact]
    public void ExecutionOptionsConfiguration_Default_ReturnsNewInstance()
    {
        var result = ExecutionOptionsConfiguration.Default;

        _ = result.Should().NotBeNull();
        _ = result.NodeExecutionAnnotations.Should().BeNull();
        _ = result.Visualizer.Should().BeNull();
    }

    [Fact]
    public void ExecutionOptionsConfiguration_WithAnnotations_StoresProperties()
    {
        var annotations = ImmutableDictionary<string, object>.Empty
            .Add("key1", "value1");

        ExecutionOptionsConfiguration config = new()
        {
            NodeExecutionAnnotations = annotations,
        };

        _ = config.NodeExecutionAnnotations.Should().NotBeNull();
        _ = config.NodeExecutionAnnotations?.Count.Should().Be(1);
    }

    [Fact]
    public void ExecutionOptionsConfiguration_Default_MultipleCalls_ReturnEqual()
    {
        var default1 = ExecutionOptionsConfiguration.Default;
        var default2 = ExecutionOptionsConfiguration.Default;

        _ = default1.Should().Be(default2);
    }

    [Fact]
    public void ExecutionOptionsConfiguration_CanBeModified()
    {
        var original = ExecutionOptionsConfiguration.Default;
        var newAnnotations = ImmutableDictionary<string, object>.Empty;

        var modified = original with
        {
            NodeExecutionAnnotations = newAnnotations,
        };

        _ = original.NodeExecutionAnnotations.Should().BeNull();
        _ = modified.NodeExecutionAnnotations.Should().NotBeNull();
    }

    #endregion

    #region Configuration Record Integration Tests

    [Fact]
    public void ConfigurationRecords_AreRecords_SupportEquality()
    {
        ErrorHandlingConfiguration config1 = new();
        ErrorHandlingConfiguration config2 = new();
        var config3 = config1 with { };

        _ = config1.Should().Be(config2);
        _ = config1.Should().Be(config3);
    }

    [Fact]
    public void ErrorHandlingConfiguration_WithCircuitBreakerOptions_Stores()
    {
        PipelineCircuitBreakerOptions cbOptions = new(1, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), false);

        ErrorHandlingConfiguration config = new()
        {
            CircuitBreakerOptions = cbOptions,
        };

        _ = config.CircuitBreakerOptions.Should().Be(cbOptions);
    }

    [Fact]
    public void ErrorHandlingConfiguration_WithCircuitBreakerMemoryOptions_Stores()
    {
        var memoryOptions = CircuitBreakerMemoryManagementOptions.Disabled;

        ErrorHandlingConfiguration config = new()
        {
            CircuitBreakerMemoryOptions = memoryOptions,
        };

        _ = config.CircuitBreakerMemoryOptions.Should().Be(memoryOptions);
    }

    [Fact]
    public void LineageConfiguration_WithLineageOptions_Stores()
    {
        LineageConfiguration config = new()
        {
            ItemLevelLineageEnabled = true,
        };

        _ = config.ItemLevelLineageEnabled.Should().BeTrue();
    }

    [Fact]
    public void ErrorHandlingConfiguration_AllPropertiesCanBeSet()
    {
        PipelineRetryOptions retryOpts = new(1, MaxNodeRestartAttempts: 2, MaxSequentialNodeAttempts: 3);
        PipelineCircuitBreakerOptions cbOpts = new(1, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), false);
        var memoryOptions = CircuitBreakerMemoryManagementOptions.Default;

        ErrorHandlingConfiguration config = new()
        {
            RetryOptions = retryOpts,
            CircuitBreakerOptions = cbOpts,
            CircuitBreakerMemoryOptions = memoryOptions,
            PipelineErrorHandlerType = typeof(object),
            DeadLetterSinkType = typeof(object),
        };

        _ = config.RetryOptions.Should().Be(retryOpts);
        _ = config.CircuitBreakerOptions.Should().Be(cbOpts);
        _ = config.CircuitBreakerMemoryOptions.Should().Be(memoryOptions);
        _ = config.PipelineErrorHandlerType.Should().NotBeNull();
        _ = config.DeadLetterSinkType.Should().NotBeNull();
    }

    #endregion
}
