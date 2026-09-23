using System.Collections.Immutable;
using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.Reliability;

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
        _ = result.ResiliencePolicy.Should().BeNull();
        _ = result.DeadLetterSink.Should().BeNull();
        _ = result.ResiliencePolicyType.Should().BeNull();
        _ = result.DeadLetterSinkType.Should().BeNull();
        _ = result.Resilience.Should().BeNull();
        _ = result.NodeResilience.Should().BeNull();
    }

    [Fact]
    public void ErrorHandlingConfiguration_WithValues_StoresProperties()
    {
        var resilience = PipelineResilienceOptions.None with { ItemRetry = new ItemRetryOptions { MaxRetries = 1 } };

        ErrorHandlingConfiguration config = new()
        {
            Resilience = resilience,
        };

        _ = config.Resilience.Should().NotBeNull();
        _ = config.Resilience?.ItemRetry.MaxRetries.Should().Be(1);
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
            ResiliencePolicyType = typeof(object),
        };

        _ = original.ResiliencePolicyType.Should().BeNull();
        _ = modified.ResiliencePolicyType.Should().NotBeNull();
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
    public void ErrorHandlingConfiguration_WithNodeResilience_Stores()
    {
        var nodeOptions = PipelineResilienceOptions.None with { CircuitBreaker = new CircuitBreakerOptions { ConsecutiveFailures = 1 } };

        ErrorHandlingConfiguration config = new()
        {
            NodeResilience = ImmutableDictionary<string, PipelineResilienceOptions>.Empty.Add("node", nodeOptions),
        };

        _ = config.NodeResilience!["node"].Should().BeSameAs(nodeOptions);
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
        var resilience = PipelineResilienceOptions.None with { NodeRetry = new NodeRetryOptions { MaxRetries = 2 } };
        var nodeResilience = ImmutableDictionary<string, PipelineResilienceOptions>.Empty.Add("node", resilience);
        ErrorHandlingConfiguration config = new()
        {
            Resilience = resilience,
            NodeResilience = nodeResilience,
            ResiliencePolicyType = typeof(object),
            DeadLetterSinkType = typeof(object),
        };

        _ = config.Resilience.Should().Be(resilience);
        _ = config.NodeResilience.Should().BeSameAs(nodeResilience);
        _ = config.ResiliencePolicyType.Should().NotBeNull();
        _ = config.DeadLetterSinkType.Should().NotBeNull();
    }

    #endregion
}
