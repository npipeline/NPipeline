using NPipeline.Observability.Configuration;

namespace NPipeline.Observability.Tests;

/// <summary>
///     Tests for the <see cref="ObservabilityOptions" /> record.
/// </summary>
public sealed class ObservabilityOptionsTests
{
    [Fact]
    public void Default_HasExpectedSettings()
    {
        // Act
        var options = ObservabilityOptions.Default;

        // Assert
        Assert.True(options.RecordTiming);
        Assert.True(options.RecordItemCounts);
        Assert.False(options.RecordMemoryUsage);
        Assert.True(options.RecordThreadInfo);
        Assert.True(options.RecordPerformanceMetrics);
    }

    [Fact]
    public void Full_HasAllSettingsEnabled()
    {
        // Act
        var options = ObservabilityOptions.Full;

        // Assert
        Assert.True(options.RecordTiming);
        Assert.True(options.RecordItemCounts);
        Assert.True(options.RecordMemoryUsage);
        Assert.True(options.RecordThreadInfo);
        Assert.True(options.RecordPerformanceMetrics);
    }

    [Fact]
    public void Minimal_HasOnlyTimingEnabled()
    {
        // Act
        var options = ObservabilityOptions.Minimal;

        // Assert
        Assert.True(options.RecordTiming);
        Assert.False(options.RecordItemCounts);
        Assert.False(options.RecordMemoryUsage);
        Assert.False(options.RecordThreadInfo);
        Assert.False(options.RecordPerformanceMetrics);
    }

    [Fact]
    public void Disabled_HasAllSettingsDisabled()
    {
        // Act
        var options = ObservabilityOptions.Disabled;

        // Assert
        Assert.False(options.RecordTiming);
        Assert.False(options.RecordItemCounts);
        Assert.False(options.RecordMemoryUsage);
        Assert.False(options.RecordThreadInfo);
        Assert.False(options.RecordPerformanceMetrics);
    }

    [Fact]
    public void CustomOptions_CanBeCreatedWithWith()
    {
        // Act
        var options = ObservabilityOptions.Default with
        {
            RecordMemoryUsage = true,
            RecordPerformanceMetrics = false,
        };

        // Assert
        Assert.True(options.RecordTiming);
        Assert.True(options.RecordItemCounts);
        Assert.True(options.RecordMemoryUsage);
        Assert.True(options.RecordThreadInfo);
        Assert.False(options.RecordPerformanceMetrics);
    }
}
