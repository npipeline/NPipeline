using AwesomeAssertions;
using NPipeline.Observability.Logging;
using NPipeline.Observability.Tracing;

namespace NPipeline.Tests.Observability;

public sealed class NullImplementationsTests
{
    #region NullPipelineLogger Tests

    [Fact]
    public void NullPipelineLogger_Instance_IsNotNull()
    {
        // Act & Assert
        NullPipelineLogger.Instance.Should().NotBeNull();
    }

    [Fact]
    public void NullPipelineLogger_Instance_ReturnsSameInstance()
    {
        // Act & Assert
        NullPipelineLogger.Instance.Should().BeSameAs(NullPipelineLogger.Instance);
    }

    [Fact]
    public void NullPipelineLogger_Log_WithMessage_DoesNotThrow()
    {
        // Arrange
        var logger = NullPipelineLogger.Instance;

        // Act
        var act = () => logger.Log(LogLevel.Information, "test message");

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void NullPipelineLogger_Log_WithMessageAndArgs_DoesNotThrow()
    {
        // Arrange
        var logger = NullPipelineLogger.Instance;

        // Act
        var act = () => logger.Log(LogLevel.Information, "test message {0}", "arg");

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void NullPipelineLogger_Log_WithException_DoesNotThrow()
    {
        // Arrange
        var logger = NullPipelineLogger.Instance;
        var ex = new InvalidOperationException("test");

        // Act
        var act = () => logger.Log(LogLevel.Error, ex, "error message");

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void NullPipelineLogger_IsEnabled_AlwaysReturnsFalse()
    {
        // Arrange
        var logger = NullPipelineLogger.Instance;

        // Act & Assert
        logger.IsEnabled(LogLevel.Debug).Should().BeFalse();
        logger.IsEnabled(LogLevel.Information).Should().BeFalse();
        logger.IsEnabled(LogLevel.Warning).Should().BeFalse();
        logger.IsEnabled(LogLevel.Error).Should().BeFalse();
    }

    #endregion

    #region NullPipelineLoggerFactory Tests

    [Fact]
    public void NullPipelineLoggerFactory_Instance_IsNotNull()
    {
        // Act & Assert
        NullPipelineLoggerFactory.Instance.Should().NotBeNull();
    }

    [Fact]
    public void NullPipelineLoggerFactory_Instance_ReturnsSameInstance()
    {
        // Act & Assert
        NullPipelineLoggerFactory.Instance.Should().BeSameAs(NullPipelineLoggerFactory.Instance);
    }

    [Fact]
    public void NullPipelineLoggerFactory_CreateLogger_ReturnsNullLogger()
    {
        // Arrange
        var factory = NullPipelineLoggerFactory.Instance;

        // Act
        var logger = factory.CreateLogger("test");

        // Assert
        logger.Should().BeOfType<NullPipelineLogger>();
    }

    [Fact]
    public void NullPipelineLoggerFactory_CreateLogger_WithDifferentNames_ReturnsSameInstance()
    {
        // Arrange
        var factory = NullPipelineLoggerFactory.Instance;

        // Act
        var logger1 = factory.CreateLogger("logger1");
        var logger2 = factory.CreateLogger("logger2");

        // Assert
        logger1.Should().BeSameAs(logger2);
        logger1.Should().BeSameAs(NullPipelineLogger.Instance);
    }

    #endregion

    #region NullPipelineTracer Tests

    [Fact]
    public void NullPipelineTracer_Instance_IsNotNull()
    {
        // Act & Assert
        NullPipelineTracer.Instance.Should().NotBeNull();
    }

    [Fact]
    public void NullPipelineTracer_Instance_ReturnsSameInstance()
    {
        // Act & Assert
        NullPipelineTracer.Instance.Should().BeSameAs(NullPipelineTracer.Instance);
    }

    [Fact]
    public void NullPipelineTracer_StartActivity_ReturnsNullActivity()
    {
        // Arrange
        var tracer = NullPipelineTracer.Instance;

        // Act
        var activity = tracer.StartActivity("test");

        // Assert
        activity.Should().BeOfType<NullPipelineActivity>();
    }

    [Fact]
    public void NullPipelineTracer_StartActivity_WithDifferentNames_ReturnsSameInstance()
    {
        // Arrange
        var tracer = NullPipelineTracer.Instance;

        // Act
        var activity1 = tracer.StartActivity("activity1");
        var activity2 = tracer.StartActivity("activity2");

        // Assert
        activity1.Should().BeSameAs(activity2);
        activity1.Should().BeSameAs(NullPipelineActivity.Instance);
    }

    [Fact]
    public void NullPipelineTracer_CurrentActivity_ReturnsNullActivity()
    {
        // Arrange
        var tracer = NullPipelineTracer.Instance;

        // Act
        var activity = tracer.CurrentActivity;

        // Assert
        activity.Should().BeOfType<NullPipelineActivity>();
        activity.Should().BeSameAs(NullPipelineActivity.Instance);
    }

    #endregion

    #region NullPipelineActivity Tests

    [Fact]
    public void NullPipelineActivity_Instance_IsNotNull()
    {
        // Act & Assert
        NullPipelineActivity.Instance.Should().NotBeNull();
    }

    [Fact]
    public void NullPipelineActivity_Instance_ReturnsSameInstance()
    {
        // Act & Assert
        NullPipelineActivity.Instance.Should().BeSameAs(NullPipelineActivity.Instance);
    }

    [Fact]
    public void NullPipelineActivity_Dispose_DoesNotThrow()
    {
        // Arrange
        var activity = NullPipelineActivity.Instance;

        // Act
        var act = () => activity.Dispose();

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void NullPipelineActivity_Dispose_CanBeCalledMultipleTimes()
    {
        // Arrange
        var activity = NullPipelineActivity.Instance;

        // Act
        var act = () =>
        {
            activity.Dispose();
            activity.Dispose();
        };

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void NullPipelineActivity_IsImplementsIDisposable()
    {
        // Act & Assert
        NullPipelineActivity.Instance.Should().BeAssignableTo<IDisposable>();
    }

    #endregion

    #region Integration Tests

    [Fact]
    public void NullImplementations_CanBeUsedTogether()
    {
        // Arrange
        var loggerFactory = NullPipelineLoggerFactory.Instance;
        var tracer = NullPipelineTracer.Instance;

        // Act
        var logger = loggerFactory.CreateLogger("test");
        var activity = tracer.StartActivity("test-activity");
        logger.Log(LogLevel.Information, "test message");

        // Assert
        logger.IsEnabled(LogLevel.Information).Should().BeFalse();
        activity.Should().BeOfType<NullPipelineActivity>();
    }

    [Fact]
    public void NullImplementations_Disposal_WorksCorrectly()
    {
        // Arrange
        var tracer = NullPipelineTracer.Instance;
        var activity = tracer.StartActivity("test");

        // Act
        activity.Dispose();

        // Assert - should not throw
    }

    #endregion
}
