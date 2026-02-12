using AwesomeAssertions;
using Microsoft.Extensions.Logging;

namespace NPipeline.Extensions.Testing.Tests;

public class CapturingLoggerTests
{
    [Fact]
    public void Constructor_ShouldCreateEmptyLogEntries()
    {
        // Arrange & Act
        var logger = new CapturingLogger();

        // Assert
        logger.LogEntries.Should().BeEmpty();
    }

    [Fact]
    public void Log_WithMessageAndArgs_ShouldCaptureLogEntry()
    {
        // Arrange
        var logger = new CapturingLogger();
        var logLevel = LogLevel.Information;
        var message = "Test message {param}";
        var args = new object[] { "value" };

        // Act
        logger.Log(logLevel, message, args);

        // Assert
        logger.LogEntries.Should().HaveCount(1);
        var entry = logger.LogEntries[0];
        entry.LogLevel.Should().Be(logLevel);
        entry.Message.Should().Be(message);
        entry.Args.Should().BeEquivalentTo(args);
        entry.Exception.Should().BeNull();
    }

    [Fact]
    public void Log_WithMessageExceptionAndArgs_ShouldCaptureLogEntryWithException()
    {
        // Arrange
        var logger = new CapturingLogger();
        var logLevel = LogLevel.Error;
        var message = "Error message {param}";
        var args = new object[] { "value" };
        var exception = new InvalidOperationException("Test exception");

        // Act
        logger.Log(logLevel, exception, message, args);

        // Assert
        logger.LogEntries.Should().HaveCount(1);
        var entry = logger.LogEntries[0];
        entry.LogLevel.Should().Be(logLevel);
        entry.Message.Should().Be(message);
        entry.Args.Should().BeEquivalentTo(args);
        entry.Exception.Should().Be(exception);
    }

    [Fact]
    public void Log_WithNullException_ShouldCaptureLogEntryWithNullException()
    {
        // Arrange
        var logger = new CapturingLogger();
        var logLevel = LogLevel.Warning;
        var message = "Warning message";

        // Act
        logger.Log(logLevel, null!, message);

        // Assert
        logger.LogEntries.Should().HaveCount(1);
        var entry = logger.LogEntries[0];
        entry.LogLevel.Should().Be(logLevel);
        entry.Message.Should().Be(message);
        entry.Args.Should().BeNull();
        entry.Exception.Should().BeNull();
    }

    [Fact]
    public void Log_WithNullArgs_ShouldCaptureLogEntryWithNullArgs()
    {
        // Arrange
        var logger = new CapturingLogger();
        var logLevel = LogLevel.Debug;
        var message = "Debug message";
        object[]? args = null;

        // Act
        logger.Log(logLevel, message, args!);

        // Assert
        logger.LogEntries.Should().HaveCount(1);
        var entry = logger.LogEntries[0];
        entry.LogLevel.Should().Be(logLevel);
        entry.Message.Should().Be(message);
        entry.Args.Should().BeNull();
        entry.Exception.Should().BeNull();
    }

    [Fact]
    public void Log_WithEmptyArgs_ShouldCaptureLogEntryWithNullArgs()
    {
        // Arrange
        var logger = new CapturingLogger();
        var logLevel = LogLevel.Trace;
        var message = "Trace message";
        var args = Array.Empty<object>();

        // Act
        logger.Log(logLevel, message, args);

        // Assert
        logger.LogEntries.Should().HaveCount(1);
        var entry = logger.LogEntries[0];
        entry.LogLevel.Should().Be(logLevel);
        entry.Message.Should().Be(message);
        entry.Args.Should().BeNull();
        entry.Exception.Should().BeNull();
    }

    [Fact]
    public void IsEnabled_ShouldAlwaysReturnTrue()
    {
        // Arrange
        var logger = new CapturingLogger();

        // Act & Assert
        logger.IsEnabled(LogLevel.Trace).Should().BeTrue();
        logger.IsEnabled(LogLevel.Debug).Should().BeTrue();
        logger.IsEnabled(LogLevel.Information).Should().BeTrue();
        logger.IsEnabled(LogLevel.Warning).Should().BeTrue();
        logger.IsEnabled(LogLevel.Error).Should().BeTrue();
        logger.IsEnabled(LogLevel.Critical).Should().BeTrue();
    }

    [Fact]
    public void Log_WithNullMessage_ShouldCaptureLogEntryWithNullPlaceholder()
    {
        // Arrange
        var logger = new CapturingLogger();
        var logLevel = LogLevel.Information;
        string? message = null;

        // Act
        logger.Log(logLevel, message!);

        // Assert
        // Note: ILogger extension methods convert null messages to "[null]"
        logger.LogEntries.Should().HaveCount(1);
        var entry = logger.LogEntries[0];
        entry.LogLevel.Should().Be(logLevel);
        entry.Message.Should().Be("[null]");
        entry.Args.Should().BeNull();
        entry.Exception.Should().BeNull();
    }

    [Fact]
    public void Log_WithEmptyMessage_ShouldCaptureLogEntryWithEmptyMessage()
    {
        // Arrange
        var logger = new CapturingLogger();
        var logLevel = LogLevel.Information;
        var message = string.Empty;

        // Act
        logger.Log(logLevel, message);

        // Assert
        logger.LogEntries.Should().HaveCount(1);
        var entry = logger.LogEntries[0];
        entry.LogLevel.Should().Be(logLevel);
        entry.Message.Should().BeEmpty();
        entry.Args.Should().BeNull();
        entry.Exception.Should().BeNull();
    }

    [Fact]
    public void Log_WithWhitespaceMessage_ShouldCaptureLogEntryWithWhitespaceMessage()
    {
        // Arrange
        var logger = new CapturingLogger();
        var logLevel = LogLevel.Information;
        var message = "   ";

        // Act
        logger.Log(logLevel, message);

        // Assert
        logger.LogEntries.Should().HaveCount(1);
        var entry = logger.LogEntries[0];
        entry.LogLevel.Should().Be(logLevel);
        entry.Message.Should().Be("   ");
        entry.Args.Should().BeNull();
        entry.Exception.Should().BeNull();
    }

    [Fact]
    public void Log_MultipleEntries_ShouldCaptureAllEntries()
    {
        // Arrange
        var logger = new CapturingLogger();

        // Act
        logger.Log(LogLevel.Information, "Info message {param}", "value1");
        logger.Log(LogLevel.Warning, "Warning message {param}", "value2");
        logger.Log(LogLevel.Error, new InvalidOperationException("Test error"), "Error message {param}", "value3");

        // Assert
        logger.LogEntries.Should().HaveCount(3);

        logger.LogEntries[0].LogLevel.Should().Be(LogLevel.Information);
        logger.LogEntries[0].Message.Should().Be("Info message {param}");
        logger.LogEntries[0].Args.Should().BeEquivalentTo(new object[] { "value1" });
        logger.LogEntries[0].Exception.Should().BeNull();

        logger.LogEntries[1].LogLevel.Should().Be(LogLevel.Warning);
        logger.LogEntries[1].Message.Should().Be("Warning message {param}");
        logger.LogEntries[1].Args.Should().BeEquivalentTo(new object[] { "value2" });
        logger.LogEntries[1].Exception.Should().BeNull();

        logger.LogEntries[2].LogLevel.Should().Be(LogLevel.Error);
        logger.LogEntries[2].Message.Should().Be("Error message {param}");
        logger.LogEntries[2].Args.Should().BeEquivalentTo(new object[] { "value3" });
        logger.LogEntries[2].Exception.Should().NotBeNull();
        logger.LogEntries[2].Exception!.Message.Should().Be("Test error");
    }

    [Fact]
    public void Log_WithAllLogLevels_ShouldCaptureAllEntries()
    {
        // Arrange
        var logger = new CapturingLogger();

        // Act
        logger.Log(LogLevel.Trace, "Trace message");
        logger.Log(LogLevel.Debug, "Debug message");
        logger.Log(LogLevel.Information, "Information message");
        logger.Log(LogLevel.Warning, "Warning message");
        logger.Log(LogLevel.Error, "Error message");
        logger.Log(LogLevel.Critical, "Critical message");

        // Assert
        logger.LogEntries.Should().HaveCount(6);
        logger.LogEntries[0].LogLevel.Should().Be(LogLevel.Trace);
        logger.LogEntries[1].LogLevel.Should().Be(LogLevel.Debug);
        logger.LogEntries[2].LogLevel.Should().Be(LogLevel.Information);
        logger.LogEntries[3].LogLevel.Should().Be(LogLevel.Warning);
        logger.LogEntries[4].LogLevel.Should().Be(LogLevel.Error);
        logger.LogEntries[5].LogLevel.Should().Be(LogLevel.Critical);
    }

    [Fact]
    public void Log_WithComplexArgs_ShouldCaptureComplexArgs()
    {
        // Arrange
        var logger = new CapturingLogger();
        var logLevel = LogLevel.Information;
        var message = "Complex message {object} {array} {list}";
        var complexObject = new { Name = "Test", Value = 42 };
        var array = new[] { 1, 2, 3 };
        var list = new List<string> { "a", "b", "c" };
        var args = new object[] { complexObject, array, list };

        // Act
        logger.Log(logLevel, message, args);

        // Assert
        logger.LogEntries.Should().HaveCount(1);
        var entry = logger.LogEntries[0];
        entry.LogLevel.Should().Be(logLevel);
        entry.Message.Should().Be(message);
        entry.Args.Should().BeEquivalentTo(args);
        entry.Exception.Should().BeNull();
    }

    [Fact]
    public void Log_WithSameLogLevel_ShouldMaintainOrder()
    {
        // Arrange
        var logger = new CapturingLogger();

        // Act
        logger.Log(LogLevel.Information, "First message");
        logger.Log(LogLevel.Information, "Second message");
        logger.Log(LogLevel.Information, "Third message");

        // Assert
        logger.LogEntries.Should().HaveCount(3);
        logger.LogEntries[0].Message.Should().Be("First message");
        logger.LogEntries[1].Message.Should().Be("Second message");
        logger.LogEntries[2].Message.Should().Be("Third message");
    }

    [Fact]
    public void Log_WithDifferentLogLevels_ShouldMaintainOrder()
    {
        // Arrange
        var logger = new CapturingLogger();

        // Act
        logger.Log(LogLevel.Information, "Info message");
        logger.Log(LogLevel.Error, "Error message");
        logger.Log(LogLevel.Warning, "Warning message");
        logger.Log(LogLevel.Debug, "Debug message");

        // Assert
        logger.LogEntries.Should().HaveCount(4);
        logger.LogEntries[0].LogLevel.Should().Be(LogLevel.Information);
        logger.LogEntries[1].LogLevel.Should().Be(LogLevel.Error);
        logger.LogEntries[2].LogLevel.Should().Be(LogLevel.Warning);
        logger.LogEntries[3].LogLevel.Should().Be(LogLevel.Debug);
    }

    [Fact]
    public void Log_WithNullMessageAndException_ShouldCaptureNullMessageAsPlaceholder()
    {
        // Arrange
        var logger = new CapturingLogger();
        var logLevel = LogLevel.Information;
        string? message = null;
        Exception? exception = null;

        // Act
        logger.Log(logLevel, exception!, message!);

        // Assert
        // Note: ILogger extension methods convert null messages to "[null]"
        logger.LogEntries.Should().HaveCount(1);
        var entry = logger.LogEntries[0];
        entry.LogLevel.Should().Be(logLevel);
        entry.Message.Should().Be("[null]");
        entry.Args.Should().BeNull();
        entry.Exception.Should().BeNull();
    }
}
