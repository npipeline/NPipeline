using System.Text.Json;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using NPipeline.Lineage;

namespace NPipeline.Extensions.Lineage.Tests;

public class LoggingPipelineLineageSinkTests
{
    [Fact]
    public async Task RecordAsync_WithValidReport_ShouldLogReport()
    {
        // Arrange
        var logger = new TestLogger();
        var sink = new LoggingPipelineLineageSink(logger);
        var report = CreateTestReport();

        // Act
        await sink.RecordAsync(report, CancellationToken.None);

        // Assert
        logger.LogEntries.Should().HaveCountGreaterThan(0);
        var logEntry = logger.LogEntries.FirstOrDefault();
        logEntry.Should().NotBeNull();
        logEntry!.LogLevel.Should().Be(LogLevel.Information);
        logEntry.Message.Should().Contain(report.Pipeline);
        logEntry.Message.Should().Contain(report.RunId.ToString());
    }

    [Fact]
    public async Task RecordAsync_WithNullReport_ShouldNotThrow()
    {
        // Arrange
        var logger = new TestLogger();
        var sink = new LoggingPipelineLineageSink(logger);

        // Act & Assert
        var exception = await Record.ExceptionAsync(() => sink.RecordAsync(null!, CancellationToken.None));
        exception.Should().BeNull();
    }

    [Fact]
    public async Task RecordAsync_WithNullLogger_ShouldUseNullLogger()
    {
        // Arrange
        var sink = new LoggingPipelineLineageSink();
        var report = CreateTestReport();

        // Act & Assert
        var exception = await Record.ExceptionAsync(() => sink.RecordAsync(report, CancellationToken.None));
        exception.Should().BeNull();
    }

    [Fact]
    public async Task RecordAsync_WithCustomJsonOptions_ShouldUseCustomOptions()
    {
        // Arrange
        var logger = new TestLogger();

        var jsonOptions = new JsonSerializerOptions
        {
            WriteIndented = false,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        };

        var sink = new LoggingPipelineLineageSink(logger, jsonOptions);
        var report = CreateTestReport();

        // Act
        await sink.RecordAsync(report, CancellationToken.None);

        // Assert
        logger.LogEntries.Should().HaveCountGreaterThan(0);
        var logEntry = logger.LogEntries.FirstOrDefault();
        logEntry.Should().NotBeNull();
        logEntry!.Message.Should().Contain(report.Pipeline);
    }

    [Fact]
    public async Task RecordAsync_WithComplexReport_ShouldSerializeCorrectly()
    {
        // Arrange
        var logger = new TestLogger();
        var sink = new LoggingPipelineLineageSink(logger);
        var report = CreateComplexReport();

        // Act
        await sink.RecordAsync(report, CancellationToken.None);

        // Assert
        logger.LogEntries.Should().HaveCountGreaterThan(0);
        var logEntry = logger.LogEntries.FirstOrDefault();
        logEntry.Should().NotBeNull();
        logEntry!.Message.Should().Contain(report.Pipeline);
        logEntry.Message.Should().Contain("node1");
        logEntry.Message.Should().Contain("node2");
        logEntry.Message.Should().Contain("source1");
    }

    [Fact]
    public async Task RecordAsync_WithCancellationRequested_ShouldComplete()
    {
        // Arrange
        var logger = new TestLogger();
        var sink = new LoggingPipelineLineageSink(logger);
        var report = CreateTestReport();
        var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act & Assert
        var exception = await Record.ExceptionAsync(() => sink.RecordAsync(report, cts.Token));
        exception.Should().BeNull();
    }

    [Fact]
    public async Task RecordAsync_WithMultipleReports_ShouldLogAllReports()
    {
        // Arrange
        var logger = new TestLogger();
        var sink = new LoggingPipelineLineageSink(logger);
        var report1 = CreateTestReport("Pipeline1");
        var report2 = CreateTestReport("Pipeline2");
        var report3 = CreateTestReport("Pipeline3");

        // Act
        await sink.RecordAsync(report1, CancellationToken.None);
        await sink.RecordAsync(report2, CancellationToken.None);
        await sink.RecordAsync(report3, CancellationToken.None);

        // Assert
        logger.LogEntries.Should().HaveCountGreaterThan(0);
        var logMessages = logger.LogEntries.Select(e => e.Message).ToList();
        logMessages.Should().Contain(m => m.Contains("Pipeline1"));
        logMessages.Should().Contain(m => m.Contains("Pipeline2"));
        logMessages.Should().Contain(m => m.Contains("Pipeline3"));
    }

    [Fact]
    public async Task RecordAsync_WithEmptyNodes_ShouldLogReport()
    {
        // Arrange
        var logger = new TestLogger();
        var sink = new LoggingPipelineLineageSink(logger);

        var report = new PipelineLineageReport(
            "EmptyPipeline",
            Guid.NewGuid(),
            [],
            []
        );

        // Act
        await sink.RecordAsync(report, CancellationToken.None);

        // Assert
        logger.LogEntries.Should().HaveCountGreaterThan(0);
        var logEntry = logger.LogEntries.FirstOrDefault();
        logEntry.Should().NotBeNull();
        logEntry!.Message.Should().Contain("EmptyPipeline");
    }

    [Fact]
    public async Task RecordAsync_WithLargeReport_ShouldLogReport()
    {
        // Arrange
        var logger = new TestLogger();
        var sink = new LoggingPipelineLineageSink(logger);

        var nodes = Enumerable.Range(0, 100)
            .Select(i => new NodeLineageInfo($"node{i}", "Transform", "int", "int"))
            .ToList();

        var edges = Enumerable.Range(0, 99)
            .Select(i => new EdgeLineageInfo($"node{i}", $"node{i + 1}"))
            .ToList();

        var report = new PipelineLineageReport(
            "LargePipeline",
            Guid.NewGuid(),
            nodes,
            edges
        );

        // Act
        await sink.RecordAsync(report, CancellationToken.None);

        // Assert
        logger.LogEntries.Should().HaveCountGreaterThan(0);
        var logEntry = logger.LogEntries.FirstOrDefault();
        logEntry.Should().NotBeNull();
        logEntry!.Message.Should().Contain("LargePipeline");
    }

    [Fact]
    public async Task RecordAsync_WithSpecialCharactersInPipelineName_ShouldLogReport()
    {
        // Arrange
        var logger = new TestLogger();
        var sink = new LoggingPipelineLineageSink(logger);

        var report = new PipelineLineageReport(
            "Pipeline with <special> & \"characters\"",
            Guid.NewGuid(),
            [new NodeLineageInfo("node1", "Transform", "int", "int")],
            []
        );

        // Act
        await sink.RecordAsync(report, CancellationToken.None);

        // Assert
        logger.LogEntries.Should().HaveCountGreaterThan(0);
        var logEntry = logger.LogEntries.FirstOrDefault();
        logEntry.Should().NotBeNull();
        logEntry!.Message.Should().Contain("Pipeline with");
    }

    [Fact]
    public async Task RecordAsync_WithNullLogger_ShouldNotThrow()
    {
        // Arrange
        var sink = new LoggingPipelineLineageSink();
        var report = CreateTestReport();

        // Act & Assert
        var exception = await Record.ExceptionAsync(() => sink.RecordAsync(report, CancellationToken.None));
        exception.Should().BeNull();
    }

    [Fact]
    public void Constructor_WithDefaultLogger_ShouldUseNullLogger()
    {
        // Arrange & Act
        var sink = new LoggingPipelineLineageSink();

        // Assert
        sink.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithProvidedLogger_ShouldUseProvidedLogger()
    {
        // Arrange
        var logger = new TestLogger();

        // Act
        var sink = new LoggingPipelineLineageSink(logger);

        // Assert
        sink.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithCustomJsonOptions_ShouldUseCustomOptions()
    {
        // Arrange
        var jsonOptions = new JsonSerializerOptions
        {
            WriteIndented = false,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        };

        // Act
        var sink = new LoggingPipelineLineageSink(null, jsonOptions);

        // Assert
        sink.Should().NotBeNull();
    }

    [Fact]
    public async Task RecordAsync_WithConcurrentCalls_ShouldNotThrow()
    {
        // Arrange
        var logger = new TestLogger();
        var sink = new LoggingPipelineLineageSink(logger);

        var tasks = Enumerable.Range(0, 100)
            .Select(i => sink.RecordAsync(CreateTestReport($"Pipeline{i}"), CancellationToken.None));

        // Act & Assert
        var exception = await Record.ExceptionAsync(async () => await Task.WhenAll(tasks));
        exception.Should().BeNull();
    }

    private static PipelineLineageReport CreateTestReport(string pipelineName = "TestPipeline")
    {
        return new PipelineLineageReport(
            pipelineName,
            Guid.NewGuid(),
            [
                new NodeLineageInfo("node1", "Transform", "int", "int"),
                new NodeLineageInfo("node2", "Transform", "int", "int"),
            ],
            [
                new EdgeLineageInfo("node1", "node2"),
            ]
        );
    }

    private static PipelineLineageReport CreateComplexReport()
    {
        return new PipelineLineageReport(
            "ComplexPipeline",
            Guid.NewGuid(),
            [
                new NodeLineageInfo("source1", "Source", null, "int"),
                new NodeLineageInfo("node1", "Transform", "int", "int"),
                new NodeLineageInfo("node2", "Transform", "int", "int"),
                new NodeLineageInfo("sink1", "Sink", "int", null),
            ],
            [
                new EdgeLineageInfo("source1", "node1"),
                new EdgeLineageInfo("node1", "node2"),
                new EdgeLineageInfo("node2", "sink1"),
            ]
        );
    }

    private sealed class TestLogger : ILogger<LoggingPipelineLineageSink>
    {
        public List<LogEntry> LogEntries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull
        {
            return null;
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            LogEntries.Add(new LogEntry(logLevel, formatter(state, exception), state));
        }
    }

    private sealed record LogEntry(LogLevel LogLevel, string Message, object? State);
}
