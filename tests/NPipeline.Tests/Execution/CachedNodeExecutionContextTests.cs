using AwesomeAssertions;
using Microsoft.Extensions.Logging;
using NPipeline.Configuration;
using NPipeline.Execution;
using NPipeline.Observability.Logging;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Tests.Execution;

/// <summary>
///     Tests for <see cref="CachedNodeExecutionContext" /> performance optimization.
/// </summary>
public sealed class CachedNodeExecutionContextTests
{
    [Fact]
    public void Create_WithBasicContext_ShouldCaptureExpectedValues()
    {
        // Arrange
        var context = PipelineContext.CreateDefault();
        var nodeId = "testNode";

        // Act
        var cached = CachedNodeExecutionContext.Create(context, nodeId);

        // Assert
        _ = cached.NodeId.Should().Be(nodeId);
        _ = cached.Resilience.Should().BeSameAs(context.ExecutionConfiguration.Resilience);
        _ = cached.TracingEnabled.Should().BeFalse(); // Default uses NullPipelineTracer
        _ = cached.LoggingEnabled.Should().BeFalse(); // Default uses NullLoggerFactory
        _ = cached.CancellationToken.Should().Be(context.CancellationToken);
    }

    [Fact]
    public void Create_WithNodeSpecificResilienceOptions_ShouldUseNodeOptions()
    {
        // Arrange
        var context = PipelineContext.CreateDefault();
        var nodeId = "testNode";
        var nodeOptions = PipelineResilienceOptions.None with { ItemRetry = new ItemRetryOptions { MaxRetries = 5 } };
        context.ExecutionConfiguration.SetNodeResilienceOptions(nodeId, nodeOptions);

        // Act
        var cached = CachedNodeExecutionContext.Create(context, nodeId);

        // Assert
        _ = cached.Resilience.Should().BeSameAs(nodeOptions);
        _ = cached.Resilience.ItemRetry.MaxRetries.Should().Be(5);
    }

    [Fact]
    public void Create_WithPipelineResilienceOptions_ShouldUsePipelineOptions()
    {
        // Arrange
        var context = PipelineContext.CreateDefault();
        var pipelineOptions = PipelineResilienceOptions.None with { ItemRetry = new ItemRetryOptions { MaxRetries = 10 } };
        context.ExecutionConfiguration.Resilience = pipelineOptions;
        context.ExecutionConfiguration.SetNodeResilienceOptions("otherNode", PipelineResilienceOptions.None);

        // Act
        var cached = CachedNodeExecutionContext.Create(context, "testNode");

        // Assert
        _ = cached.Resilience.Should().BeSameAs(pipelineOptions);
        _ = cached.Resilience.ItemRetry.MaxRetries.Should().Be(10);
    }

    [Fact]
    public void Create_WithTracingEnabled_ShouldDetectTracingEnabled()
    {
        // Arrange
        var config = new PipelineContextConfiguration
        {
            Tracer = new TestPipelineTracer(), // Non-null tracer
        };

        var context = new PipelineContext(config);
        var nodeId = "testNode";

        // Act
        var cached = CachedNodeExecutionContext.Create(context, nodeId);

        // Assert
        _ = cached.TracingEnabled.Should().BeTrue();
    }

    [Fact]
    public void Create_WithLoggingEnabled_ShouldDetectLoggingEnabled()
    {
        // Arrange
        var config = new PipelineContextConfiguration
        {
            LoggerFactory = new TestPipelineLoggerFactory(), // Non-null logger factory
        };

        var context = new PipelineContext(config);
        var nodeId = "testNode";

        // Act
        var cached = CachedNodeExecutionContext.Create(context, nodeId);

        // Assert
        _ = cached.LoggingEnabled.Should().BeTrue();
    }

    [Fact]
    public void Create_WithCancellationToken_ShouldCaptureCancellationToken()
    {
        // Arrange
        var cts = new CancellationTokenSource();

        var config = new PipelineContextConfiguration
        {
            CancellationToken = cts.Token,
        };

        var context = new PipelineContext(config);
        var nodeId = "testNode";

        // Act
        var cached = CachedNodeExecutionContext.Create(context, nodeId);

        // Assert
        _ = cached.CancellationToken.Should().Be(cts.Token);
    }

    [Fact]
    public void Create_WithNullContext_ShouldThrowArgumentNullException()
    {
        // Arrange
        PipelineContext context = null!;
        var nodeId = "testNode";

        // Act
        var act = () => CachedNodeExecutionContext.Create(context, nodeId);

        // Assert
        _ = act.Should().Throw<ArgumentNullException>()
            .WithParameterName("context");
    }

    [Fact]
    public void Create_WithNullNodeId_ShouldThrowArgumentNullException()
    {
        // Arrange
        var context = PipelineContext.CreateDefault();
        string nodeId = null!;

        // Act
        var act = () => CachedNodeExecutionContext.Create(context, nodeId);

        // Assert
        _ = act.Should().Throw<ArgumentNullException>()
            .WithParameterName("nodeId");
    }

    // Test helper classes
    private sealed class TestPipelineTracer : IPipelineTracer
    {
        public IPipelineActivity? CurrentActivity => null;

        public IPipelineActivity StartActivity(string name) => NullPipelineActivity.Instance;
    }

    private sealed class TestPipelineLoggerFactory : ILoggerFactory
    {
        public ILogger CreateLogger(string categoryName) => NullLogger.Instance;

        public void AddProvider(ILoggerProvider provider)
        {
        }

        public void Dispose()
        {
        }
    }
}
