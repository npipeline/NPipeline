using AwesomeAssertions;
using Microsoft.Extensions.Logging;
using NPipeline.Configuration;
using NPipeline.Execution;
using NPipeline.Observability.Logging;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;

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
        var context = PipelineContext.Default;
        var nodeId = "testNode";

        // Act
        var cached = CachedNodeExecutionContext.Create(context, nodeId);

        // Assert
        _ = cached.NodeId.Should().Be(nodeId);
        _ = cached.RetryOptions.Should().Be(context.RetryOptions);
        _ = cached.TracingEnabled.Should().BeFalse(); // Default uses NullPipelineTracer
        _ = cached.LoggingEnabled.Should().BeFalse(); // Default uses NullLoggerFactory
        _ = cached.CancellationToken.Should().Be(context.CancellationToken);
    }

    [Fact]
    public void Create_WithNodeSpecificRetryOptions_ShouldUseNodeOptions()
    {
        // Arrange
        var context = PipelineContext.Default;
        var nodeId = "testNode";

        var nodeRetryOptions = new PipelineRetryOptions(
            5,
            MaxNodeRestartAttempts: 3,
            MaxSequentialNodeAttempts: 5);

        context.Items[PipelineContextKeys.NodeRetryOptions(nodeId)] = nodeRetryOptions;

        // Act
        var cached = CachedNodeExecutionContext.Create(context, nodeId);

        // Assert
        _ = cached.RetryOptions.Should().BeSameAs(nodeRetryOptions);
        _ = cached.RetryOptions.MaxItemRetries.Should().Be(5);
        _ = cached.RetryOptions.MaxNodeRestartAttempts.Should().Be(3);
    }

    [Fact]
    public void Create_WithGlobalRetryOptions_ShouldUseGlobalOptions()
    {
        // Arrange
        var context = PipelineContext.Default;
        var nodeId = "testNode";

        var globalRetryOptions = new PipelineRetryOptions(
            10,
            MaxNodeRestartAttempts: 2,
            MaxSequentialNodeAttempts: 0);

        context.Items[PipelineContextKeys.GlobalRetryOptions] = globalRetryOptions;

        // Act
        var cached = CachedNodeExecutionContext.Create(context, nodeId);

        // Assert
        _ = cached.RetryOptions.Should().BeSameAs(globalRetryOptions);
        _ = cached.RetryOptions.MaxItemRetries.Should().Be(10);
    }

    [Fact]
    public void Create_WithBothNodeAndGlobalOptions_ShouldPreferNodeOptions()
    {
        // Arrange
        var context = PipelineContext.Default;
        var nodeId = "testNode";
        var nodeRetryOptions = new PipelineRetryOptions(5, MaxNodeRestartAttempts: 0, MaxSequentialNodeAttempts: 0);
        var globalRetryOptions = new PipelineRetryOptions(10, MaxNodeRestartAttempts: 0, MaxSequentialNodeAttempts: 0);

        context.Items[PipelineContextKeys.NodeRetryOptions(nodeId)] = nodeRetryOptions;
        context.Items[PipelineContextKeys.GlobalRetryOptions] = globalRetryOptions;

        // Act
        var cached = CachedNodeExecutionContext.Create(context, nodeId);

        // Assert
        cached.RetryOptions.Should().BeSameAs(nodeRetryOptions);
        cached.RetryOptions.MaxItemRetries.Should().Be(5);
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
        var context = PipelineContext.Default;
        string nodeId = null!;

        // Act
        var act = () => CachedNodeExecutionContext.Create(context, nodeId);

        // Assert
        _ = act.Should().Throw<ArgumentNullException>()
            .WithParameterName("nodeId");
    }

    [Fact]
    public void CreateWithRetryOptions_ShouldUseProvidedRetryOptions()
    {
        // Arrange
        var context = PipelineContext.Default;
        var nodeId = "testNode";

        var preResolvedRetryOptions = new PipelineRetryOptions(
            15,
            MaxNodeRestartAttempts: 0,
            MaxSequentialNodeAttempts: 0);

        // Act
        var cached = CachedNodeExecutionContext.CreateWithRetryOptions(context, nodeId, preResolvedRetryOptions);

        // Assert
        _ = cached.RetryOptions.Should().BeSameAs(preResolvedRetryOptions);
        _ = cached.RetryOptions.MaxItemRetries.Should().Be(15);
    }

    [Fact]
    public void CreateWithRetryOptions_WithNullRetryOptions_ShouldThrowArgumentNullException()
    {
        // Arrange
        var context = PipelineContext.Default;
        var nodeId = "testNode";
        PipelineRetryOptions preResolvedRetryOptions = null!;

        // Act
        var act = () => CachedNodeExecutionContext.CreateWithRetryOptions(context, nodeId, preResolvedRetryOptions);

        // Assert
        _ = act.Should().Throw<ArgumentNullException>()
            .WithParameterName("preResolvedRetryOptions");
    }

    // Test helper classes
    private sealed class TestPipelineTracer : IPipelineTracer
    {
        public IPipelineActivity? CurrentActivity => null;

        public IPipelineActivity StartActivity(string name)
        {
            return NullPipelineActivity.Instance;
        }
    }

    private sealed class TestPipelineLoggerFactory : ILoggerFactory
    {
        public ILogger CreateLogger(string categoryName)
        {
            return NullLogger.Instance;
        }

        public void AddProvider(ILoggerProvider provider) { }

        public void Dispose() { }
    }
}
