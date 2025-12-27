// ReSharper disable ClassNeverInstantiated.Local

using AwesomeAssertions;
using NPipeline.ErrorHandling;

namespace NPipeline.Tests.ErrorHandling;

public sealed class ExceptionPropertiesTests
{
    #region CircuitBreakerTrippedException Tests

    [Fact]
    public void CircuitBreakerTrippedException_DefaultConstructor_HasErrorCode()
    {
        // Act
        var exception = new CircuitBreakerTrippedException();

        // Assert
        _ = exception.ErrorCode.Should().Be("CIRCUIT_BREAKER_TRIPPED");
    }

    [Fact]
    public void CircuitBreakerTrippedException_MessageConstructor_PreservesMessage()
    {
        // Arrange
        var message = "Custom message";

        // Act
        var exception = new CircuitBreakerTrippedException(message);

        // Assert
        _ = exception.Message.Should().Be(message);
        _ = exception.ErrorCode.Should().Be("CIRCUIT_BREAKER_TRIPPED");
    }

    [Fact]
    public void CircuitBreakerTrippedException_WithInnerException_PreservesIt()
    {
        // Arrange
        var inner = new InvalidOperationException("inner");

        // Act
        var exception = new CircuitBreakerTrippedException("message", inner);

        // Assert
        _ = exception.InnerException.Should().Be(inner);
    }

    [Fact]
    public void CircuitBreakerTrippedException_ThresholdConstructor_SetsThreshold()
    {
        // Act
        var exception = new CircuitBreakerTrippedException(5);

        // Assert
        _ = exception.FailureThreshold.Should().Be(5);
    }

    [Fact]
    public void CircuitBreakerTrippedException_ThresholdAndNodeIdConstructor_SetsBoth()
    {
        // Act
        var exception = new CircuitBreakerTrippedException(3, "MyNode");

        // Assert
        _ = exception.FailureThreshold.Should().Be(3);
        _ = exception.NodeId.Should().Be("MyNode");
    }

    #endregion

    #region RetryExhaustedException Tests

    [Fact]
    public void RetryExhaustedException_DefaultConstructor_HasErrorCode()
    {
        // Act
        var exception = new RetryExhaustedException();

        // Assert
        _ = exception.ErrorCode.Should().Be("RETRY_EXHAUSTED");
        _ = exception.NodeId.Should().Be(string.Empty);
    }

    [Fact]
    public void RetryExhaustedException_MessageConstructor_PreservesMessage()
    {
        // Arrange
        var message = "Custom message";

        // Act
        var exception = new RetryExhaustedException(message);

        // Assert
        _ = exception.Message.Should().Be(message);
    }

    [Fact]
    public void RetryExhaustedException_WithInnerException_PreservesIt()
    {
        // Arrange
        var inner = new TimeoutException("timeout");

        // Act
        var exception = new RetryExhaustedException("message", inner);

        // Assert
        _ = exception.InnerException.Should().Be(inner);
    }

    [Fact]
    public void RetryExhaustedException_NodeIdAttemptsConstructor_SetsProperties()
    {
        // Arrange
        var inner = new IOException("io error");

        // Act
        var exception = new RetryExhaustedException("NodeX", 10, inner);

        // Assert
        _ = exception.NodeId.Should().Be("NodeX");
        _ = exception.AttemptCount.Should().Be(10);
        _ = exception.InnerException.Should().Be(inner);
    }

    #endregion

    #region PipelineExecutionException Tests

    [Fact]
    public void PipelineExecutionException_DefaultConstructor_HasErrorCode()
    {
        // Act
        var exception = new PipelineExecutionException();

        // Assert
        _ = exception.ErrorCode.Should().Be("PIPELINE_EXECUTION_ERROR");
    }

    [Fact]
    public void PipelineExecutionException_MessageConstructor_PreservesMessage()
    {
        // Arrange
        var message = "Pipeline failed";

        // Act
        var exception = new PipelineExecutionException(message);

        // Assert
        _ = exception.Message.Should().Contain(message);
    }

    [Fact]
    public void PipelineExecutionException_WithInnerException_PreservesIt()
    {
        // Arrange
        var inner = new NodeExecutionException("node", "error");

        // Act
        var exception = new PipelineExecutionException("message", inner);

        // Assert
        _ = exception.InnerException.Should().Be(inner);
    }

    #endregion

    #region NodeExecutionException Tests

    [Fact]
    public void NodeExecutionException_HasErrorCode()
    {
        // Act
        var exception = new NodeExecutionException("node1", "error message");

        // Assert
        _ = exception.ErrorCode.Should().Be("NODE_EXECUTION_ERROR");
    }

    [Fact]
    public void NodeExecutionException_StoresNodeId()
    {
        // Arrange
        var nodeId = "MyProcessingNode";

        // Act
        var exception = new NodeExecutionException(nodeId, "error");

        // Assert
        _ = exception.NodeId.Should().Be(nodeId);
    }

    [Fact]
    public void NodeExecutionException_WithInnerException_PreservesIt()
    {
        // Arrange
        var inner = new ArgumentException("invalid arg");

        // Act
        var exception = new NodeExecutionException("node", "error", inner);

        // Assert
        _ = exception.InnerException.Should().Be(inner);
    }

    #endregion

    #region Exception Polymorphism Tests

    [Fact]
    public void AllExceptions_CanBeTreatedAsPipelineException()
    {
        // Arrange & Act
        PipelineException ex1 = new NodeExecutionException("node", "error");
        PipelineException ex2 = new PipelineExecutionException("error");
        PipelineException ex3 = new CircuitBreakerTrippedException();
        PipelineException ex4 = new RetryExhaustedException();

        // Assert
        _ = ex1.Should().NotBeNull();
        _ = ex2.Should().NotBeNull();
        _ = ex3.Should().NotBeNull();
        _ = ex4.Should().NotBeNull();
    }

    [Fact]
    public void AllExceptions_CanBeTreatedAsException()
    {
        // Arrange & Act
        Exception ex1 = new NodeExecutionException("node", "error");
        Exception ex2 = new PipelineExecutionException("error");
        Exception ex3 = new CircuitBreakerTrippedException();
        Exception ex4 = new RetryExhaustedException();

        // Assert
        _ = ex1.Should().NotBeNull();
        _ = ex2.Should().NotBeNull();
        _ = ex3.Should().NotBeNull();
        _ = ex4.Should().NotBeNull();
    }

    #endregion

    #region Message Content Tests

    [Fact]
    public void NodeExecutionException_MessageContainsNodeId()
    {
        // Act
        var exception = new NodeExecutionException("TestNode", "Processing failed");

        // Assert
        _ = exception.Message.Should().Contain("TestNode");
    }

    [Fact]
    public void CircuitBreakerTrippedException_WithThreshold_MessageContainsThreshold()
    {
        // Act
        var exception = new CircuitBreakerTrippedException(5);

        // Assert
        _ = exception.Message.Should().Contain("5");
    }

    [Fact]
    public void RetryExhaustedException_WithAttempts_MessageContainsAttemptCount()
    {
        // Act
        var exception = new RetryExhaustedException("node", 10, new Exception());

        // Assert
        _ = exception.Message.Should().Contain("10");
    }

    #endregion
}
