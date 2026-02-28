using System.Data.Common;
using System.Net.Http;
using System.Net.Sockets;
using NPipeline.Connectors.Snowflake.Exceptions;

namespace NPipeline.Connectors.Snowflake.Tests.Exceptions;

public sealed class SnowflakeTransientErrorDetectorTests
{
    [Fact]
    public void IsTransient_WithTimeoutException_ShouldReturnTrue()
    {
        var ex = new TimeoutException("Operation timed out");
        Assert.True(SnowflakeTransientErrorDetector.IsTransient(ex));
    }

    [Fact]
    public void IsTransient_WithOperationCanceledException_ShouldReturnTrue()
    {
        var ex = new OperationCanceledException("Operation canceled");
        Assert.True(SnowflakeTransientErrorDetector.IsTransient(ex));
    }

    [Fact]
    public void IsTransient_WithHttpRequestException_ShouldReturnTrue()
    {
        var ex = new HttpRequestException("Network error");
        Assert.True(SnowflakeTransientErrorDetector.IsTransient(ex));
    }

    [Fact]
    public void IsTransient_WithSocketException_ShouldReturnTrue()
    {
        var ex = new SocketException((int)SocketError.ConnectionRefused);
        Assert.True(SnowflakeTransientErrorDetector.IsTransient(ex));
    }

    [Fact]
    public void IsTransient_WithInvalidOperationExceptionContainingTimeout_ShouldReturnTrue()
    {
        var ex = new InvalidOperationException("The operation timeout was exceeded");
        Assert.True(SnowflakeTransientErrorDetector.IsTransient(ex));
    }

    [Fact]
    public void IsTransient_WithInvalidOperationExceptionContainingConnection_ShouldReturnTrue()
    {
        var ex = new InvalidOperationException("Connection pool exhausted");
        Assert.True(SnowflakeTransientErrorDetector.IsTransient(ex));
    }

    [Fact]
    public void IsTransient_WithRegularException_ShouldReturnFalse()
    {
        var ex = new Exception("Some regular error");
        Assert.False(SnowflakeTransientErrorDetector.IsTransient(ex));
    }

    [Fact]
    public void IsTransient_WithArgumentException_ShouldReturnFalse()
    {
        var ex = new ArgumentException("Invalid argument");
        Assert.False(SnowflakeTransientErrorDetector.IsTransient(ex));
    }

    [Fact]
    public void IsTransient_WithTransientInnerException_ShouldReturnTrue()
    {
        var inner = new TimeoutException("Timed out");
        var ex = new Exception("Wrapper", inner);
        Assert.True(SnowflakeTransientErrorDetector.IsTransient(ex));
    }

    [Fact]
    public void IsTransientError_WithTransientCode390114_ShouldReturnTrue()
    {
        Assert.True(SnowflakeTransientErrorDetector.IsTransientError(390114));
    }

    [Fact]
    public void IsTransientError_WithTransientCode390144_ShouldReturnTrue()
    {
        Assert.True(SnowflakeTransientErrorDetector.IsTransientError(390144));
    }

    [Fact]
    public void IsTransientError_WithTransientCode200002_ShouldReturnTrue()
    {
        Assert.True(SnowflakeTransientErrorDetector.IsTransientError(200002));
    }

    [Fact]
    public void IsTransientError_WithNonTransientCode_ShouldReturnFalse()
    {
        Assert.False(SnowflakeTransientErrorDetector.IsTransientError(99999));
    }
}
