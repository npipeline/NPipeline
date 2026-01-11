using System.Diagnostics;
using NPipeline.Extensions.Observability.Tracing;

namespace NPipeline.Extensions.Observability.Tests;

/// <summary>
///     Comprehensive tests for <see cref="PipelineActivity" />.
/// </summary>
public sealed class PipelineActivityTests
{
    #region Constructor Tests

    [Fact]
    public void Constructor_WithValidActivity_ShouldInitialize()
    {
        // Arrange
        using var activity = new Activity("TestActivity");
        _ = activity.Start();

        // Act
        var pipelineActivity = new PipelineActivity(activity);

        // Assert
        Assert.NotNull(pipelineActivity);
        pipelineActivity.Dispose();
    }

    [Fact]
    public void Constructor_WithNullActivity_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        var ex = Assert.Throws<ArgumentNullException>(() => new PipelineActivity(null!));
        Assert.Equal("activity", ex.ParamName);
    }

    #endregion

    #region SetTag Tests

    [Fact]
    public void SetTag_WithValidKeyAndValue_ShouldSetTagOnUnderlyingActivity()
    {
        // Arrange
        using var activity = new Activity("TestActivity");
        _ = activity.Start();
        var pipelineActivity = new PipelineActivity(activity);

        // Act
        pipelineActivity.SetTag("test_key", "test_value");

        // Assert
        Assert.Equal("test_value", activity.GetTagItem("test_key"));
        pipelineActivity.Dispose();
    }

    [Fact]
    public void SetTag_WithMultipleTags_ShouldSetAllTags()
    {
        // Arrange
        using var activity = new Activity("TestActivity");
        _ = activity.Start();
        var pipelineActivity = new PipelineActivity(activity);

        // Act
        pipelineActivity.SetTag("key1", "value1");
        pipelineActivity.SetTag("key2", 42);
        pipelineActivity.SetTag("key3", 3.14);

        // Assert
        Assert.Equal("value1", activity.GetTagItem("key1"));
        Assert.Equal(42, activity.GetTagItem("key2"));
        Assert.Equal(3.14, activity.GetTagItem("key3"));
        pipelineActivity.Dispose();
    }

    [Fact]
    public void SetTag_WithNumericValue_ShouldPreserveType()
    {
        // Arrange
        using var activity = new Activity("TestActivity");
        _ = activity.Start();
        var pipelineActivity = new PipelineActivity(activity);

        // Act
        pipelineActivity.SetTag("int_tag", 123);
        pipelineActivity.SetTag("double_tag", 45.67);

        // Assert
        Assert.Equal(123, activity.GetTagItem("int_tag"));
        Assert.Equal(45.67, activity.GetTagItem("double_tag"));
        pipelineActivity.Dispose();
    }

    #endregion

    #region RecordException Tests

    [Fact]
    public void RecordException_WithValidException_ShouldAddExceptionEvent()
    {
        // Arrange
        using var activity = new Activity("TestActivity");
        _ = activity.Start();
        var pipelineActivity = new PipelineActivity(activity);
        var testException = new InvalidOperationException("Test error message");

        // Act
        pipelineActivity.RecordException(testException);

        // Assert
        Assert.Equal(ActivityStatusCode.Error, activity.Status);
        Assert.Equal("Test error message", activity.StatusDescription);

        var events = activity.Events.ToList();
        _ = Assert.Single(events);
        Assert.Equal("exception", events[0].Name);

        pipelineActivity.Dispose();
    }

    [Fact]
    public void RecordException_ShouldIncludeExceptionDetails()
    {
        // Arrange
        using var activity = new Activity("TestActivity");
        _ = activity.Start();
        var pipelineActivity = new PipelineActivity(activity);
        var testException = new InvalidOperationException("Test error");

        // Act
        pipelineActivity.RecordException(testException);

        // Assert
        var events = activity.Events.ToList();
        var eventTagsList = events[0].Tags.ToList();

        Assert.NotEmpty(eventTagsList);
        var exceptionType = eventTagsList.FirstOrDefault(t => t.Key == "exception.type").Value;
        var exceptionMessage = eventTagsList.FirstOrDefault(t => t.Key == "exception.message").Value;
        var exceptionStackTrace = eventTagsList.FirstOrDefault(t => t.Key == "exception.stacktrace").Value;

        Assert.Equal(typeof(InvalidOperationException).FullName, exceptionType);
        Assert.Equal("Test error", exceptionMessage);
        Assert.NotNull(exceptionStackTrace);

        pipelineActivity.Dispose();
    }

    [Fact]
    public void RecordException_WithInnerException_ShouldIncludeStackTrace()
    {
        // Arrange
        using var activity = new Activity("TestActivity");
        _ = activity.Start();
        var pipelineActivity = new PipelineActivity(activity);

        Exception? thrownException = null;

        try
        {
            throw new InvalidOperationException("Outer exception",
                new ArgumentException("Inner exception"));
        }
        catch (Exception ex)
        {
            thrownException = ex;
        }

        // Act
        pipelineActivity.RecordException(thrownException);

        // Assert
        var events = activity.Events.ToList();
        var eventTagsList = events[0].Tags.ToList();
        var stackTrace = (string?)eventTagsList.FirstOrDefault(t => t.Key == "exception.stacktrace").Value;

        Assert.NotNull(stackTrace);
        Assert.Contains("InvalidOperationException", stackTrace);

        pipelineActivity.Dispose();
    }

    [Fact]
    public void RecordException_WithNullException_ShouldThrowArgumentNullException()
    {
        // Arrange
        using var activity = new Activity("TestActivity");
        _ = activity.Start();
        var pipelineActivity = new PipelineActivity(activity);

        // Act & Assert
        var ex = Assert.Throws<ArgumentNullException>(() => pipelineActivity.RecordException(null!));
        Assert.Equal("exception", ex.ParamName);

        pipelineActivity.Dispose();
    }

    [Fact]
    public void RecordException_WithDifferentExceptionTypes_ShouldRecordCorrectType()
    {
        // Arrange
        var exceptionTypes = new Exception[]
        {
            new InvalidOperationException("invalid operation"),
            new ArgumentNullException("testParam"),
            new TimeoutException("timeout"),
            new NotImplementedException("not implemented"),
        };

        using var activity = new Activity("TestActivity");
        _ = activity.Start();
        var pipelineActivity = new PipelineActivity(activity);

        // Act & Assert
        foreach (var exception in exceptionTypes)
        {
            pipelineActivity.RecordException(exception);

            var events = activity.Events.ToList();
            var eventTagsList = events[^1].Tags.ToList(); // Get the last event
            var exceptionType = eventTagsList.FirstOrDefault(t => t.Key == "exception.type").Value;
            Assert.Equal(exception.GetType().FullName, exceptionType);
        }

        pipelineActivity.Dispose();
    }

    #endregion

    #region Dispose Tests

    [Fact]
    public void Dispose_ShouldDisposeUnderlyingActivity()
    {
        // Arrange
        var activity = new Activity("TestActivity");
        _ = activity.Start();
        var pipelineActivity = new PipelineActivity(activity);

        // Act
        pipelineActivity.Dispose();

        // Assert
        // After disposal, the activity should be stopped
        Assert.Null(Activity.Current);
    }

    [Fact]
    public void Dispose_MultipleCallsShouldNotThrow()
    {
        // Arrange
        using var activity = new Activity("TestActivity");
        _ = activity.Start();
        var pipelineActivity = new PipelineActivity(activity);

        // Act & Assert - should not throw
        pipelineActivity.Dispose();
        pipelineActivity.Dispose();
    }

    #endregion

    #region Integration Tests

    [Fact]
    public void FullWorkflow_ShouldHandleCompleteActivityLifecycle()
    {
        // Arrange
        using var activity = new Activity("TestPipeline");
        _ = activity.Start();
        var pipelineActivity = new PipelineActivity(activity);

        // Act
        pipelineActivity.SetTag("node_id", "node_123");
        pipelineActivity.SetTag("batch_size", 50);
        pipelineActivity.SetTag("duration_ms", 1234);

        var exception = new InvalidOperationException("Processing failed");
        pipelineActivity.RecordException(exception);

        // Assert
        Assert.Equal("node_123", activity.GetTagItem("node_id"));
        Assert.Equal(50, activity.GetTagItem("batch_size"));
        Assert.Equal(1234, activity.GetTagItem("duration_ms"));
        Assert.Equal(ActivityStatusCode.Error, activity.Status);

        var events = activity.Events.ToList();
        Assert.Single(events);
        Assert.Equal("exception", events[0].Name);

        // Cleanup
        pipelineActivity.Dispose();
    }

    [Fact]
    public void MultipleActivities_ShouldWorkIndependently()
    {
        // Arrange
        using var activity1 = new Activity("Activity1");
        _ = activity1.Start();
        var pipelineActivity1 = new PipelineActivity(activity1);

        using var activity2 = new Activity("Activity2");
        _ = activity2.Start();
        var pipelineActivity2 = new PipelineActivity(activity2);

        // Act
        pipelineActivity1.SetTag("source", "activity1");
        pipelineActivity2.SetTag("source", "activity2");

        // Assert
        Assert.Equal("activity1", activity1.GetTagItem("source"));
        Assert.Equal("activity2", activity2.GetTagItem("source"));

        // Cleanup
        pipelineActivity1.Dispose();
        pipelineActivity2.Dispose();
    }

    #endregion
}
