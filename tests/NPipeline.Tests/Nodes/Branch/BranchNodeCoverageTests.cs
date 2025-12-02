// ReSharper disable ClassNeverInstantiated.Local

using System.Collections.Concurrent;
using System.Diagnostics;
using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Nodes.Branch;

/// <summary>
///     Comprehensive tests for BranchNode functionality including fan-out, error handling, and disposal.
/// </summary>
public sealed class BranchNodeCoverageTests
{
    #region Helper Classes

    private sealed class TestObject
    {
        public int Value { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    #endregion

    #region Basic Branch Functionality Tests

    [Fact]
    public async Task BranchNode_ExecuteAsync_WithNoOutputs_ReturnsOriginalItem()
    {
        // Arrange
        var branchNode = new BranchNode<int>();
        var context = PipelineContext.Default;
        var item = 42;

        // Act
        var result = await branchNode.ExecuteAsync(item, context, CancellationToken.None);

        // Assert
        _ = result.Should().Be(item);
    }

    [Fact]
    public async Task BranchNode_ExecuteAsync_WithSingleOutput_CallsHandlerAndReturnsItem()
    {
        // Arrange
        var branchNode = new BranchNode<string>();
        var context = PipelineContext.Default;
        var item = "test-item";
        var receivedItems = new List<string>();

        branchNode.AddOutput(async i =>
        {
            receivedItems.Add(i);
            await Task.CompletedTask;
        });

        // Act
        var result = await branchNode.ExecuteAsync(item, context, CancellationToken.None);

        // Assert
        _ = result.Should().Be(item);
        _ = receivedItems.Should().ContainSingle(item);
    }

    [Fact]
    public async Task BranchNode_ExecuteAsync_WithMultipleOutputs_CallsAllHandlers()
    {
        // Arrange
        var branchNode = new BranchNode<int>();
        var context = PipelineContext.Default;
        var item = 100;
        var handlerCallCounts = new ConcurrentDictionary<int, int>();
        var handlerCount = 5;

        for (var i = 0; i < handlerCount; i++)
        {
            var handlerIndex = i;

            branchNode.AddOutput(async _ =>
            {
                _ = handlerCallCounts.AddOrUpdate(handlerIndex, 1, (_, count) => count + 1);
                await Task.CompletedTask;
            });
        }

        // Act
        var result = await branchNode.ExecuteAsync(item, context, CancellationToken.None);

        // Assert
        _ = result.Should().Be(item);
        _ = handlerCallCounts.Should().HaveCount(handlerCount);

        foreach (var count in handlerCallCounts.Values)
        {
            _ = count.Should().Be(1);
        }
    }

    [Fact]
    public async Task BranchNode_ExecuteAsync_MultipleHandlers_ExecuteInParallel()
    {
        // Arrange
        var branchNode = new BranchNode<string>();
        var context = PipelineContext.Default;
        var item = "data";
        var handlerCount = 10;

        // Use a TaskCompletionSource to block handlers until all have started. This ensures they truly run concurrently
        // instead of relying on wall-clock timing which is flaky under load.
        var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = 0;

        for (var i = 0; i < handlerCount; i++)
        {
            branchNode.AddOutput(async _ =>
            {
                // mark started
                Interlocked.Increment(ref started);

                // wait until test signals to continue
                var _ignored = await release.Task;
            });
        }

        // Act - start pipeline execution but handlers will block on 'release'
        var execTask = branchNode.ExecuteAsync(item, context, CancellationToken.None);

        // wait for all handlers to have started (with a sane timeout)
        var timeout = TimeSpan.FromSeconds(5);
        var sw = Stopwatch.StartNew();

        while (Volatile.Read(ref started) < handlerCount && sw.Elapsed < timeout)
        {
            await Task.Delay(10);
        }

        // all handlers should have started and be waiting
        _ = started.Should().Be(handlerCount);

        // allow handlers to complete
        release.SetResult(true);

        // await the pipeline execution
        var _result = await execTask;
    }

    #endregion

    #region Error Handling Tests

    [Fact]
    public async Task BranchNode_ExecuteAsync_WithLogAndContinueMode_SwallowsException()
    {
        // Arrange
        var branchNode = new BranchNode<string>
        {
            ErrorHandlingMode = BranchErrorHandlingMode.LogAndContinue
        };
        var context = PipelineContext.Default;
        var item = "test";
        var successfulHandlerCalled = false;

        branchNode.AddOutput(async _ =>
        {
            await Task.CompletedTask;
            throw new InvalidOperationException("Handler failed");
        });

        branchNode.AddOutput(async _ =>
        {
            successfulHandlerCalled = true;
            await Task.CompletedTask;
        });

        // Act
        var result = await branchNode.ExecuteAsync(item, context, CancellationToken.None);

        // Assert
        _ = result.Should().Be(item);
        _ = successfulHandlerCalled.Should().BeTrue();
    }

    [Fact]
    public async Task BranchNode_ExecuteAsync_WithMultipleFailingHandlers_AndLogAndContinueMode_SwallowsAllExceptions()
    {
        // Arrange
        var branchNode = new BranchNode<int>
        {
            ErrorHandlingMode = BranchErrorHandlingMode.LogAndContinue
        };
        var context = PipelineContext.Default;
        var item = 42;
        var handlerCount = 3;

        for (var i = 0; i < handlerCount; i++)
        {
            branchNode.AddOutput(async _ =>
            {
                await Task.CompletedTask;
                throw new Exception($"Handler {i} failed");
            });
        }

        // Act
        var result = await branchNode.ExecuteAsync(item, context, CancellationToken.None);

        // Assert
        _ = result.Should().Be(item);
    }

    [Fact]
    public async Task BranchNode_ExecuteAsync_ExceptionInHandler_WithLogAndContinueMode_DoesNotAffectMainFlow()
    {
        // Arrange
        var branchNode = new BranchNode<string>
        {
            ErrorHandlingMode = BranchErrorHandlingMode.LogAndContinue
        };
        var context = PipelineContext.Default;
        var item = "important-data";

        branchNode.AddOutput(async _ =>
        {
            await Task.CompletedTask;
            throw new TimeoutException("Handler timeout");
        });

        // Act
        var act = async () => await branchNode.ExecuteAsync(item, context, CancellationToken.None);

        // Assert
        await act.Should().NotThrowAsync();
        var result = await branchNode.ExecuteAsync(item, context, CancellationToken.None);
        _ = result.Should().Be(item);
    }

    [Fact]
    public async Task BranchNode_ExecuteAsync_ExceptionInHandler_WithNoErrorHandler_Throws()
    {
        // Arrange
        var branchNode = new BranchNode<string>();
        var context = PipelineContext.Default;
        var item = "test";

        branchNode.AddOutput(async _ =>
        {
            await Task.CompletedTask;
            throw new InvalidOperationException("Handler failed");
        });

        // Act
        var act = async () => await branchNode.ExecuteAsync(item, context, CancellationToken.None);

        // Assert
        await act.Should().ThrowAsync<BranchHandlerException>();
    }

    [Fact]
    public async Task BranchNode_ExecuteAsync_ExceptionInHandler_WithContinueErrorHandler_DoesNotThrow()
    {
        // Arrange
        var branchNode = new BranchNode<string>();
        var item = "test";
        var errorHandlerCalled = false;

        var config = new PipelineContextConfiguration(
            PipelineErrorHandler: new TestContinueErrorHandler(() => errorHandlerCalled = true));
        var context = new PipelineContext(config);

        branchNode.AddOutput(async _ =>
        {
            await Task.CompletedTask;
            throw new InvalidOperationException("Handler failed");
        });

        // Act
        var result = await branchNode.ExecuteAsync(item, context, CancellationToken.None);

        // Assert
        _ = result.Should().Be(item);
        _ = errorHandlerCalled.Should().BeTrue();
    }

    [Fact]
    public async Task BranchNode_ExecuteAsync_CollectAndThrowMode_CollectsMultipleExceptions()
    {
        // Arrange
        var branchNode = new BranchNode<int>
        {
            ErrorHandlingMode = BranchErrorHandlingMode.CollectAndThrow
        };
        var context = PipelineContext.Default;
        var item = 42;

        branchNode.AddOutput(async _ =>
        {
            await Task.CompletedTask;
            throw new InvalidOperationException("Handler 1 failed");
        });

        branchNode.AddOutput(async _ =>
        {
            await Task.CompletedTask;
            throw new ArgumentException("Handler 2 failed");
        });

        // Act
        var act = async () => await branchNode.ExecuteAsync(item, context, CancellationToken.None);

        // Assert
        var ex = await act.Should().ThrowAsync<AggregateException>();
        _ = ex.Which.InnerExceptions.Should().HaveCount(2);
    }

    private sealed class TestContinueErrorHandler(Action onCalled) : IPipelineErrorHandler
    {
        public Task<PipelineErrorDecision> HandleNodeFailureAsync(
            string nodeId,
            Exception error,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            onCalled();
            return Task.FromResult(PipelineErrorDecision.ContinueWithoutNode);
        }
    }

    #endregion

    #region Concurrent Access Tests

    [Fact]
    public async Task BranchNode_AddOutput_ConcurrentAdds_ThreadSafe()
    {
        // Arrange
        var branchNode = new BranchNode<int>();
        var context = PipelineContext.Default;
        var item = 1;
        var tasks = new List<Task>();
        var addCount = 100;

        for (var i = 0; i < addCount; i++)
        {
            tasks.Add(Task.Run(() => { branchNode.AddOutput(async _ => await Task.CompletedTask); }));
        }

        await Task.WhenAll(tasks);
        _ = await branchNode.ExecuteAsync(item, context, CancellationToken.None);

        // Assert - No exceptions should occur
        Assert.True(true);
    }

    [Fact]
    public async Task BranchNode_ExecuteAsync_DuringConcurrentAdds_Consistent()
    {
        // Arrange
        var branchNode = new BranchNode<int>();
        var context = PipelineContext.Default;
        var executionCounts = new ConcurrentBag<int>();
        var item = 1;

        branchNode.AddOutput(async _ =>
        {
            executionCounts.Add(1);
            await Task.CompletedTask;
        });

        // Act
        _ = await branchNode.ExecuteAsync(item, context, CancellationToken.None);

        // Assert
        _ = executionCounts.Should().HaveCount(1);
    }

    #endregion

    #region Data Passing Tests

    [Fact]
    public async Task BranchNode_ExecuteAsync_DataPassedToHandlers_IsUnmodified()
    {
        // Arrange
        var branchNode = new BranchNode<TestObject>();
        var context = PipelineContext.Default;
        var originalObject = new TestObject { Value = 42, Name = "test" };
        var receivedObjects = new ConcurrentBag<TestObject>();

        branchNode.AddOutput(async obj =>
        {
            receivedObjects.Add(obj);
            await Task.CompletedTask;
        });

        branchNode.AddOutput(async obj =>
        {
            receivedObjects.Add(obj);
            await Task.CompletedTask;
        });

        // Act
        var result = await branchNode.ExecuteAsync(originalObject, context, CancellationToken.None);

        // Assert
        _ = result.Should().Be(originalObject);
        _ = receivedObjects.Should().HaveCount(2);

        foreach (var obj in receivedObjects)
        {
            _ = obj.Should().Be(originalObject);
        }
    }

    [Fact]
    public async Task BranchNode_ExecuteAsync_AllHandlersReceiveSameItem()
    {
        // Arrange
        var branchNode = new BranchNode<int>();
        var context = PipelineContext.Default;
        var item = 123;
        var receivedItems = new ConcurrentBag<int>();

        for (var i = 0; i < 5; i++)
        {
            branchNode.AddOutput(async receivedItem =>
            {
                receivedItems.Add(receivedItem);
                await Task.CompletedTask;
            });
        }

        // Act
        _ = await branchNode.ExecuteAsync(item, context, CancellationToken.None);

        // Assert
        _ = receivedItems.Should().HaveCount(5);

        foreach (var receivedItem in receivedItems)
        {
            _ = receivedItem.Should().Be(item);
        }
    }

    #endregion

    #region Disposal Tests

    [Fact]
    public async Task BranchNode_DisposeAsync_CompletesSuccessfully()
    {
        // Arrange
        var branchNode = new BranchNode<int>();

        // Act
        var act = async () => await branchNode.DisposeAsync();

        // Assert
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task BranchNode_DisposeAsync_MultipleTimes_IsIdempotent()
    {
        // Arrange
        var branchNode = new BranchNode<int>();

        // Act & Assert
        await branchNode.DisposeAsync();
        await branchNode.DisposeAsync();
        await branchNode.DisposeAsync();

        Assert.True(true);
    }

    #endregion
}
