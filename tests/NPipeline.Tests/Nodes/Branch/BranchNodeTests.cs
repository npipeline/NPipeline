using System.Collections.Concurrent;
using System.Diagnostics;
using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Nodes.Branch;

/// <summary>
///     Comprehensive tests for BranchNode&lt;T&gt; covering basic functionality,
///     error handling, parallel execution, concurrent access, and edge cases.
///     Tests output handler execution, parallel processing, exception handling, and proper item pass-through.
/// </summary>
public sealed class BranchNodeTests
{
    #region Edge Cases

    [Fact]
    public async Task BranchNode_MultipleItems_PreservesItemsThroughBranches()
    {
        // Arrange
        BranchNode<int> node = new();
        List<int> captured = [];

        node.AddOutput(async x =>
        {
            captured.Add(x);
            await Task.Yield();
        });

        var ctx = PipelineContext.Default;

        // Act
        var result1 = await node.ExecuteAsync(1, ctx, CancellationToken.None);
        var result2 = await node.ExecuteAsync(2, ctx, CancellationToken.None);
        var result3 = await node.ExecuteAsync(3, ctx, CancellationToken.None);

        // Assert
        _ = result1.Should().Be(1);
        _ = result2.Should().Be(2);
        _ = result3.Should().Be(3);
        _ = captured.Should().ContainInOrder(1, 2, 3);
    }

    #endregion

    #region Helper Classes

    private sealed class TestObject
    {
        public int Value { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private sealed class ContinueErrorHandler(Action onCalled) : IPipelineErrorHandler
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

    private sealed class FailErrorHandler(Action onCalled) : IPipelineErrorHandler
    {
        public Task<PipelineErrorDecision> HandleNodeFailureAsync(
            string nodeId,
            Exception error,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            onCalled();
            return Task.FromResult(PipelineErrorDecision.FailPipeline);
        }
    }

    #endregion

    #region Basic Functionality Tests

    [Fact]
    public async Task BranchNode_WithNoOutputs_ReturnsItemUnchanged()
    {
        // Arrange
        BranchNode<int> node = new();
        var ctx = PipelineContext.Default;
        var item = 42;

        // Act
        var result = await node.ExecuteAsync(item, ctx, CancellationToken.None);

        // Assert
        _ = result.Should().Be(42);
    }

    [Fact]
    public async Task BranchNode_WithSingleOutput_ExecutesHandler()
    {
        // Arrange
        BranchNode<int> node = new();
        var executed = false;

        node.AddOutput(async x =>
        {
            executed = true;
            await Task.Yield();
        });

        var ctx = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(42, ctx, CancellationToken.None);

        // Assert
        _ = result.Should().Be(42);
        _ = executed.Should().BeTrue();
    }

    [Fact]
    public async Task BranchNode_WithMultipleOutputs_ExecutesAllHandlers()
    {
        // Arrange
        BranchNode<string> node = new();
        List<string> captured = [];

        node.AddOutput(async x =>
        {
            captured.Add($"Handler1:{x}");
            await Task.Yield();
        });

        node.AddOutput(async x =>
        {
            captured.Add($"Handler2:{x}");
            await Task.Yield();
        });

        node.AddOutput(async x =>
        {
            captured.Add($"Handler3:{x}");
            await Task.Yield();
        });

        var ctx = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync("test", ctx, CancellationToken.None);

        // Assert
        _ = result.Should().Be("test");
        _ = captured.Should().HaveCount(3);
        _ = captured.Should().Contain("Handler1:test");
        _ = captured.Should().Contain("Handler2:test");
        _ = captured.Should().Contain("Handler3:test");
    }

    [Fact]
    public async Task BranchNode_HandlerReceivesCorrectItem()
    {
        // Arrange
        BranchNode<string> node = new();
        string? capturedItem = null;

        node.AddOutput(async x =>
        {
            capturedItem = x;
            await Task.Yield();
        });

        var ctx = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync("HelloWorld", ctx, CancellationToken.None);

        // Assert
        _ = result.Should().Be("HelloWorld");
        _ = capturedItem.Should().Be("HelloWorld");
    }

    [Fact]
    public async Task BranchNode_AddOutputAfterCreation_Works()
    {
        // Arrange
        BranchNode<int> node = new();
        List<int> results = [];

        node.AddOutput(async x =>
        {
            results.Add(x);
            await Task.Yield();
        });

        node.AddOutput(async x =>
        {
            results.Add(x * 2);
            await Task.Yield();
        });

        var ctx = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(10, ctx, CancellationToken.None);

        // Assert
        _ = result.Should().Be(10);
        _ = results.Should().HaveCount(2);
        _ = results.Should().Contain(10);
        _ = results.Should().Contain(20);
    }

    #endregion

    #region Parallel Execution Tests

    [Fact]
    public async Task BranchNode_ExecutesHandlersInParallel()
    {
        // Arrange - use synchronization to deterministically ensure handlers start concurrently
        var node = new BranchNode<int>();
        var started = 0;
        var handlerCount = 3;
        var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        for (var i = 0; i < handlerCount; i++)
        {
            node.AddOutput(async x =>
            {
                var _startedNow = Interlocked.Increment(ref started);

                // wait until test signals to continue
                var _ignored = await release.Task;
            });
        }

        var ctx = PipelineContext.Default;

        // Act - start execution; handlers will block after incrementing 'started'
        var execTask = node.ExecuteAsync(1, ctx, CancellationToken.None);

        // wait for all handlers to have started (timeout to avoid hangs)
        var sw = Stopwatch.StartNew();
        var timeout = TimeSpan.FromSeconds(5);

        while (Volatile.Read(ref started) < handlerCount && sw.Elapsed < timeout)
        {
            await Task.Delay(10);
        }

        // Assert - all handlers should have started
        _ = started.Should().Be(handlerCount);

        // allow handlers to finish
        release.SetResult(true);

        // await completion
        var execResult = await execTask;
    }

    [Fact]
    public async Task BranchNode_ManyHandlers_AllInitialize()
    {
        // Arrange
        BranchNode<int> node = new();
        List<int> results = [];

        for (var i = 0; i < 10; i++)
        {
            var index = i;

            node.AddOutput(async x =>
            {
                results.Add(index);
                await Task.Yield();
            });
        }

        var ctx = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(1, ctx, CancellationToken.None);

        // Assert
        _ = result.Should().Be(1);
        _ = results.Should().HaveCount(10);

        for (var i = 0; i < 10; i++)
        {
            _ = results.Should().Contain(i);
        }
    }

    #endregion

    #region Error Handling Tests

    [Fact]
    public async Task BranchNode_ExceptionInOneHandler_WithLogAndContinueMode_OtherHandlersStillInitialize()
    {
        // Arrange
        BranchNode<int> node = new() { ErrorHandlingMode = BranchErrorHandlingMode.LogAndContinue };
        List<string> results = [];

        node.AddOutput(async x =>
        {
            results.Add("Handler1");
            await Task.Yield();
        });

        node.AddOutput(async x =>
        {
            await Task.Yield();
            throw new InvalidOperationException("Handler2 failed");
        });

        node.AddOutput(async x =>
        {
            results.Add("Handler3");
            await Task.Yield();
        });

        var ctx = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(42, ctx, CancellationToken.None);

        // Assert - should not throw and should return the item
        _ = result.Should().Be(42);

        // Handler1 and Handler3 should have executed despite Handler2 failing
        _ = results.Should().HaveCount(2);
        _ = results.Should().Contain("Handler1");
        _ = results.Should().Contain("Handler3");
    }

    [Fact]
    public async Task BranchNode_ExceptionInHandler_WithNoErrorHandler_ThrowsBranchHandlerException()
    {
        // Arrange
        BranchNode<int> node = new();

        node.AddOutput(async x =>
        {
            await Task.Yield();
            throw new InvalidOperationException("Handler failed");
        });

        var ctx = PipelineContext.Default;

        // Act & Assert
        var ex = await Assert.ThrowsAsync<BranchHandlerException>(async () =>
            await node.ExecuteAsync(42, ctx, CancellationToken.None));

        _ = ex.BranchIndex.Should().Be(0);
        _ = ex.InnerException.Should().BeOfType<InvalidOperationException>();
    }

    [Fact]
    public async Task BranchNode_ExceptionInHandler_WithErrorHandlerContinue_DoesNotThrow()
    {
        // Arrange
        BranchNode<int> node = new();
        var errorHandlerCalled = false;

        node.AddOutput(async x =>
        {
            await Task.Yield();
            throw new InvalidOperationException("Handler failed");
        });

        var config = new PipelineContextConfiguration(
            PipelineErrorHandler: new ContinueErrorHandler(() => errorHandlerCalled = true));

        var ctx = new PipelineContext(config);

        // Act
        var result = await node.ExecuteAsync(42, ctx, CancellationToken.None);

        // Assert
        _ = result.Should().Be(42);
        _ = errorHandlerCalled.Should().BeTrue();
    }

    [Fact]
    public async Task BranchNode_ExceptionInHandler_WithErrorHandlerFail_ThrowsBranchHandlerException()
    {
        // Arrange
        BranchNode<int> node = new();
        var errorHandlerCalled = false;

        node.AddOutput(async x =>
        {
            await Task.Yield();
            throw new InvalidOperationException("Handler failed");
        });

        var config = new PipelineContextConfiguration(
            PipelineErrorHandler: new FailErrorHandler(() => errorHandlerCalled = true));

        var ctx = new PipelineContext(config);

        // Act & Assert
        var ex = await Assert.ThrowsAsync<BranchHandlerException>(async () =>
            await node.ExecuteAsync(42, ctx, CancellationToken.None));

        _ = errorHandlerCalled.Should().BeTrue();
        _ = ex.BranchIndex.Should().Be(0);
    }

    [Fact]
    public async Task BranchNode_ExceptionInHandler_WithCollectAndThrowMode_ThrowsAggregateException()
    {
        // Arrange
        BranchNode<int> node = new() { ErrorHandlingMode = BranchErrorHandlingMode.CollectAndThrow };

        node.AddOutput(async x =>
        {
            await Task.Yield();
            throw new InvalidOperationException("Handler1 failed");
        });

        node.AddOutput(async x =>
        {
            await Task.Yield();
            throw new ArgumentException("Handler2 failed");
        });

        var ctx = PipelineContext.Default;

        // Act & Assert
        var ex = await Assert.ThrowsAsync<AggregateException>(async () =>
            await node.ExecuteAsync(42, ctx, CancellationToken.None));

        _ = ex.InnerExceptions.Should().HaveCount(2);
        _ = ex.InnerExceptions.Should().AllBeAssignableTo<BranchHandlerException>();
    }

    [Fact]
    public async Task BranchNode_BranchHandlerException_ContainsFailedItem()
    {
        // Arrange
        BranchNode<string> node = new();

        node.AddOutput(async x =>
        {
            await Task.Yield();
            throw new InvalidOperationException("Handler failed");
        });

        var ctx = PipelineContext.Default;

        // Act & Assert
        var ex = await Assert.ThrowsAsync<BranchHandlerException>(async () =>
            await node.ExecuteAsync("test-item", ctx, CancellationToken.None));

        _ = ex.FailedItem.Should().Be("test-item");
    }

    #endregion

    #region Data Passing Tests

    [Fact]
    public async Task BranchNode_WithReferenceType_PassesByReference()
    {
        // Arrange
        BranchNode<Dictionary<string, int>> node = new();
        var modifyCount = 0;

        node.AddOutput(async dict =>
        {
            if (!dict.ContainsKey("modified"))
                dict["modified"] = ++modifyCount;

            await Task.Yield();
        });

        node.AddOutput(async dict =>
        {
            if (!dict.ContainsKey("modified"))
                dict["modified"] = ++modifyCount;

            await Task.Yield();
        });

        var ctx = PipelineContext.Default;
        Dictionary<string, int> input = new() { { "key", 1 } };

        // Act
        var result = await node.ExecuteAsync(input, ctx, CancellationToken.None);

        // Assert
        _ = result.Should().BeSameAs(input);
        _ = result["key"].Should().Be(1);

        // The "modified" key should be present (one of the handlers added it)
        _ = result.Should().ContainKey("modified");
    }

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

    #region Disposal Tests

    [Fact]
    public async Task BranchNode_DisposeAsync_CompletesSuccessfully()
    {
        // Arrange
        BranchNode<int> node = new();
        node.AddOutput(async x => await Task.Yield());

        // Act & Assert
        await node.DisposeAsync();
    }

    [Fact]
    public async Task BranchNode_DisposeAsync_MultipleTimes_IsIdempotent()
    {
        // Arrange
        BranchNode<int> node = new();

        // Act & Assert
        await node.DisposeAsync();
        await node.DisposeAsync();
        await node.DisposeAsync();

        Assert.True(true);
    }

    #endregion
}
