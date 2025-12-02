using System.Diagnostics;
using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Nodes.Branch;

/// <summary>
///     Comprehensive tests for BranchNode&lt;T&gt;.
///     Tests output handler execution, parallel processing, exception handling, and proper item pass-through.
/// </summary>
public sealed class BranchNodeTests
{
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
    public async Task BranchNode_ExceptionInOneHandler_WithLogAndContinueMode_OtherHandlersStillExecute()
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
    public async Task BranchNode_DisposalDoesNotThrow()
    {
        // Arrange
        BranchNode<int> node = new();
        node.AddOutput(async x => await Task.Yield());

        // Act & Assert
        await node.DisposeAsync();
    }

    [Fact]
    public async Task BranchNode_ManyHandlers_AllExecute()
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
}
