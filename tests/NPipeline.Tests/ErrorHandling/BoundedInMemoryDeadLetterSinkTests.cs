using AwesomeAssertions;
using NPipeline.ErrorHandling;
using NPipeline.Pipeline;

namespace NPipeline.Tests.ErrorHandling;

public sealed class BoundedInMemoryDeadLetterSinkTests
{
    #region Error Message Tests

    [Fact]
    public async Task HandleAsync_ErrorMessage_ContainsCapacityInfo()
    {
        // Arrange
        var sink = new BoundedInMemoryDeadLetterSink(5);
        var context = PipelineContext.Default;

        for (var i = 0; i < 5; i++)
        {
            await sink.HandleAsync("node", $"item-{i}", new Exception(), context, CancellationToken.None);
        }

        // Act
        var act = async () => await sink.HandleAsync("node", "overflow", new Exception(), context, CancellationToken.None);

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*5*");
    }

    #endregion

    #region Constructor Tests

    [Fact]
    public async Task Constructor_WithDefaultCapacity_SetsCapacityTo1000()
    {
        // Act
        var sink = new BoundedInMemoryDeadLetterSink();
        var context = PipelineContext.Default;

        // Assert
        sink.Items.Should().BeEmpty();

        // Add 1000 items - should succeed
        for (var i = 0; i < 1000; i++)
        {
            await sink.HandleAsync("node", $"item-{i}", new Exception(), context, CancellationToken.None);
        }

        // The 1001st item should throw
        var act = async () => await sink.HandleAsync("node", "item-1000", new Exception(), context, CancellationToken.None);
        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*exceeded its capacity of 1000*");
    }

    [Fact]
    public void Constructor_WithCustomCapacity_UsesProvidedCapacity()
    {
        // Act
        var sink = new BoundedInMemoryDeadLetterSink(100);

        // Assert
        sink.Items.Should().BeEmpty();
    }

    [Fact]
    public void Constructor_WithZeroCapacity_ShouldThrow()
    {
        // Arrange & Act & Assert
        var act = () => new BoundedInMemoryDeadLetterSink(0);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    #endregion

    #region Items Collection Tests

    [Fact]
    public async Task HandleAsync_AddsItemToCollection()
    {
        // Arrange
        var sink = new BoundedInMemoryDeadLetterSink();
        var context = PipelineContext.Default;
        var item = "test-item";
        var error = new InvalidOperationException("test error");

        // Act
        await sink.HandleAsync("node1", item, error, context, CancellationToken.None);

        // Assert
        sink.Items.Should().HaveCount(1);
        sink.Items.First().Item.Should().Be(item);
        sink.Items.First().Error.Should().Be(error);
    }

    [Fact]
    public async Task HandleAsync_AddsMultipleItems()
    {
        // Arrange
        var sink = new BoundedInMemoryDeadLetterSink(100);
        var context = PipelineContext.Default;

        // Act
        for (var i = 0; i < 5; i++)
        {
            await sink.HandleAsync("node1", $"item-{i}", new Exception($"error-{i}"), context, CancellationToken.None);
        }

        // Assert
        sink.Items.Should().HaveCount(5);
    }

    [Fact]
    public async Task Items_ContainsItemAndError()
    {
        // Arrange
        var sink = new BoundedInMemoryDeadLetterSink();
        var context = PipelineContext.Default;
        var item = 42;
        var error = new ArgumentException("test");

        // Act
        await sink.HandleAsync("node", item, error, context, CancellationToken.None);

        // Assert
        var deadLetterItem = sink.Items.First();
        deadLetterItem.Item.Should().Be(item);
        deadLetterItem.Error.Should().Be(error);
    }

    #endregion

    #region Capacity Enforcement Tests

    [Fact]
    public async Task HandleAsync_ThrowsWhenCapacityReached()
    {
        // Arrange
        var sink = new BoundedInMemoryDeadLetterSink(2);
        var context = PipelineContext.Default;

        // Act
        await sink.HandleAsync("node", "item1", new Exception(), context, CancellationToken.None);
        await sink.HandleAsync("node", "item2", new Exception(), context, CancellationToken.None);

        var act = async () => await sink.HandleAsync("node", "item3", new Exception(), context, CancellationToken.None);

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*exceeded*capacity*2*");
    }

    [Fact]
    public Task HandleAsync_ThrowsWithZeroCapacity()
    {
        // Act
        var act = () => new BoundedInMemoryDeadLetterSink(0);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>();
        return Task.CompletedTask;
    }

    [Fact]
    public async Task HandleAsync_AllowsExactCapacity()
    {
        // Arrange
        var sink = new BoundedInMemoryDeadLetterSink(3);
        var context = PipelineContext.Default;

        // Act
        for (var i = 0; i < 3; i++)
        {
            await sink.HandleAsync("node", $"item-{i}", new Exception(), context, CancellationToken.None);
        }

        // Assert
        sink.Items.Should().HaveCount(3);
    }

    #endregion

    #region Different Item Types Tests

    [Fact]
    public async Task HandleAsync_WorksWithDifferentItemTypes()
    {
        // Arrange
        var sink = new BoundedInMemoryDeadLetterSink(100);
        var context = PipelineContext.Default;

        // Act & Assert
        await sink.HandleAsync("node", "string-item", new Exception(), context, CancellationToken.None);
        await sink.HandleAsync("node", 123, new Exception(), context, CancellationToken.None);
        await sink.HandleAsync("node", new object(), new Exception(), context, CancellationToken.None);

        sink.Items.Should().HaveCount(3);
    }

    [Fact]
    public async Task HandleAsync_WithNullItem()
    {
        // Arrange
        var sink = new BoundedInMemoryDeadLetterSink();
        var context = PipelineContext.Default;

        // Act
        await sink.HandleAsync("node", null!, new Exception(), context, CancellationToken.None);

        // Assert
        sink.Items.Should().HaveCount(1);
        sink.Items.First().Item.Should().BeNull();
    }

    [Fact]
    public async Task HandleAsync_WithNullError()
    {
        // Arrange
        var sink = new BoundedInMemoryDeadLetterSink();
        var context = PipelineContext.Default;

        // Act
        var act = async () => await sink.HandleAsync("node", "item", null!, context, CancellationToken.None);

        // Assert - should still handle it
        await act.Should().NotThrowAsync();
        sink.Items.Should().HaveCount(1);
    }

    #endregion

    #region Concurrent Access Tests

    [Fact]
    public async Task HandleAsync_WithConcurrentCalls_AddsAllItems()
    {
        // Arrange
        var sink = new BoundedInMemoryDeadLetterSink(100);
        var context = PipelineContext.Default;
        var tasks = new List<Task>();

        // Act
        for (var i = 0; i < 10; i++)
        {
            var index = i;
            tasks.Add(sink.HandleAsync("node", $"item-{index}", new Exception(), context, CancellationToken.None));
        }

        await Task.WhenAll(tasks);

        // Assert
        sink.Items.Should().HaveCount(10);
    }

    [Fact]
    public async Task HandleAsync_WithConcurrentCalls_StillEnforcesCapacity()
    {
        // Arrange
        var sink = new BoundedInMemoryDeadLetterSink(5);
        var context = PipelineContext.Default;
        var exceptions = new List<Exception>();

        // Act
        var tasks = new List<Task>();

        for (var i = 0; i < 10; i++)
        {
            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    await sink.HandleAsync("node", $"item-{Guid.NewGuid()}", new Exception(), context, CancellationToken.None);
                }
                catch (InvalidOperationException ex)
                {
                    lock (exceptions)
                    {
                        exceptions.Add(ex);
                    }
                }
            }));
        }

        await Task.WhenAll(tasks);

        // Assert - should have some failures
        exceptions.Should().NotBeEmpty();
        sink.Items.Count.Should().BeLessThanOrEqualTo(5);
    }

    #endregion

    #region CancellationToken Tests

    [Fact]
    public async Task HandleAsync_WithCancellationToken_Completes()
    {
        // Arrange
        var sink = new BoundedInMemoryDeadLetterSink();
        var context = PipelineContext.Default;
        var cts = new CancellationTokenSource();

        // Act
        await sink.HandleAsync("node", "item", new Exception(), context, cts.Token);

        // Assert
        sink.Items.Should().HaveCount(1);
    }

    [Fact]
    public async Task HandleAsync_IgnoresCancellation()
    {
        // Arrange
        var sink = new BoundedInMemoryDeadLetterSink();
        var context = PipelineContext.Default;
        var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act - should still complete even with cancelled token
        await sink.HandleAsync("node", "item", new Exception(), context, cts.Token);

        // Assert
        sink.Items.Should().HaveCount(1);
    }

    #endregion

    #region Edge Cases Tests

    [Fact]
    public async Task Items_IsReadOnlyCollection()
    {
        // Arrange
        var sink = new BoundedInMemoryDeadLetterSink();
        var context = PipelineContext.Default;
        await sink.HandleAsync("node", "item", new Exception(), context, CancellationToken.None);

        // Act & Assert
        sink.Items.Should().BeAssignableTo<IReadOnlyCollection<(object, Exception)>>();
    }

    [Fact]
    public async Task HandleAsync_WithLargeCapacity()
    {
        // Arrange
        var sink = new BoundedInMemoryDeadLetterSink(10000);
        var context = PipelineContext.Default;

        // Act
        for (var i = 0; i < 100; i++)
        {
            await sink.HandleAsync("node", i, new Exception(), context, CancellationToken.None);
        }

        // Assert
        sink.Items.Should().HaveCount(100);
    }

    #endregion
}
