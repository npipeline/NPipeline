using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.Execution.Pooling;
using NPipeline.Nodes;

namespace NPipeline.Tests.Core.Execution.Pooling;

/// <summary>
///     Tests for the PipelineObjectPool static class.
/// </summary>
public class PipelineObjectPoolTests
{
    [Fact]
    public void RentStringList_ReturnsEmptyList()
    {
        // Act
        var list = PipelineObjectPool.RentStringList();

        // Assert
        Assert.NotNull(list);
        Assert.Empty(list);
        Assert.IsType<List<string>>(list);

        // Cleanup
        PipelineObjectPool.Return(list);
    }

    [Fact]
    public void RentStringList_WithReturn_ReusesInstance()
    {
        // Arrange
        var list1 = PipelineObjectPool.RentStringList();
        list1.Add("test");

        // Act - Return and rent again
        PipelineObjectPool.Return(list1);
        var list2 = PipelineObjectPool.RentStringList();

        // Assert - Should get same instance, but cleared
        Assert.Same(list1, list2);
        Assert.Empty(list2);

        // Cleanup
        PipelineObjectPool.Return(list2);
    }

    [Fact]
    public void RentStringList_MultipleRents_ReturnsNewInstances()
    {
        // Act
        var list1 = PipelineObjectPool.RentStringList();
        var list2 = PipelineObjectPool.RentStringList();
        var list3 = PipelineObjectPool.RentStringList();

        // Assert - Should get different instances
        Assert.NotSame(list1, list2);
        Assert.NotSame(list2, list3);
        Assert.NotSame(list1, list3);

        // Cleanup
        PipelineObjectPool.Return(list1);
        PipelineObjectPool.Return(list2);
        PipelineObjectPool.Return(list3);
    }

    [Fact]
    public void ReturnStringList_WithOversizedList_DoesNotPool()
    {
        // Arrange
        var list = PipelineObjectPool.RentStringList();

        for (var i = 0; i < 150; i++) // Exceed MaxPooledCapacity (100)
        {
            list.Add($"item{i}");
        }

        // Act
        PipelineObjectPool.Return(list);
        var newList = PipelineObjectPool.RentStringList();

        // Assert - Should get different instance (original not pooled)
        Assert.NotSame(list, newList);

        // Cleanup
        PipelineObjectPool.Return(newList);
    }

    [Fact]
    public void RentStringIntDictionary_ReturnsEmptyDictionary()
    {
        // Act
        var dict = PipelineObjectPool.RentStringIntDictionary();

        // Assert
        Assert.NotNull(dict);
        Assert.Empty(dict);
        Assert.IsType<Dictionary<string, int>>(dict);

        // Cleanup
        PipelineObjectPool.Return(dict);
    }

    [Fact]
    public void RentStringIntDictionary_WithReturn_ReusesInstance()
    {
        // Arrange
        var dict1 = PipelineObjectPool.RentStringIntDictionary();
        dict1["key"] = 42;

        // Act - Return and rent again
        PipelineObjectPool.Return(dict1);
        var dict2 = PipelineObjectPool.RentStringIntDictionary();

        // Assert - Should get same instance, but cleared
        Assert.Same(dict1, dict2);
        Assert.Empty(dict2);

        // Cleanup
        PipelineObjectPool.Return(dict2);
    }

    [Fact]
    public void ReturnStringIntDictionary_WithOversizedDictionary_DoesNotPool()
    {
        // Arrange
        var dict = PipelineObjectPool.RentStringIntDictionary();

        for (var i = 0; i < 150; i++) // Exceed MaxPooledCapacity (100)
        {
            dict[$"key{i}"] = i;
        }

        // Act
        PipelineObjectPool.Return(dict);
        var newDict = PipelineObjectPool.RentStringIntDictionary();

        // Assert - Should get different instance (original not pooled)
        Assert.NotSame(dict, newDict);

        // Cleanup
        PipelineObjectPool.Return(newDict);
    }

    [Fact]
    public void RentStringObjectDictionary_ReturnsEmptyDictionary()
    {
        // Act
        var dict = PipelineObjectPool.RentStringObjectDictionary();

        // Assert
        Assert.NotNull(dict);
        Assert.Empty(dict);

        // Cleanup
        PipelineObjectPool.Return(dict);
    }

    [Fact]
    public void RentStringObjectDictionary_WithReturn_ReusesInstance()
    {
        // Arrange
        var dict1 = PipelineObjectPool.RentStringObjectDictionary();
        dict1["key"] = "value";

        // Act
        PipelineObjectPool.Return(dict1);
        var dict2 = PipelineObjectPool.RentStringObjectDictionary();

        // Assert
        Assert.Same(dict1, dict2);
        Assert.Empty(dict2);

        // Cleanup
        PipelineObjectPool.Return(dict2);
    }

    [Fact]
    public void ReturnStringObjectDictionary_WithOversizedDictionary_DoesNotPool()
    {
        // Arrange
        var dict = PipelineObjectPool.RentStringObjectDictionary();

        for (var i = 0; i < 150; i++)
        {
            dict[$"key{i}"] = i;
        }

        // Act
        PipelineObjectPool.Return(dict);
        var newDict = PipelineObjectPool.RentStringObjectDictionary();

        // Assert
        Assert.NotSame(dict, newDict);

        // Cleanup
        PipelineObjectPool.Return(newDict);
    }

    [Fact]
    public void RentStringQueue_ReturnsEmptyQueue()
    {
        // Act
        var queue = PipelineObjectPool.RentStringQueue();

        // Assert
        Assert.NotNull(queue);
        Assert.Empty(queue);
        Assert.IsType<Queue<string>>(queue);

        // Cleanup
        PipelineObjectPool.Return(queue);
    }

    [Fact]
    public void RentStringQueue_WithReturn_ReusesInstance()
    {
        // Arrange
        var queue1 = PipelineObjectPool.RentStringQueue();
        queue1.Enqueue("test");

        // Act - Return and rent again
        PipelineObjectPool.Return(queue1);
        var queue2 = PipelineObjectPool.RentStringQueue();

        // Assert - Should get same instance, but cleared
        Assert.Same(queue1, queue2);
        Assert.Empty(queue2);

        // Cleanup
        PipelineObjectPool.Return(queue2);
    }

    [Fact]
    public void ReturnStringQueue_WithOversizedQueue_DoesNotPool()
    {
        // Arrange
        var queue = PipelineObjectPool.RentStringQueue();

        for (var i = 0; i < 150; i++) // Exceed MaxPooledCapacity (100)
        {
            queue.Enqueue($"item{i}");
        }

        // Act
        PipelineObjectPool.Return(queue);
        var newQueue = PipelineObjectPool.RentStringQueue();

        // Assert - Should get different instance (original not pooled)
        Assert.NotSame(queue, newQueue);

        // Cleanup
        PipelineObjectPool.Return(newQueue);
    }

    [Fact]
    public void RentStringHashSet_ReturnsEmptyHashSet()
    {
        // Act
        var hashSet = PipelineObjectPool.RentStringHashSet();

        // Assert
        Assert.NotNull(hashSet);
        Assert.Empty(hashSet);
        Assert.IsType<HashSet<string>>(hashSet);

        // Cleanup
        PipelineObjectPool.Return(hashSet);
    }

    [Fact]
    public void RentStringHashSet_WithReturn_ReusesInstance()
    {
        // Arrange
        var hashSet1 = PipelineObjectPool.RentStringHashSet();
        hashSet1.Add("test");

        // Act - Return and rent again
        PipelineObjectPool.Return(hashSet1);
        var hashSet2 = PipelineObjectPool.RentStringHashSet();

        // Assert - Should get same instance, but cleared
        Assert.Same(hashSet1, hashSet2);
        Assert.Empty(hashSet2);

        // Cleanup
        PipelineObjectPool.Return(hashSet2);
    }

    [Fact]
    public void ReturnStringHashSet_WithOversizedHashSet_DoesNotPool()
    {
        // Arrange
        var hashSet = PipelineObjectPool.RentStringHashSet();

        for (var i = 0; i < 150; i++) // Exceed MaxPooledCapacity (100)
        {
            hashSet.Add($"item{i}");
        }

        // Act
        PipelineObjectPool.Return(hashSet);
        var newHashSet = PipelineObjectPool.RentStringHashSet();

        // Assert - Should get different instance (original not pooled)
        Assert.NotSame(hashSet, newHashSet);

        // Cleanup
        PipelineObjectPool.Return(newHashSet);
    }

    [Fact]
    public async Task ConcurrentRentReturn_StringList_IsThreadSafe()
    {
        // Arrange
        const int threadCount = 10;
        const int iterationsPerThread = 100;
        var tasks = new Task[threadCount];

        // Act
        for (var i = 0; i < threadCount; i++)
        {
            tasks[i] = Task.Run(async () =>
            {
                for (var j = 0; j < iterationsPerThread; j++)
                {
                    var list = PipelineObjectPool.RentStringList();
                    list.Add("test");
                    list.Add("test2");
                    await Task.Delay(1); // Simulate some work
                    PipelineObjectPool.Return(list);
                }
            });
        }

        await Task.WhenAll(tasks);

        // Assert - Verify pool is still functional
        var finalList = PipelineObjectPool.RentStringList();
        Assert.NotNull(finalList);
        Assert.Empty(finalList);
        PipelineObjectPool.Return(finalList);
    }

    [Fact]
    public async Task ConcurrentRentReturn_StringIntDictionary_IsThreadSafe()
    {
        // Arrange
        const int threadCount = 10;
        const int iterationsPerThread = 100;
        var tasks = new Task[threadCount];

        // Act
        for (var i = 0; i < threadCount; i++)
        {
            tasks[i] = Task.Run(async () =>
            {
                for (var j = 0; j < iterationsPerThread; j++)
                {
                    var dict = PipelineObjectPool.RentStringIntDictionary();
                    dict["key1"] = 1;
                    dict["key2"] = 2;
                    await Task.Delay(1); // Simulate some work
                    PipelineObjectPool.Return(dict);
                }
            });
        }

        await Task.WhenAll(tasks);

        // Assert - Verify pool is still functional
        var finalDict = PipelineObjectPool.RentStringIntDictionary();
        Assert.NotNull(finalDict);
        Assert.Empty(finalDict);
        PipelineObjectPool.Return(finalDict);
    }

    [Fact]
    public async Task ConcurrentRentReturn_StringQueue_IsThreadSafe()
    {
        // Arrange
        const int threadCount = 10;
        const int iterationsPerThread = 100;
        var tasks = new Task[threadCount];

        // Act
        for (var i = 0; i < threadCount; i++)
        {
            tasks[i] = Task.Run(async () =>
            {
                for (var j = 0; j < iterationsPerThread; j++)
                {
                    var queue = PipelineObjectPool.RentStringQueue();
                    queue.Enqueue("test1");
                    queue.Enqueue("test2");
                    await Task.Delay(1); // Simulate some work
                    PipelineObjectPool.Return(queue);
                }
            });
        }

        await Task.WhenAll(tasks);

        // Assert - Verify pool is still functional
        var finalQueue = PipelineObjectPool.RentStringQueue();
        Assert.NotNull(finalQueue);
        Assert.Empty(finalQueue);
        PipelineObjectPool.Return(finalQueue);
    }

    [Fact]
    public async Task ConcurrentRentReturn_StringHashSet_IsThreadSafe()
    {
        // Arrange
        const int threadCount = 10;
        const int iterationsPerThread = 100;
        var tasks = new Task[threadCount];

        // Act
        for (var i = 0; i < threadCount; i++)
        {
            tasks[i] = Task.Run(async () =>
            {
                for (var j = 0; j < iterationsPerThread; j++)
                {
                    var hashSet = PipelineObjectPool.RentStringHashSet();
                    hashSet.Add("test1");
                    hashSet.Add("test2");
                    await Task.Delay(1); // Simulate some work
                    PipelineObjectPool.Return(hashSet);
                }
            });
        }

        await Task.WhenAll(tasks);

        // Assert - Verify pool is still functional
        var finalHashSet = PipelineObjectPool.RentStringHashSet();
        Assert.NotNull(finalHashSet);
        Assert.Empty(finalHashSet);
        PipelineObjectPool.Return(finalHashSet);
    }

    [Fact]
    public void RentNodeDictionary_ReturnsEmptyDictionary()
    {
        // Act
        var dict = PipelineObjectPool.RentNodeDictionary();

        // Assert
        Assert.NotNull(dict);
        Assert.Empty(dict);

        // Cleanup
        PipelineObjectPool.Return(dict);
    }

    [Fact]
    public void RentNodeDictionary_WithReturn_ReusesInstance()
    {
        // Arrange
        var dict1 = PipelineObjectPool.RentNodeDictionary();
        dict1["node"] = new TestNode();

        // Act
        PipelineObjectPool.Return(dict1);
        var dict2 = PipelineObjectPool.RentNodeDictionary();

        // Assert
        Assert.Same(dict1, dict2);
        Assert.Empty(dict2);

        // Cleanup
        PipelineObjectPool.Return(dict2);
    }

    [Fact]
    public void ReturnNodeDictionary_WithOversizedDictionary_DoesNotPool()
    {
        // Arrange
        var dict = PipelineObjectPool.RentNodeDictionary();

        for (var i = 0; i < 150; i++)
        {
            dict[$"node{i}"] = new TestNode();
        }

        // Act
        PipelineObjectPool.Return(dict);
        var newDict = PipelineObjectPool.RentNodeDictionary();

        // Assert
        Assert.NotSame(dict, newDict);

        // Cleanup
        PipelineObjectPool.Return(newDict);
    }

    [Fact]
    public void RentNodeOutputDictionary_ReturnsEmptyDictionary()
    {
        // Act
        var dict = PipelineObjectPool.RentNodeOutputDictionary();

        // Assert
        Assert.NotNull(dict);
        Assert.Empty(dict);

        // Cleanup
        PipelineObjectPool.Return(dict);
    }

    [Fact]
    public void RentNodeOutputDictionary_WithReturn_ReusesInstance()
    {
        // Arrange
        var dict1 = PipelineObjectPool.RentNodeOutputDictionary();
        dict1["node"] = new TestDataPipe();

        // Act
        PipelineObjectPool.Return(dict1);
        var dict2 = PipelineObjectPool.RentNodeOutputDictionary();

        // Assert
        Assert.Same(dict1, dict2);
        Assert.Empty(dict2);

        // Cleanup
        PipelineObjectPool.Return(dict2);
    }

    [Fact]
    public void ReturnNodeOutputDictionary_WithOversizedDictionary_DoesNotPool()
    {
        // Arrange
        var dict = PipelineObjectPool.RentNodeOutputDictionary();

        for (var i = 0; i < 150; i++)
        {
            dict[$"node{i}"] = new TestDataPipe();
        }

        // Act
        PipelineObjectPool.Return(dict);
        var newDict = PipelineObjectPool.RentNodeOutputDictionary();

        // Assert
        Assert.NotSame(dict, newDict);

        // Cleanup
        PipelineObjectPool.Return(newDict);
    }

    [Fact]
    public async Task ConcurrentRentReturn_StringObjectDictionary_IsThreadSafe()
    {
        // Arrange
        const int threadCount = 10;
        const int iterationsPerThread = 100;
        var tasks = new Task[threadCount];

        // Act
        for (var i = 0; i < threadCount; i++)
        {
            tasks[i] = Task.Run(async () =>
            {
                for (var j = 0; j < iterationsPerThread; j++)
                {
                    var dict = PipelineObjectPool.RentStringObjectDictionary();
                    dict["key1"] = "value";
                    dict["key2"] = 2;
                    await Task.Delay(1);
                    PipelineObjectPool.Return(dict);
                }
            });
        }

        await Task.WhenAll(tasks);

        // Assert
        var finalDict = PipelineObjectPool.RentStringObjectDictionary();
        Assert.NotNull(finalDict);
        Assert.Empty(finalDict);
        PipelineObjectPool.Return(finalDict);
    }

    [Fact]
    public async Task ConcurrentRentReturn_NodeDictionary_IsThreadSafe()
    {
        // Arrange
        const int threadCount = 10;
        const int iterationsPerThread = 100;
        var tasks = new Task[threadCount];

        // Act
        for (var i = 0; i < threadCount; i++)
        {
            tasks[i] = Task.Run(async () =>
            {
                for (var j = 0; j < iterationsPerThread; j++)
                {
                    var dict = PipelineObjectPool.RentNodeDictionary();
                    dict[$"node{j}"] = new TestNode();
                    await Task.Delay(1);
                    PipelineObjectPool.Return(dict);
                }
            });
        }

        await Task.WhenAll(tasks);

        // Assert
        var finalDict = PipelineObjectPool.RentNodeDictionary();
        Assert.NotNull(finalDict);
        Assert.Empty(finalDict);
        PipelineObjectPool.Return(finalDict);
    }

    [Fact]
    public async Task ConcurrentRentReturn_NodeOutputDictionary_IsThreadSafe()
    {
        // Arrange
        const int threadCount = 10;
        const int iterationsPerThread = 100;
        var tasks = new Task[threadCount];

        // Act
        for (var i = 0; i < threadCount; i++)
        {
            tasks[i] = Task.Run(async () =>
            {
                for (var j = 0; j < iterationsPerThread; j++)
                {
                    var dict = PipelineObjectPool.RentNodeOutputDictionary();
                    dict[$"node{j}"] = new TestDataPipe();
                    await Task.Delay(1);
                    PipelineObjectPool.Return(dict);
                }
            });
        }

        await Task.WhenAll(tasks);

        // Assert
        var finalDict = PipelineObjectPool.RentNodeOutputDictionary();
        Assert.NotNull(finalDict);
        Assert.Empty(finalDict);
        PipelineObjectPool.Return(finalDict);
    }

    private sealed class TestNode : INode
    {
        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class TestDataPipe : IDataPipe
    {
        public string StreamName => "test";

        public Type GetDataType()
        {
            return typeof(object);
        }

        public IAsyncEnumerable<object?> ToAsyncEnumerable(CancellationToken cancellationToken = default)
        {
            return Empty(cancellationToken);
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }

        private static async IAsyncEnumerable<object?> Empty([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }
    }
}
