using System.Runtime.CompilerServices;
using NPipeline.DataFlow;

namespace NPipeline.Tests.Common;

/// <summary>
///     A basic, in-memory implementation of IDataPipe for testing purposes.
///     It holds all items in a List, which is ideal for providing mock data to nodes in unit tests.
///     THIS IMPLEMENTATION SHOULD NOT BE USED IN PRODUCTION CODE as it buffers entire stream in memory.
/// </summary>
public sealed class InMemoryDataPipe<T>(IEnumerable<T> data, string streamName = "TestStream") : IDataPipe<T>
    where T : notnull
{
    private readonly List<T> _data = data?.ToList() ?? throw new ArgumentNullException(nameof(data));

    public string StreamName { get; } = streamName;

    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return new InMemoryDataPipeEnumerator(_data, cancellationToken);
    }

    public async IAsyncEnumerable<object?> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var item in _data)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return await Task.FromResult(item);
        }
    }

    public Type GetDataType()
    {
        return typeof(T);
    }

    public ValueTask DisposeAsync()
    {
        // Nothing to dispose for an in-memory list
        return ValueTask.CompletedTask;
    }

    private sealed class InMemoryDataPipeEnumerator(List<T> data, CancellationToken cancellationToken) : IAsyncEnumerator<T>
    {
        private int _currentIndex = -1;

        public T Current => data[_currentIndex];

        public ValueTask<bool> MoveNextAsync()
        {
            cancellationToken.ThrowIfCancellationRequested();
            _currentIndex++;
            return new ValueTask<bool>(_currentIndex < data.Count);
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }
}
