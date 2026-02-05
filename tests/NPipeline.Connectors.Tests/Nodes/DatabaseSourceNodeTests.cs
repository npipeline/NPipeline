using System.Data;
using System.Reflection;
using FluentAssertions;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.Nodes;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Abstractions;
using Xunit;

namespace NPipeline.Connectors.Tests.Nodes;

public class DatabaseSourceNodeTests
{
    [Fact]
    public void Initialize_WithStreamingResults_ReturnsStreamingDataPipe()
    {
        var node = new TestDatabaseSourceNode(new[] { 1, 2, 3 }, true);

        var result = node.Initialize(new PipelineContext(), CancellationToken.None);

        result.Should().BeOfType<StreamingDataPipe<int>>();
    }

    [Fact]
    public void Initialize_WithBufferedResults_ReturnsAllItems()
    {
        ResetCheckpoints();
        var node = new TestDatabaseSourceNode(new[] { 1, 2, 3 }, false);

        var result = node.Initialize(new PipelineContext(), CancellationToken.None);

        var dataPipe = result.Should().BeOfType<InMemoryDataPipe<int>>().Subject;
        dataPipe.Items.Should().Equal(1, 2, 3);
    }

    [Fact]
    public void TryMapRow_SkipsItemsWhenMapperReturnsFalse()
    {
        ResetCheckpoints();

        var node = new TestDatabaseSourceNode(
            new[] { 1, 2, 3, 4 },
            false,
            shouldEmit: value => value % 2 == 0);

        var result = node.Initialize(new PipelineContext(), CancellationToken.None);

        var dataPipe = result.Should().BeOfType<InMemoryDataPipe<int>>().Subject;
        dataPipe.Items.Should().Equal(2, 4);
    }

    [Fact]
    public void Initialize_WithInMemoryCheckpoint_SkipsProcessedRowsAndClearsCheckpoint()
    {
        const string checkpointId = "TestCheckpoint";
        ResetCheckpoints();
        SetCheckpoint(checkpointId, 2);

        var node = new TestDatabaseSourceNode(
            new[] { 1, 2, 3, 4 },
            false,
            CheckpointStrategy.InMemory,
            checkpointId: checkpointId);

        var result = node.Initialize(new PipelineContext(), CancellationToken.None);

        var dataPipe = result.Should().BeOfType<InMemoryDataPipe<int>>().Subject;
        dataPipe.Items.Should().Equal(3, 4);
        GetCheckpointStore().Should().BeEmpty();
    }

    private static Dictionary<string, long> GetCheckpointStore()
    {
        var field = typeof(DatabaseSourceNode<,>)
            .MakeGenericType(typeof(FakeDatabaseReader), typeof(int))
            .GetField("_checkpoints", BindingFlags.Static | BindingFlags.NonPublic);

        return (Dictionary<string, long>)(field?.GetValue(null) ?? new Dictionary<string, long>());
    }

    private static void ResetCheckpoints()
    {
        GetCheckpointStore().Clear();
    }

    private static void SetCheckpoint(string checkpointId, long value)
    {
        var store = GetCheckpointStore();
        store[checkpointId] = value;
    }

    private sealed class TestDatabaseSourceNode : DatabaseSourceNode<FakeDatabaseReader, int>
    {
        private readonly IReadOnlyList<int> _data;
        private readonly Func<int, bool>? _shouldEmit;

        public TestDatabaseSourceNode(
            IReadOnlyList<int> data,
            bool streamResults,
            CheckpointStrategy checkpointStrategy = CheckpointStrategy.None,
            Func<int, bool>? shouldEmit = null,
            string? checkpointId = null)
        {
            _data = data;
            StreamResults = streamResults;
            CheckpointStrategy = checkpointStrategy;
            _shouldEmit = shouldEmit;
            CheckpointId = checkpointId ?? GetType().Name;
        }

        protected override bool StreamResults { get; }

        protected override CheckpointStrategy CheckpointStrategy { get; }

        protected override string CheckpointId { get; }

        protected override Task<IDatabaseConnection> GetConnectionAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult<IDatabaseConnection>(new NoopDatabaseConnection());
        }

        protected override Task<FakeDatabaseReader> ExecuteQueryAsync(IDatabaseConnection connection, CancellationToken cancellationToken)
        {
            return Task.FromResult(new FakeDatabaseReader(_data));
        }

        protected override int MapRow(FakeDatabaseReader reader)
        {
            return reader.GetFieldValue<int>(0);
        }

        protected override bool TryMapRow(FakeDatabaseReader reader, out int item)
        {
            item = MapRow(reader);
            return _shouldEmit?.Invoke(item) ?? true;
        }
    }

    private sealed class FakeDatabaseReader : IDatabaseReader
    {
        private readonly IReadOnlyList<int> _data;
        private int _index = -1;

        public FakeDatabaseReader(IReadOnlyList<int> data)
        {
            _data = data;
        }

        public bool HasRows => _data.Count > 0;
        public int FieldCount => 1;

        public string GetName(int ordinal)
        {
            return "Value";
        }

        public Type GetFieldType(int ordinal)
        {
            return typeof(int);
        }

        public Task<bool> ReadAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _index++;
            return Task.FromResult(_index < _data.Count);
        }

        public Task<bool> NextResultAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromResult(false);
        }

        public T? GetFieldValue<T>(int ordinal)
        {
            return (T?)(object?)_data[_index];
        }

        public bool IsDBNull(int ordinal)
        {
            return false;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class NoopDatabaseConnection : IDatabaseConnection
    {
        public bool IsOpen => true;

        public Task OpenAsync(CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        public Task CloseAsync(CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        public Task<IDatabaseCommand> CreateCommandAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromResult<IDatabaseCommand>(new NoopDatabaseCommand());
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class NoopDatabaseCommand : IDatabaseCommand
    {
        public string CommandText { get; set; } = string.Empty;
        public int CommandTimeout { get; set; } = 30;
        public CommandType CommandType { get; set; } = CommandType.Text;

        public void AddParameter(string name, object? value)
        {
        }

        public Task<IDatabaseReader> ExecuteReaderAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromResult<IDatabaseReader>(new FakeDatabaseReader(Array.Empty<int>()));
        }

        public Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromResult(0);
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }
}
