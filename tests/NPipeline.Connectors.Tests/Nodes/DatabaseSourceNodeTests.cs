using System.Data;
using AwesomeAssertions;
using NPipeline.Connectors.Checkpointing;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.Nodes;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Abstractions;
using Xunit;

namespace NPipeline.Connectors.Tests.Nodes;

public class DatabaseSourceNodeTests
{
    [Fact]
    public void Initialize_WithStreamingResults_ReturnsStreamingDataStream()
    {
        var node = new TestDatabaseSourceNode(new[] { 1, 2, 3 }, true);

        var result = node.OpenStream(new PipelineContext(), CancellationToken.None);

        result.Should().BeOfType<DataStream<int>>();
    }

    [Fact]
    public void Initialize_WithBufferedResults_ReturnsAllItems()
    {
        var node = new TestDatabaseSourceNode(new[] { 1, 2, 3 }, false);

        var result = node.OpenStream(new PipelineContext(), CancellationToken.None);

        var dataStream = result.Should().BeOfType<InMemoryDataStream<int>>().Subject;
        dataStream.Items.Should().Equal(1, 2, 3);
    }

    [Fact]
    public void TryMapRow_SkipsItemsWhenMapperReturnsFalse()
    {
        var node = new TestDatabaseSourceNode(
            new[] { 1, 2, 3, 4 },
            false,
            shouldEmit: value => value % 2 == 0);

        var result = node.OpenStream(new PipelineContext(), CancellationToken.None);

        var dataStream = result.Should().BeOfType<InMemoryDataStream<int>>().Subject;
        dataStream.Items.Should().Equal(2, 4);
    }

    [Fact]
    public async Task Initialize_WithInMemoryCheckpoint_SkipsAlreadyProcessedRows()
    {
        // Arrange — pre-seed checkpoint: 2 rows already processed.
        // PipelineContext.CurrentNodeId defaults to string.Empty (not null),
        // so DatabaseSourceNode uses "" as pipelineId (not "default").
        const string checkpointId = "TestCheckpoint";
        const string pipelineId = "";
        var storage = new InMemoryCheckpointStorage();
        await storage.SaveAsync(pipelineId, checkpointId, Checkpoint.Create("2"));

        var node = new TestDatabaseSourceNode(
            new[] { 1, 2, 3, 4 },
            false,
            CheckpointStrategy.InMemory,
            checkpointId: checkpointId,
            checkpointStorage: storage);

        // Act
        var result = node.OpenStream(new PipelineContext(), CancellationToken.None);

        // Assert — only rows 3 and 4 should be emitted
        var dataStream = result.Should().BeOfType<InMemoryDataStream<int>>().Subject;
        dataStream.Items.Should().Equal(3, 4);

        // Checkpoint should now reflect all rows processed (position 4)
        var finalCheckpoint = await storage.LoadAsync(pipelineId, checkpointId);
        finalCheckpoint.Should().NotBeNull();
        finalCheckpoint!.GetAsOffset().Should().Be(4);
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
            string? checkpointId = null,
            ICheckpointStorage? checkpointStorage = null)
        {
            _data = data;
            StreamResults = streamResults;
            CheckpointStrategy = checkpointStrategy;
            _shouldEmit = shouldEmit;
            CheckpointId = checkpointId ?? GetType().Name;
            CheckpointStorage = checkpointStorage;
        }

        protected override bool StreamResults { get; }

        protected override CheckpointStrategy CheckpointStrategy { get; }

        protected override string CheckpointId { get; }

        protected override ICheckpointStorage? CheckpointStorage { get; }

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

        public IDatabaseTransaction? CurrentTransaction => null;

        public Task<IDatabaseTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException("Transactions are not supported by this noop connection.");
        }

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
