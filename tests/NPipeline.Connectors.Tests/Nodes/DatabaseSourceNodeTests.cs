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
    public void Initialize_WithStreamingResults_ReturnsDataStream()
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
        // Arrange - pre-seed checkpoint: 2 rows already processed.
        // A node outside a pipeline cannot resolve a node id, so the checkpoint is keyed on PipelineId.
        const string checkpointId = "TestCheckpoint";
        const string pipelineId = "default";
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

        // Assert - only rows 3 and 4 should be emitted
        var dataStream = result.Should().BeOfType<InMemoryDataStream<int>>().Subject;
        dataStream.Items.Should().Equal(3, 4);

        // Checkpoint should now reflect all rows processed (position 4)
        var finalCheckpoint = await storage.LoadAsync(pipelineId, checkpointId);
        finalCheckpoint.Should().NotBeNull();
        finalCheckpoint!.GetAsOffset().Should().Be(4);
    }

    [Fact]
    public async Task Initialize_WithinAPipeline_KeysTheCheckpointOnTheNodeId()
    {
        const string checkpointId = "TestCheckpoint";
        var storage = new InMemoryCheckpointStorage();
        await storage.SaveAsync("reader-node", checkpointId, Checkpoint.Create("2"));

        var node = new TestDatabaseSourceNode(
            new[] { 1, 2, 3, 4 },
            false,
            CheckpointStrategy.InMemory,
            checkpointId: checkpointId,
            checkpointStorage: storage);

        var context = new PipelineContext();
        context.NodeEnvironment.RegisterNode("reader-node", node);

        var result = node.OpenStream(context, CancellationToken.None);

        // The node resolves its own id, so it resumes from the checkpoint saved under that id.
        var dataStream = result.Should().BeOfType<InMemoryDataStream<int>>().Subject;
        dataStream.Items.Should().Equal(3, 4);
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

        protected override Task<IDatabaseConnection> GetConnectionAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IDatabaseConnection>(new NoopDatabaseConnection());

        protected override Task<FakeDatabaseReader> ExecuteQueryAsync(IDatabaseConnection connection, CancellationToken cancellationToken) =>
            Task.FromResult(new FakeDatabaseReader(_data));

        protected override int MapRow(FakeDatabaseReader reader) => reader.GetFieldValue<int>(0);

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

        public string GetName(int ordinal) => "Value";

        public Type GetFieldType(int ordinal) => typeof(int);

        public Task<bool> ReadAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _index++;
            return Task.FromResult(_index < _data.Count);
        }

        public Task<bool> NextResultAsync(CancellationToken cancellationToken = default) => Task.FromResult(false);

        public T? GetFieldValue<T>(int ordinal) => (T?)(object?)_data[_index];

        public bool IsDBNull(int ordinal) => false;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class NoopDatabaseConnection : IDatabaseConnection
    {
        public bool IsOpen => true;

        public IDatabaseTransaction? CurrentTransaction => null;

        public Task<IDatabaseTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default) =>
            throw new NotSupportedException("Transactions are not supported by this noop connection.");

        public Task OpenAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task CloseAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task<IDatabaseCommand> CreateCommandAsync(CancellationToken cancellationToken = default) =>
            Task.FromResult<IDatabaseCommand>(new NoopDatabaseCommand());

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class NoopDatabaseCommand : IDatabaseCommand
    {
        public string CommandText { get; set; } = string.Empty;
        public int CommandTimeout { get; set; } = 30;
        public CommandType CommandType { get; set; } = CommandType.Text;

        public void AddParameter(string name, object? value)
        {
        }

        public Task<IDatabaseReader> ExecuteReaderAsync(CancellationToken cancellationToken = default) =>
            Task.FromResult<IDatabaseReader>(new FakeDatabaseReader(Array.Empty<int>()));

        public Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default) => Task.FromResult(0);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
