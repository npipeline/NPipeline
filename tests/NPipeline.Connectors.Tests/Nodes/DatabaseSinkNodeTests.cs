using System.Data;
using AwesomeAssertions;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.Nodes;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Abstractions;
using Xunit;

namespace NPipeline.Connectors.Tests.Nodes;

public class DatabaseSinkNodeTests
{
    [Fact]
    public void Defaults_ExposeExpectedValues()
    {
        var node = new InspectableDatabaseSinkNode<int>();

        node.UseTransactionSetting.Should().BeFalse();
        node.BatchSizeSetting.Should().Be(100);
        node.DeliverySemanticSetting.Should().Be(DeliverySemantic.AtLeastOnce);
        node.CheckpointStrategySetting.Should().Be(CheckpointStrategy.None);
        node.ContinueOnErrorSetting.Should().BeFalse();
    }

    [Fact]
    public void Overrides_ExposeCustomValues()
    {
        var node = new InspectableDatabaseSinkNode<int>(
            true,
            32,
            DeliverySemantic.ExactlyOnce,
            CheckpointStrategy.InMemory,
            true);

        node.UseTransactionSetting.Should().BeTrue();
        node.BatchSizeSetting.Should().Be(32);
        node.DeliverySemanticSetting.Should().Be(DeliverySemantic.ExactlyOnce);
        node.CheckpointStrategySetting.Should().Be(CheckpointStrategy.InMemory);
        node.ContinueOnErrorSetting.Should().BeTrue();
    }

    [Fact]
    public async Task ExecuteAsync_BatchesDataAndFlushesOnce()
    {
        var node = new InspectableDatabaseSinkNode<int>(batchSize: 2);
        var pipe = new InMemoryDataPipe<int>(new[] { 1, 2, 3, 4, 5 });

        await node.ExecuteAsync(pipe, new PipelineContext(), CancellationToken.None);

        node.Writer.Batches.Should().HaveCount(3);
        node.Writer.Batches.SelectMany(batch => batch).Should().Equal(1, 2, 3, 4, 5);
        node.Writer.FlushCount.Should().Be(1);
    }

    [Fact]
    public async Task ExecuteAsync_WhenWriterFailsAndContinueOnErrorFalse_PropagatesException()
    {
        var writer = new FailingDatabaseWriter<int>();
        var node = new DelegatingDatabaseSinkNode<int>(writer, false, 1);
        var pipe = new InMemoryDataPipe<int>(new[] { 1 });

        var act = () => node.ExecuteAsync(pipe, new PipelineContext(), CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>();
        writer.CallCount.Should().Be(1);
    }

    [Fact]
    public async Task ExecuteAsync_WhenWriterFailsAndContinueOnErrorTrue_SwallowsException()
    {
        var writer = new FailingDatabaseWriter<int>();
        var node = new DelegatingDatabaseSinkNode<int>(writer, true, 1);
        var pipe = new InMemoryDataPipe<int>(new[] { 1, 2 });

        await node.ExecuteAsync(pipe, new PipelineContext(), CancellationToken.None);

        writer.CallCount.Should().Be(2);
    }

    [Fact]
    public async Task ExecuteAsync_WhenCancelled_ThrowsOperationCanceled()
    {
        var node = new InspectableDatabaseSinkNode<int>();
        var cts = new CancellationTokenSource();
        cts.Cancel();

        var pipe = new InMemoryDataPipe<int>(new[] { 1, 2, 3 });

        var act = () => node.ExecuteAsync(pipe, new PipelineContext(), cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    private sealed class InspectableDatabaseSinkNode<T> : DatabaseSinkNode<T>
    {
        private readonly int? _batchSize;
        private readonly CheckpointStrategy? _checkpointStrategy;
        private readonly bool? _continueOnError;
        private readonly DeliverySemantic? _deliverySemantic;
        private readonly bool? _useTransaction;

        public InspectableDatabaseSinkNode(
            bool? useTransaction = null,
            int? batchSize = null,
            DeliverySemantic? deliverySemantic = null,
            CheckpointStrategy? checkpointStrategy = null,
            bool? continueOnError = null)
        {
            _useTransaction = useTransaction;
            _batchSize = batchSize;
            _deliverySemantic = deliverySemantic;
            _checkpointStrategy = checkpointStrategy;
            _continueOnError = continueOnError;
        }

        public bool UseTransactionSetting => UseTransaction;
        public int BatchSizeSetting => BatchSize;
        public DeliverySemantic DeliverySemanticSetting => DeliverySemantic;
        public CheckpointStrategy CheckpointStrategySetting => CheckpointStrategy;
        public bool ContinueOnErrorSetting => ContinueOnError;
        public RecordingDatabaseWriter<T> Writer { get; private set; } = null!;

        protected override bool UseTransaction => _useTransaction ?? base.UseTransaction;
        protected override int BatchSize => _batchSize ?? base.BatchSize;
        protected override DeliverySemantic DeliverySemantic => _deliverySemantic ?? base.DeliverySemantic;
        protected override CheckpointStrategy CheckpointStrategy => _checkpointStrategy ?? base.CheckpointStrategy;
        protected override bool ContinueOnError => _continueOnError ?? base.ContinueOnError;

        protected override Task<IDatabaseConnection> GetConnectionAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult<IDatabaseConnection>(new NoopDatabaseConnection());
        }

        protected override Task<IDatabaseWriter<T>> CreateWriterAsync(IDatabaseConnection connection, CancellationToken cancellationToken)
        {
            Writer = new RecordingDatabaseWriter<T>();
            return Task.FromResult<IDatabaseWriter<T>>(Writer);
        }
    }

    private sealed class DelegatingDatabaseSinkNode<T> : DatabaseSinkNode<T>
    {
        private readonly int? _batchSize;
        private readonly IDatabaseWriter<T> _writer;

        public DelegatingDatabaseSinkNode(IDatabaseWriter<T> writer, bool continueOnError, int? batchSize)
        {
            _writer = writer;
            ContinueOnError = continueOnError;
            _batchSize = batchSize;
        }

        protected override bool ContinueOnError { get; }

        protected override int BatchSize => _batchSize ?? base.BatchSize;

        protected override Task<IDatabaseConnection> GetConnectionAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult<IDatabaseConnection>(new NoopDatabaseConnection());
        }

        protected override Task<IDatabaseWriter<T>> CreateWriterAsync(IDatabaseConnection connection, CancellationToken cancellationToken)
        {
            return Task.FromResult(_writer);
        }
    }

    private sealed class RecordingDatabaseWriter<T> : IDatabaseWriter<T>
    {
        public List<IReadOnlyList<T>> Batches { get; } = new();
        public int FlushCount { get; private set; }

        public Task WriteAsync(T item, CancellationToken cancellationToken = default)
        {
            return WriteBatchAsync(new[] { item }, cancellationToken);
        }

        public Task WriteBatchAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
        {
            Batches.Add(items.ToList());
            return Task.CompletedTask;
        }

        public Task FlushAsync(CancellationToken cancellationToken = default)
        {
            FlushCount++;
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class FailingDatabaseWriter<T> : IDatabaseWriter<T>
    {
        public int CallCount { get; private set; }

        public Task WriteAsync(T item, CancellationToken cancellationToken = default)
        {
            return WriteBatchAsync(new[] { item }, cancellationToken);
        }

        public Task WriteBatchAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
        {
            CallCount++;
            throw new InvalidOperationException("Write failure");
        }

        public Task FlushAsync(CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
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
            return Task.FromResult<IDatabaseReader>(new NoopDatabaseReader());
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

    private sealed class NoopDatabaseReader : IDatabaseReader
    {
        public bool HasRows => false;
        public int FieldCount => 0;

        public string GetName(int ordinal)
        {
            return string.Empty;
        }

        public Type GetFieldType(int ordinal)
        {
            return typeof(object);
        }

        public Task<bool> ReadAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromResult(false);
        }

        public Task<bool> NextResultAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromResult(false);
        }

        public T? GetFieldValue<T>(int ordinal)
        {
            return default;
        }

        public bool IsDBNull(int ordinal)
        {
            return true;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }
}
