using System.Data.Common;
using NPipeline.Connectors.Database;
using NPipeline.Connectors.Mapping;
using NPipeline.Connectors.Sql;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Tests.Sql;

/// <summary>A dialect with SQL Server's quotes and a small, adjustable parameter limit.</summary>
public sealed class TestDialect(int maxParameters = 100) : SqlDialect
{
    public override string Name => "test";

    public override int MaxParameters => maxParameters;

    protected override char OpenQuote => '[';

    protected override char CloseQuote => ']';

    public override string Upsert(string table, IReadOnlyList<string> columns, IReadOnlyList<string> keys, SqlUpsertAction onMatch, int rows) =>
        $"UPSERT {table} ({string.Join(", ", columns)}) ON ({string.Join(", ", keys)}) {onMatch} VALUES {Values(columns.Count, rows)}";
}

/// <summary>A connection provider that is never actually used: the test nodes below always supply a connector connection.</summary>
public sealed class UnreachableDatabaseConnectionProvider : IDatabaseConnectionProvider
{
    public string GetConnectionString(StorageUri uri) => throw new NotSupportedException();

    public Task<IDatabaseConnection> OpenConnectionAsync(StorageUri uri, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException();
}

public sealed record TestReadOptions : SqlSourceOptions
{
    public FakeDatabase? Database { get; init; }

    protected override bool HasConnectorConnection => Database is not null;
}

public sealed record TestWriteOptions : SqlSinkOptions
{
    public FakeDatabase? Database { get; init; }

    /// <summary>Attempts per batch for a <see cref="TransientFakeException" />, as a connector's resilience would make.</summary>
    public int Attempts { get; init; } = 1;

    public bool PerRow { get; init; }

    protected override bool HasConnectorConnection => Database is not null;
}

public sealed class TestSource<T> : SqlSourceNode<T>
{
    private readonly TestReadOptions _options;

    public TestSource(TestReadOptions options, Func<SqlRow, T>? map = null)
        : base(options, map, new RecordShapeOptions { Naming = options.Naming })
    {
        _options = options;
    }

    protected override string ConnectorName => "test";

    protected override IDatabaseConnectionProvider DefaultProvider => new UnreachableDatabaseConnectionProvider();

    protected override DbConnection CreateConnection(string connectionString) => throw new NotSupportedException();

    protected override async Task<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = _options.Database!.Connect();
        await connection.OpenAsync(cancellationToken);
        return connection;
    }
}

public sealed class TestSink<T> : SqlSinkNode<T>
{
    private readonly TestWriteOptions _options;

    public TestSink(TestWriteOptions options, SqlDialect? dialect = null)
        : base(options, dialect ?? new TestDialect(), new RecordShapeOptions { Naming = options.Naming })
    {
        _options = options;
    }

    protected override IDatabaseConnectionProvider DefaultProvider => new UnreachableDatabaseConnectionProvider();

    protected override DbConnection CreateConnection(string connectionString) => throw new NotSupportedException();

    protected override async Task<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = _options.Database!.Connect();
        await connection.OpenAsync(cancellationToken);
        return connection;
    }

    protected override SqlWriter<T> CreateWriter() => _options.PerRow ? new SqlPerRowWriter<T>(Target) : new SqlBatchWriter<T>(Target);

    protected override async Task ExecuteAsync(DbConnection connection, Func<CancellationToken, Task> write, CancellationToken cancellationToken)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await write(cancellationToken);
                return;
            }
            catch (TransientFakeException) when (attempt < _options.Attempts)
            {
            }
        }
    }
}

public static class SqlNodeRunner
{
    public static async Task WriteAsync<T>(SinkNode<T> sink, IEnumerable<T> items, PipelineContext? context = null)
    {
        await using var input = new InMemoryDataStream<T>([.. items]);
        await sink.ConsumeAsync(input, context ?? new PipelineContext(), CancellationToken.None);
    }

    public static async Task<List<T>> ReadAsync<T>(SourceNode<T> source, PipelineContext? context = null)
    {
        var items = new List<T>();

        await foreach (var item in source.OpenStream(context ?? new PipelineContext(), CancellationToken.None))
        {
            items.Add(item);
        }

        return items;
    }
}
