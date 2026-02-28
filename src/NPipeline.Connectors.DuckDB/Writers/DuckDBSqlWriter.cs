using System.Globalization;
using System.Text;
using DuckDB.NET.Data;
using NPipeline.Connectors.DuckDB.Mapping;

namespace NPipeline.Connectors.DuckDB.Writers;

/// <summary>
///     SQL INSERT-based writer with batching support.
///     Slower than the Appender but supports upsert via INSERT OR REPLACE.
/// </summary>
internal sealed class DuckDBSqlWriter<T> : IDuckDBWriter<T>
{
    private readonly int _batchSize;
    private readonly List<T> _buffer;
    private readonly string[] _columnNames;
    private readonly DuckDBConnection _connection;
    private readonly IDuckDBConnectorObserver? _observer;
    private readonly string _tableName;
    private readonly Func<object, object?>[] _valueGetters;
    private bool _disposed;
    private long _totalRows;

    public DuckDBSqlWriter(
        DuckDBConnection connection,
        string tableName,
        int batchSize = 1000,
        IDuckDBConnectorObserver? observer = null)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        _batchSize = batchSize;
        _observer = observer;
        _columnNames = DuckDBWriterMapperBuilder.GetColumnNames<T>();
        _valueGetters = DuckDBWriterMapperBuilder.GetValueGetters<T>();
        _buffer = new List<T>(_batchSize);
    }

    public async Task WriteAsync(T item, CancellationToken cancellationToken)
    {
        _buffer.Add(item);

        if (_buffer.Count >= _batchSize)
            await FlushBufferAsync(cancellationToken);
    }

    public async Task FlushAsync(CancellationToken cancellationToken)
    {
        if (_buffer.Count > 0)
            await FlushBufferAsync(cancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed)
            return ValueTask.CompletedTask;

        _disposed = true;
        _buffer.Clear();
        return ValueTask.CompletedTask;
    }

    private async Task FlushBufferAsync(CancellationToken cancellationToken)
    {
        if (_buffer.Count == 0)
            return;

        var sql = BuildInsertSql(_buffer);

        await using var command = _connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(cancellationToken);

        _totalRows += _buffer.Count;
        _observer?.OnBatchFlushed(_buffer.Count, _totalRows);
        _buffer.Clear();
    }

    private string BuildInsertSql(List<T> items)
    {
        var sb = new StringBuilder();
        sb.Append($"INSERT INTO \"{_tableName}\" (");
        sb.Append(string.Join(", ", _columnNames.Select(c => $"\"{c}\"")));
        sb.Append(") VALUES ");

        for (var i = 0; i < items.Count; i++)
        {
            if (i > 0)
                sb.Append(", ");

            sb.Append('(');

            for (var j = 0; j < _valueGetters.Length; j++)
            {
                if (j > 0)
                    sb.Append(", ");

                var value = _valueGetters[j](items[i]!);
                sb.Append(FormatSqlValue(value));
            }

            sb.Append(')');
        }

        return sb.ToString();
    }

    private static string FormatSqlValue(object? value)
    {
        if (value is null)
            return "NULL";

        return value switch
        {
            string s => $"'{s.Replace("'", "''")}'",
            bool b => b
                ? "true"
                : "false",
            DateTime dt => $"'{dt:yyyy-MM-dd HH:mm:ss.ffffff}'",
            DateTimeOffset dto => $"'{dto.UtcDateTime:yyyy-MM-dd HH:mm:ss.ffffff}'",
            DateOnly d => $"'{d:yyyy-MM-dd}'",
            TimeOnly t => $"'{t:HH:mm:ss.ffffff}'",
            byte[] bytes => $"'\\x{Convert.ToHexString(bytes)}'",
            decimal m => m.ToString(CultureInfo.InvariantCulture),
            float f => f.ToString(CultureInfo.InvariantCulture),
            double d => d.ToString(CultureInfo.InvariantCulture),
            _ => value.ToString()!,
        };
    }
}
