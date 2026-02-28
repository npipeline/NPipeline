using DuckDB.NET.Data;
using NPipeline.Connectors.DuckDB.Mapping;

namespace NPipeline.Connectors.DuckDB.Writers;

/// <summary>
///     High-performance writer using DuckDB's native Appender API.
///     Appends rows directly to a table with minimal overhead.
/// </summary>
internal sealed class DuckDBAppenderWriter<T> : IDuckDBWriter<T>
{
    private readonly DuckDBConnection _connection;
    private readonly string _tableName;
    private readonly Func<object, object?>[] _valueGetters;
    private DuckDBAppender? _appender;
    private bool _disposed;

    public DuckDBAppenderWriter(DuckDBConnection connection, string tableName)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        _valueGetters = DuckDBWriterMapperBuilder.GetValueGetters<T>();
    }

    public Task WriteAsync(T item, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        _appender ??= _connection.CreateAppender(_tableName);

        var row = _appender.CreateRow();

        for (var i = 0; i < _valueGetters.Length; i++)
        {
            var value = _valueGetters[i](item!);
            AppendTypedValue(row, value);
        }

        row.EndRow();

        return Task.CompletedTask;
    }

    public Task FlushAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // DuckDB Appender has no Flush; data is flushed on Close/Dispose
        _appender?.Close();
        _appender?.Dispose();
        _appender = null;
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed)
            return ValueTask.CompletedTask;

        _disposed = true;

        _appender?.Close();
        _appender?.Dispose();

        return ValueTask.CompletedTask;
    }

    private static void AppendTypedValue(IDuckDBAppenderRow row, object? value)
    {
        if (value is null)
        {
            row.AppendNullValue();
            return;
        }

        switch (value)
        {
            case bool b:
                row.AppendValue(b);
                break;
            case byte b:
                row.AppendValue(b);
                break;
            case sbyte b:
                row.AppendValue(b);
                break;
            case short s:
                row.AppendValue(s);
                break;
            case int i:
                row.AppendValue(i);
                break;
            case long l:
                row.AppendValue(l);
                break;
            case ushort u:
                row.AppendValue(u);
                break;
            case uint u:
                row.AppendValue(u);
                break;
            case ulong u:
                row.AppendValue(u);
                break;
            case float f:
                row.AppendValue(f);
                break;
            case double d:
                row.AppendValue(d);
                break;
            case decimal m:
                row.AppendValue(m);
                break;
            case string s:
                row.AppendValue(s);
                break;
            case DateTime dt:
                row.AppendValue(dt);
                break;
            case DateTimeOffset dto:
                row.AppendValue(dto);
                break;
            case TimeSpan ts:
                row.AppendValue(ts);
                break;
            case Guid g:
                row.AppendValue(g);
                break;
            case byte[] bytes:
                row.AppendValue(bytes);
                break;
            default:
                // Fallback: convert to string (covers enums stored as string by WriterMapperBuilder)
                row.AppendValue(value.ToString());
                break;
        }
    }
}
