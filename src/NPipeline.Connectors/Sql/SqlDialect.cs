using System.Data;
using System.Data.Common;
using System.Text;
using NPipeline.Connectors.Database;

namespace NPipeline.Connectors.Sql;

/// <summary>
///     What differs between SQL databases for writing: identifier quoting, parameter naming and typing, the parameter
///     limit, and upsert syntax. Each connector supplies one.
/// </summary>
public abstract class SqlDialect
{
    /// <summary>The connector's name in metrics and messages, such as <c>sqlserver</c>.</summary>
    public abstract string Name { get; }

    /// <summary>The most parameters one command may carry.</summary>
    public abstract int MaxParameters { get; }

    /// <summary>
    ///     The most rows one multi-row statement carries, whatever the parameter limit allows. Some databases parse and
    ///     plan a statement with thousands of parameters slowly, so several smaller statements are faster.
    /// </summary>
    public virtual int MaxRowsPerStatement => int.MaxValue;

    /// <summary>The character that opens a quoted identifier.</summary>
    protected abstract char OpenQuote { get; }

    /// <summary>The character that closes a quoted identifier; it is escaped by doubling it.</summary>
    protected abstract char CloseQuote { get; }

    /// <summary>Quotes one identifier, doubling the closing quote so the name cannot end the quote early.</summary>
    public string QuoteIdentifier(string identifier)
    {
        ArgumentException.ThrowIfNullOrEmpty(identifier);
        var close = CloseQuote.ToString();
        return string.Concat(OpenQuote.ToString(), identifier.Replace(close, close + close, StringComparison.Ordinal), close);
    }

    /// <summary>The quoted, schema-qualified table name.</summary>
    public string QualifiedTable(string? schema, string table) =>
        string.IsNullOrEmpty(schema) ? QuoteIdentifier(table) : $"{QuoteIdentifier(schema)}.{QuoteIdentifier(table)}";

    /// <summary>Throws unless <paramref name="identifier" /> is a plain identifier, for <see cref="SqlSinkOptions.ValidateIdentifiers" />.</summary>
    public virtual void Validate(string identifier, string parameterName) => DatabaseIdentifierValidator.ValidateIdentifier(identifier, parameterName);

    /// <summary>The placeholder for the parameter at <paramref name="index" /> in the SQL text.</summary>
    public virtual string Placeholder(int index) => $"@p{index}";

    /// <summary>The <see cref="DbParameter.ParameterName" /> for the parameter at <paramref name="index" />; empty for positional parameters.</summary>
    public virtual string ParameterName(int index) => Placeholder(index);

    /// <summary>
    ///     Whether writers need each target column's database type (from <see cref="DbDataReader.GetDataTypeName" />) to
    ///     bind values; when <c>true</c>, the sink describes the table once per write and passes the types to
    ///     <see cref="Bind" />.
    /// </summary>
    public virtual bool NeedsColumnTypes => false;

    /// <summary>
    ///     Types <paramref name="parameter" /> for <paramref name="column" /> and sets its value; a <c>null</c> becomes
    ///     <see cref="DBNull" />. The default leaves typing to the provider; dialects set types and sizes where the provider's
    ///     inference is wrong or costly.
    /// </summary>
    /// <param name="parameter">The parameter.</param>
    /// <param name="column">The column the value is for.</param>
    /// <param name="databaseType">The target column's type name when <see cref="NeedsColumnTypes" />; otherwise <c>null</c>.</param>
    /// <param name="value">The value, as the write plan extracted it.</param>
    public virtual void Bind(DbParameter parameter, SqlColumn column, string? databaseType, object? value) => parameter.Value = value ?? DBNull.Value;

    /// <summary>A multi-row <c>INSERT</c> of <paramref name="rows" /> rows, with parameters numbered from 0 row by row.</summary>
    public virtual string Insert(string table, IReadOnlyList<string> columns, int rows) =>
        $"INSERT INTO {table} ({string.Join(", ", columns)}) VALUES {Values(columns.Count, rows)}";

    /// <summary>
    ///     A multi-row upsert of <paramref name="rows" /> rows on <paramref name="keys" />, with parameters numbered as for
    ///     <see cref="Insert" />.
    /// </summary>
    public abstract string Upsert(string table, IReadOnlyList<string> columns, IReadOnlyList<string> keys, SqlUpsertAction onMatch, int rows);

    /// <summary><c>(@p0, @p1), (@p2, @p3)</c> for two rows of two columns.</summary>
    protected string Values(int columns, int rows)
    {
        var text = new StringBuilder(rows * columns * 6);

        for (var row = 0; row < rows; row++)
        {
            if (row > 0)
                text.Append(", ");

            text.Append('(');

            for (var column = 0; column < columns; column++)
            {
                if (column > 0)
                    text.Append(", ");

                text.Append(Placeholder((row * columns) + column));
            }

            text.Append(')');
        }

        return text.ToString();
    }

    /// <summary>The standard <c>DbType</c> for a member type, or <c>null</c> to let the provider infer it.</summary>
    protected static DbType? StandardDbType(Type memberType) =>
        (Nullable.GetUnderlyingType(memberType) ?? memberType) switch
        {
            var t when t == typeof(int) => DbType.Int32,
            var t when t == typeof(long) => DbType.Int64,
            var t when t == typeof(short) => DbType.Int16,
            var t when t == typeof(byte) => DbType.Byte,
            var t when t == typeof(bool) => DbType.Boolean,
            var t when t == typeof(decimal) => DbType.Decimal,
            var t when t == typeof(double) => DbType.Double,
            var t when t == typeof(float) => DbType.Single,
            var t when t == typeof(Guid) => DbType.Guid,
            var t when t == typeof(byte[]) => DbType.Binary,
            _ => null,
        };
}
