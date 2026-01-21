using System.Data;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using ExcelDataReader;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Exceptions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Excel;

/// <summary>
///     Source node that reads Excel data using a pluggable <see cref="IStorageProvider" />.
/// </summary>
/// <typeparam name="T">Type emitted for each Excel row.</typeparam>
/// <remarks>
///     <para>
///         This source node supports reading both legacy XLS (binary) and modern XLSX (Open XML) formats
///         using ExcelDataReader. It provides streaming access to Excel data with configurable options
///         for sheet selection, header handling, and type conversion.
///     </para>
///     <para>
///         The node supports two constructor patterns:
///         <list type="bullet">
///             <item>
///                 <description>Using an <see cref="IStorageResolver" /> to resolve the provider at execution time</description>
///             </item>
///             <item>
///                 <description>Using a specific <see cref="IStorageProvider" /> instance</description>
///             </item>
///         </list>
///     </para>
///     <para>
///         Data mapping is performed using the explicit <see cref="ExcelRow" /> mapper delegate supplied by the caller.
///         This avoids reflection and supports positional records and custom conversion logic.
///     </para>
/// </remarks>
public sealed class ExcelSourceNode<T> : SourceNode<T>
{
    private static readonly Lazy<IStorageResolver> DefaultResolver = new(
        () => StorageProviderFactory.CreateResolver(),
        LazyThreadSafetyMode.ExecutionAndPublication);

    private readonly ExcelConfiguration _configuration;
    private readonly IStorageProvider? _provider;
    private readonly IStorageResolver? _resolver;
    private readonly Func<ExcelRow, T> _rowMapper;
    private readonly StorageUri _uri;

    private ExcelSourceNode(
        StorageUri uri,
        ExcelConfiguration? configuration,
        Func<ExcelRow, T> rowMapper)
    {
        ArgumentNullException.ThrowIfNull(uri);
        ArgumentNullException.ThrowIfNull(rowMapper);
        _uri = uri;
        _configuration = configuration ?? new ExcelConfiguration();
        _rowMapper = rowMapper;
    }

    /// <summary>
    ///     Construct an Excel source that resolves a storage provider from a resolver at execution time.
    /// </summary>
    /// <param name="uri">The URI of the Excel file to read from.</param>
    /// <param name="resolver">
    ///     The storage resolver used to obtain the storage provider. If <c>null</c>, a default resolver
    ///     created by <see cref="StorageProviderFactory.CreateResolver" /> is used.
    /// </param>
    /// <param name="rowMapper">Row mapper used to construct <typeparamref name="T" /> from an <see cref="ExcelRow" />.</param>
    /// <param name="configuration">Optional configuration for Excel reading. If <c>null</c>, default configuration is used.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="uri" /> is <c>null</c>.</exception>
    public ExcelSourceNode(
        StorageUri uri,
        Func<ExcelRow, T> rowMapper,
        IStorageResolver? resolver = null,
        ExcelConfiguration? configuration = null)
        : this(uri, configuration, rowMapper)
    {
        _resolver = resolver;
    }

    /// <summary>
    ///     Construct an Excel source that uses a specific storage provider.
    /// </summary>
    /// <param name="provider">The storage provider to use for reading the Excel file.</param>
    /// <param name="uri">The URI of the Excel file to read from.</param>
    /// <param name="rowMapper">Row mapper used to construct <typeparamref name="T" /> from an <see cref="ExcelRow" />.</param>
    /// <param name="configuration">Optional configuration for Excel reading. If <c>null</c>, default configuration is used.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="provider" /> or <paramref name="uri" /> is <c>null</c>.</exception>
    public ExcelSourceNode(
        IStorageProvider provider,
        StorageUri uri,
        Func<ExcelRow, T> rowMapper,
        ExcelConfiguration? configuration = null)
        : this(uri, configuration, rowMapper)
    {
        ArgumentNullException.ThrowIfNull(provider);
        _provider = provider;
    }

    /// <inheritdoc />
    public override IDataPipe<T> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        var provider = _provider ?? StorageProviderFactory.GetProviderOrThrow(
            _resolver ?? DefaultResolver.Value,
            _uri);

        if (provider is IStorageProviderMetadataProvider metaProvider)
        {
            var meta = metaProvider.GetMetadata();

            if (!meta.SupportsRead)
                throw new UnsupportedStorageCapabilityException(_uri, "read", meta.Name);
        }

        var stream = Read(provider, _uri, _configuration, cancellationToken);
        return new StreamingDataPipe<T>(stream, $"ExcelSourceNode<{typeof(T).Name}>");
    }

    private async IAsyncEnumerable<T> Read(
        IStorageProvider provider,
        StorageUri uri,
        ExcelConfiguration config,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Open the stream per-enumeration so disposal is bound to consumer lifetime
        await using var stream = await provider.OpenReadAsync(uri, cancellationToken).ConfigureAwait(false);

        // Configure ExcelDataReader
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

        var readerConfig = new ExcelReaderConfiguration
        {
            AutodetectSeparators = config.AutodetectSeparators
                ? new[] { ';', ',', '\t', '|', '#' }
                : null,
            AnalyzeInitialCsvRows = config.AnalyzeAllColumns
                ? 0
                : config.AnalyzeInitialRowCount,
            FallbackEncoding = config.Encoding ?? Encoding.UTF8,
        };

        using var reader = ExcelReaderFactory.CreateReader(stream, readerConfig);

        // Select the appropriate sheet
        if (!string.IsNullOrEmpty(config.SheetName))
        {
            var sheetFound = false;

            do
            {
                if (reader.Name.Equals(config.SheetName, StringComparison.OrdinalIgnoreCase))
                {
                    sheetFound = true;
                    break;
                }
            } while (reader.NextResult());

            if (!sheetFound)
                throw new InvalidOperationException($"Sheet '{config.SheetName}' not found in Excel file.");
        }

        // Use first sheet (already positioned)
        // Read header row if configured
        var headers = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        if (config.FirstRowIsHeader && reader.Read())
        {
            for (var i = 0; i < reader.FieldCount; i++)
            {
                var header = reader.GetString(i);

                if (!string.IsNullOrEmpty(header))
                    headers[header] = i;
            }
        }

        // Read data rows
        while (reader.Read())
        {
            var item = CreateInstance(reader, headers, config.FirstRowIsHeader, _rowMapper);

            if (item is not null)
                yield return item;
        }
    }

    private static T? CreateInstance(
        IDataReader reader,
        Dictionary<string, int> headers,
        bool hasHeaders,
        Func<ExcelRow, T> rowMapper)
    {
        try
        {
            var excelRow = new ExcelRow(reader, headers, hasHeaders);
            return rowMapper(excelRow);
        }
        catch
        {
            return default;
        }
    }
}

/// <summary>
///     Represents the current Excel row and provides typed accessors.
/// </summary>
public readonly struct ExcelRow
{
    private readonly IDataRecord _record;
    private readonly IReadOnlyDictionary<string, int> _headers;
    private readonly bool _hasHeaders;

    /// <summary>
    ///     Initialize a new instance of <see cref="ExcelRow" />.
    /// </summary>
    /// <param name="record">The data record representing the current row.</param>
    /// <param name="headers">Header lookup map.</param>
    /// <param name="hasHeaders">Whether header records are present.</param>
    public ExcelRow(IDataRecord record, IReadOnlyDictionary<string, int> headers, bool hasHeaders)
    {
        _record = record;
        _headers = headers;
        _hasHeaders = hasHeaders;
    }

    /// <summary>
    ///     Try to read a field by header name and convert it to <typeparamref name="T" />.
    /// </summary>
    /// <param name="name">Header name.</param>
    /// <param name="value">Converted value or <paramref name="defaultValue" />.</param>
    /// <param name="defaultValue">Value used when the field is missing or conversion fails.</param>
    /// <typeparam name="T">Target type.</typeparam>
    /// <returns><c>true</c> when a value is present and converted; otherwise <c>false</c>.</returns>
    public bool TryGet<T>(string name, out T? value, T? defaultValue = default)
    {
        value = defaultValue;

        if (!_hasHeaders || !_headers.TryGetValue(name, out var index))
            return false;

        if (index >= _record.FieldCount)
            return false;

        var raw = _record.GetValue(index);

        if (raw == DBNull.Value)
            return false;

        var converted = ExcelValueConverter.Convert(raw, typeof(T));

        if (converted is T typed)
        {
            value = typed;
            return true;
        }

        return false;
    }

    /// <summary>
    ///     Read a field by header name and return a converted value or <paramref name="defaultValue" />.
    /// </summary>
    /// <param name="name">Header name.</param>
    /// <param name="defaultValue">Value used when the field is missing or conversion fails.</param>
    /// <typeparam name="T">Target type.</typeparam>
    /// <returns>Converted value or <paramref name="defaultValue" />.</returns>
    public T? Get<T>(string name, T? defaultValue = default)
    {
        return TryGet(name, out var value, defaultValue)
            ? value
            : defaultValue;
    }

    /// <summary>
    ///     Read a field by index and return a converted value or <paramref name="defaultValue" />.
    /// </summary>
    /// <param name="index">Field index.</param>
    /// <param name="defaultValue">Value used when the field is missing or conversion fails.</param>
    /// <typeparam name="T">Target type.</typeparam>
    /// <returns>Converted value or <paramref name="defaultValue" />.</returns>
    public T? GetByIndex<T>(int index, T? defaultValue = default)
    {
        if (index < 0 || index >= _record.FieldCount)
            return defaultValue;

        var raw = _record.GetValue(index);

        if (raw == DBNull.Value)
            return defaultValue;

        var converted = ExcelValueConverter.Convert(raw, typeof(T));

        return converted is T typed
            ? typed
            : defaultValue;
    }
}

internal static class ExcelValueConverter
{
    public static object? Convert(object? value, Type targetType)
    {
        if (value is null || value == DBNull.Value)
            return null;

        if (targetType.IsAssignableFrom(value.GetType()))
            return value;

        try
        {
            var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

            if (underlyingType == typeof(string))
                return value.ToString();

            if (underlyingType == typeof(int) || underlyingType == typeof(int?))
                return System.Convert.ToInt32(value);

            if (underlyingType == typeof(long) || underlyingType == typeof(long?))
                return System.Convert.ToInt64(value);

            if (underlyingType == typeof(short) || underlyingType == typeof(short?))
                return System.Convert.ToInt16(value);

            if (underlyingType == typeof(decimal) || underlyingType == typeof(decimal?))
                return System.Convert.ToDecimal(value);

            if (underlyingType == typeof(double) || underlyingType == typeof(double?))
                return System.Convert.ToDouble(value);

            if (underlyingType == typeof(float) || underlyingType == typeof(float?))
                return System.Convert.ToSingle(value);

            if (underlyingType == typeof(bool) || underlyingType == typeof(bool?))
            {
                if (value is bool b)
                    return b;

                if (value is string s)
                {
                    return bool.TryParse(s, out var boolResult)
                        ? boolResult
                        : false;
                }

                return System.Convert.ToBoolean(value);
            }

            if (underlyingType == typeof(DateTime) || underlyingType == typeof(DateTime?))
            {
                if (value is DateTime dt)
                    return dt;

                if (double.TryParse(value.ToString(), out var oaDate))
                    return DateTime.FromOADate(oaDate);

                return System.Convert.ToDateTime(value);
            }

            if (underlyingType == typeof(Guid) || underlyingType == typeof(Guid?))
            {
                if (value is Guid g)
                    return g;

                return Guid.TryParse(value.ToString(), out var guidResult)
                    ? guidResult
                    : Guid.Empty;
            }

            return System.Convert.ChangeType(value, underlyingType, CultureInfo.InvariantCulture);
        }
        catch
        {
            return null;
        }
    }
}
