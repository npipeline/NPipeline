using System.Data;
using System.Globalization;
using System.Reflection;
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
/// <typeparam name="T">Record type to deserialize each Excel row to.</typeparam>
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
///         Data mapping is performed using reflection to map Excel columns to properties of type <typeparamref name="T" />.
///         When <see cref="ExcelConfiguration.FirstRowIsHeader" /> is <c>true</c>, column names from the first row
///         are used for property mapping. When <c>false</c>, properties are mapped by column index.
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
    private readonly StorageUri _uri;

    private ExcelSourceNode(
        StorageUri uri,
        ExcelConfiguration? configuration)
    {
        ArgumentNullException.ThrowIfNull(uri);
        _uri = uri;
        _configuration = configuration ?? new ExcelConfiguration();
    }

    /// <summary>
    ///     Construct an Excel source that resolves a storage provider from a resolver at execution time.
    /// </summary>
    /// <param name="uri">The URI of the Excel file to read from.</param>
    /// <param name="resolver">
    ///     The storage resolver used to obtain the storage provider. If <c>null</c>, a default resolver
    ///     created by <see cref="StorageProviderFactory.CreateResolver" /> is used.
    /// </param>
    /// <param name="configuration">Optional configuration for Excel reading. If <c>null</c>, default configuration is used.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="uri" /> is <c>null</c>.</exception>
    public ExcelSourceNode(
        StorageUri uri,
        IStorageResolver? resolver = null,
        ExcelConfiguration? configuration = null)
        : this(uri, configuration)
    {
        _resolver = resolver;
    }

    /// <summary>
    ///     Construct an Excel source that uses a specific storage provider.
    /// </summary>
    /// <param name="provider">The storage provider to use for reading the Excel file.</param>
    /// <param name="uri">The URI of the Excel file to read from.</param>
    /// <param name="configuration">Optional configuration for Excel reading. If <c>null</c>, default configuration is used.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="provider" /> or <paramref name="uri" /> is <c>null</c>.</exception>
    public ExcelSourceNode(
        IStorageProvider provider,
        StorageUri uri,
        ExcelConfiguration? configuration = null)
        : this(uri, configuration)
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

    private static async IAsyncEnumerable<T> Read(
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
            var item = CreateInstance(reader, headers, config.FirstRowIsHeader);

            if (item is not null)
                yield return item;
        }
    }

    private static T? CreateInstance(IDataReader reader, Dictionary<string, int> headers, bool hasHeaders)
    {
        var type = typeof(T);

        // Handle primitive types and strings
        if (type.IsPrimitive || type == typeof(string) || type == typeof(decimal) || type == typeof(DateTime) || type == typeof(Guid))
        {
            if (reader.FieldCount > 0)
            {
                var value = reader.GetValue(0);
                return ConvertValue(value);
            }

            return default;
        }

        // Handle complex types
        var instance = Activator.CreateInstance<T>();

        if (instance is null)
            return default;

        var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

        foreach (var property in properties)
        {
            if (!property.CanWrite)
                continue;

            int columnIndex;

            if (hasHeaders)
            {
                // Map by header name
                if (!headers.TryGetValue(property.Name, out columnIndex))
                    continue;
            }
            else
            {
                // Map by property order (using property name hash as a stable order)
                columnIndex = Math.Abs(property.Name.GetHashCode()) % reader.FieldCount;
            }

            if (columnIndex >= reader.FieldCount)
                continue;

            try
            {
                var value = reader.GetValue(columnIndex);

                if (value == DBNull.Value)
                    continue;

                var convertedValue = ConvertValue(value, property.PropertyType);

                if (convertedValue is not null)
                    property.SetValue(instance, convertedValue);
            }
            catch
            {
                // Skip conversion errors and continue with next property
            }
        }

        return instance;
    }

    private static T? ConvertValue(object? value)
    {
        if (value is null || value == DBNull.Value)
            return default;

        var targetType = typeof(T);
        var convertedValue = ConvertValue(value, targetType);

        return convertedValue is T tValue
            ? tValue
            : default;
    }

    private static object? ConvertValue(object? value, Type targetType)
    {
        if (value is null || value == DBNull.Value)
            return null;

        if (targetType.IsAssignableFrom(value.GetType()))
            return value;

        try
        {
            // Handle nullable types
            var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

            // Handle string conversion
            if (underlyingType == typeof(string))
                return value.ToString();

            // Handle numeric types
            if (underlyingType == typeof(int) || underlyingType == typeof(int?))
                return Convert.ToInt32(value);

            if (underlyingType == typeof(long) || underlyingType == typeof(long?))
                return Convert.ToInt64(value);

            if (underlyingType == typeof(short) || underlyingType == typeof(short?))
                return Convert.ToInt16(value);

            if (underlyingType == typeof(decimal) || underlyingType == typeof(decimal?))
                return Convert.ToDecimal(value);

            if (underlyingType == typeof(double) || underlyingType == typeof(double?))
                return Convert.ToDouble(value);

            if (underlyingType == typeof(float) || underlyingType == typeof(float?))
                return Convert.ToSingle(value);

            // Handle boolean
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

                return Convert.ToBoolean(value);
            }

            // Handle DateTime
            if (underlyingType == typeof(DateTime) || underlyingType == typeof(DateTime?))
            {
                if (value is DateTime dt)
                    return dt;

                if (double.TryParse(value.ToString(), out var oaDate))
                    return DateTime.FromOADate(oaDate);

                return Convert.ToDateTime(value);
            }

            // Handle Guid
            if (underlyingType == typeof(Guid) || underlyingType == typeof(Guid?))
            {
                if (value is Guid g)
                    return g;

                return Guid.TryParse(value.ToString(), out var guidResult)
                    ? guidResult
                    : Guid.Empty;
            }

            // Default conversion
            return Convert.ChangeType(value, underlyingType, CultureInfo.InvariantCulture);
        }
        catch
        {
            return null;
        }
    }
}
