using System.Data;
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
    ///     Construct an Excel source that resolves a storage provider from a resolver at execution time.
    ///     Uses attribute-based mapping for automatic property-to-column mapping.
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
        : this(uri, configuration, ExcelMapperBuilder.Build<T>())
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

    /// <summary>
    ///     Construct an Excel source that uses a specific storage provider.
    ///     Uses attribute-based mapping for automatic property-to-column mapping.
    /// </summary>
    /// <param name="provider">The storage provider to use for reading the Excel file.</param>
    /// <param name="uri">The URI of the Excel file to read from.</param>
    /// <param name="configuration">Optional configuration for Excel reading. If <c>null</c>, default configuration is used.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="provider" /> or <paramref name="uri" /> is <c>null</c>.</exception>
    public ExcelSourceNode(
        IStorageProvider provider,
        StorageUri uri,
        ExcelConfiguration? configuration = null)
        : this(uri, configuration, ExcelMapperBuilder.Build<T>())
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
