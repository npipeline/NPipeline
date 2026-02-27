using System.Runtime.CompilerServices;
using NPipeline.Connectors.Parquet.Mapping;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Models;
using Parquet;
using Parquet.Schema;

namespace NPipeline.Connectors.Parquet;

/// <summary>
///     Source node that reads Parquet data using a pluggable <see cref="IStorageProvider" />.
///     Supports streaming row-group reading with bounded memory usage.
/// </summary>
/// <typeparam name="T">Type emitted for each Parquet row.</typeparam>
public sealed class ParquetSourceNode<T> : SourceNode<T>
{
    private static readonly Lazy<IStorageResolver> DefaultResolver = new(
        () => StorageProviderFactory.CreateResolver(),
        LazyThreadSafetyMode.ExecutionAndPublication);

    private static readonly string[] ParquetExtensions = [".parquet", ".snappy.parquet", ".gz.parquet", ".parquet.gzip"];

    private readonly ParquetConfiguration _configuration;
    private readonly IStorageProvider? _provider;
    private readonly IStorageResolver? _resolver;
    private readonly Func<ParquetRow, T> _rowMapper;
    private readonly StorageUri _uri;

    private ParquetSourceNode(
        StorageUri uri,
        ParquetConfiguration? configuration,
        Func<ParquetRow, T> rowMapper)
    {
        ArgumentNullException.ThrowIfNull(uri);
        ArgumentNullException.ThrowIfNull(rowMapper);
        _uri = uri;
        _configuration = configuration ?? new ParquetConfiguration();
        _configuration.Validate();
        _rowMapper = rowMapper;
    }

    /// <summary>
    ///     Construct a Parquet source that uses attribute-based mapping.
    ///     Properties are mapped using ParquetColumnAttribute or convention (property name as-is).
    /// </summary>
    /// <param name="uri">The URI of the Parquet file or directory to read from.</param>
    /// <param name="resolver">
    ///     The storage resolver used to obtain the storage provider. If <c>null</c>, a default resolver
    ///     created by <see cref="StorageProviderFactory.CreateResolver" /> is used.
    /// </param>
    /// <param name="configuration">Optional configuration for Parquet reading. If <c>null</c>, default configuration is used.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="uri" /> is <c>null</c>.</exception>
    public ParquetSourceNode(
        StorageUri uri,
        IStorageResolver? resolver = null,
        ParquetConfiguration? configuration = null)
        : this(uri, configuration, ParquetMapperBuilder.Build<T>())
    {
        _resolver = resolver;
    }

    /// <summary>
    ///     Construct a Parquet source that uses a specific storage provider with attribute-based mapping.
    ///     Properties are mapped using ParquetColumnAttribute or convention (property name as-is).
    /// </summary>
    /// <param name="provider">The storage provider to use for reading.</param>
    /// <param name="uri">The URI of the Parquet file or directory to read from.</param>
    /// <param name="configuration">Optional configuration for Parquet reading. If <c>null</c>, default configuration is used.</param>
    public ParquetSourceNode(
        IStorageProvider provider,
        StorageUri uri,
        ParquetConfiguration? configuration = null)
        : this(uri, configuration, ParquetMapperBuilder.Build<T>())
    {
        ArgumentNullException.ThrowIfNull(provider);
        _provider = provider;
    }

    /// <summary>
    ///     Construct a Parquet source that resolves a storage provider from a resolver at execution time.
    /// </summary>
    /// <param name="uri">The URI of the Parquet file or directory to read from.</param>
    /// <param name="rowMapper">Row mapper used to construct <typeparamref name="T" /> from a <see cref="ParquetRow" />.</param>
    /// <param name="resolver">
    ///     The storage resolver used to obtain the storage provider. If <c>null</c>, a default resolver
    ///     created by <see cref="StorageProviderFactory.CreateResolver" /> is used.
    /// </param>
    /// <param name="configuration">Optional configuration for Parquet reading. If <c>null</c>, default configuration is used.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="uri" /> is <c>null</c>.</exception>
    public ParquetSourceNode(
        StorageUri uri,
        Func<ParquetRow, T> rowMapper,
        IStorageResolver? resolver = null,
        ParquetConfiguration? configuration = null)
        : this(uri, configuration, rowMapper)
    {
        _resolver = resolver;
    }

    /// <summary>
    ///     Construct a Parquet source that uses a specific storage provider.
    /// </summary>
    /// <param name="provider">The storage provider to use for reading.</param>
    /// <param name="uri">The URI of the Parquet file or directory to read from.</param>
    /// <param name="rowMapper">Row mapper used to construct <typeparamref name="T" /> from a <see cref="ParquetRow" />.</param>
    /// <param name="configuration">Optional configuration for Parquet reading. If <c>null</c>, default configuration is used.</param>
    public ParquetSourceNode(
        IStorageProvider provider,
        StorageUri uri,
        Func<ParquetRow, T> rowMapper,
        ParquetConfiguration? configuration = null)
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
            {
                throw new UnsupportedStorageCapabilityException(_uri, "read", meta.Name);
            }
        }

        var stream = ReadAll(provider, _uri, _configuration, cancellationToken);
        return new StreamingDataPipe<T>(stream, $"ParquetSourceNode<{typeof(T).Name}>");
    }

    private async IAsyncEnumerable<T> ReadAll(
        IStorageProvider provider,
        StorageUri uri,
        ParquetConfiguration config,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var files = await DiscoverParquetFiles(provider, uri, config, cancellationToken);

        foreach (var fileUri in files)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await foreach (var item in ReadFile(provider, fileUri, config, cancellationToken))
            {
                yield return item;
            }
        }
    }

    private async Task<IReadOnlyList<StorageUri>> DiscoverParquetFiles(
        IStorageProvider provider,
        StorageUri uri,
        ParquetConfiguration config,
        CancellationToken cancellationToken)
    {
        // Check if the URI points to a single file
        var path = uri.Path ?? string.Empty;
        if (ParquetExtensions.Any(ext => path.EndsWith(ext, StringComparison.OrdinalIgnoreCase)))
        {
            return [uri];
        }

        // Directory listing - ListAsync returns IAsyncEnumerable
        var parquetFiles = new List<StorageUri>();
        await foreach (var item in provider.ListAsync(uri, recursive: config.RecursiveDiscovery, cancellationToken))
        {
            // Use !IsDirectory to check for files
            if (!item.IsDirectory && ParquetExtensions.Any(ext => item.Uri.Path?.EndsWith(ext, StringComparison.OrdinalIgnoreCase) == true))
            {
                parquetFiles.Add(item.Uri);
            }
        }

        return [.. parquetFiles.OrderBy(u => u.Path, StringComparer.OrdinalIgnoreCase)];
    }

    private async IAsyncEnumerable<T> ReadFile(
        IStorageProvider provider,
        StorageUri fileUri,
        ParquetConfiguration config,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var observer = config.Observer;
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        long totalRows = 0;
        long totalBytes = 0;

        observer?.OnFileReadStarted(fileUri);

        await using var stream = await provider.OpenReadAsync(fileUri, cancellationToken);
        totalBytes = stream.Length;

        using var reader = await ParquetReader.CreateAsync(stream, cancellationToken: cancellationToken);

        // Validate schema if configured
        if (config.SchemaValidator is not null)
        {
            if (!config.SchemaValidator(reader.Schema))
            {
                throw new ParquetSchemaException($"Schema validation failed for file '{fileUri}'");
            }
        }

        // Build column name to DataField mapping (computed once per file)
        var dataFields = reader.Schema.GetDataFields();
        var columnNameToField = new Dictionary<string, DataField>(StringComparer.OrdinalIgnoreCase);
        var columnNameToIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        for (var i = 0; i < dataFields.Length; i++)
        {
            var field = dataFields[i];
            if (!string.IsNullOrEmpty(field.Name))
            {
                columnNameToField[field.Name] = field;
                columnNameToIndex[field.Name] = i;
            }
        }

        // Determine which columns to read
        string[]? columnsToRead = null;
        if (config.ProjectedColumns is not null && config.ProjectedColumns.Count > 0)
        {
            columnsToRead = [.. config.ProjectedColumns];
        }

        // Read each row group
        for (var rowGroupIndex = 0; rowGroupIndex < reader.RowGroupCount; rowGroupIndex++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using var rowGroupReader = reader.OpenRowGroupReader(rowGroupIndex);
            var rowCount = rowGroupReader.RowCount;

            observer?.OnRowGroupRead(fileUri, rowGroupIndex, rowCount);

            // Read column data
            var columnData = await ReadRowGroupColumns(rowGroupReader, columnNameToField, columnsToRead, cancellationToken);

            // Yield rows from this row group
            for (var rowIndex = 0L; rowIndex < rowCount; rowIndex++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var row = CreateParquetRow(columnData, rowIndex, columnNameToIndex, reader.Schema);

                // Apply row filter if configured
                if (config.RowFilter is not null && !config.RowFilter(row))
                {
                    continue;
                }

                T? record;
                try
                {
                    record = _rowMapper(row);
                }
                catch (Exception ex)
                {
                    observer?.OnRowMappingError(fileUri, ex);

                    var handler = config.RowErrorHandler;
                    if (handler is not null && handler(ex, row))
                    {
                        continue; // handler opted to skip
                    }

                    throw;
                }

                if (record is not null)
                {
                    totalRows++;
                    yield return record;
                }
            }
        }

        stopwatch.Stop();
        observer?.OnFileReadCompleted(fileUri, totalRows, totalBytes, stopwatch.Elapsed);
    }

    private async Task<Dictionary<string, object?[]>> ReadRowGroupColumns(
        ParquetRowGroupReader rowGroupReader,
        Dictionary<string, DataField> columnNameToField,
        string[]? columnsToRead,
        CancellationToken cancellationToken)
    {
        var columnData = new Dictionary<string, object?[]>(StringComparer.OrdinalIgnoreCase);

        var columns = columnsToRead ?? [.. columnNameToField.Keys];

        foreach (var columnName in columns)
        {
            if (!columnNameToField.TryGetValue(columnName, out var field))
            {
                continue;
            }

            // Read the column using the DataField
            var data = await rowGroupReader.ReadColumnAsync(field, cancellationToken);

            // Convert DataColumn.Data array to object array
            var dataArray = data.Data;
            var values = new object?[dataArray.Length];
            for (var i = 0; i < dataArray.Length; i++)
            {
                values[i] = dataArray.GetValue(i);
            }

            columnData[columnName] = values;
        }

        return columnData;
    }

    private ParquetRow CreateParquetRow(
        Dictionary<string, object?[]> columnData,
        long rowIndex,
        Dictionary<string, int> columnNameToIndex,
        ParquetSchema schema)
    {
        var values = new object?[columnNameToIndex.Count];

        foreach (var kvp in columnNameToIndex)
        {
            var columnName = kvp.Key;
            var index = kvp.Value;

            if (columnData.TryGetValue(columnName, out var columnValues) && rowIndex < columnValues.Length)
            {
                values[index] = columnValues[rowIndex];
            }
        }

        return new ParquetRow(values, columnNameToIndex, schema);
    }
}
