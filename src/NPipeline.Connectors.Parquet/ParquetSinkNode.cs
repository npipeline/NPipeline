using System.Diagnostics;
using System.Reflection;
using NPipeline.Connectors.Parquet.Mapping;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Models;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace NPipeline.Connectors.Parquet;

/// <summary>
///     Sink node that writes items to Parquet files using a pluggable <see cref="IStorageProvider" />.
///     Supports row-group buffered writing with bounded memory usage and atomic write support.
/// </summary>
/// <typeparam name="T">Record type to serialize for each Parquet row.</typeparam>
public sealed class ParquetSinkNode<T> : SinkNode<T>
{
    private static readonly Lazy<IStorageResolver> DefaultResolver =
        new(() => StorageProviderFactory.CreateResolver());

    private readonly ParquetConfiguration _configuration;
    private readonly IStorageProvider? _provider;
    private readonly IStorageResolver? _resolver;
    private readonly StorageUri _uri;

    /// <summary>
    ///     Construct a Parquet sink node that resolves a storage provider from a resolver at execution time.
    ///     Uses attribute-based mapping for automatic property-to-column mapping.
    /// </summary>
    /// <param name="uri">The URI of the Parquet file to write to.</param>
    /// <param name="resolver">The storage resolver used to obtain storage provider. If <c>null</c>, a default resolver is used.</param>
    /// <param name="configuration">Optional configuration for Parquet writing. If <c>null</c>, default configuration is used.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="uri" /> is <c>null</c>.</exception>
    public ParquetSinkNode(
        StorageUri uri,
        IStorageResolver? resolver = null,
        ParquetConfiguration? configuration = null)
    {
        ArgumentNullException.ThrowIfNull(uri);
        _uri = uri;
        _configuration = configuration ?? new ParquetConfiguration();
        _configuration.Validate();
        _resolver = resolver ?? DefaultResolver.Value;
    }

    /// <summary>
    ///     Construct a Parquet sink node that uses a specific storage provider instance.
    ///     Uses attribute-based mapping for automatic property-to-column mapping.
    /// </summary>
    /// <param name="provider">The storage provider to use for writing.</param>
    /// <param name="uri">The URI of the Parquet file to write to.</param>
    /// <param name="configuration">Optional configuration for Parquet writing. If <c>null</c>, default configuration is used.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="provider" /> or <paramref name="uri" /> is <c>null</c>.</exception>
    public ParquetSinkNode(
        IStorageProvider provider,
        StorageUri uri,
        ParquetConfiguration? configuration = null)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(uri);
        _uri = uri;
        _configuration = configuration ?? new ParquetConfiguration();
        _configuration.Validate();
        _provider = provider;
    }

    /// <inheritdoc />
    public override async Task ExecuteAsync(IDataPipe<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        var provider = _provider ?? StorageProviderFactory.GetProviderOrThrow(
            _resolver ?? throw new InvalidOperationException("No storage resolver configured for ParquetSinkNode."),
            _uri);

        if (provider is IStorageProviderMetadataProvider metaProvider)
        {
            var meta = metaProvider.GetMetadata();

            if (!meta.SupportsWrite)
                throw new UnsupportedStorageCapabilityException(_uri, "write", meta.Name);
        }

        await WriteParquetAsync(provider, input, cancellationToken);
    }

    private async Task WriteParquetAsync(
        IStorageProvider provider,
        IDataPipe<T> input,
        CancellationToken cancellationToken)
    {
        var observer = _configuration.Observer;
        var stopwatch = Stopwatch.StartNew();
        long totalRows = 0;
        var rowGroupCount = 0;

        // Get schema and column information
        var schema = ParquetSchemaBuilder.Build<T>();
        var columnNames = ParquetWriterMapperBuilder.GetColumnNames<T>();
        var valueGetters = ParquetWriterMapperBuilder.GetValueGetters<T>();
        var properties = ParquetWriterMapperBuilder.GetProperties<T>();

        // Determine target URI (potentially temp file for atomic write)
        var targetUri = _uri;
        var useAtomicWrite = _configuration.UseAtomicWrite;

        var tempUri = useAtomicWrite
            ? CreateTempUri(_uri)
            : _uri;

        try
        {
            await using var stream = await provider.OpenWriteAsync(tempUri, cancellationToken);
            await using var writer = await CreateParquetWriter(stream, schema, cancellationToken);

            // Buffer for accumulating rows before writing a row group
            var buffer = new List<T>(_configuration.RowGroupSize);

            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                if (item is null)
                    continue;

                buffer.Add(item);

                if (buffer.Count >= _configuration.RowGroupSize)
                {
                    await WriteRowGroup(writer, buffer, schema, columnNames, valueGetters, properties, cancellationToken);
                    observer?.OnRowGroupWritten(tempUri, rowGroupCount, buffer.Count);
                    totalRows += buffer.Count;
                    rowGroupCount++;
                    buffer.Clear();
                }
            }

            // Write any remaining buffered records as a final (potentially partial) row group
            if (buffer.Count > 0)
            {
                await WriteRowGroup(writer, buffer, schema, columnNames, valueGetters, properties, cancellationToken);
                observer?.OnRowGroupWritten(tempUri, rowGroupCount, buffer.Count);
                totalRows += buffer.Count;
                rowGroupCount++;
            }

            await writer.DisposeAsync();
            await stream.FlushAsync(cancellationToken);
        }
        catch
        {
            // On failure, attempt to clean up temp file if atomic write was enabled
            // Use CancellationToken.None since the original token may be cancelled
            if (useAtomicWrite)
            {
                try
                {
                    // Best-effort cleanup - we don't want to mask the original exception
                    if (provider is IDeletableStorageProvider deletableProvider)
                        await deletableProvider.DeleteAsync(tempUri, CancellationToken.None);
                }
                catch
                {
                    // Ignore cleanup failures
                }
            }

            throw;
        }

        // For atomic write, now publish the temp file to the final location
        if (useAtomicWrite && !tempUri.Equals(targetUri))
            await PublishAtomicWrite(provider, tempUri, targetUri, cancellationToken);

        stopwatch.Stop();
        observer?.OnFileWriteCompleted(targetUri, totalRows, -1, stopwatch.Elapsed);
    }

    private async Task<ParquetWriter> CreateParquetWriter(
        Stream stream,
        ParquetSchema schema,
        CancellationToken cancellationToken)
    {
        // Parquet.Net 5.x uses compression directly on the writer
        var writer = await ParquetWriter.CreateAsync(schema, stream, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        // Apply compression setting from configuration
        writer.CompressionMethod = _configuration.Compression;

        return writer;
    }

    private async Task WriteRowGroup(
        ParquetWriter writer,
        List<T> buffer,
        ParquetSchema schema,
        string[] columnNames,
        Func<T, object?>[] valueGetters,
        PropertyInfo[] properties,
        CancellationToken cancellationToken)
    {
        using var rowGroupWriter = writer.CreateRowGroup();

        // Write each column
        for (var colIndex = 0; colIndex < columnNames.Length; colIndex++)
        {
            var columnName = columnNames[colIndex];
            var field = schema.Fields.FirstOrDefault(f => f.Name == columnName);

            if (field is null)
                continue;

            var property = properties[colIndex];
            var columnData = CreateColumnData(field, property.PropertyType, buffer, valueGetters[colIndex]);

            await rowGroupWriter.WriteColumnAsync(columnData, cancellationToken);
        }
    }

    private DataColumn CreateColumnData(
        Field field,
        Type propertyType,
        List<T> buffer,
        Func<T, object?> valueGetter)
    {
        var underlyingType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;
        var isNullableProperty = Nullable.GetUnderlyingType(propertyType) is not null;

        // Handle different types appropriately
        if (underlyingType == typeof(string))
        {
            var data = new string?[buffer.Count];

            for (var i = 0; i < buffer.Count; i++)
            {
                data[i] = valueGetter(buffer[i]) as string;
            }

            return new DataColumn((DataField)field, data);
        }

        if (underlyingType == typeof(int))
        {
            if (isNullableProperty)
            {
                var data = new int?[buffer.Count];

                for (var i = 0; i < buffer.Count; i++)
                {
                    var value = valueGetter(buffer[i]);

                    data[i] = value is int intValue
                        ? intValue
                        : null;
                }

                return new DataColumn((DataField)field, data);
            }
            else
            {
                var data = new int[buffer.Count];

                for (var i = 0; i < buffer.Count; i++)
                {
                    var value = valueGetter(buffer[i]);

                    data[i] = value is int intValue
                        ? intValue
                        : default;
                }

                return new DataColumn((DataField)field, data);
            }
        }

        if (underlyingType == typeof(long))
        {
            if (isNullableProperty)
            {
                var data = new long?[buffer.Count];

                for (var i = 0; i < buffer.Count; i++)
                {
                    var value = valueGetter(buffer[i]);

                    data[i] = value is long longValue
                        ? longValue
                        : null;
                }

                return new DataColumn((DataField)field, data);
            }
            else
            {
                var data = new long[buffer.Count];

                for (var i = 0; i < buffer.Count; i++)
                {
                    var value = valueGetter(buffer[i]);

                    data[i] = value is long longValue
                        ? longValue
                        : default;
                }

                return new DataColumn((DataField)field, data);
            }
        }

        if (underlyingType == typeof(short))
        {
            if (isNullableProperty)
            {
                var data = new short?[buffer.Count];

                for (var i = 0; i < buffer.Count; i++)
                {
                    var value = valueGetter(buffer[i]);

                    data[i] = value is short shortValue
                        ? shortValue
                        : null;
                }

                return new DataColumn((DataField)field, data);
            }
            else
            {
                var data = new short[buffer.Count];

                for (var i = 0; i < buffer.Count; i++)
                {
                    var value = valueGetter(buffer[i]);

                    data[i] = value is short shortValue
                        ? shortValue
                        : default;
                }

                return new DataColumn((DataField)field, data);
            }
        }

        if (underlyingType == typeof(byte))
        {
            if (isNullableProperty)
            {
                var data = new byte?[buffer.Count];

                for (var i = 0; i < buffer.Count; i++)
                {
                    var value = valueGetter(buffer[i]);

                    data[i] = value is byte byteValue
                        ? byteValue
                        : null;
                }

                return new DataColumn((DataField)field, data);
            }
            else
            {
                var data = new byte[buffer.Count];

                for (var i = 0; i < buffer.Count; i++)
                {
                    var value = valueGetter(buffer[i]);

                    data[i] = value is byte byteValue
                        ? byteValue
                        : default;
                }

                return new DataColumn((DataField)field, data);
            }
        }

        if (underlyingType == typeof(float))
        {
            if (isNullableProperty)
            {
                var data = new float?[buffer.Count];

                for (var i = 0; i < buffer.Count; i++)
                {
                    var value = valueGetter(buffer[i]);

                    data[i] = value is float floatValue
                        ? floatValue
                        : null;
                }

                return new DataColumn((DataField)field, data);
            }
            else
            {
                var data = new float[buffer.Count];

                for (var i = 0; i < buffer.Count; i++)
                {
                    var value = valueGetter(buffer[i]);

                    data[i] = value is float floatValue
                        ? floatValue
                        : default;
                }

                return new DataColumn((DataField)field, data);
            }
        }

        if (underlyingType == typeof(double))
        {
            if (isNullableProperty)
            {
                var data = new double?[buffer.Count];

                for (var i = 0; i < buffer.Count; i++)
                {
                    var value = valueGetter(buffer[i]);

                    data[i] = value is double doubleValue
                        ? doubleValue
                        : null;
                }

                return new DataColumn((DataField)field, data);
            }
            else
            {
                var data = new double[buffer.Count];

                for (var i = 0; i < buffer.Count; i++)
                {
                    var value = valueGetter(buffer[i]);

                    data[i] = value is double doubleValue
                        ? doubleValue
                        : default;
                }

                return new DataColumn((DataField)field, data);
            }
        }

        if (underlyingType == typeof(bool))
        {
            if (isNullableProperty)
            {
                var data = new bool?[buffer.Count];

                for (var i = 0; i < buffer.Count; i++)
                {
                    var value = valueGetter(buffer[i]);

                    data[i] = value is bool boolValue
                        ? boolValue
                        : null;
                }

                return new DataColumn((DataField)field, data);
            }
            else
            {
                var data = new bool[buffer.Count];

                for (var i = 0; i < buffer.Count; i++)
                {
                    var value = valueGetter(buffer[i]);

                    data[i] = value is bool boolValue
                        ? boolValue
                        : default;
                }

                return new DataColumn((DataField)field, data);
            }
        }

        if (underlyingType == typeof(decimal))
        {
            if (isNullableProperty)
            {
                var data = new decimal?[buffer.Count];

                for (var i = 0; i < buffer.Count; i++)
                {
                    var value = valueGetter(buffer[i]);

                    data[i] = value is decimal decimalValue
                        ? decimalValue
                        : null;
                }

                return new DataColumn((DataField)field, data);
            }
            else
            {
                var data = new decimal[buffer.Count];

                for (var i = 0; i < buffer.Count; i++)
                {
                    var value = valueGetter(buffer[i]);

                    data[i] = value is decimal decimalValue
                        ? decimalValue
                        : default;
                }

                return new DataColumn((DataField)field, data);
            }
        }

        if (underlyingType == typeof(DateTime))
        {
            if (isNullableProperty)
            {
                var data = new DateTime?[buffer.Count];

                for (var i = 0; i < buffer.Count; i++)
                {
                    var value = valueGetter(buffer[i]);

                    data[i] = value is DateTime dateTimeValue
                        ? dateTimeValue
                        : null;
                }

                return new DataColumn((DataField)field, data);
            }
            else
            {
                var data = new DateTime[buffer.Count];

                for (var i = 0; i < buffer.Count; i++)
                {
                    var value = valueGetter(buffer[i]);

                    data[i] = value is DateTime dateTimeValue
                        ? dateTimeValue
                        : default;
                }

                return new DataColumn((DataField)field, data);
            }
        }

        if (underlyingType == typeof(DateTimeOffset))
        {
            // DateTimeOffset is converted to DateTime (UTC) for storage
            if (isNullableProperty)
            {
                var data = new DateTime?[buffer.Count];

                for (var i = 0; i < buffer.Count; i++)
                {
                    var value = valueGetter(buffer[i]);

                    // Value getter already converts DateTimeOffset to DateTime via UtcDateTime property
                    data[i] = value is DateTime dateTimeValue
                        ? dateTimeValue
                        : null;
                }

                return new DataColumn((DataField)field, data);
            }
            else
            {
                var data = new DateTime[buffer.Count];

                for (var i = 0; i < buffer.Count; i++)
                {
                    var value = valueGetter(buffer[i]);

                    // Value getter already converts DateTimeOffset to DateTime via UtcDateTime property
                    data[i] = value is DateTime dateTimeValue
                        ? dateTimeValue
                        : default;
                }

                return new DataColumn((DataField)field, data);
            }
        }

        if (underlyingType == typeof(DateOnly))
        {
            // DateOnly is converted to DateTime for storage
            if (isNullableProperty)
            {
                var data = new DateTime?[buffer.Count];

                for (var i = 0; i < buffer.Count; i++)
                {
                    var value = valueGetter(buffer[i]);

                    // Value getter already converts DateOnly to DateTime via ToDateTime method
                    data[i] = value is DateTime dateTimeValue
                        ? dateTimeValue
                        : null;
                }

                return new DataColumn((DataField)field, data);
            }
            else
            {
                var data = new DateTime[buffer.Count];

                for (var i = 0; i < buffer.Count; i++)
                {
                    var value = valueGetter(buffer[i]);

                    // Value getter already converts DateOnly to DateTime via ToDateTime method
                    data[i] = value is DateTime dateTimeValue
                        ? dateTimeValue
                        : default;
                }

                return new DataColumn((DataField)field, data);
            }
        }

        if (underlyingType == typeof(byte[]))
        {
            var data = new byte[buffer.Count][];

            for (var i = 0; i < buffer.Count; i++)
            {
                var value = valueGetter(buffer[i]);
                data[i] = value as byte[] ?? [];
            }

            return new DataColumn((DataField)field, data);
        }

        // Default: convert to string representation
        {
            var data = new string?[buffer.Count];

            for (var i = 0; i < buffer.Count; i++)
            {
                var value = valueGetter(buffer[i]);
                data[i] = value?.ToString();
            }

            return new DataColumn((DataField)field, data);
        }
    }

    private static StorageUri CreateTempUri(StorageUri uri)
    {
        var tempSuffix = $".tmp-{Guid.NewGuid():N}";

        // Use Combine to append the temp suffix to the path
        // First, get the filename and add the temp suffix
        var path = uri.Path ?? string.Empty;
        var lastSlashIndex = path.LastIndexOf('/');

        var fileName = lastSlashIndex >= 0
            ? path[(lastSlashIndex + 1)..]
            : path;

        var directory = lastSlashIndex >= 0
            ? path[..lastSlashIndex]
            : "";

        var tempFileName = fileName + tempSuffix;

        var tempPath = string.IsNullOrEmpty(directory)
            ? "/" + tempFileName
            : directory + "/" + tempFileName;

        // Parse the modified URI string to create a new StorageUri
        return StorageUri.Parse($"{uri.Scheme}://{uri.Host ?? ""}{tempPath}");
    }

    private async Task PublishAtomicWrite(
        IStorageProvider provider,
        StorageUri tempUri,
        StorageUri targetUri,
        CancellationToken cancellationToken)
    {
        // For providers that support rename/move, use that
        // Otherwise, copy and delete
        if (provider is IMoveableStorageProvider moveableProvider)
        {
            await moveableProvider.MoveAsync(tempUri, targetUri, cancellationToken);

            // MoveAsync atomically moves the file - no separate cleanup needed
        }
        else
        {
            // Fallback: copy then delete
            await using var readStream = await provider.OpenReadAsync(tempUri, cancellationToken);
            await using var writeStream = await provider.OpenWriteAsync(targetUri, cancellationToken);
            await readStream.CopyToAsync(writeStream, cancellationToken);
            await writeStream.FlushAsync(cancellationToken);

            if (provider is IDeletableStorageProvider deletableProvider)
                await deletableProvider.DeleteAsync(tempUri, CancellationToken.None);
        }
    }
}
