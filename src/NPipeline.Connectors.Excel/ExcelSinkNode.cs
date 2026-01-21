using System.Globalization;
using System.Reflection;
using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Exceptions;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Excel;

/// <summary>
///     Sink node that writes items to Excel files using a pluggable <see cref="IStorageProvider" />.
/// </summary>
/// <typeparam name="T">Record type to serialize for each Excel row.</typeparam>
/// <remarks>
///     <para>
///         This sink node writes data to Excel files in XLSX (Open XML) format. It provides configurable options
///         for sheet selection, header writing, and type conversion.
///     </para>
///     <para>
///         The node supports multiple constructor patterns:
///         <list type="bullet">
///             <item>
///                 <description>Using the default <see cref="IStorageResolver" /> (recommended for simplicity)</description>
///             </item>
///             <item>
///                 <description>Using a custom <see cref="IStorageResolver" /> for resolver-based provider resolution at execution time</description>
///             </item>
///             <item>
///                 <description>Using a specific <see cref="IStorageProvider" /> instance for direct provider injection</description>
///             </item>
///         </list>
///     </para>
///     <para>
///         Data mapping is performed using reflection to map properties of type <typeparamref name="T" /> to Excel columns.
///         When <see cref="ExcelConfiguration.FirstRowIsHeader" /> is <c>true</c>, property names are written as the header row.
///     </para>
///     <para>
///         <note type="important">
///             This sink node only supports writing XLSX (Open XML) format. Legacy XLS (binary) format is not supported for writing.
///         </note>
///     </para>
/// </remarks>
public sealed class ExcelSinkNode<T> : SinkNode<T>
{
    private static readonly Lazy<IStorageResolver> DefaultResolver =
        new(() => StorageProviderFactory.CreateResolver());

    private readonly ExcelConfiguration _configuration;
    private readonly IStorageProvider? _provider;
    private readonly IStorageResolver? _resolver;
    private readonly StorageUri _uri;

    private ExcelSinkNode(
        StorageUri uri,
        ExcelConfiguration? configuration)
    {
        ArgumentNullException.ThrowIfNull(uri);
        _uri = uri;
        _configuration = configuration ?? new ExcelConfiguration();
    }

    /// <summary>
    ///     Construct an Excel sink node that resolves a storage provider from a resolver at execution time.
    /// </summary>
    /// <param name="uri">The URI of the Excel file to write to.</param>
    /// <param name="resolver">The storage resolver used to obtain the storage provider. If <c>null</c>, a default resolver is used.</param>
    /// <param name="configuration">Optional configuration for Excel writing. If <c>null</c>, default configuration is used.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="uri" /> is <c>null</c>.</exception>
    public ExcelSinkNode(
        StorageUri uri,
        IStorageResolver? resolver = null,
        ExcelConfiguration? configuration = null)
        : this(uri, configuration)
    {
        _resolver = resolver ?? DefaultResolver.Value;
    }

    /// <summary>
    ///     Construct an Excel sink node that uses a specific storage provider instance.
    /// </summary>
    /// <param name="provider">The storage provider to use for writing the Excel file.</param>
    /// <param name="uri">The URI of the Excel file to write to.</param>
    /// <param name="configuration">Optional configuration for Excel writing. If <c>null</c>, default configuration is used.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="provider" /> or <paramref name="uri" /> is <c>null</c>.</exception>
    public ExcelSinkNode(
        IStorageProvider provider,
        StorageUri uri,
        ExcelConfiguration? configuration = null)
        : this(uri, configuration)
    {
        ArgumentNullException.ThrowIfNull(provider);
        _provider = provider;
    }

    /// <inheritdoc />
    public override async Task ExecuteAsync(IDataPipe<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        var provider = _provider ?? StorageProviderFactory.GetProviderOrThrow(
            _resolver ?? throw new InvalidOperationException("No storage resolver configured for ExcelSinkNode."),
            _uri);

        if (provider is IStorageProviderMetadataProvider metaProvider)
        {
            var meta = metaProvider.GetMetadata();

            if (!meta.SupportsWrite)
                throw new UnsupportedStorageCapabilityException(_uri, "write", meta.Name);
        }

        await using var stream = await provider.OpenWriteAsync(_uri, cancellationToken).ConfigureAwait(false);
        await WriteToExcelStream(stream, input, _configuration, cancellationToken).ConfigureAwait(false);
    }

    private static async Task WriteToExcelStream(
        Stream stream,
        IDataPipe<T> source,
        ExcelConfiguration config,
        CancellationToken cancellationToken)
    {
        var targetStream = stream;
        var requiresCopyBack = !stream.CanRead || !stream.CanSeek;

        // OpenXML packaging requires a readable, seekable stream. If the provider only supplies a write-only stream,
        // fall back to a temporary buffer and copy the result back to the original stream once complete.
        if (requiresCopyBack)
            targetStream = new MemoryStream();

        using (var spreadsheetDocument = SpreadsheetDocument.Create(targetStream, SpreadsheetDocumentType.Workbook))
        {
            var workbookPart = spreadsheetDocument.AddWorkbookPart();
            workbookPart.Workbook = new Workbook();

            var worksheetPart = workbookPart.AddNewPart<WorksheetPart>();

            using (var writer = OpenXmlWriter.Create(worksheetPart))
            {
                writer.WriteStartElement(new Worksheet());
                writer.WriteStartElement(new SheetData());

                var type = typeof(T);
                var isComplexType = type.IsClass && type != typeof(string);

                var properties = isComplexType
                    ? type.GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.CanRead).ToArray()
                    : Array.Empty<PropertyInfo>();

                uint rowIndex = 1;

                if (config.FirstRowIsHeader && isComplexType && properties.Length > 0)
                {
                    WriteHeaderRow(writer, properties, rowIndex);
                    rowIndex++;
                }

                await foreach (var item in source.WithCancellation(cancellationToken))
                {
                    if (item is null)
                        continue;

                    WriteDataRow(writer, item, properties, isComplexType, rowIndex);
                    rowIndex++;
                }

                writer.WriteEndElement(); // SheetData
                writer.WriteEndElement(); // Worksheet
            }

            var sheets = workbookPart.Workbook.AppendChild(new Sheets());

            sheets.Append(new Sheet
            {
                Id = workbookPart.GetIdOfPart(worksheetPart),
                SheetId = 1,
                Name = config.SheetName ?? "Sheet1",
            });

            workbookPart.Workbook.Save();
        }

        if (requiresCopyBack && targetStream is MemoryStream buffer)
        {
            buffer.Position = 0;
            await buffer.CopyToAsync(stream, cancellationToken).ConfigureAwait(false);
            await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private static void WriteHeaderRow(OpenXmlWriter writer, IReadOnlyList<PropertyInfo> properties, uint rowIndex)
    {
        writer.WriteStartElement(new Row { RowIndex = rowIndex });

        for (var i = 0; i < properties.Count; i++)
        {
            WriteInlineStringCell(writer, properties[i].Name, rowIndex, (uint)(i + 1));
        }

        writer.WriteEndElement();
    }

    private static void WriteDataRow(
        OpenXmlWriter writer,
        T item,
        IReadOnlyList<PropertyInfo> properties,
        bool isComplexType,
        uint rowIndex)
    {
        writer.WriteStartElement(new Row { RowIndex = rowIndex });

        if (isComplexType && properties.Count > 0)
        {
            for (var i = 0; i < properties.Count; i++)
            {
                var value = properties[i].GetValue(item, null);
                WriteCell(writer, value, rowIndex, (uint)(i + 1));
            }
        }
        else
            WriteCell(writer, item, rowIndex, 1);

        writer.WriteEndElement();
    }

    private static void WriteInlineStringCell(OpenXmlWriter writer, string text, uint rowIndex, uint columnIndex)
    {
        var cell = new Cell
        {
            CellReference = GetCellReference(rowIndex, columnIndex),
            DataType = CellValues.InlineString,
        };

        writer.WriteStartElement(cell);
        writer.WriteElement(new InlineString(new Text(text)));
        writer.WriteEndElement();
    }

    private static void WriteCell(OpenXmlWriter writer, object? value, uint rowIndex, uint columnIndex)
    {
        var reference = GetCellReference(rowIndex, columnIndex);
        var (cell, inlineString) = CreateCellValue(value, reference);

        writer.WriteStartElement(cell);

        if (inlineString is not null)
            writer.WriteElement(inlineString);
        else if (cell.CellValue is not null)
            writer.WriteElement(cell.CellValue);

        writer.WriteEndElement();
    }

    private static (Cell Cell, InlineString? InlineString) CreateCellValue(object? value, string cellReference)
    {
        var cell = new Cell
        {
            CellReference = cellReference,
        };

        if (value is null || value == DBNull.Value)
            return (cell, null);

        switch (value)
        {
            case string s:
                cell.DataType = CellValues.InlineString;
                return (cell, new InlineString(new Text(s)));

            case bool b:
                cell.DataType = CellValues.Boolean;

                cell.CellValue = new CellValue(b
                    ? "1"
                    : "0");

                return (cell, null);

            case DateTime dt:
                cell.DataType = CellValues.Number;
                cell.CellValue = new CellValue(dt.ToOADate().ToString(CultureInfo.InvariantCulture));
                return (cell, null);

            case DateTimeOffset dto:
                cell.DataType = CellValues.Number;
                cell.CellValue = new CellValue(dto.UtcDateTime.ToOADate().ToString(CultureInfo.InvariantCulture));
                return (cell, null);

            case int i:
                cell.DataType = CellValues.Number;
                cell.CellValue = new CellValue(i.ToString(CultureInfo.InvariantCulture));
                return (cell, null);

            case long l:
                cell.DataType = CellValues.Number;
                cell.CellValue = new CellValue(l.ToString(CultureInfo.InvariantCulture));
                return (cell, null);

            case short s16:
                cell.DataType = CellValues.Number;
                cell.CellValue = new CellValue(s16.ToString(CultureInfo.InvariantCulture));
                return (cell, null);

            case decimal m:
                cell.DataType = CellValues.Number;
                cell.CellValue = new CellValue(m.ToString(CultureInfo.InvariantCulture));
                return (cell, null);

            case double d:
                cell.DataType = CellValues.Number;
                cell.CellValue = new CellValue(d.ToString(CultureInfo.InvariantCulture));
                return (cell, null);

            case float f:
                cell.DataType = CellValues.Number;
                cell.CellValue = new CellValue(f.ToString(CultureInfo.InvariantCulture));
                return (cell, null);

            case uint ui:
                cell.DataType = CellValues.Number;
                cell.CellValue = new CellValue(ui.ToString(CultureInfo.InvariantCulture));
                return (cell, null);

            case ulong ul:
                cell.DataType = CellValues.Number;
                cell.CellValue = new CellValue(ul.ToString(CultureInfo.InvariantCulture));
                return (cell, null);

            case ushort us:
                cell.DataType = CellValues.Number;
                cell.CellValue = new CellValue(us.ToString(CultureInfo.InvariantCulture));
                return (cell, null);

            case Enum e:
                cell.DataType = CellValues.InlineString;
                return (cell, new InlineString(new Text(e.ToString())));

            default:
                cell.DataType = CellValues.InlineString;
                return (cell, new InlineString(new Text(value.ToString() ?? string.Empty)));
        }
    }

    private static string GetCellReference(uint row, uint column)
    {
        var columnName = string.Empty;
        var temp = column;

        while (temp > 0)
        {
            temp--;
            columnName = Convert.ToChar('A' + temp % 26) + columnName;
            temp /= 26;
        }

        return $"{columnName}{row}";
    }
}
