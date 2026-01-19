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

        // Collect all items to write (Excel writing requires all data upfront)
        var items = new List<T>();

        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            if (item is not null)
                items.Add(item);
        }

        // Write to a memory stream first, then to the provider
        using var memoryStream = new MemoryStream();
        WriteToExcel(memoryStream, items, _configuration, cancellationToken);

        memoryStream.Position = 0;

        await using var stream = await provider.OpenWriteAsync(_uri, cancellationToken).ConfigureAwait(false);
        await memoryStream.CopyToAsync(stream, _configuration.BufferSize, cancellationToken).ConfigureAwait(false);
    }

    private static void WriteToExcel(Stream stream, List<T> items, ExcelConfiguration config, CancellationToken cancellationToken)
    {
        using var spreadsheetDocument = SpreadsheetDocument.Create(stream, SpreadsheetDocumentType.Workbook);

        var workbookPart = spreadsheetDocument.AddWorkbookPart();
        workbookPart.Workbook = new Workbook();

        var worksheetPart = workbookPart.AddNewPart<WorksheetPart>();
        var sheetData = new SheetData();
        worksheetPart.Worksheet = new Worksheet(sheetData);

        var sheets = workbookPart.Workbook.AppendChild(new Sheets());

        var sheet = new Sheet
        {
            Id = workbookPart.GetIdOfPart(worksheetPart),
            SheetId = 1,
            Name = config.SheetName ?? "Sheet1",
        };

        sheets.Append(sheet);

        // Check if T is a complex type with properties or a primitive type
        var type = typeof(T);
        var isComplexType = type.IsClass && type != typeof(string);

        var properties = isComplexType
            ? type.GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.CanRead).ToList()
            : null;

        // Write header row if configured and T is a complex type
        uint rowIndex = 1;

        if (config.FirstRowIsHeader && isComplexType && properties != null)
        {
            var headerRow = new Row { RowIndex = rowIndex };

            for (var i = 0; i < properties.Count; i++)
            {
                var cell = CreateTextCell(properties[i].Name, rowIndex, (uint)(i + 1));
                headerRow.AppendChild(cell);
            }

            sheetData.AppendChild(headerRow);
            rowIndex++;
        }

        // Write data rows
        foreach (var item in items)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var row = new Row { RowIndex = rowIndex };

            if (isComplexType && properties != null)
            {
                // Complex type: write each property as a column
                for (var i = 0; i < properties.Count; i++)
                {
                    var value = properties[i].GetValue(item, null);
                    var cell = CreateCell(value, rowIndex, (uint)(i + 1));
                    row.AppendChild(cell);
                }
            }
            else
            {
                // Primitive type: write value directly
                var cell = CreateCell(item, rowIndex, 1);
                row.AppendChild(cell);
            }

            sheetData.AppendChild(row);
            rowIndex++;
        }

        workbookPart.Workbook.Save();
    }

    private static Cell CreateTextCell(string value, uint rowIndex, uint columnIndex)
    {
        var cell = new Cell
        {
            DataType = CellValues.String,
            CellReference = GetCellReference(rowIndex, columnIndex),
        };

        // Inline string for better compatibility
        var inlineString = new InlineString(new Text(value));
        cell.AppendChild(inlineString);

        return cell;
    }

    private static Cell CreateCell(object? value, uint rowIndex, uint columnIndex)
    {
        var cell = new Cell
        {
            CellReference = GetCellReference(rowIndex, columnIndex),
        };

        if (value is null || value == DBNull.Value)
            return cell;

        var type = value.GetType();

        if (type == typeof(string))
        {
            cell.DataType = CellValues.String;
            var inlineString = new InlineString(new Text((string)value));
            cell.AppendChild(inlineString);
        }
        else if (type == typeof(bool) || type == typeof(bool?))
        {
            var boolValue = (bool)value;
            cell.DataType = CellValues.Boolean;

            // FIX: Use "1" for true and "0" for false to match Open XML specification
            cell.CellValue = new CellValue(boolValue
                ? "1"
                : "0");
        }
        else if (type == typeof(DateTime) || type == typeof(DateTime?))
        {
            cell.DataType = CellValues.Number;
            cell.CellValue = new CellValue(((DateTime)value).ToOADate().ToString(CultureInfo.InvariantCulture));
        }
        else if (type == typeof(int) || type == typeof(int?) ||
                 type == typeof(long) || type == typeof(long?) ||
                 type == typeof(short) || type == typeof(short?) ||
                 type == typeof(decimal) || type == typeof(decimal?) ||
                 type == typeof(double) || type == typeof(double?) ||
                 type == typeof(float) || type == typeof(float?))
        {
            cell.DataType = CellValues.Number;
            cell.CellValue = new CellValue(Convert.ToDouble(value).ToString(CultureInfo.InvariantCulture));
        }
        else
        {
            // Default to string for other types
            cell.DataType = CellValues.String;
            var inlineString = new InlineString(new Text(value.ToString() ?? string.Empty));
            cell.AppendChild(inlineString);
        }

        return cell;
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
