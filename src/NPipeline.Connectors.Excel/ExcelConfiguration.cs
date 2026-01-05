using System.Text;

namespace NPipeline.Connectors.Excel;

/// <summary>
///     Configuration for Excel operations with NPipeline-specific settings.
/// </summary>
/// <remarks>
///     <para>
///         This class provides configuration options for reading and writing Excel files using ExcelDataReader.
///         It includes settings for sheet selection, header handling, and buffer sizes for optimal performance.
///     </para>
///     <para>
///         ExcelDataReader supports both legacy XLS (binary) and modern XLSX (Open XML) formats.
///         The configuration is applied consistently across both formats where applicable.
///     </para>
/// </remarks>
public class ExcelConfiguration
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="ExcelConfiguration" /> class with default settings.
    /// </summary>
    public ExcelConfiguration()
    {
    }

    /// <summary>
    ///     Gets or sets the buffer size for stream operations.
    ///     Default value is 4096 (4KB), which provides good performance for most scenarios.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         The buffer size affects the efficiency of Excel I/O operations. Larger buffers reduce the number
    ///         of system calls and can improve throughput, but consume more memory. The default of 4KB balances
    ///         performance and memory usage for typical Excel processing workloads.
    ///     </para>
    ///     <para>
    ///         For high-throughput scenarios with large Excel files, consider increasing to 8192 (8KB) or higher.
    ///         For memory-constrained environments, smaller values can be used.
    ///     </para>
    /// </remarks>
    public int BufferSize { get; set; } = 4096;

    /// <summary>
    ///     Gets or sets the name of the sheet to read from or write to.
    ///     Default value is <c>null</c>, which uses the first sheet for reading or creates a default sheet for writing.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         When reading, if this value is <c>null</c>, the first sheet in the workbook will be used.
    ///         When writing, if this value is <c>null</c>, a default sheet name (e.g., "Sheet1") will be created.
    ///     </para>
    ///     <para>
    ///         Sheet names are case-insensitive when reading. When writing, the sheet name will be created
    ///         exactly as specified.
    ///     </para>
    /// </remarks>
    public string? SheetName { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether the first row contains column headers.
    ///     Default value is <c>true</c>.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         When <c>true</c>, the first row is treated as headers and used for property mapping when reading,
    ///         and written as header row when writing.
    ///     </para>
    ///     <para>
    ///         When <c>false</c>, the first row is treated as data. For reading, properties are mapped by column index.
    ///         For writing, no header row is written.
    ///     </para>
    ///     <para>
    ///         This setting is only applicable when the target type is a class or record type.
    ///         For primitive types, this setting is ignored.
    ///     </para>
    /// </remarks>
    public bool FirstRowIsHeader { get; set; } = true;

    /// <summary>
    ///     Gets or sets the encoding to use for reading legacy XLS files with text data.
    ///     Default value is <c>null</c>, which uses ExcelDataReader's default encoding detection.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         This setting primarily affects legacy XLS (binary) files that contain text data.
    ///         Modern XLSX files use UTF-8 encoding by default and are not affected by this setting.
    ///     </para>
    ///     <para>
    ///         If set to <c>null</c>, ExcelDataReader will attempt to detect the encoding automatically.
    ///         For consistent behavior, specify an explicit encoding such as <see cref="System.Text.Encoding.UTF8" />.
    ///     </para>
    /// </remarks>
    public Encoding? Encoding { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether to convert data types automatically.
    ///     Default value is <c>true</c>.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         When <c>true</c>, ExcelDataReader will attempt to convert cell values to the target type
    ///         using its built-in conversion logic.
    ///     </para>
    ///     <para>
    ///         When <c>false</c>, cell values are returned as strings, and type conversion must be handled
    ///         manually in the mapping logic.
    ///     </para>
    /// </remarks>
    public bool AutodetectSeparators { get; set; } = true;

    /// <summary>
    ///     Gets or sets a value indicating whether to analyze entire workbook to determine data types.
    ///     Default value is <c>false</c>.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         When <c>true</c>, ExcelDataReader will analyze all rows in each column to determine most
    ///         appropriate data type. This provides more accurate type detection but is slower for large files.
    ///     </para>
    ///     <para>
    ///         When <c>false</c>, ExcelDataReader analyzes only the first few rows to determine data types.
    ///         This is faster but may lead to incorrect type detection if data types vary throughout the column.
    ///     </para>
    /// </remarks>
    public bool AnalyzeAllColumns { get; set; }

    /// <summary>
    ///     Gets or sets the number of rows to analyze for data type detection when <see cref="AnalyzeAllColumns" /> is <c>false</c>.
    ///     Default value is 30.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         This setting controls how many rows ExcelDataReader analyzes to determine the data type for each column.
    ///         Larger values provide more accurate type detection but are slower.
    ///     </para>
    ///     <para>
    ///         The default value of 30 provides a good balance between accuracy and performance for most scenarios.
    ///     </para>
    /// </remarks>
    public int AnalyzeInitialRowCount { get; set; } = 30;

    /// <summary>
    ///     Gets or sets a value indicating whether to use the first row as the column header names.
    ///     Default value is <c>true</c>.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         This property is a convenience wrapper around <see cref="FirstRowIsHeader" />.
    ///         Both properties control the same behavior and are kept in sync.
    ///     </para>
    /// </remarks>
    public bool HasHeaderRow
    {
        get => FirstRowIsHeader;
        set => FirstRowIsHeader = value;
    }
}
