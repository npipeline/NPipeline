using System.Text.Json;

namespace NPipeline.Connectors.Json;

/// <summary>
///     Configuration for JSON operations with NPipeline-specific settings.
/// </summary>
/// <remarks>
///     <para>
///         This class provides configuration options for reading and writing JSON files using System.Text.Json.
///         It includes settings for buffer size, format, indentation, property naming, and error handling.
///     </para>
///     <para>
///         The configuration is designed to provide sensible defaults while allowing customization
///         for specific use cases. The default settings align with the CSV and Excel connectors
///         for consistent behavior across different data formats.
///     </para>
/// </remarks>
public class JsonConfiguration
{
    private sealed class LowerCaseNamingPolicy : JsonNamingPolicy
    {
        public static readonly LowerCaseNamingPolicy Instance = new();

        public override string ConvertName(string name)
        {
            return string.IsNullOrEmpty(name) ? name : name.ToLowerInvariant();
        }
    }

    private sealed class PascalCaseNamingPolicy : JsonNamingPolicy
    {
        public static readonly PascalCaseNamingPolicy Instance = new();

        public override string ConvertName(string name)
        {
            if (string.IsNullOrEmpty(name))
                return name;

            return char.ToUpperInvariant(name[0]) + name[1..];
        }
    }

    private JsonSerializerOptions? _serializerOptions;
    private JsonPropertyNamingPolicy _propertyNamingPolicy = JsonPropertyNamingPolicy.LowerCase;

    /// <summary>
    ///     Initializes a new instance of the <see cref="JsonConfiguration" /> class with default settings.
    /// </summary>
    public JsonConfiguration()
    {
    }

    /// <summary>
    ///     Gets or sets the buffer size for stream operations.
    ///     Default value is 4096 (4KB), which provides good performance for most scenarios.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         The buffer size affects the efficiency of JSON I/O operations. Larger buffers reduce the number
    ///         of system calls and can improve throughput, but consume more memory. The default of 4KB balances
    ///         performance and memory usage for typical JSON processing workloads.
    ///     </para>
    ///     <para>
    ///         For high-throughput scenarios with large JSON files, consider increasing to 8192 (8KB) or higher.
    ///         For memory-constrained environments, smaller values can be used.
    ///     </para>
    /// </remarks>
    public int BufferSize { get; set; } = 4096;

    /// <summary>
    ///     Gets or sets the format of JSON data when reading or writing.
    ///     Default value is <see cref="JsonFormat.Array" />.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         When <see cref="JsonFormat.Array" />, JSON data is structured as a JSON array containing JSON objects.
    ///         This is the most common format for JSON files.
    ///     </para>
    ///     <para>
    ///         When <see cref="JsonFormat.NewlineDelimited" />, JSON data is structured as newline-delimited JSON (NDJSON),
    ///         where each line contains a separate JSON object. This format is useful for streaming and log files.
    ///     </para>
    /// </remarks>
    public JsonFormat Format { get; set; } = JsonFormat.Array;

    /// <summary>
    ///     Gets or sets a value indicating whether JSON output should be formatted with indentation.
    ///     Default value is <c>false</c>.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         When <c>true</c>, JSON output is formatted with indentation and newlines for human readability.
    ///         This increases the file size but makes the JSON easier to read and debug.
    ///     </para>
    ///     <para>
    ///         When <c>false</c>, JSON output is compact without whitespace. This reduces file size and
    ///         improves parsing performance, but is harder for humans to read.
    ///     </para>
    /// </remarks>
    public bool WriteIndented { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether property name comparison is case-insensitive.
    ///     Default value is <c>true</c>.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         When <c>true</c>, property names are matched case-insensitively. This aligns with the behavior
    ///         of the CSV and Excel connectors, which also use case-insensitive header/column matching.
    ///     </para>
    ///     <para>
    ///         When <c>false</c>, property names must match exactly, including case.
    ///     </para>
    /// </remarks>
    public bool PropertyNameCaseInsensitive { get; set; } = true;

    /// <summary>
    ///     Gets or sets the naming policy for JSON property names.
    ///     Default value is <see cref="JsonPropertyNamingPolicy.LowerCase" />.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         This property controls how property names are transformed when mapping between .NET properties
    ///         and JSON properties. The default of <see cref="JsonPropertyNamingPolicy.LowerCase" /> provides
    ///         consistency with the CSV and Excel connectors.
    ///     </para>
    ///     <para>
    ///         Setting this property updates the <see cref="SerializerOptions" /> with the appropriate
    ///         <see cref="System.Text.Json.JsonNamingPolicy" />.
    ///     </para>
    /// </remarks>
    public JsonPropertyNamingPolicy PropertyNamingPolicy
    {
        get => _propertyNamingPolicy;
        set
        {
            _propertyNamingPolicy = value;
            _serializerOptions = null; // Force recreation of serializer options
        }
    }

    /// <summary>
    ///     Gets or sets an optional handler invoked when a row mapping throws.
    ///     Return true to skip the row and continue; return false or rethrow to fail the pipeline.
    ///     Default value is <c>null</c>.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         This handler provides a way to handle mapping errors gracefully without failing the entire pipeline.
    ///         The handler receives the exception that was thrown and the <see cref="JsonRow" /> that caused the error.
    ///     </para>
    ///     <para>
    ///         When the handler returns <c>true</c>, the row is skipped and processing continues with the next row.
    ///         When the handler returns <c>false</c> or rethrows the exception, the pipeline fails.
    ///     </para>
    /// </remarks>
    public Func<Exception, JsonRow, bool>? RowErrorHandler { get; set; }

    /// <summary>
    ///     Gets the <see cref="JsonSerializerOptions" /> configured based on this <see cref="JsonConfiguration" />.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         This property returns a <see cref="JsonSerializerOptions" /> instance that is configured
    ///         based on the settings in this <see cref="JsonConfiguration" />. The options are created
    ///         on first access and cached for subsequent uses.
    ///     </para>
    ///     <para>
    ///         The following settings are applied:
    ///         <list type="bullet">
    ///             <item>
    ///                 <description>
    ///                     <see cref="JsonSerializerOptions.PropertyNameCaseInsensitive" /> is set to the value of
    ///                     <see cref="PropertyNameCaseInsensitive" />.
    ///                 </description>
    ///             </item>
    ///             <item>
    ///                 <description>
    ///                     <see cref="JsonSerializerOptions.PropertyNamingPolicy" /> is set based on the value of
    ///                     <see cref="PropertyNamingPolicy" />.
    ///                 </description>
    ///             </item>
    ///             <item>
    ///                 <description>
    ///                     <see cref="JsonSerializerOptions.WriteIndented" /> is set to the value of
    ///                     <see cref="WriteIndented" />.
    ///                 </description>
    ///             </item>
    ///         </list>
    ///     </para>
    /// </remarks>
    public JsonSerializerOptions SerializerOptions
    {
        get
        {
            if (_serializerOptions is not null)
                return _serializerOptions;

            var options = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = PropertyNameCaseInsensitive,
                WriteIndented = WriteIndented
            };

            options.PropertyNamingPolicy = PropertyNamingPolicy switch
            {
                JsonPropertyNamingPolicy.LowerCase => LowerCaseNamingPolicy.Instance,
                JsonPropertyNamingPolicy.CamelCase => JsonNamingPolicy.CamelCase,
                JsonPropertyNamingPolicy.SnakeCase => JsonNamingPolicy.SnakeCaseLower,
                JsonPropertyNamingPolicy.PascalCase => PascalCaseNamingPolicy.Instance,
                JsonPropertyNamingPolicy.AsIs => null,
                _ => LowerCaseNamingPolicy.Instance
            };

            _serializerOptions = options;
            return _serializerOptions;
        }
    }
}
