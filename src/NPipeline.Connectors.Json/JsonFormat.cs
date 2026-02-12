namespace NPipeline.Connectors.Json;

/// <summary>
///     Specifies the format of JSON data when reading or writing.
/// </summary>
/// <remarks>
///     <para>
///         JSON data can be structured in different formats for different use cases.
///         The <see cref="JsonFormat" /> enum allows you to specify which format to use.
///     </para>
///     <para>
///         <list type="bullet">
///             <item>
///                 <description>
///                     <see cref="Array" />: JSON data is structured as a JSON array containing JSON objects.
///                     This is the most common format for JSON files and is the default.
///                 </description>
///             </item>
///             <item>
///                 <description>
///                     <see cref="NewlineDelimited" />: JSON data is structured as newline-delimited JSON (NDJSON),
///                     where each line contains a separate JSON object. This format is useful for streaming
///                     and log files where each record is independent.
///                 </description>
///             </item>
///         </list>
///     </para>
/// </remarks>
public enum JsonFormat
{
    /// <summary>
    ///     JSON data is structured as a JSON array containing JSON objects.
    ///     Example: <c>[{"name": "John"}, {"name": "Jane"}]</c>
    /// </summary>
    Array,

    /// <summary>
    ///     JSON data is structured as newline-delimited JSON (NDJSON),
    ///     where each line contains a separate JSON object.
    ///     Example: <c>{"name": "John"}\n{"name": "Jane"}</c>
    /// </summary>
    NewlineDelimited,
}
