namespace NPipeline.Connectors.Json;

/// <summary>
///     Exception thrown when a JSON mapping error occurs.
/// </summary>
/// <remarks>
///     <para>
///         This exception is thrown when there is an error mapping JSON data to a .NET type,
///         or when a property cannot be accessed or converted.
///     </para>
///     <para>
///         Common scenarios where this exception is thrown:
///         <list type="bullet">
///             <item>
///                 <description>A required property is missing from the JSON object.</description>
///             </item>
///             <item>
///                 <description>A property value cannot be converted to the target type.</description>
///             </item>
///             <item>
///                 <description>A nested property path is invalid or does not exist.</description>
///             </item>
///             <item>
///                 <description>The JSON structure does not match the expected schema.</description>
///             </item>
///         </list>
///     </para>
/// </remarks>
public sealed class JsonMappingException : Exception
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="JsonMappingException" /> class.
    /// </summary>
    public JsonMappingException()
        : base("An error occurred while mapping JSON data.")
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="JsonMappingException" /> class with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public JsonMappingException(string message)
        : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="JsonMappingException" /> class with a specified error message
    ///     and a reference to the inner exception that is the cause of this exception.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">The exception that is the cause of the current exception.</param>
    public JsonMappingException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
