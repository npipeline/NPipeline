namespace NPipeline.Connectors.Exceptions;

/// <summary>
///     Represents a database parameter with name and value.
/// </summary>
/// <param name="Name">The parameter name.</param>
/// <param name="Value">The parameter value.</param>
public record DatabaseParameter(string Name, object? Value);
