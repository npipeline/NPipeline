using NPipeline.Connectors.Exceptions;

namespace NPipeline.Connectors.Abstractions;

/// <summary>
/// Database mapper abstraction for mapping between database rows and objects.
/// </summary>
/// <typeparam name="T">The type being mapped.</typeparam>
public interface IDatabaseMapper<T>
{
    /// <summary>
    /// Maps a database reader row to an object of type T.
    /// </summary>
    /// <param name="reader">The database reader.</param>
    /// <returns>The mapped object.</returns>
    T MapFromReader(IDatabaseReader reader);

    /// <summary>
    /// Maps an object of type T to database parameters.
    /// </summary>
    /// <param name="item">The object to map.</param>
    /// <returns>The database parameters.</returns>
    IEnumerable<DatabaseParameter> MapToParameters(T item);
}
