using NPipeline.Connectors.MongoDB.Exceptions;

namespace NPipeline.Connectors.MongoDB.Mapping;

/// <summary>
///     High-level mapper that converts MongoDB documents to CLR types using the MongoMapperBuilder.
///     Provides a simple API for mapping documents with caching support.
/// </summary>
public static class MongoDocumentMapper
{
    /// <summary>
    ///     Maps a MongoRow to the specified type.
    /// </summary>
    /// <typeparam name="T">The target type.</typeparam>
    /// <param name="row">The MongoRow to map.</param>
    /// <returns>The mapped object.</returns>
    /// <exception cref="MongoMappingException">Thrown when mapping fails.</exception>
    public static T Map<T>(MongoRow row)
    {
        ArgumentNullException.ThrowIfNull(row);

        var mapper = MongoMapperBuilder.GetOrCreateMapper<T>();
        return mapper(row);
    }

    /// <summary>
    ///     Attempts to map a MongoRow to the specified type.
    /// </summary>
    /// <typeparam name="T">The target type.</typeparam>
    /// <param name="row">The MongoRow to map.</param>
    /// <param name="result">The mapped object if successful.</param>
    /// <returns>True if mapping succeeded; otherwise false.</returns>
    public static bool TryMap<T>(MongoRow row, out T? result)
    {
        if (row == null)
        {
            result = default;
            return false;
        }

        try
        {
            var mapper = MongoMapperBuilder.GetOrCreateMapper<T>();
            result = mapper(row);
            return true;
        }
        catch (MongoMappingException)
        {
            result = default;
            return false;
        }
    }

    /// <summary>
    ///     Maps a sequence of MongoRows to the specified type.
    /// </summary>
    /// <typeparam name="T">The target type.</typeparam>
    /// <param name="rows">The sequence of MongoRows to map.</param>
    /// <returns>An enumerable of mapped objects.</returns>
    /// <exception cref="MongoMappingException">Thrown when mapping fails.</exception>
    public static IEnumerable<T> MapAll<T>(IEnumerable<MongoRow> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);

        var mapper = MongoMapperBuilder.GetOrCreateMapper<T>();
        return rows.Select(mapper);
    }

    /// <summary>
    ///     Clears the cached mappers.
    ///     Use this if the mapping configuration has changed at runtime.
    /// </summary>
    public static void ClearCache()
    {
        MongoMapperBuilder.ClearCache();
        MongoWriteDocumentMapper.ClearCache();
    }
}
