namespace NPipeline.StorageProviders.Abstractions;

/// <summary>
///     Factory for creating database mappers.
/// </summary>
public interface IDatabaseMapperFactory
{
    /// <summary>
    ///     Creates a mapper for the specified type.
    /// </summary>
    /// <typeparam name="T">The type to create mapper for.</typeparam>
    /// <returns>The database mapper.</returns>
    IDatabaseMapper<T> CreateMapper<T>();
}