using MongoDB.Driver;
using NPipeline.Connectors.MongoDB.Configuration;

namespace NPipeline.Connectors.MongoDB.Connection;

/// <summary>
///     Factory for creating configured <see cref="IMongoClient" /> instances.
/// </summary>
public static class MongoConnectionFactory
{
    /// <summary>
    ///     Creates a new <see cref="IMongoClient" /> with the specified connection string and configuration.
    /// </summary>
    /// <param name="connectionString">The MongoDB connection string.</param>
    /// <param name="configuration">Optional MongoDB configuration for client settings.</param>
    /// <returns>A configured <see cref="IMongoClient" /> instance.</returns>
    public static IMongoClient CreateClient(string connectionString, MongoConfiguration? configuration = null)
    {
        ArgumentNullException.ThrowIfNull(connectionString);

        var settings = MongoClientSettings.FromConnectionString(connectionString);
        ApplyConfiguration(settings, configuration);

        return new MongoClient(settings);
    }

    /// <summary>
    ///     Creates a new <see cref="IMongoClient" /> with the specified settings.
    /// </summary>
    /// <param name="settings">The MongoDB client settings.</param>
    /// <returns>A configured <see cref="IMongoClient" /> instance.</returns>
    public static IMongoClient CreateClient(MongoClientSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);

        return new MongoClient(settings);
    }

    /// <summary>
    ///     Creates <see cref="MongoClientSettings" /> from a connection string with optional configuration.
    /// </summary>
    /// <param name="connectionString">The MongoDB connection string.</param>
    /// <param name="configuration">Optional MongoDB configuration for client settings.</param>
    /// <returns>Configured <see cref="MongoClientSettings" />.</returns>
    public static MongoClientSettings CreateSettings(string connectionString, MongoConfiguration? configuration = null)
    {
        ArgumentNullException.ThrowIfNull(connectionString);

        var settings = MongoClientSettings.FromConnectionString(connectionString);
        ApplyConfiguration(settings, configuration);

        return settings;
    }

    private static void ApplyConfiguration(MongoClientSettings settings, MongoConfiguration? configuration)
    {
        if (configuration == null)
            return;

        // Apply timeout settings
        if (configuration.CommandTimeoutSeconds > 0)
        {
            settings.ServerSelectionTimeout = TimeSpan.FromSeconds(configuration.CommandTimeoutSeconds);
            settings.ConnectTimeout = TimeSpan.FromSeconds(configuration.CommandTimeoutSeconds);
            settings.SocketTimeout = TimeSpan.FromSeconds(configuration.CommandTimeoutSeconds);
        }

        // Apply read preference
        if (configuration.ReadPreference.HasValue)
        {
            settings.ReadPreference = configuration.ReadPreference.Value switch
            {
                ReadPreferenceMode.Primary => ReadPreference.Primary,
                ReadPreferenceMode.PrimaryPreferred => ReadPreference.PrimaryPreferred,
                ReadPreferenceMode.Secondary => ReadPreference.Secondary,
                ReadPreferenceMode.SecondaryPreferred => ReadPreference.SecondaryPreferred,
                ReadPreferenceMode.Nearest => ReadPreference.Nearest,
                _ => ReadPreference.Primary,
            };
        }
    }
}
