using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.Connectors.PostgreSQL.Connection;
using NPipeline.Connectors.PostgreSQL.Configuration;

namespace NPipeline.Connectors.PostgreSQL.DependencyInjection
{
    /// <summary>
    /// Extension methods for configuring PostgreSQL connector in dependency injection.
    /// </summary>
    public static class PostgresServiceCollectionExtensions
    {
        /// <summary>
        /// Adds the PostgreSQL connector to the service collection.
        /// </summary>
        /// <param name="services">The service collection.</param>
        /// <param name="configure">Optional configuration action.</param>
        /// <returns>The service collection for chaining.</returns>
        public static IServiceCollection AddPostgresConnector(
            this IServiceCollection services,
            Action<PostgresOptions>? configure = null)
        {
            var options = new PostgresOptions();
            configure?.Invoke(options);

            services.TryAddSingleton(options);
            services.TryAddSingleton<IPostgresConnectionPool>(sp =>
            {
                var opts = sp.GetRequiredService<PostgresOptions>();
                return new PostgresConnectionPool(opts);
            });

            services.TryAddSingleton<PostgresSourceNodeFactory>();
            services.TryAddSingleton<PostgresSinkNodeFactory>();

            return services;
        }

        /// <summary>
        /// Adds a named PostgreSQL connection to the service collection.
        /// </summary>
        /// <param name="services">The service collection.</param>
        /// <param name="name">The connection name.</param>
        /// <param name="connectionString">The connection string.</param>
        /// <returns>The service collection for chaining.</returns>
        public static IServiceCollection AddPostgresConnection(
            this IServiceCollection services,
            string name,
            string connectionString)
        {
            var options = services.FirstOrDefault(sd => sd.ServiceType == typeof(PostgresOptions))?.ImplementationInstance as PostgresOptions
                ?? new PostgresOptions();
            options.AddOrUpdateConnection(name, connectionString);
            services.TryAddSingleton(options);

            return services;
        }

        /// <summary>
        /// Adds the default PostgreSQL connection to the service collection.
        /// </summary>
        /// <param name="services">The service collection.</param>
        /// <param name="connectionString">The connection string.</param>
        /// <returns>The service collection for chaining.</returns>
        public static IServiceCollection AddDefaultPostgresConnection(
            this IServiceCollection services,
            string connectionString)
        {
            var options = services.FirstOrDefault(sd => sd.ServiceType == typeof(PostgresOptions))?.ImplementationInstance as PostgresOptions
                ?? new PostgresOptions();
            options.DefaultConnectionString = connectionString;
            services.TryAddSingleton(options);

            return services;
        }

        /// <summary>
        /// Adds a keyed PostgreSQL connection pool to the service collection.
        /// </summary>
        /// <param name="services">The service collection.</param>
        /// <param name="name">The connection name/key.</param>
        /// <param name="connectionString">The connection string.</param>
        /// <returns>The service collection for chaining.</returns>
        public static IServiceCollection AddKeyedPostgresConnection(
            this IServiceCollection services,
            string name,
            string connectionString)
        {
            _ = services.AddKeyedSingleton<IPostgresConnectionPool>(name, (sp, key) =>
            {
                return new PostgresConnectionPool(connectionString);
            });

            return services;
        }
    }
}
