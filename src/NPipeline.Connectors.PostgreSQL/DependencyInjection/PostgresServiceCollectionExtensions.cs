using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
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
            services.AddOptions<PostgresOptions>();
            if (configure != null)
            {
                services.Configure(configure);
            }

            services.TryAddSingleton(sp => sp.GetRequiredService<IOptions<PostgresOptions>>().Value);
            services.TryAddSingleton<IPostgresConnectionPool>(sp =>
            {
                var opts = sp.GetRequiredService<IOptions<PostgresOptions>>().Value;
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
            services.AddOptions<PostgresOptions>()
                .Configure(o => o.AddOrUpdateConnection(name, connectionString));

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
            services.AddOptions<PostgresOptions>()
                .Configure(o => o.DefaultConnectionString = connectionString);

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
