using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Connectors.PostgreSQL.Exceptions;

namespace NPipeline.Connectors.PostgreSQL.Mapping
{
    /// <summary>
    /// Builds cached mapping delegates from <see cref="PostgresRow"/> to CLR types using attributes.
    /// </summary>
    internal static class PostgresMapperBuilder
    {
        public static Func<PostgresRow, T> Build<T>()
        {
            var properties = typeof(T)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanWrite && !IsIgnored(p))
                .Select(p => new
                {
                    Property = p,
                    Attribute = p.GetCustomAttribute<PostgresColumnAttribute>()
                })
                .ToList();

            var mappings = properties
                .Select(p => new
                {
                    ColumnName = p.Attribute?.Name ?? p.Property.Name,
                    PropertyName = p.Property.Name,
                    Apply = BuildApplyDelegate<T>(p.Property, p.Attribute?.Name ?? p.Property.Name)
                })
                .ToList();

            return row =>
            {
                var instance = Activator.CreateInstance<T>()
                    ?? throw new PostgresMappingException($"Failed to create instance of type {typeof(T).FullName}");

                foreach (var mapping in mappings)
                {
                    try
                    {
                        mapping.Apply(instance, row);
                    }
                    catch (Exception ex)
                    {
                        throw new PostgresMappingException($"Failed to map column '{mapping.ColumnName}' to property '{mapping.PropertyName}'", ex);
                    }
                }

                return instance;
            };
        }

        private static Action<T, PostgresRow> BuildApplyDelegate<T>(PropertyInfo property, string columnName)
        {
            var instanceParam = Expression.Parameter(typeof(T), "instance");
            var rowParam = Expression.Parameter(typeof(PostgresRow), "row");

            var hasColumnMethod = typeof(PostgresRow).GetMethod(nameof(PostgresRow.HasColumn))
                ?? throw new InvalidOperationException("PostgresRow.HasColumn not found");

            var getMethod = typeof(PostgresRow)
                .GetMethod(nameof(PostgresRow.Get), [typeof(string), property.PropertyType])?
                .MakeGenericMethod(property.PropertyType)
                ?? throw new InvalidOperationException("PostgresRow.Get<T>(string, T) overload not found");

            var hasColumnCall = Expression.Call(rowParam, hasColumnMethod, Expression.Constant(columnName));
            var getCall = Expression.Call(rowParam, getMethod, Expression.Constant(columnName), Expression.Default(property.PropertyType));
            var assign = Expression.Assign(Expression.Property(instanceParam, property), getCall);
            var body = Expression.IfThen(hasColumnCall, assign);

            return Expression.Lambda<Action<T, PostgresRow>>(body, instanceParam, rowParam).Compile();
        }

        private static bool IsIgnored(PropertyInfo property)
        {
            var columnAttribute = property.GetCustomAttribute<PostgresColumnAttribute>();
            var ignoredByAttribute = columnAttribute?.Ignore == true;
            var hasIgnoreMarker = property.IsDefined(typeof(PostgresIgnoreAttribute), inherit: true);
            return ignoredByAttribute || hasIgnoreMarker;
        }
    }
}
