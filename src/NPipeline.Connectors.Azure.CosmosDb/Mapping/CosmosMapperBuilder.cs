using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Connectors.Attributes;

namespace NPipeline.Connectors.Azure.CosmosDb.Mapping;

/// <summary>
///     Builds compiled mapper functions for Cosmos DB row to object mapping.
/// </summary>
internal static class CosmosMapperBuilder
{
    /// <summary>
    ///     Builds a compiled mapper function for the specified type.
    /// </summary>
    /// <typeparam name="T">The target type.</typeparam>
    /// <returns>A compiled mapper function.</returns>
    public static Func<CosmosRow, T> Build<T>()
    {
        var type = typeof(T);

        if (type.IsValueType || type == typeof(string))
            return BuildValueTypeMapper<T>();

        return BuildObjectMapper<T>();
    }

    private static Func<CosmosRow, T> BuildValueTypeMapper<T>()
    {
        return row => row.Get<T>(0);
    }

    private static Func<CosmosRow, T> BuildObjectMapper<T>()
    {
        var type = typeof(T);
        var rowParam = Expression.Parameter(typeof(CosmosRow), "row");

        // Create instance
        var ctor = type.GetConstructor(Type.EmptyTypes);

        if (ctor == null)
            throw new InvalidOperationException($"Type '{type.FullName}' does not have a parameterless constructor");

        var instanceVar = Expression.Variable(type, "instance");
        var createInstance = Expression.Assign(instanceVar, Expression.New(ctor));

        var bindings = new List<Expression>();

        foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!property.CanWrite)
                continue;

            if (property.IsDefined(typeof(IgnoreColumnAttribute), true))
                continue;

            var columnName = property.Name;

            // Get the typed Get<T>(string, T) method
            var getMethod = typeof(CosmosRow)
                .GetMethods(BindingFlags.Public | BindingFlags.Instance)
                .FirstOrDefault(m =>
                    m.Name == nameof(CosmosRow.Get) &&
                    m.IsGenericMethodDefinition &&
                    m.GetParameters().Length == 2 &&
                    m.GetParameters()[0].ParameterType == typeof(string));

            if (getMethod == null)
                continue;

            var propertyType = property.PropertyType;
            var defaultValue = Expression.Default(propertyType);
            var genericGetMethod = getMethod.MakeGenericMethod(propertyType);

            var valueExpr = Expression.Call(
                rowParam,
                genericGetMethod,
                Expression.Constant(columnName),
                defaultValue);

            // Set the property
            var setProperty = Expression.Call(instanceVar, property.SetMethod!, valueExpr);
            bindings.Add(setProperty);
        }

        var body = Expression.Block(
            [instanceVar],
            [createInstance, .. bindings, instanceVar]);

        return Expression.Lambda<Func<CosmosRow, T>>(body, rowParam).Compile();
    }
}
