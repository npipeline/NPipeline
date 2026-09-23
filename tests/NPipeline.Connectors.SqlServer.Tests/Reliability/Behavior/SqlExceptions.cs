using System.Reflection;
using Microsoft.Data.SqlClient;

namespace NPipeline.Connectors.SqlServer.Tests.Reliability.Behavior;

/// <summary>
///     Builds <see cref="SqlException" />s with a chosen error number. SqlClient keeps their constructors internal, so this
///     goes through reflection.
/// </summary>
internal static class SqlExceptions
{
    private const BindingFlags AnyInstance = BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public;

    public static SqlException WithNumber(int number)
    {
        var error = CreateError(number);

        var collection = (SqlErrorCollection)Activator.CreateInstance(typeof(SqlErrorCollection), true)!;
        _ = typeof(SqlErrorCollection).GetMethod("Add", AnyInstance)!.Invoke(collection, [error]);

        var create = typeof(SqlException)
            .GetMethods(BindingFlags.Static | BindingFlags.NonPublic)
            .First(m => m.Name == "CreateException" && m.GetParameters() is [{ ParameterType: var first }, { ParameterType: var second }]
                        && first == typeof(SqlErrorCollection) && second == typeof(string));

        return (SqlException)create.Invoke(null, [collection, "16.0"])!;
    }

    private static SqlError CreateError(int number)
    {
        // Fill the widest constructor by parameter type; the first int is the error number.
        var constructor = typeof(SqlError).GetConstructors(AnyInstance).OrderByDescending(c => c.GetParameters().Length).First();
        var numberAssigned = false;

        var arguments = constructor.GetParameters().Select(p =>
        {
            if (p.ParameterType == typeof(int) && !numberAssigned)
            {
                numberAssigned = true;
                return (object?)number;
            }

            return p.ParameterType switch
            {
                var t when t == typeof(string) => $"injected {number}",
                var t when t == typeof(int) => 0,
                var t when t == typeof(uint) => 0u,
                var t when t == typeof(byte) => (byte)0,
                _ => null,
            };
        }).ToArray();

        return (SqlError)constructor.Invoke(arguments);
    }
}
