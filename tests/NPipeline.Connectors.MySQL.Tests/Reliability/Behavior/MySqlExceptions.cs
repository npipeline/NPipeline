using System.Reflection;
using MySqlConnector;

namespace NPipeline.Connectors.MySql.Tests.Reliability.Behavior;

/// <summary>
///     Builds <see cref="MySqlConnector.MySqlException" />s with a chosen error number. MySqlConnector keeps their
///     constructors internal, so this goes through reflection.
/// </summary>
internal static class MySqlExceptions
{
    public static MySqlException WithNumber(int number)
    {
        var constructor = typeof(MySqlException)
            .GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
            .Where(c => c.GetParameters() is [{ ParameterType: var first }, ..] && first == typeof(MySqlErrorCode))
            .OrderBy(c => c.GetParameters().Length)
            .First();

        var arguments = constructor.GetParameters().Select((p, i) => i == 0
            ? Enum.ToObject(typeof(MySqlErrorCode), number)
            : p.ParameterType == typeof(string)
                ? $"injected {number}"
                : null).ToArray();

        return (MySqlException)constructor.Invoke(arguments);
    }
}
