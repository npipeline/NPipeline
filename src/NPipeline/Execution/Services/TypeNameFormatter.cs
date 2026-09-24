namespace NPipeline.Execution.Services;

internal static class TypeNameFormatter
{
    public static string GetAssemblyQualifiedName(Type type) => type.AssemblyQualifiedName ?? type.FullName ?? type.Name;
}
