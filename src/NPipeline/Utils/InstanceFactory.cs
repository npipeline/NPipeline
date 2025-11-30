namespace NPipeline.Utils;

/// <summary>
///     Internal helper class for creating instances via reflection.
/// </summary>
internal static class InstanceFactory
{
    /// <summary>
    ///     Attempts to create an instance of the specified type.
    /// </summary>
    /// <typeparam name="T">The type to create an instance of.</typeparam>
    /// <param name="type">The type to instantiate.</param>
    /// <param name="instance">When this method returns, contains the created instance if successful; otherwise, null.</param>
    /// <param name="error">When this method returns, contains the error message if unsuccessful; otherwise, null.</param>
    /// <returns>true if the instance was created successfully; otherwise, false.</returns>
    public static bool TryCreate<T>(Type type, out T? instance, out string? error) where T : class
    {
        instance = null;
        error = null;

        if (!typeof(T).IsAssignableFrom(type))
        {
            error = $"Type {type.FullName} does not implement {typeof(T).FullName}";
            return false;
        }

        try
        {
            instance = Activator.CreateInstance(type) as T;

            if (instance is null)
                error = $"Activator returned null for {type.FullName}";

            return instance != null;
        }
        catch (Exception ex)
        {
            error = ex.Message;
            return false;
        }
    }
}
