using System.Diagnostics;

namespace NPipeline.Lineage;

/// <summary>
///     Default implementation of <see cref="ILineageFactory" /> that creates lineage-related components
///     using reflection with proper error handling and logging.
/// </summary>
internal sealed class DefaultLineageFactory : ILineageFactory
{
    /// <summary>
    ///     Creates an instance of the specified lineage sink type.
    /// </summary>
    /// <param name="sinkType">The type of the lineage sink to create.</param>
    /// <returns>An instance of <see cref="ILineageSink" />, or null if it cannot be created.</returns>
    public ILineageSink? CreateLineageSink(Type sinkType)
    {
        return TryCreateInstance<ILineageSink>(sinkType, nameof(CreateLineageSink));
    }

    /// <summary>
    ///     Creates an instance of the specified pipeline lineage sink type (explicit configuration path).
    /// </summary>
    /// <remarks>
    ///     Use this when a concrete sink type is explicitly configured via the builder or context. This is an imperative,
    ///     unambiguous request to construct that sink (typically via DI and falling back to ActivatorUtilities).
    /// </remarks>
    /// <param name="sinkType">The type of the pipeline lineage sink to create.</param>
    /// <returns>An instance of <see cref="IPipelineLineageSink" />, or null if it cannot be created.</returns>
    public IPipelineLineageSink? CreatePipelineLineageSink(Type sinkType)
    {
        return TryCreateInstance<IPipelineLineageSink>(sinkType, nameof(CreatePipelineLineageSink));
    }

    /// <summary>
    ///     Resolves an optional provider capable of supplying a default pipeline lineage sink (implicit default path).
    /// </summary>
    /// <remarks>
    ///     This is consulted only when no explicit sink (instance or type) is configured and item-level lineage is enabled.
    ///     It allows optional packages (e.g., NPipeline.Lineage) to supply a sensible default without reflection.
    ///     Returns null when no provider is registered or available.
    /// </remarks>
    /// <returns>An <see cref="IPipelineLineageSinkProvider" /> instance or null.</returns>
    public IPipelineLineageSinkProvider? ResolvePipelineLineageSinkProvider()
    {
        // No DI container available in the default factory; cannot supply a provider.
        // This is expected behavior - lineage sink providers require dependency injection.
        return null;
    }

    /// <summary>
    ///     Resolves an optional lineage collector for tracking data lineage.
    /// </summary>
    /// <returns>An <see cref="ILineageCollector" /> instance or null if lineage is not enabled.</returns>
    public ILineageCollector? ResolveLineageCollector()
    {
        // No DI container available in the default factory; cannot supply a collector.
        // Lineage collection requires DI registration through services.AddLineageTracking().
        return null;
    }

    /// <summary>
    ///     Attempts to create an instance of the specified type with improved error handling.
    /// </summary>
    /// <typeparam name="T">The interface type expected.</typeparam>
    /// <param name="type">The concrete type to instantiate.</param>
    /// <param name="methodName">The name of the calling method for diagnostics.</param>
    /// <returns>The created instance or null if creation fails.</returns>
    private static T? TryCreateInstance<T>(Type type, string methodName) where T : class
    {
        if (!typeof(T).IsAssignableFrom(type))
        {
            Debug.WriteLine($"DefaultLineageFactory.{methodName}: Type {type.FullName} does not implement {typeof(T).Name}");
            return null;
        }

        try
        {
            var instance = Activator.CreateInstance(type);

            if (instance is T typedInstance)
            {
                Debug.WriteLine($"DefaultLineageFactory.{methodName}: Successfully created instance of {type.FullName}");
                return typedInstance;
            }

            Debug.WriteLine($"DefaultLineageFactory.{methodName}: Created instance of {type.FullName} but it's not assignable to {typeof(T).Name}");
            return null;
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"DefaultLineageFactory.{methodName}: Failed to create instance of {type.FullName}: {ex.Message}");
            return null;
        }
    }
}
