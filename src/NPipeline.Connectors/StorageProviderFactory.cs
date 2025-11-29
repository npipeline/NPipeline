using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.Exceptions;

namespace NPipeline.Connectors;

/// <summary>
///     Factory helpers to create and configure storage provider resolvers without DI.
/// </summary>
/// <remarks>
///     Goals:
///     - Keep NPipeline.Connectors dependency-free
///     - Support manual configuration (code-first or config-model driven)
///     - Provide safe defaults (e.g., FileSystem provider)
///     - Offer convenience APIs to resolve providers with helpful exceptions
/// </remarks>
public static class StorageProviderFactory
{
    /// <summary>
    ///     Creates a resolver with optional built-in providers, additional providers, and configuration.
    /// </summary>
    /// <param name="configuration">Optional connector configuration describing provider instances.</param>
    /// <param name="additionalProviders">Optional additional providers to register.</param>
    /// <param name="includeFileSystem">When true, registers <see cref="FileSystemStorageProvider" />.</param>
    public static IStorageResolver CreateResolver(
        ConnectorConfiguration? configuration = null,
        IEnumerable<IStorageProvider>? additionalProviders = null,
        bool includeFileSystem = true)
    {
        var resolver = new StorageResolver();

        if (includeFileSystem)
            resolver.RegisterProvider(new FileSystemStorageProvider());

        if (configuration?.Providers is { Count: > 0 })
        {
            foreach (var kvp in configuration.Providers)
            {
                var cfg = kvp.Value;

                if (!cfg.Enabled)
                    continue;

                if (TryCreateProviderInstance(cfg, out var provider) && provider != null)
                    resolver.RegisterProvider(provider);
            }
        }

        if (additionalProviders is not null)
        {
            foreach (var provider in additionalProviders)
            {
                resolver.RegisterProvider(provider);
            }
        }

        return resolver;
    }

    /// <summary>
    ///     Creates a resolver and returns per-provider creation errors when building from configuration.
    /// </summary>
    public static IStorageResolver CreateResolver(
        ConnectorConfiguration? configuration,
        IEnumerable<IStorageProvider>? additionalProviders,
        bool includeFileSystem,
        out IReadOnlyDictionary<string, IReadOnlyList<string>> errors)
    {
        var resolver = new StorageResolver();
        var errorDict = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase);

        if (includeFileSystem)
            resolver.RegisterProvider(new FileSystemStorageProvider());

        if (configuration?.Providers is { Count: > 0 })
        {
            foreach (var kvp in configuration.Providers)
            {
                var name = kvp.Key;
                var cfg = kvp.Value;

                if (!cfg.Enabled)
                    continue;

                if (TryCreateProviderInstance(cfg, out var instance, out var errs))
                    resolver.RegisterProvider(instance!);
                else
                {
                    errorDict[name] = errs != null && errs.Count > 0
                        ? errs
                        : new List<string> { $"Failed to create provider '{cfg.ProviderType}'" };
                }
            }
        }

        if (additionalProviders is not null)
        {
            foreach (var provider in additionalProviders)
            {
                resolver.RegisterProvider(provider);
            }
        }

        errors = errorDict;
        return resolver;
    }

    /// <summary>
    ///     Attempts to resolve a provider for specified URI; throws a helpful exception if not found.
    /// </summary>
    public static IStorageProvider GetProviderOrThrow(IStorageResolver resolver, StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(resolver);
        ArgumentNullException.ThrowIfNull(uri);

        var provider = resolver.ResolveProvider(uri);

        if (provider is null)
            throw new StorageProviderNotFoundException(uri);

        return provider;
    }

    /// <summary>
    ///     Resolves a provider if possible; otherwise returns null. Never throws.
    /// </summary>
    public static IStorageProvider? TryGetProvider(IStorageResolver resolver, StorageUri uri)
    {
        try
        {
            return resolver.ResolveProvider(uri);
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    ///     Attempts to create a single provider instance from configuration.
    /// </summary>
    public static bool TryCreateProviderInstance(
        StorageProviderConfig config,
        out IStorageProvider? instance)
    {
        instance = null;

        if (string.IsNullOrWhiteSpace(config.ProviderType))
            return false;

        var type = ResolveType(config.ProviderType);

        if (type == null || !typeof(IStorageProvider).IsAssignableFrom(type))
            return false;

        var ctor = type.GetConstructor(Type.EmptyTypes);

        if (ctor == null)
            return false;

        try
        {
            instance = (IStorageProvider?)Activator.CreateInstance(type);

            if (instance is IConfigurableStorageProvider configurable && config.Settings is { Count: > 0 })
                configurable.Configure(config.Settings);

            return instance != null;
        }
        catch
        {
            // Swallow and indicate failure; detailed overload below captures errors when needed.
            return false;
        }
    }

    /// <summary>
    ///     Attempts to create a single provider instance from configuration and returns detailed errors.
    /// </summary>
    public static bool TryCreateProviderInstance(
        StorageProviderConfig config,
        out IStorageProvider? instance,
        out IReadOnlyList<string> errors)
    {
        instance = null;
        var errorList = new List<string>();

        if (string.IsNullOrWhiteSpace(config.ProviderType))
        {
            errorList.Add("ProviderType is required.");
            errors = errorList;
            return false;
        }

        var type = ResolveType(config.ProviderType);

        if (type == null)
        {
            errorList.Add($"Could not resolve provider type '{config.ProviderType}'. Ensure the assembly is loaded.");
            errors = errorList;
            return false;
        }

        if (!typeof(IStorageProvider).IsAssignableFrom(type))
        {
            errorList.Add($"Type '{type.FullName}' does not implement {nameof(IStorageProvider)}.");
            errors = errorList;
            return false;
        }

        var ctor = type.GetConstructor(Type.EmptyTypes);

        if (ctor == null)
        {
            errorList.Add($"Type '{type.FullName}' does not have a public parameterless constructor.");
            errors = errorList;
            return false;
        }

        try
        {
            instance = (IStorageProvider?)Activator.CreateInstance(type);
        }
        catch (Exception ex)
        {
            errorList.Add($"Failed to construct '{type.FullName}': {ex.Message}");
            instance = null;
            errors = errorList;
            return false;
        }

        if (instance is IConfigurableStorageProvider configurable && config.Settings is { Count: > 0 })
        {
            try
            {
                configurable.Configure(config.Settings);
            }
            catch (Exception ex)
            {
                errorList.Add($"Failed to configure '{type.FullName}': {ex.Message}");
                instance = null;
                errors = errorList;
                return false;
            }
        }

        errors = errorList;
        return instance != null;
    }

    /// <summary>
    ///     Creates providers from <see cref="ConnectorConfiguration" /> while collecting per-entry errors.
    ///     This is intended for opt-in DI packages to materialize providers before registration.
    /// </summary>
    /// <returns>A name -> provider map for successful entries.</returns>
    public static IReadOnlyDictionary<string, IStorageProvider> CreateProvidersFromConfiguration(
        ConnectorConfiguration configuration,
        out IReadOnlyDictionary<string, IReadOnlyList<string>> errors)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var result = new Dictionary<string, IStorageProvider>(StringComparer.OrdinalIgnoreCase);
        var errorDict = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase);

        if (configuration.Providers is { Count: > 0 })
        {
            foreach (var kvp in configuration.Providers)
            {
                var name = kvp.Key;
                var cfg = kvp.Value;

                if (!cfg.Enabled)
                    continue;

                if (TryCreateProviderInstance(cfg, out var instance, out var errs))
                    result[name] = instance!;
                else
                {
                    errorDict[name] = errs != null && errs.Count > 0
                        ? errs
                        : new List<string> { $"Failed to create provider '{cfg.ProviderType}'" };
                }
            }
        }

        errors = errorDict;
        return result;
    }

    private static Type? ResolveType(string typeName)
    {
        // Try assembly-qualified or fully-qualified name first
        var t = Type.GetType(typeName, false, false);

        if (t != null)
            return t;

        // Fall back to searching loaded assemblies by FullName / Namespace+Name
        var asms = AppDomain.CurrentDomain.GetAssemblies();

        foreach (var asm in asms)
        {
            if (asm.IsDynamic)
                continue;

            try
            {
                var candidate = asm.GetType(typeName, false, false);

                if (candidate != null)
                    return candidate;
            }
            catch
            {
                // ignore
            }
        }

        return null;
    }
}
