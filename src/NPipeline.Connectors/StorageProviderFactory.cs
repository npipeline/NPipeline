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
    ///     Creates a resolver with optional built-in providers and additional providers.
    /// </summary>
    /// <param name="includeFileSystem">When true, registers <see cref="FileSystemStorageProvider" />.</param>
    /// <param name="additionalProviders">Optional additional providers to register.</param>
    public static IStorageResolver CreateResolver(
        bool includeFileSystem = true,
        IEnumerable<IStorageProvider>? additionalProviders = null)
    {
        var resolver = new StorageResolver();

        if (includeFileSystem)
            resolver.RegisterProvider(new FileSystemStorageProvider());

        if (additionalProviders is not null)
        {
            foreach (var p in additionalProviders)
            {
                resolver.RegisterProvider(p);
            }
        }

        return resolver;
    }

    /// <summary>
    ///     Creates a resolver from a configuration model and optionally also includes built-in providers.
    /// </summary>
    /// <param name="configuration">Connector configuration describing provider instances.</param>
    /// <param name="includeFileSystem">When true, registers <see cref="FileSystemStorageProvider" />.</param>
    public static IStorageResolver CreateResolverFromConfiguration(
        ConnectorConfiguration configuration,
        bool includeFileSystem = true)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var resolver = new StorageResolver();

        if (includeFileSystem)
            resolver.RegisterProvider(new FileSystemStorageProvider());

        if (configuration.Providers is { Count: > 0 })
        {
            foreach (var kvp in configuration.Providers)
            {
                var cfg = kvp.Value;

                if (cfg.Enabled is false)
                    continue;

                var instance = CreateProviderInstance(cfg, out _);

                if (instance is null)

                    // Skip invalid entries; in non-DI scenarios we prefer soft-fail and let usage sites throw if needed.
                    // A future enhancement could expose a validation API that returns all errors.
                    continue;

                resolver.RegisterProvider(instance);
            }
        }

        return resolver;
    }

    /// <summary>
    ///     Attempts to resolve a provider for the specified URI; throws a helpful exception if not found.
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

    private static IStorageProvider? CreateProviderInstance(
        StorageProviderConfig config,
        out List<string> errors)
    {
        errors = [];

        if (string.IsNullOrWhiteSpace(config.ProviderType))
        {
            errors.Add("ProviderType is required.");
            return null;
        }

        var type = ResolveType(config.ProviderType);

        if (type is null)
        {
            errors.Add($"Could not resolve provider type '{config.ProviderType}'. Ensure the assembly is loaded.");
            return null;
        }

        if (!typeof(IStorageProvider).IsAssignableFrom(type))
        {
            errors.Add($"Type '{type.FullName}' does not implement {nameof(IStorageProvider)}.");
            return null;
        }

        var ctor = type.GetConstructor(Type.EmptyTypes);

        if (ctor is null)
        {
            errors.Add($"Type '{type.FullName}' does not have a public parameterless constructor.");
            return null;
        }

        IStorageProvider? instance;

        try
        {
            instance = (IStorageProvider?)Activator.CreateInstance(type);
        }
        catch (Exception ex)
        {
            errors.Add($"Failed to construct '{type.FullName}': {ex.Message}");
            return null;
        }

        if (instance is IConfigurableStorageProvider configurable
            && config.Settings is { Count: > 0 })
        {
            try
            {
                configurable.Configure(config.Settings);
            }
            catch (Exception ex)
            {
                errors.Add($"Failed to configure '{type.FullName}': {ex.Message}");
                return null;
            }
        }

        return instance;
    }

    private static Type? ResolveType(string typeName)
    {
        // Try assembly-qualified or fully-qualified name first
        var t = Type.GetType(typeName, false, false);

        if (t is not null)
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

                if (candidate is not null)
                    return candidate;
            }
            catch
            {
                // ignore
            }
        }

        return null;
    }

    /// <summary>
    ///     Builds a resolver from an existing set of providers. Useful for DI scenarios where the container
    ///     already constructed providers and we only need a resolver facade.
    /// </summary>
    /// <param name="providers">Providers to register in the resolver.</param>
    /// <param name="includeFileSystem">When true, also registers <see cref="FileSystemStorageProvider" />.</param>
    public static IStorageResolver CreateResolverFromProviders(
        IEnumerable<IStorageProvider> providers,
        bool includeFileSystem = false)
    {
        var resolver = new StorageResolver();

        if (includeFileSystem)
            resolver.RegisterProvider(new FileSystemStorageProvider());

        foreach (var p in providers)
        {
            resolver.RegisterProvider(p);
        }

        return resolver;
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

                if (cfg.Enabled is false)
                    continue;

                var instance = CreateProviderInstance(cfg, out var errs);

                if (instance is null)
                {
                    if (errs.Count > 0)
                        errorDict[name] = errs;

                    continue;
                }

                result[name] = instance;
            }
        }

        errors = errorDict;
        return result;
    }

    /// <summary>
    ///     Overload of <see cref="CreateResolverFromConfiguration(ConnectorConfiguration, bool)" /> that also
    ///     returns per-entry errors rather than silently skipping invalid entries.
    /// </summary>
    public static IStorageResolver CreateResolverFromConfiguration(
        ConnectorConfiguration configuration,
        bool includeFileSystem,
        out IReadOnlyDictionary<string, IReadOnlyList<string>> errors)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var providers = CreateProvidersFromConfiguration(configuration, out errors);
        return CreateResolverFromProviders(providers.Values, includeFileSystem);
    }

    /// <summary>
    ///     Attempts to create a single provider instance from configuration with detailed errors.
    /// </summary>
    public static bool TryCreateProviderInstance(
        StorageProviderConfig config,
        out IStorageProvider? instance,
        out IReadOnlyList<string> errors)
    {
        instance = null;
        var created = CreateProviderInstance(config, out var errorList);
        errors = errorList;

        if (created is null)
            return false;

        instance = created;
        return true;
    }
}
