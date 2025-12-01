using System.Collections.ObjectModel;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.Exceptions;

namespace NPipeline.Connectors;

/// <summary>
///     Factory helpers to create and configure storage provider resolvers without DI.
/// </summary>
public static class StorageProviderFactory
{
    private static readonly IReadOnlyDictionary<string, IReadOnlyList<string>> EmptyErrorMap =
        new ReadOnlyDictionary<string, IReadOnlyList<string>>(new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase));

    /// <summary>
    ///     Creates a resolver using the supplied options.
    /// </summary>
    /// <remarks>
    ///     Set <see cref="StorageResolverOptions.CollectErrors" /> to capture per-provider creation errors in the result.
    /// </remarks>
    public static StorageResolverResult CreateResolver(StorageResolverOptions? options = null)
    {
        options ??= new StorageResolverOptions();

        var resolver = new StorageResolver();

        var errors = options.CollectErrors
            ? new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase)
            : null;

        if (options.IncludeFileSystem)
            resolver.RegisterProvider(new FileSystemStorageProvider());

        RegisterConfiguredProviders(resolver, options.Configuration, errors);

        if (options.AdditionalProviders is not null)
        {
            foreach (var provider in options.AdditionalProviders)
            {
                resolver.RegisterProvider(provider);
            }
        }

        return new StorageResolverResult(resolver, errors ?? EmptyErrorMap);
    }

    /// <summary>
    ///     Registers an alias that maps to a concrete provider type.
    /// </summary>
    public static void RegisterProviderAlias(string alias, Type providerType)
    {
        StorageProviderRegistry.RegisterAlias(alias, providerType);
    }

    /// <summary>
    ///     Returns a snapshot of current provider aliases.
    /// </summary>
    public static IReadOnlyDictionary<string, Type> GetRegisteredProviderAliases()
    {
        return StorageProviderRegistry.GetSnapshot();
    }

    /// <summary>
    ///     Attempts to resolve a provider for specified URI; throws a helpful exception if not found.
    /// </summary>
    public static IStorageProvider GetProviderOrThrow(IStorageResolver resolver, StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(resolver);
        ArgumentNullException.ThrowIfNull(uri);

        return resolver.ResolveProvider(uri) ?? throw new StorageProviderNotFoundException(uri);
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
            errorList.Add($"Could not resolve provider type '{config.ProviderType}'. Register an alias or use an assembly-qualified name.");
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
                        : [$"Failed to create provider '{cfg.ProviderType}'"];
                }
            }
        }

        errors = errorDict;
        return result;
    }

    private static void RegisterConfiguredProviders(
        StorageResolver resolver,
        ConnectorConfiguration? configuration,
        Dictionary<string, IReadOnlyList<string>>? errors)
    {
        if (configuration?.Providers is not { Count: > 0 })
            return;

        foreach (var (name, cfg) in configuration.Providers)
        {
            if (!cfg.Enabled)
                continue;

            if (TryCreateProviderInstance(cfg, out var provider, out var providerErrors))
                resolver.RegisterProvider(provider!);
            else if (errors is not null)
            {
                errors[name] = providerErrors.Count > 0
                    ? providerErrors
                    : [$"Failed to create provider '{cfg.ProviderType}'"];
            }
        }
    }

    private static Type? ResolveType(string typeName)
    {
        var type = Type.GetType(typeName, false, false);

        return type ?? (StorageProviderRegistry.TryResolve(typeName, out var aliasType)
            ? aliasType
            : null);
    }
}
