using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.StorageProviders;

/// <summary>
///     Maintains known storage provider aliases to concrete <see cref="Type" /> instances.
/// </summary>
internal static class StorageProviderRegistry
{
    private const string FileSystemProviderTypeName = "NPipeline.StorageProviders.FileSystemStorageProvider, NPipeline.StorageProviders";
    private static readonly Dictionary<string, Type> Aliases = new(StringComparer.OrdinalIgnoreCase);
    private static readonly object Sync = new();

    static StorageProviderRegistry()
    {
        var fileSystemProviderType = Type.GetType(FileSystemProviderTypeName, false, false);

        if (fileSystemProviderType is not null && typeof(IStorageProvider).IsAssignableFrom(fileSystemProviderType))
        {
            RegisterAlias("file", fileSystemProviderType);
            RegisterAlias("filesystem", fileSystemProviderType);
        }
    }

    public static void RegisterAlias(string alias, Type providerType)
    {
        ArgumentException.ThrowIfNullOrEmpty(alias);
        ArgumentNullException.ThrowIfNull(providerType);

        if (!typeof(IStorageProvider).IsAssignableFrom(providerType))
            throw new ArgumentException($"Type '{providerType.FullName}' must implement {nameof(IStorageProvider)}.", nameof(providerType));

        lock (Sync)
        {
            Aliases[alias] = providerType;
        }
    }

    public static bool TryResolve(string alias, out Type? providerType)
    {
        if (string.IsNullOrWhiteSpace(alias))
        {
            providerType = null;
            return false;
        }

        lock (Sync)
        {
            if (Aliases.TryGetValue(alias, out var type))
            {
                providerType = type;
                return true;
            }
        }

        providerType = null;
        return false;
    }

    public static IReadOnlyDictionary<string, Type> GetSnapshot()
    {
        lock (Sync)
        {
            return new Dictionary<string, Type>(Aliases, StringComparer.OrdinalIgnoreCase);
        }
    }
}
