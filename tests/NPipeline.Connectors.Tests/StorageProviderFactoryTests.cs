using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Configuration;

namespace NPipeline.Connectors.Tests;

public class StorageProviderFactoryTests
{
    [Fact]
    public void TryCreateProviderInstance_ReturnsErrors_WhenTypeNotFound()
    {
        var cfg = new StorageProviderConfig
        {
            ProviderType = "Nonexistent.Namespace.MissingProvider",
            Enabled = true,
        };

        var ok = StorageProviderFactory.TryCreateProviderInstance(cfg, out var instance, out var errors);

        Assert.False(ok);
        Assert.Null(instance);
        Assert.NotNull(errors);
        Assert.Contains(errors, e => e.Contains("Could not resolve provider type"));
    }

    [Fact]
    public void TryCreateProviderInstance_ReturnsErrors_WhenNoParameterlessCtor()
    {
        var cfg = new StorageProviderConfig
        {
            ProviderType = typeof(NoParameterlessCtorProvider).AssemblyQualifiedName!,
            Enabled = true,
        };

        var ok = StorageProviderFactory.TryCreateProviderInstance(cfg, out var instance, out var errors);

        Assert.False(ok);
        Assert.Null(instance);
        Assert.NotNull(errors);
        Assert.Contains(errors, e => e.Contains("does not have a public parameterless constructor"));
    }

    [Fact]
    public void TryCreateProviderInstance_ReturnsErrors_WhenConfigureThrows()
    {
        var cfg = new StorageProviderConfig
        {
            ProviderType = typeof(ConfigureThrowsProvider).AssemblyQualifiedName!,
            Enabled = true,
            Settings = new Dictionary<string, object> { ["x"] = "y" },
        };

        var ok = StorageProviderFactory.TryCreateProviderInstance(cfg, out var instance, out var errors);

        Assert.False(ok);
        Assert.Null(instance);
        Assert.NotNull(errors);
        Assert.Contains(errors, e => e.Contains("Failed to configure"));
    }

    [Fact]
    public void CreateResolver_ReturnsErrors_ForInvalidConfiguredProvider()
    {
        var config = new ConnectorConfiguration
        {
            Providers = new Dictionary<string, StorageProviderConfig>
            {
                ["bad"] = new()
                {
                    ProviderType = "No.Such.Type",
                    Enabled = true,
                },
            },
        };

        var (resolver, errors) = StorageProviderFactory.CreateResolverWithErrors(new StorageResolverOptions
        {
            Configuration = config,
            CollectErrors = true,
        });

        Assert.NotNull(resolver);
        Assert.NotNull(errors);
        Assert.True(errors.ContainsKey("bad"));
        Assert.Contains(errors["bad"], e => e.Contains("Could not resolve provider type") || e.Contains("Failed to create provider"));
    }

    [Fact]
    public void CreateResolver_NoErrors_WhenAdditionalProviderRegistered()
    {
        var config = new ConnectorConfiguration();
        var extras = new List<IStorageProvider> { new TestProvider() };

        var (resolver, errors) = StorageProviderFactory.CreateResolverWithErrors(new StorageResolverOptions
        {
            Configuration = config,
            AdditionalProviders = extras,
            CollectErrors = true,
        });

        Assert.NotNull(resolver);
        Assert.NotNull(errors);
        Assert.Empty(errors);
    }

    private sealed class NoParameterlessCtorProvider : IStorageProvider
    {
        public NoParameterlessCtorProvider(string s)
        {
        }

        public StorageScheme Scheme => new("test");

        public bool CanHandle(StorageUri uri)
        {
            return false;
        }

        public Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public IAsyncEnumerable<StorageItem> ListAsync(StorageUri prefix, bool recursive = false, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<StorageMetadata?> GetMetadataAsync(StorageUri uri, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }
    }

    private sealed class ConfigureThrowsProvider : IStorageProvider, IConfigurableStorageProvider
    {
        public void Configure(IReadOnlyDictionary<string, object> settings)
        {
            throw new InvalidOperationException("bad config");
        }

        public StorageScheme Scheme => new("test");

        public bool CanHandle(StorageUri uri)
        {
            return false;
        }

        public Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public IAsyncEnumerable<StorageItem> ListAsync(StorageUri prefix, bool recursive = false, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<StorageMetadata?> GetMetadataAsync(StorageUri uri, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }
    }

    private sealed class TestProvider : IStorageProvider
    {
        public StorageScheme Scheme => "test";

        public bool CanHandle(StorageUri uri)
        {
            return false;
        }

        public Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public IAsyncEnumerable<StorageItem> ListAsync(StorageUri prefix, bool recursive = false, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<StorageMetadata?> GetMetadataAsync(StorageUri uri, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }
    }
}
