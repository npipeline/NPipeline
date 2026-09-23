using AwesomeAssertions;
using Azure.Core;
using Azure.Storage.Files.DataLake;
using Xunit;

namespace NPipeline.StorageProviders.Adls.Tests;

public class AdlsGen2RetryOptionsTests
{
    [Fact]
    public void Defaults_MatchThePreviouslyHardcodedSdkSettings()
    {
        var retry = new AdlsGen2StorageProviderOptions().Retry;

        retry.Mode.Should().Be(RetryMode.Exponential);
        retry.MaxRetries.Should().Be(5);
        retry.Delay.Should().Be(TimeSpan.FromMilliseconds(800));
        retry.MaxDelay.Should().Be(TimeSpan.FromSeconds(8));
        retry.NetworkTimeout.Should().Be(TimeSpan.FromSeconds(100));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Defaults_ReachBothClientOptions(bool withServiceVersion)
    {
        var options = new AdlsGen2StorageProviderOptions();

        if (withServiceVersion)
            options.ServiceVersion = DataLakeClientOptions.ServiceVersion.V2021_12_02;

        var factory = new AdlsGen2ClientFactory(options);

        AssertRetry(factory.CreateClientOptions().Retry, RetryMode.Exponential, 5, TimeSpan.FromMilliseconds(800),
            TimeSpan.FromSeconds(8), TimeSpan.FromSeconds(100));

        AssertRetry(factory.CreateBlobClientOptions().Retry, RetryMode.Exponential, 5, TimeSpan.FromMilliseconds(800),
            TimeSpan.FromSeconds(8), TimeSpan.FromSeconds(100));
    }

    [Fact]
    public void CustomValues_ReachBothClientOptions()
    {
        var options = new AdlsGen2StorageProviderOptions
        {
            Retry = new AdlsGen2RetryOptions
            {
                Mode = RetryMode.Fixed,
                MaxRetries = 2,
                Delay = TimeSpan.FromMilliseconds(250),
                MaxDelay = TimeSpan.FromSeconds(3),
                NetworkTimeout = TimeSpan.FromSeconds(20),
            },
        };

        var factory = new AdlsGen2ClientFactory(options);

        AssertRetry(factory.CreateClientOptions().Retry, RetryMode.Fixed, 2, TimeSpan.FromMilliseconds(250),
            TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(20));

        AssertRetry(factory.CreateBlobClientOptions().Retry, RetryMode.Fixed, 2, TimeSpan.FromMilliseconds(250),
            TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(20));
    }

    [Fact]
    public void ZeroRetries_IsAllowed()
    {
        var factory = new AdlsGen2ClientFactory(new AdlsGen2StorageProviderOptions
        {
            Retry = new AdlsGen2RetryOptions { MaxRetries = 0 },
        });

        factory.CreateClientOptions().Retry.MaxRetries.Should().Be(0);
    }

    [Fact]
    public void InvalidValues_AreRejected()
    {
        var retry = new AdlsGen2RetryOptions();

        FluentActions.Invoking(() => retry.MaxRetries = -1).Should().Throw<ArgumentOutOfRangeException>();
        FluentActions.Invoking(() => retry.Delay = TimeSpan.FromSeconds(-1)).Should().Throw<ArgumentOutOfRangeException>();
        FluentActions.Invoking(() => retry.MaxDelay = TimeSpan.FromSeconds(-1)).Should().Throw<ArgumentOutOfRangeException>();
        FluentActions.Invoking(() => retry.NetworkTimeout = TimeSpan.Zero).Should().Throw<ArgumentOutOfRangeException>();
        FluentActions.Invoking(() => new AdlsGen2StorageProviderOptions { Retry = null! }).Should().Throw<ArgumentNullException>();
    }

    private static void AssertRetry(
        RetryOptions retry,
        RetryMode mode,
        int maxRetries,
        TimeSpan delay,
        TimeSpan maxDelay,
        TimeSpan networkTimeout)
    {
        retry.Mode.Should().Be(mode);
        retry.MaxRetries.Should().Be(maxRetries);
        retry.Delay.Should().Be(delay);
        retry.MaxDelay.Should().Be(maxDelay);
        retry.NetworkTimeout.Should().Be(networkTimeout);
    }
}
