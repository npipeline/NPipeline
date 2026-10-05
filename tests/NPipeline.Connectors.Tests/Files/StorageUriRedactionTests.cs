using Xunit;
using AwesomeAssertions;
using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Tests.Files;

public sealed class StorageUriRedactionTests
{
    [Fact]
    public void UnsupportedCapability_MessageDoesNotContainSecrets()
    {
        var uri = StorageUri.Parse("sftp://user:hunter2@host/x.csv?secretKey=abc123&sasToken=sig456&region=ap-southeast-2");

        var message = new UnsupportedStorageCapabilityException(uri, "write", "Test").Message;

        message.Should().NotContain("hunter2").And.NotContain("abc123").And.NotContain("sig456");
        message.Should().Contain("region=ap-southeast-2");
    }
}
