using Azure;
using AwesomeAssertions;
using Xunit;

namespace NPipeline.StorageProviders.Azure.Tests;

public class AzureErrorsTests
{
    [Theory]
    [InlineData(404, "BlobNotFound", typeof(FileNotFoundException))]
    [InlineData(404, "ContainerNotFound", typeof(FileNotFoundException))]
    [InlineData(404, null, typeof(FileNotFoundException))]
    [InlineData(401, null, typeof(UnauthorizedAccessException))]
    [InlineData(403, "AuthorizationFailure", typeof(UnauthorizedAccessException))]
    [InlineData(403, null, typeof(UnauthorizedAccessException))]
    [InlineData(400, "InvalidResourceName", typeof(ArgumentException))]
    [InlineData(400, null, typeof(ArgumentException))]
    [InlineData(0, "BlobNotFound", typeof(FileNotFoundException))]
    [InlineData(0, "AuthenticationFailed", typeof(UnauthorizedAccessException))]
    [InlineData(0, "InvalidQueryParameterValue", typeof(ArgumentException))]
    [InlineData(500, "InternalError", typeof(IOException))]
    [InlineData(409, "BlobAlreadyExists", typeof(IOException))]
    public void Translate_MapsStatusFirstThenErrorCode(int status, string? errorCode, Type expectedType)
    {
        var ex = new RequestFailedException(status, "failure", errorCode, null);

        AzureErrors.Translate(ex, "container", "blob").Should().BeOfType(expectedType);
    }

    [Fact]
    public void Translate_KeepsInnerExceptionAndCopiesData()
    {
        var ex = new RequestFailedException(500, "failure", "InternalError", null);
        ex.Data["retried"] = true;

        var translated = AzureErrors.Translate(ex, "container", "blob");

        translated.InnerException.Should().BeSameAs(ex);
        translated.Data["retried"].Should().Be(true);
    }
}
