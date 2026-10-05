using AwesomeAssertions;
using Azure;
using Xunit;

namespace NPipeline.StorageProviders.Adls.Tests;

public class AdlsErrorsTests
{
    [Theory]
    [InlineData(404, "PathNotFound", typeof(FileNotFoundException))]
    [InlineData(404, "FilesystemNotFound", typeof(FileNotFoundException))]
    [InlineData(404, null, typeof(FileNotFoundException))]
    [InlineData(401, null, typeof(UnauthorizedAccessException))]
    [InlineData(403, "AuthorizationPermissionMismatch", typeof(UnauthorizedAccessException))]
    [InlineData(403, null, typeof(UnauthorizedAccessException))]
    [InlineData(400, "InvalidResourceName", typeof(ArgumentException))]
    [InlineData(400, null, typeof(ArgumentException))]
    [InlineData(0, "PathNotFound", typeof(FileNotFoundException))]
    [InlineData(0, "AuthenticationFailed", typeof(UnauthorizedAccessException))]
    [InlineData(409, "PathAlreadyExists", typeof(IOException))]
    [InlineData(429, "ServerBusy", typeof(IOException))]
    [InlineData(500, "InternalError", typeof(IOException))]
    public void Translate_MapsStatusFirstThenErrorCode(int status, string? errorCode, Type expectedType)
    {
        var ex = new RequestFailedException(status, "failure", errorCode, null);

        AdlsErrors.Translate(ex, "filesystem", "path").Should().BeOfType(expectedType);
    }

    [Fact]
    public void Translate_KeepsInnerExceptionAndCopiesData()
    {
        var ex = new RequestFailedException(500, "failure", "InternalError", null);
        ex.Data["retried"] = true;

        var translated = AdlsErrors.Translate(ex, "filesystem", "path");

        translated.InnerException.Should().BeSameAs(ex);
        translated.Data["retried"].Should().Be(true);
    }
}
