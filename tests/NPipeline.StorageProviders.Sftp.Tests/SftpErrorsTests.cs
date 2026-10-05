using System.Net.Sockets;
using Renci.SshNet.Common;

namespace NPipeline.StorageProviders.Sftp.Tests;

public class SftpErrorsTests
{
    [Theory]
    [InlineData("notfound", typeof(FileNotFoundException))]
    [InlineData("permission", typeof(UnauthorizedAccessException))]
    [InlineData("auth", typeof(UnauthorizedAccessException))]
    [InlineData("connection", typeof(IOException))]
    [InlineData("timeout", typeof(IOException))]
    [InlineData("ssh", typeof(IOException))]
    [InlineData("socket", typeof(IOException))]
    public void Translate_MapsToContractException(string kind, Type expected)
    {
        Exception source = kind switch
        {
            "notfound" => new SftpPathNotFoundException("missing"),
            "permission" => new SftpPermissionDeniedException("denied"),
            "auth" => new SshAuthenticationException("bad credentials"),
            "connection" => new SshConnectionException("dropped"),
            "timeout" => new SshOperationTimeoutException("slow"),
            "socket" => new SocketException((int)SocketError.ConnectionRefused),
            _ => new SshException("other"),
        };

        var translated = SftpErrors.Translate(source, "host", "/path");

        translated.GetType().Should().Be(expected);
        translated.InnerException.Should().BeSameAs(source);
    }

    [Fact]
    public void Translate_CopiesExceptionData()
    {
        var source = new SshException("boom");
        source.Data["marker"] = "retried";

        SftpErrors.Translate(source, "host", "/path").Data["marker"].Should().Be("retried");
    }

    [Fact]
    public void IsTranslatable_ExcludesCancellationAndArgumentErrors()
    {
        SftpErrors.IsTranslatable(new OperationCanceledException()).Should().BeFalse();
        SftpErrors.IsTranslatable(new ArgumentException("x")).Should().BeFalse();
        SftpErrors.IsTranslatable(new SshException("x")).Should().BeTrue();
    }
}
