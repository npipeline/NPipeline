using NPipeline.StorageProviders.Models;
using Renci.SshNet;

namespace NPipeline.StorageProviders.Sftp.Tests;

public class SftpClientPoolTests
{
    private static StorageUri Uri(string spec) => StorageUri.Parse(spec);

    [Fact]
    public async Task Return_AfterPoolDisposed_DisposesQuietly()
    {
        var pool = new SftpClientPool(
            new SftpStorageProviderOptions(),
            (_, _) => Task.FromResult(new SftpClient("127.0.0.1", 22, "user", "pass")));

        var lease = await pool.AcquireAsync(Uri("sftp://user@127.0.0.1/file"), CancellationToken.None);
        await pool.DisposeAsync();

        var sync = () => lease.Dispose();
        sync.Should().NotThrow();

        // A second return is ignored.
        var async = async () => await lease.DisposeAsync();
        await async.Should().NotThrowAsync();
    }

    [Fact]
    public async Task Dispose_AfterPoolDisposed_DisposesQuietlyForEveryLease()
    {
        var pool = new SftpClientPool(
            new SftpStorageProviderOptions(),
            (_, _) => Task.FromResult(new SftpClient("127.0.0.1", 22, "user", "pass")));

        var first = await pool.AcquireAsync(Uri("sftp://user@127.0.0.1/a"), CancellationToken.None);
        var second = await pool.AcquireAsync(Uri("sftp://user@127.0.0.1/b"), CancellationToken.None);
        await pool.DisposeAsync();

        var act = async () =>
        {
            await first.DisposeAsync();
            await second.DisposeAsync();
        };

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public void BuildPoolKey_DifferentUserInformationPasswords_DiffersAndNeverContainsThePassword()
    {
        var a = SftpClientPool.BuildPoolKey(Uri("sftp://user:first-secret@host/file"));
        var b = SftpClientPool.BuildPoolKey(Uri("sftp://user:second-secret@host/file"));

        a.Should().NotBe(b);
        a.Should().NotContain("first-secret");
        b.Should().NotContain("second-secret");
    }

    [Fact]
    public void BuildPoolKey_SameUserInformationPassword_IsStable()
    {
        SftpClientPool.BuildPoolKey(Uri("sftp://user:secret@host/a"))
            .Should().Be(SftpClientPool.BuildPoolKey(Uri("sftp://user:secret@host/b")));
    }

    [Fact]
    public void BuildPoolKey_UserInformationPassword_DiffersFromNoPassword()
    {
        SftpClientPool.BuildPoolKey(Uri("sftp://user:x@host/a"))
            .Should().NotBe(SftpClientPool.BuildPoolKey(Uri("sftp://user@host/a")));
    }
}
