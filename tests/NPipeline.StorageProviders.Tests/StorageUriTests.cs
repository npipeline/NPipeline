using AwesomeAssertions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Tests;

public sealed class StorageUriTests
{
    [Fact]
    public void FromFilePath_WithAbsoluteWindowsPath_NormalizesWithLeadingSlash()
    {
        // Arrange
        var path = Path.GetFullPath("C:\\Temp\\file.csv").Replace('\\', '/');

        // Act
        var uri = StorageUri.FromFilePath(path);

        // Assert
        uri.Scheme.ToString().Should().Be("file");
        uri.Host.Should().BeNull();
        uri.Path.Should().StartWith("/");
        uri.ToString().Should().StartWith("file:///");
    }

    [Fact]
    public void Parse_WithAbsoluteUri_ParsesSchemeHostPathAndParams()
    {
        // Arrange
        var text = "file:///C:/data/test.csv?encoding=utf-8&detectDelimiter=true";

        // Act
        var uri = StorageUri.Parse(text);

        // Assert
        uri.Scheme.ToString().Should().Be("file");
        uri.Host.Should().BeNullOrEmpty();
        uri.Path.Should().Be("/C:/data/test.csv");
        uri.Parameters.Should().ContainKey("encoding");
        uri.Parameters["encoding"].Should().Be("utf-8");
        uri.Parameters.Should().ContainKey("detectDelimiter");
        uri.Parameters["detectDelimiter"].Should().Be("true");
    }

    [Fact]
    public void TryParse_WithRelativeFilePath_FallsBackToFileScheme()
    {
        // Arrange
        var relative = "./data/input.csv";

        // Act
        var ok = StorageUri.TryParse(relative, out var uri, out var error);

        // Assert
        ok.Should().BeTrue(error);
        uri!.Scheme.ToString().Should().Be("file");
        uri.Path.Should().StartWith("/");
    }

    [Fact]
    public void ToString_RendersCanonicalForm()
    {
        var uri = StorageUri.Parse("file:///C:/work/pipeline.csv");
        var rendered = uri.ToString();

        rendered.Should().Be("file:///C:/work/pipeline.csv"); // file:/// + C:/...
        rendered.Should().Contain("file:");
    }

    [Fact]
    public void Equals_SameText_IsEqualAndHashesEqual()
    {
        var a = StorageUri.Parse("s3://bucket/key.csv?region=ap-southeast-2");
        var b = StorageUri.Parse("s3://bucket/key.csv?region=ap-southeast-2");

        a.Should().Be(b);
        a.GetHashCode().Should().Be(b.GetHashCode());
        (a == b).Should().BeTrue();
    }

    [Fact]
    public void Equals_ParameterOrderAndKeyCase_DoNotMatter()
    {
        var a = StorageUri.Parse("s3://Bucket/key?a=1&b=2");
        var b = StorageUri.Parse("s3://bucket/key?B=2&A=1");

        a.Should().Be(b);
        a.GetHashCode().Should().Be(b.GetHashCode());
    }

    [Fact]
    public void Equals_DifferentValuesPathOrPassword_AreNotEqual()
    {
        var uri = StorageUri.Parse("sftp://u:p@host:22/a?x=1");

        uri.Should().NotBe(StorageUri.Parse("sftp://u:p@host:22/a?x=2"));
        uri.Should().NotBe(StorageUri.Parse("sftp://u:p@host:22/A?x=1"));
        uri.Should().NotBe(StorageUri.Parse("sftp://u:q@host:22/a?x=1"));
        uri.Should().NotBe(StorageUri.Parse("sftp://u:p@host:23/a?x=1"));
    }

    [Fact]
    public void Parse_DecodesPercentEscapesInPath()
    {
        StorageUri.Parse("s3://bucket/my file.csv").Path.Should().Be("/my file.csv");
        StorageUri.Parse("s3://bucket/a%20b.csv").Path.Should().Be("/a b.csv");
        StorageUri.Parse("s3://bucket/a%2520b").Path.Should().Be("/a%20b");
    }

    [Theory]
    [InlineData("s3://bucket/reports/run#1.csv", "/reports/run#1.csv")]
    [InlineData("s3://bucket/dir/../x.csv", "/dir/../x.csv")]
    [InlineData("s3://bucket/a//b", "/a//b")]
    [InlineData("s3://bucket/what%3Fnow", "/what?now")]
    [InlineData("s3://bucket", "/")]
    public void Parse_KeepsHashDotSegmentsAndDoubleSlashes(string text, string expectedPath)
    {
        StorageUri.Parse(text).Path.Should().Be(expectedPath);
    }

    [Fact]
    public void Parse_ExtractsAuthorityParts()
    {
        var uri = StorageUri.Parse("sftp://us%40er:p%3Ass@Example.COM:2222/dir/file.csv?keyPath=%2Fk%2Fid&flag");

        uri.Scheme.Should().Be(StorageScheme.Sftp);
        uri.Host.Should().Be("example.com");
        uri.Port.Should().Be(2222);
        uri.UserName.Should().Be("us@er");
        uri.Password.Should().Be("p:ss");
        uri.Path.Should().Be("/dir/file.csv");
        uri.Parameters["keyPath"].Should().Be("/k/id");
        uri.Parameters["FLAG"].Should().BeEmpty();
    }

    [Theory]
    [InlineData("sftp://host:abc/x")]
    [InlineData("sftp://host:99999/x")]
    public void TryParse_InvalidPort_ReturnsFalse(string text)
    {
        StorageUri.TryParse(text, out var uri, out var error).Should().BeFalse();
        uri.Should().BeNull();
        error.Should().Contain("port");
    }

    [Fact]
    public void Parse_PlainLocalPath_IsFileUri()
    {
        var path = Path.Combine(Path.GetTempPath(), "a b", "c#d.csv");

        var uri = StorageUri.Parse(path);

        uri.Scheme.Should().Be(StorageScheme.File);
        uri.Path.Should().Be(StorageUri.FromFilePath(path).Path);
        uri.Path.Should().EndWith("/a b/c#d.csv");
    }

    [Theory]
    [InlineData("a b")]
    [InlineData("100%")]
    [InlineData("a#b")]
    [InlineData("a+b")]
    [InlineData("a&b=c")]
    [InlineData("a?b")]
    [InlineData("a:b")]
    [InlineData("ünï/cødé 日本")]
    [InlineData("a//b/../c")]
    [InlineData("%41")]
    public void ToUnredactedString_RoundTripsThroughParse(string key)
    {
        var uri = StorageUri.Parse("s3://bucket/")
            .WithPath("/dir/" + key)
            .WithParameter("name", key)
            .WithParameter(key, "v");

        var reparsed = StorageUri.Parse(uri.ToUnredactedString());

        reparsed.Should().Be(uri);
        reparsed.Path.Should().Be("/dir/" + key);
        reparsed.Parameters["name"].Should().Be(key);
    }

    [Fact]
    public void ToUnredactedString_RoundTripsUserInfo()
    {
        var uri = StorageUri.Parse("sftp://u%3Aser:p%40ss%2Fw%3Ard@host:22/x");

        uri.UserName.Should().Be("u:ser");
        uri.Password.Should().Be("p@ss/w:rd");
        StorageUri.Parse(uri.ToUnredactedString()).Should().Be(uri);
    }

    [Fact]
    public void ToString_RedactsPassword()
    {
        var uri = StorageUri.Parse("sftp://user:hunter2@host/x");

        uri.ToString().Should().Be("sftp://user:***@host/x");
        uri.ToString().Should().NotContain("hunter2");
        uri.ToUnredactedString().Should().Contain("hunter2");
    }

    [Theory]
    [InlineData("password")]
    [InlineData("pwd")]
    [InlineData("secretKey")]
    [InlineData("sessionToken")]
    [InlineData("sasToken")]
    [InlineData("accountKey")]
    [InlineData("connectionString")]
    [InlineData("accessToken")]
    [InlineData("keyPassphrase")]
    [InlineData("accessKey")]
    [InlineData("key")]
    [InlineData("token")]
    [InlineData("apiKey")]
    [InlineData("credentialsPath")]
    [InlineData("SECRETKEY")]
    public void ToString_RedactsSecretParameters(string name)
    {
        var uri = StorageUri.Parse("s3://bucket/key").WithParameter(name, "s3cr3t-value").WithParameter("region", "ap-southeast-2");

        uri.ToString().Should().NotContain("s3cr3t-value").And.Contain("region=ap-southeast-2");
        uri.ToUnredactedString().Should().Contain("s3cr3t-value");
        StorageUri.SecretParameterNames.Should().Contain(name);
    }

    [Fact]
    public void WithPath_NormalisesLeadingSlash()
    {
        var uri = StorageUri.Parse("s3://bucket/a").WithPath("no-leading//slash");

        uri.Path.Should().Be("/no-leading//slash");
        uri.WithPath("").Path.Should().Be("/");
    }

    [Fact]
    public void WithPath_KeepsHostPortUserAndParameters()
    {
        var uri = StorageUri.Parse("sftp://u:p@host:2222/a?x=1").WithPath("/b");

        uri.Should().Be(StorageUri.Parse("sftp://u:p@host:2222/b?x=1"));
    }

    [Fact]
    public void Parameters_CannotBeMutated()
    {
        var uri = StorageUri.Parse("s3://bucket/key?a=1");

        uri.Parameters.Should().NotBeOfType<Dictionary<string, string>>();
        ((Action)(() => ((IDictionary<string, string>)uri.Parameters).Add("x", "y"))).Should().Throw<NotSupportedException>();
        uri.WithParameter("b", "2").Parameters.Should().HaveCount(2);
        uri.Parameters.Should().HaveCount(1);
    }

    [Fact]
    public void WithoutParameter_RemovesKeyIgnoringCase()
    {
        var uri = StorageUri.Parse("s3://bucket/key?A=1&b=2").WithoutParameter("a");

        uri.Should().Be(StorageUri.Parse("s3://bucket/key?b=2"));
    }

    [Theory]
    [InlineData("/a/b", "c", "/a/b/c")]
    [InlineData("/a/b/", "/c/d", "/a/b/c/d")]
    [InlineData("/", "c", "/c")]
    [InlineData("/a/", "c/", "/a/c/")]
    public void Combine_JoinsWithOneSlash(string basePath, string relative, string expected)
    {
        StorageUri.Parse("s3://bucket/").WithPath(basePath).Combine(relative).Path.Should().Be(expected);
    }

    [Theory]
    [InlineData("/a/b/c.csv", "c.csv", "/a/b/", false)]
    [InlineData("/a/b/", "b", "/a/", true)]
    [InlineData("/a", "a", "/", false)]
    public void NameParentAndIsDirectory_DescribeThePath(string path, string name, string parentPath, bool isDirectory)
    {
        var uri = StorageUri.Parse("s3://bucket/?region=r").WithPath(path);

        uri.Name.Should().Be(name);
        uri.IsDirectory.Should().Be(isDirectory);
        uri.Parent.Should().Be(uri.WithPath(parentPath));
    }

    [Fact]
    public void Parent_OfRoot_IsNull()
    {
        StorageUri.Parse("s3://bucket/").Parent.Should().BeNull();
        StorageUri.Parse("s3://bucket/").Name.Should().BeEmpty();
    }
}
