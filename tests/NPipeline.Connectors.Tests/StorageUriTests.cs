using AwesomeAssertions;

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
}
