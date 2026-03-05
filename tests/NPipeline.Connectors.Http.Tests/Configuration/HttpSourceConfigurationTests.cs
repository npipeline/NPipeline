using System.Reflection;
using System.Runtime.ExceptionServices;
using NPipeline.Connectors.Http.Configuration;

namespace NPipeline.Connectors.Http.Tests.Configuration;

public class HttpSourceConfigurationTests
{
    private static HttpSourceConfiguration ValidConfig()
    {
        return new HttpSourceConfiguration { BaseUri = new Uri("https://api.example.com/items") };
    }

    [Fact]
    public void Validate_WithAbsoluteUri_DoesNotThrow()
    {
        var config = ValidConfig();

        var act = () => config.GetType()
            .GetMethod("Validate", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(config, null);

        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_WithRelativeUri_ThrowsArgumentException()
    {
        var config = new HttpSourceConfiguration { BaseUri = new Uri("/relative/path", UriKind.Relative) };
        var act = () => InvokeValidate(config);

        act.Should().Throw<ArgumentException>()
            .WithMessage("*BaseUri*");
    }

    [Fact]
    public void Validate_WithZeroTimeout_ThrowsArgumentException()
    {
        var config = new HttpSourceConfiguration
        {
            BaseUri = new Uri("https://api.example.com"),
            Timeout = TimeSpan.Zero,
        };

        var act = () => InvokeValidate(config);

        act.Should().Throw<ArgumentException>()
            .WithMessage("*Timeout*");
    }

    [Fact]
    public void Validate_WithNegativeTimeout_ThrowsArgumentException()
    {
        var config = new HttpSourceConfiguration
        {
            BaseUri = new Uri("https://api.example.com"),
            Timeout = TimeSpan.FromSeconds(-1),
        };

        var act = () => InvokeValidate(config);

        act.Should().Throw<ArgumentException>()
            .WithMessage("*Timeout*");
    }

    [Fact]
    public void Validate_WithMaxPagesZero_ThrowsArgumentException()
    {
        var config = new HttpSourceConfiguration
        {
            BaseUri = new Uri("https://api.example.com"),
            MaxPages = 0,
        };

        var act = () => InvokeValidate(config);

        act.Should().Throw<ArgumentException>()
            .WithMessage("*MaxPages*");
    }

    [Fact]
    public void Validate_WithMaxResponseBytesZero_ThrowsArgumentException()
    {
        var config = new HttpSourceConfiguration
        {
            BaseUri = new Uri("https://api.example.com"),
            MaxResponseBytes = 0,
        };

        var act = () => InvokeValidate(config);

        act.Should().Throw<ArgumentException>()
            .WithMessage("*MaxResponseBytes*");
    }

    [Fact]
    public void Validate_WithPositiveMaxPages_DoesNotThrow()
    {
        var config = new HttpSourceConfiguration
        {
            BaseUri = new Uri("https://api.example.com"),
            MaxPages = 10,
        };

        var act = () => InvokeValidate(config);
        act.Should().NotThrow();
    }

    private static void InvokeValidate(HttpSourceConfiguration config)
    {
        try
        {
            config.GetType()
                .GetMethod("Validate", BindingFlags.Instance | BindingFlags.NonPublic)!
                .Invoke(config, null);
        }
        catch (TargetInvocationException tie) when (tie.InnerException != null)
        {
            ExceptionDispatchInfo.Capture(tie.InnerException).Throw();
        }
    }
}
