using System.Reflection;
using System.Runtime.ExceptionServices;
using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Reliability;
using NResilience;

namespace NPipeline.Connectors.Http.Tests.Configuration;

public class HttpSourceConfigurationTests
{
    private static HttpSourceConfiguration ValidConfig() => new() { BaseUri = new Uri("https://api.example.com/items") };

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
    public void Validate_WithInvalidResilience_ThrowsResilienceConfigurationException()
    {
        var config = new HttpSourceConfiguration
        {
            BaseUri = new Uri("https://api.example.com"),
#pragma warning disable NRES003 // The invalid value is the point of the test.
            Resilience = HttpConnectorResilience.Default with { Attempts = 0 },
#pragma warning restore NRES003
        };

        var act = () => InvokeValidate(config);

        act.Should().Throw<ResilienceConfigurationException>();
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
