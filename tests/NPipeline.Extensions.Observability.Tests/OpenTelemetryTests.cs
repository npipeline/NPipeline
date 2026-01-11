using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Extensions.Observability.OpenTelemetry;
using NPipeline.Extensions.Observability.OpenTelemetry.DependencyInjection;
using NPipeline.Extensions.Observability.Tracing;
using NPipeline.Observability.Tracing;

namespace NPipeline.Extensions.Observability.Tests;

/// <summary>
///     Tests for <see cref="OpenTelemetryPipelineTracer" />.
/// </summary>
public sealed class OpenTelemetryPipelineTracerTests
{
    [Fact]
    public void Constructor_WithValidServiceName_ShouldInitialize()
    {
        // Act
        var tracer = new OpenTelemetryPipelineTracer("TestService");

        // Assert
        Assert.NotNull(tracer);
        Assert.Null(tracer.CurrentActivity);
    }

    [Fact]
    public void Constructor_WithNullServiceName_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        var ex = Assert.Throws<ArgumentNullException>(() => new OpenTelemetryPipelineTracer(null!));
        Assert.Equal("serviceName", ex.ParamName);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void Constructor_WithEmptyOrWhitespaceServiceName_ShouldThrowArgumentException(string serviceName)
    {
        // Act & Assert
        var ex = Assert.Throws<ArgumentException>(() => new OpenTelemetryPipelineTracer(serviceName));
        Assert.Equal("serviceName", ex.ParamName);
    }

    [Fact]
    public void StartActivity_WithValidName_ShouldCreateActivity()
    {
        // Arrange
        var tracer = new OpenTelemetryPipelineTracer("TestService");

        // Act
        var activity = tracer.StartActivity("TestActivity");

        // Assert
        Assert.NotNull(activity);
        _ = Assert.IsType<PipelineActivity>(activity);
    }

    [Fact]
    public void StartActivity_WithNullName_ShouldThrowArgumentNullException()
    {
        // Arrange
        var tracer = new OpenTelemetryPipelineTracer("TestService");

        // Act & Assert
        var ex = Assert.Throws<ArgumentNullException>(() => tracer.StartActivity(null!));
        Assert.Equal("name", ex.ParamName);
    }

    [Fact]
    public void StartActivity_ShouldSetCurrentActivity()
    {
        // Arrange
        var tracer = new OpenTelemetryPipelineTracer("TestService");

        // Act
        var activity = tracer.StartActivity("TestActivity");

        // Assert
        Assert.NotNull(tracer.CurrentActivity);
        Assert.Same(activity, tracer.CurrentActivity);
    }

    [Fact]
    public void CurrentActivity_InitiallyNull()
    {
        // Arrange
        var tracer = new OpenTelemetryPipelineTracer("TestService");

        // Assert
        Assert.Null(tracer.CurrentActivity);
    }

    [Fact]
    public void MultipleActivities_ShouldCreateIndependentInstances()
    {
        // Arrange
        var tracer = new OpenTelemetryPipelineTracer("TestService");

        // Act
        var activity1 = tracer.StartActivity("Activity1");
        var activity2 = tracer.StartActivity("Activity2");

        // Assert
        Assert.NotSame(activity1, activity2);
    }
}

/// <summary>
///     Tests for <see cref="OpenTelemetryObservabilityExtensions" />.
/// </summary>
public sealed class OpenTelemetryObservabilityExtensionsTests
{
    [Fact]
    public void AddOpenTelemetryPipelineTracer_WithServiceName_ShouldRegisterTracer()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        _ = services.AddOpenTelemetryPipelineTracer("TestService");
        var provider = services.BuildServiceProvider();

        // Assert
        var tracer = provider.GetRequiredService<IPipelineTracer>();
        Assert.NotNull(tracer);
        _ = Assert.IsType<OpenTelemetryPipelineTracer>(tracer);
    }

    [Fact]
    public void AddOpenTelemetryPipelineTracer_WithDefaultServiceName_ShouldRegisterTracer()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        _ = services.AddOpenTelemetryPipelineTracer();
        var provider = services.BuildServiceProvider();

        // Assert
        var tracer = provider.GetRequiredService<IPipelineTracer>();
        Assert.NotNull(tracer);
        _ = Assert.IsType<OpenTelemetryPipelineTracer>(tracer);
    }

    [Fact]
    public void AddOpenTelemetryPipelineTracer_RegistersAsSingleton()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddOpenTelemetryPipelineTracer("TestService");
        var provider = services.BuildServiceProvider();

        // Act
        var tracer1 = provider.GetRequiredService<IPipelineTracer>();
        var tracer2 = provider.GetRequiredService<IPipelineTracer>();

        // Assert
        Assert.Same(tracer1, tracer2);
    }

    [Fact]
    public void AddOpenTelemetryPipelineTracer_WithNullServices_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        var ex = Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddOpenTelemetryPipelineTracer("TestService"));
        Assert.Equal("services", ex.ParamName);
    }

    [Fact]
    public void AddOpenTelemetryPipelineTracer_WithNullServiceName_ShouldThrowArgumentNullException()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act & Assert
        var ex = Assert.Throws<ArgumentNullException>(() => services.AddOpenTelemetryPipelineTracer((string)null!));
        Assert.Equal("serviceName", ex.ParamName);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void AddOpenTelemetryPipelineTracer_WithEmptyServiceName_ShouldThrowArgumentException(string serviceName)
    {
        // Arrange
        var services = new ServiceCollection();

        // Act & Assert
        var ex = Assert.Throws<ArgumentException>(() => services.AddOpenTelemetryPipelineTracer(serviceName));
        Assert.Equal("serviceName", ex.ParamName);
    }

    [Fact]
    public void AddOpenTelemetryPipelineTracer_WithFactory_ShouldUseFactory()
    {
        // Arrange
        var services = new ServiceCollection();
        var factoryCalled = false;

        // Act
        _ = services.AddOpenTelemetryPipelineTracer(sp =>
        {
            factoryCalled = true;
            return new OpenTelemetryPipelineTracer("FactoryService");
        });

        var provider = services.BuildServiceProvider();
        var tracer = provider.GetRequiredService<IPipelineTracer>();

        // Assert
        Assert.True(factoryCalled);
        Assert.NotNull(tracer);
        _ = Assert.IsType<OpenTelemetryPipelineTracer>(tracer);
    }

    [Fact]
    public void AddOpenTelemetryPipelineTracer_WithFactory_WithNullServices_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        var ex = Assert.Throws<ArgumentNullException>(() =>
            ((IServiceCollection)null!).AddOpenTelemetryPipelineTracer(sp => new OpenTelemetryPipelineTracer("Test")));

        Assert.Equal("services", ex.ParamName);
    }

    [Fact]
    public void AddOpenTelemetryPipelineTracer_WithFactory_WithNullFactory_ShouldThrowArgumentNullException()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act & Assert
        var ex = Assert.Throws<ArgumentNullException>(() => services.AddOpenTelemetryPipelineTracer(
            (Func<IServiceProvider, OpenTelemetryPipelineTracer>)null!));

        Assert.Equal("factory", ex.ParamName);
    }

    [Fact]
    public void AddOpenTelemetryPipelineTracer_ShouldReturnServiceCollection()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        var result = services.AddOpenTelemetryPipelineTracer("TestService");

        // Assert
        Assert.Same(services, result);
    }
}

/// <summary>
///     Tests for <see cref="OpenTelemetryTracerBuilderExtensions" />.
/// </summary>
public sealed class OpenTelemetryTracerBuilderExtensionsTests
{
    [Fact]
    public void GetNPipelineInfo_WithValidNPipelineActivity_ShouldExtractInfo()
    {
        // Arrange
        using var activity = new Activity("MyService.ProcessData");
        _ = activity.Start();

        // Act
        var info = activity.GetNPipelineInfo();

        // Assert
        Assert.NotNull(info);
        Assert.Equal("MyService", info.ServiceName);
        Assert.Equal("ProcessData", info.ActivityName);
        Assert.Same(activity, info.Activity);

        activity.Stop();
    }

    [Fact]
    public void GetNPipelineInfo_WithoutServicePrefix_ShouldReturnNull()
    {
        // Arrange
        using var activity = new Activity("NoServicePrefix");
        _ = activity.Start();

        // Act
        var info = activity.GetNPipelineInfo();

        // Assert
        Assert.Null(info);

        activity.Stop();
    }

    [Fact]
    public void GetNPipelineInfo_WithNullActivity_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        var ex = Assert.Throws<ArgumentNullException>(() => ((Activity)null!).GetNPipelineInfo());
        Assert.Equal("activity", ex.ParamName);
    }
}
