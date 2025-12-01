---
title: Advanced Testing
description: Learn how to test complex pipeline scenarios, including nodes with dependencies, error handling, and more.
sidebar_position: 2
---

# Advanced Pipeline Testing

While the [basic testing utilities](../extensions/testing) are great for simple, stateless pipelines, real-world scenarios are often more complex. Your nodes may have dependencies on external services, implement complex error handling, or manage internal state.

This guide covers strategies for testing these advanced scenarios effectively.

> **Getting started with testing?** Start with the [Testing Extensions](../extensions/testing) overview for basic patterns.

> **This guide** focuses on: **Dependencies, mocking, error handling, state management, and integration tests**  
> **Testing Extensions** covers: **Installation, quick start, available packages, and basic patterns**

## Testing Nodes with Dependencies

When your nodes rely on external services (like a database repository or a web API client), you'll want to replace those dependencies with mock or fake implementations during tests. This isolates your node's logic and makes your tests fast and reliable.

The `NPipeline.Extensions.DependencyInjection` package is invaluable here.

### Example: Mocking a Service with Moq

Let's expand on the `NotificationTransform` example from the [Dependency Injection](../extensions/dependency-injection) page. We want to test the transform without actually sending an email. We can use a mocking library like [Moq](https://github.com/moq/moq4) to provide a mock `IEmailService`.

**1. Install required packages:**

```bash
dotnet add package Moq
```

**2. The Test:**

```csharp
using Moq;
using NPipeline.Execution;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Xunit;

public interface IEmailService
{
    Task SendEmailAsync(string to, string subject, string body);
}

public sealed class NotificationTransform : TransformNode<string, string>
{
    private readonly IEmailService _emailService;

    public NotificationTransform(IEmailService emailService)
    {
        _emailService = emailService;
    }

    public override async Task<string> ExecuteAsync(
        string item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        await _emailService.SendEmailAsync(
            "admin@example.com",
            "Processing Item",
            $"Item '{item}' was processed.");
        return $"Notified for {item}";
    }
}

public sealed class NotificationPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<InMemorySourceNode<string>, string>();
        var transform = builder.AddTransform<NotificationTransform, string, string>();
        var sink = builder.AddSink<InMemorySinkNode<string>, string>();

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}

public class NotificationTransformTests
{
    [Fact]
    public async Task Transform_Should_Call_Email_Service_For_Each_Item()
    {
        // Arrange
        var mockEmailService = new Mock<IEmailService>();

        var services = new ServiceCollection();
        
        // Approach 1: Assembly scanning (automatic discovery)
        services.AddNPipeline(typeof(NotificationTransformTests).Assembly);

        // Approach 2: Fluent configuration (explicit registration)
        // services.AddNPipeline(builder => builder
        //     .AddNode<InMemorySourceNode<string>>()
        //     .AddNode<NotificationTransform>()
        //     .AddNode<InMemorySinkNode<string>>()
        //     .AddPipeline<NotificationPipeline>()
        // );

        // Register the mock service BEFORE building the provider
        services.AddSingleton(mockEmailService.Object);

        var serviceProvider = services.BuildServiceProvider();

        var inputData = new[] { "item1", "item2" };
        var context = new PipelineContext();
        context.SetSourceData(inputData);

        // Note: IPipelineRunner is registered by AddNPipeline() in the DI container
        var runner = serviceProvider.GetRequiredService<IPipelineRunner>();

        // Act
        await runner.RunAsync<NotificationPipeline>(context);

        // Assert
        // Verify that the SendEmailAsync method was called twice
        mockEmailService.Verify(
            x => x.SendEmailAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>()),
            Times.Exactly(2));
    }
}
```

In this test, we:

1. Create a mock `IEmailService` using `new Mock<IEmailService>()`.
2. Register this mock service with the `ServiceCollection`.
3. Set up the test data in the `PipelineContext` for the `InMemorySourceNode`.
4. When the pipeline runs, the `NotificationTransform` receives the mock service.
5. Finally, we use Moq's verification API (`Verify()`) to confirm the service's method was called as expected.

## Testing Error Handling

Testing your pipeline's resilience is important. You should verify that your nodes handle errors gracefully.

### Example: Testing Error Handling in a Transform

Let's test a transform that handles parsing errors:

```csharp
using NPipeline.Execution;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Xunit;

public sealed class ParsingTransform : TransformNode<string, int>
{
    public override Task<int> ExecuteAsync(
        string item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        if (!int.TryParse(item, out var result))
        {
            throw new FormatException($"Cannot parse '{item}' as an integer");
        }
        return Task.FromResult(result);
    }
}

public sealed class ParsingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<InMemorySourceNode<string>, string>();
        var transform = builder.AddTransform<ParsingTransform, string, int>();
        var sink = builder.AddSink<InMemorySinkNode<int>, int>();

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}

public class ParsingTransformTests
{
    [Fact]
    public async Task Should_Throw_On_Invalid_Input()
    {
        // Arrange
        var inputData = new[] { "1", "two", "3" };
        var context = new PipelineContext();
        context.SetSourceData(inputData);

        var runner = PipelineRunner.Create();

        // Act & Assert
        var exception = await Assert.ThrowsAsync<FormatException>(
            async () => await runner.RunAsync<ParsingPipeline>(context));

        Assert.Contains("Cannot parse", exception.Message);
    }
}
```

## Best Practices for Advanced Testing

- **Isolate What You're Testing:** Use mocks and fakes to ensure your test focuses on a single unit of logic (e.g., one node's `ExecuteAsync` method).
- **Test Both Success and Failure:** Don't just test the happy path. Write tests for invalid input, exceptions, cancellations, and other failure modes.
- **Use `[Theory]` for Parameterized Tests:** For nodes with complex conditional logic, use xUnit's `[Theory]` attribute to test many different inputs and expected outputs concisely.
- **Keep Tests Fast:** Avoid `Task.Delay`, network calls, or file system access in your unit tests. Rely on mocks to simulate these operations.

