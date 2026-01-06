# Testing Composite Pipelines

## Overview

Testing composite pipelines requires different strategies than testing flat pipelines. This guide covers unit testing, integration testing, and test-driven development approaches for composite pipelines.

## Testing Strategies

### 1. Test Sub-Pipelines Independently

The most important principle: **test each sub-pipeline in isolation**.

```csharp
[Fact]
public async Task ValidationPipeline_WithValidData_ShouldPass()
{
    // Arrange
    var runner = PipelineRunner.Create();
    var context = new PipelineContext();
    var validCustomer = new Customer(1, "John", "john@example.com");
    context.Parameters[CompositeContextKeys.InputItem] = validCustomer;
    
    // Act
    await runner.RunAsync<ValidationPipeline>(context);
    
    // Assert
    var output = context.Parameters[CompositeContextKeys.OutputItem];
    output.Should().BeOfType<ValidatedCustomer>();
    var validated = (ValidatedCustomer)output;
    validated.IsValid.Should().BeTrue();
}

[Fact]
public async Task ValidationPipeline_WithInvalidData_ShouldFail()
{
    // Arrange
    var runner = PipelineRunner.Create();
    var context = new PipelineContext();
    var invalidCustomer = new Customer(1, "", ""); // Missing name and email
    context.Parameters[CompositeContextKeys.InputItem] = invalidCustomer;
    
    // Act
    await runner.RunAsync<ValidationPipeline>(context);
    
    // Assert
    var output = context.Parameters[CompositeContextKeys.OutputItem];
    var validated = (ValidatedCustomer)output;
    validated.IsValid.Should().BeFalse();
    validated.ValidationErrors.Should().Contain("Name is required");
    validated.ValidationErrors.Should().Contain("Email is required");
}
```

### 2. Test Parent Pipeline with Mock Sub-Pipelines

Test the parent pipeline structure without executing real sub-pipelines:

```csharp
// Mock sub-pipeline for testing
public class MockValidationPipeline : IPipelineDefinition
{
    public static int CallCount { get; set; }
    
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddSource<PipelineInputSource<Customer>, Customer>("input");
        var mock = builder.AddTransform<MockValidator, Customer, ValidatedCustomer>("mock");
        var output = builder.AddSink<PipelineOutputSink<ValidatedCustomer>, ValidatedCustomer>("output");
        
        builder.Connect(input, mock);
        builder.Connect(mock, output);
    }
}

public class MockValidator : TransformNode<Customer, ValidatedCustomer>
{
    public override Task<ValidatedCustomer> ExecuteAsync(Customer input, PipelineContext context, CancellationToken ct)
    {
        MockValidationPipeline.CallCount++;
        return Task.FromResult(new ValidatedCustomer(input, true, new List<string>()));
    }
}

[Fact]
public async Task ParentPipeline_ShouldCallValidationPipeline()
{
    // Arrange
    MockValidationPipeline.CallCount = 0;
    var runner = PipelineRunner.Create();
    var context = new PipelineContext();
    
    // Act
    await runner.RunAsync<ParentPipelineWithMocks>(context);
    
    // Assert
    MockValidationPipeline.CallCount.Should().BeGreaterThan(0);
}
```

### 3. Integration Testing

Test the complete pipeline hierarchy:

```csharp
[Fact]
public async Task CompleteWorkflow_WithRealData_ShouldProcessCorrectly()
{
    // Arrange
    var runner = PipelineRunner.Create();
    var context = new PipelineContext();
    var collector = new CollectorSink<EnrichedCustomer>();
    
    // Act
    await runner.RunAsync<CompleteProcessingPipeline>(context);
    
    // Assert
    var results = collector.CollectedItems;
    results.Should().HaveCount(ExpectedCount);
    results.All(r => r.ValidatedCustomer.IsValid).Should().BeTrue();
    results.All(r => r.LoyaltyTier != null).Should().BeTrue();
}
```

### 4. Test Context Inheritance

Verify context data flows correctly:

```csharp
[Fact]
public async Task SubPipeline_WithInheritance_ShouldReceiveParentContext()
{
    // Arrange
    var runner = PipelineRunner.Create();
    var context = new PipelineContext();
    context.Parameters["ApiKey"] = "test-key";
    context.Parameters["Environment"] = "Test";
    
    ContextCheckTransform.CapturedApiKey = null;
    ContextCheckTransform.CapturedEnvironment = null;
    
    // Act
    await runner.RunAsync<ParentWithInheritance>(context);
    
    // Assert
    ContextCheckTransform.CapturedApiKey.Should().Be("test-key");
    ContextCheckTransform.CapturedEnvironment.Should().Be("Test");
}

[Fact]
public async Task SubPipeline_WithoutInheritance_ShouldNotReceiveParentContext()
{
    // Arrange
    var runner = PipelineRunner.Create();
    var context = new PipelineContext();
    context.Parameters["ApiKey"] = "test-key";
    
    ContextCheckTransform.CapturedApiKey = null;
    
    // Act
    await runner.RunAsync<ParentWithoutInheritance>(context);
    
    // Assert
    ContextCheckTransform.CapturedApiKey.Should().BeNull();
}
```

## Test Helpers

### Helper: CollectorSink

Collects items for assertion:

```csharp
public class CollectorSink<T> : ISinkNode<T>
{
    public List<T> CollectedItems { get; } = new();
    
    public async Task ExecuteAsync(IDataPipe<T> input, PipelineContext context, CancellationToken ct)
    {
        CollectedItems.Clear();
        await foreach (var item in input.WithCancellation(ct))
        {
            CollectedItems.Add(item);
        }
    }
    
    public ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }
}
```

### Helper: StubSource

Provides test data:

```csharp
public class StubSource<T> : ISourceNode<T>
{
    private readonly IEnumerable<T> _items;
    
    public StubSource(IEnumerable<T> items)
    {
        _items = items;
    }
    
    public IDataPipe<T> Initialize(PipelineContext context, CancellationToken ct)
    {
        return new InMemoryDataPipe<T>(_items, "StubSource");
    }
    
    public ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }
}
```

### Helper: SpyTransform

Tracks calls and data:

```csharp
public class SpyTransform<T> : TransformNode<T, T>
{
    public List<T> ProcessedItems { get; } = new();
    public int CallCount { get; private set; }
    
    public override Task<T> ExecuteAsync(T input, PipelineContext context, CancellationToken ct)
    {
        CallCount++;
        ProcessedItems.Add(input);
        return Task.FromResult(input);
    }
}
```

## Test Patterns

### Pattern 1: Arrange-Act-Assert

Standard test structure:

```csharp
[Fact]
public async Task SubPipeline_ValidInput_ProducesExpectedOutput()
{
    // Arrange: Set up test data and context
    var runner = PipelineRunner.Create();
    var context = new PipelineContext();
    var testInput = CreateTestInput();
    context.Parameters[CompositeContextKeys.InputItem] = testInput;
    
    // Act: Execute the pipeline
    await runner.RunAsync<SubPipeline>(context);
    
    // Assert: Verify the output
    var output = context.Parameters[CompositeContextKeys.OutputItem];
    output.Should().NotBeNull();
    VerifyOutput(output);
}
```

### Pattern 2: Theory-Based Testing

Test multiple scenarios:

```csharp
[Theory]
[InlineData(1, "Bronze")]
[InlineData(50, "Gold")]
[InlineData(200, "Silver")]
[InlineData(1000, "Bronze")]
public async Task EnrichmentPipeline_VariousIds_AssignsCorrectTier(int customerId, string expectedTier)
{
    // Arrange
    var runner = PipelineRunner.Create();
    var context = new PipelineContext();
    var customer = new Customer(customerId, "Test", "test@example.com");
    context.Parameters[CompositeContextKeys.InputItem] = customer;
    
    // Act
    await runner.RunAsync<EnrichmentPipeline>(context);
    
    // Assert
    var output = (EnrichedCustomer)context.Parameters[CompositeContextKeys.OutputItem];
    output.LoyaltyTier.Should().Be(expectedTier);
}
```

### Pattern 3: Builder Pattern for Test Data

Create complex test data easily:

```csharp
public class CustomerBuilder
{
    private int _id = 1;
    private string _name = "Test Customer";
    private string _email = "test@example.com";
    private string? _phone = null;
    
    public CustomerBuilder WithId(int id) { _id = id; return this; }
    public CustomerBuilder WithName(string name) { _name = name; return this; }
    public CustomerBuilder WithEmail(string email) { _email = email; return this; }
    public CustomerBuilder WithPhone(string phone) { _phone = phone; return this; }
    
    public Customer Build() => new Customer(_id, _name, _email, _phone);
    
    public static CustomerBuilder Default() => new();
}

// Usage
[Fact]
public async Task ValidationPipeline_CustomerWithoutEmail_ShouldFail()
{
    var customer = CustomerBuilder.Default()
        .WithEmail("")  // Invalid
        .Build();
        
    var context = new PipelineContext();
    context.Parameters[CompositeContextKeys.InputItem] = customer;
    
    await runner.RunAsync<ValidationPipeline>(context);
    
    var output = (ValidatedCustomer)context.Parameters[CompositeContextKeys.OutputItem];
    output.IsValid.Should().BeFalse();
}
```

### Pattern 4: Fixture-Based Testing

Share setup across tests:

```csharp
public class CompositionTestFixture
{
    public PipelineRunner Runner { get; }
    public List<Customer> TestCustomers { get; }
    
    public CompositionTestFixture()
    {
        Runner = PipelineRunner.Create();
        TestCustomers = new List<Customer>
        {
            new Customer(1, "Alice", "alice@example.com"),
            new Customer(2, "Bob", "bob@example.com"),
            new Customer(3, "Charlie", "charlie@example.com")
        };
    }
}

public class CompositionTests : IClassFixture<CompositionTestFixture>
{
    private readonly CompositionTestFixture _fixture;
    
    public CompositionTests(CompositionTestFixture fixture)
    {
        _fixture = fixture;
    }
    
    [Fact]
    public async Task Test1()
    {
        var context = new PipelineContext();
        context.Parameters[CompositeContextKeys.InputItem] = _fixture.TestCustomers[0];
        await _fixture.Runner.RunAsync<ValidationPipeline>(context);
        // Assert
    }
}
```

## Testing Error Scenarios

### Test Exception Handling

```csharp
[Fact]
public async Task SubPipeline_WithInvalidData_ShouldThrowValidationException()
{
    // Arrange
    var runner = PipelineRunner.Create();
    var context = new PipelineContext();
    var invalidData = CreateInvalidData();
    context.Parameters[CompositeContextKeys.InputItem] = invalidData;
    
    // Act & Assert
    await Assert.ThrowsAsync<ValidationException>(() =>
        runner.RunAsync<ValidationPipeline>(context));
}

[Fact]
public async Task SubPipeline_ThrowsException_MessageContainsDetails()
{
    // Arrange
    var runner = PipelineRunner.Create();
    var context = new PipelineContext();
    var invalidData = CreateInvalidData();
    context.Parameters[CompositeContextKeys.InputItem] = invalidData;
    
    // Act
    var exception = await Assert.ThrowsAsync<ValidationException>(() =>
        runner.RunAsync<ValidationPipeline>(context));
    
    // Assert
    exception.Message.Should().Contain("validation");
    exception.Message.Should().Contain(invalidData.Id.ToString());
}
```

### Test Error Propagation

```csharp
[Fact]
public async Task ParentPipeline_SubPipelineThrows_ErrorPropagates()
{
    // Arrange
    var runner = PipelineRunner.Create();
    var context = new PipelineContext();
    
    // Act & Assert
    var exception = await Assert.ThrowsAsync<ProcessingException>(() =>
        runner.RunAsync<ParentPipeline>(context));
    
    exception.InnerException.Should().NotBeNull();
    exception.InnerException.Should().BeOfType<ValidationException>();
}
```

## Testing Async Behavior

### Test Cancellation

```csharp
[Fact]
public async Task SubPipeline_WhenCancelled_ShouldThrowOperationCanceledException()
{
    // Arrange
    var runner = PipelineRunner.Create();
    var cts = new CancellationTokenSource();
    var context = new PipelineContext(
        PipelineContextConfiguration.WithCancellation(cts.Token));
    
    // Cancel immediately
    cts.Cancel();
    
    // Act & Assert
    await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
        runner.RunAsync<SlowPipeline>(context));
}

[Fact]
public async Task SubPipeline_CancelledDuringExecution_ShouldStop()
{
    // Arrange
    var runner = PipelineRunner.Create();
    var cts = new CancellationTokenSource();
    var context = new PipelineContext(
        PipelineContextConfiguration.WithCancellation(cts.Token));
    
    SpyTransform<int>.ProcessedCount = 0;
    
    // Act
    var task = runner.RunAsync<LongRunningPipeline>(context);
    
    // Cancel after some processing
    await Task.Delay(100);
    cts.Cancel();
    
    // Assert
    await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
    SpyTransform<int>.ProcessedCount.Should().BeLessThan(ExpectedTotalCount);
}
```

### Test Async Transforms

```csharp
[Fact]
public async Task SubPipeline_WithAsyncTransform_ShouldCompleteSuccessfully()
{
    // Arrange
    var runner = PipelineRunner.Create();
    var context = new PipelineContext();
    var input = CreateTestData();
    context.Parameters[CompositeContextKeys.InputItem] = input;
    
    // Act
    var sw = Stopwatch.StartNew();
    await runner.RunAsync<AsyncTransformPipeline>(context);
    sw.Stop();
    
    // Assert
    sw.Elapsed.Should().BeGreaterThan(TimeSpan.FromMilliseconds(100)); // Async delay occurred
    var output = context.Parameters[CompositeContextKeys.OutputItem];
    output.Should().NotBeNull();
}
```

## Test-Driven Development

### TDD Workflow for Composite Pipelines

1. **Write failing test for sub-pipeline**:
```csharp
[Fact]
public async Task ValidationPipeline_WithEmail_ShouldValidate()
{
    // This test will fail initially
    var context = new PipelineContext();
    context.Parameters[CompositeContextKeys.InputItem] = 
        new Customer(1, "Test", "test@example.com");
    
    await runner.RunAsync<ValidationPipeline>(context);
    
    var output = (ValidatedCustomer)context.Parameters[CompositeContextKeys.OutputItem];
    output.IsValid.Should().BeTrue();
}
```

2. **Implement sub-pipeline to pass test**:
```csharp
public class ValidationPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddSource<PipelineInputSource<Customer>, Customer>("input");
        var validate = builder.AddTransform<EmailValidator, Customer, ValidatedCustomer>("validate");
        var output = builder.AddSink<PipelineOutputSink<ValidatedCustomer>, ValidatedCustomer>("output");
        
        builder.Connect(input, validate);
        builder.Connect(validate, output);
    }
}
```

3. **Write failing test for parent pipeline**:
```csharp
[Fact]
public async Task ProcessingPipeline_WithValidData_ShouldProcess()
{
    var context = new PipelineContext();
    await runner.RunAsync<ProcessingPipeline>(context);
    
    // Add assertions
}
```

4. **Implement parent pipeline**:
```csharp
public class ProcessingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<DataSource, Customer>("source");
        var validate = builder.AddComposite<Customer, ValidatedCustomer, ValidationPipeline>("validate");
        var sink = builder.AddSink<DataSink, ValidatedCustomer>("sink");
        
        builder.Connect(source, validate);
        builder.Connect(validate, sink);
    }
}
```

## Best Practices

### 1. Test Pyramid

Structure your tests following the test pyramid:

```
     /\      Integration Tests (few)
    /  \     - Test complete pipeline hierarchy
   /    \    - Test context propagation
  /      \   - End-to-end workflows
 /________\  
 Unit Tests (many)
 - Test each sub-pipeline independently
 - Test individual transforms
 - Test error conditions
```

### 2. Isolate External Dependencies

Mock external dependencies in tests:

```csharp
public class MockApiClient : IApiClient
{
    public Task<ApiResponse> CallAsync(string endpoint) =>
        Task.FromResult(new ApiResponse { Data = "mock data" });
}

[Fact]
public async Task SubPipeline_WithMockApi_ShouldProcess()
{
    var context = new PipelineContext();
    context.Items["ApiClient"] = new MockApiClient();
    
    await runner.RunAsync<ApiCallPipeline>(context);
    
    // Assert
}
```

### 3. Use Descriptive Test Names

```csharp
✅ Good test names:
ValidationPipeline_CustomerWithoutEmail_ShouldReturnInvalidResult
EnrichmentPipeline_GoldCustomer_ShouldHaveHighLoyaltyPoints
ProcessingPipeline_WithCancellation_ShouldStopGracefully

❌ Bad test names:
Test1
TestValidation
TestPipeline
```

### 4. Test One Thing Per Test

```csharp
✅ Good: Tests one specific behavior
[Fact]
public async Task ValidationPipeline_EmptyEmail_ShouldHaveEmailError()
{
    var customer = new Customer(1, "Test", "");
    var context = new PipelineContext();
    context.Parameters[CompositeContextKeys.InputItem] = customer;
    
    await runner.RunAsync<ValidationPipeline>(context);
    
    var output = (ValidatedCustomer)context.Parameters[CompositeContextKeys.OutputItem];
    output.ValidationErrors.Should().Contain("Email is required");
}

❌ Bad: Tests multiple things
[Fact]
public async Task ValidationPipeline_VariousScenarios_ShouldWork()
{
    // Test 1: Empty email
    // Test 2: Empty name
    // Test 3: Invalid phone
    // Too much in one test!
}
```

### 5. Clean Up After Tests

```csharp
public class MyTests : IDisposable
{
    private readonly PipelineRunner _runner;
    
    public MyTests()
    {
        _runner = PipelineRunner.Create();
    }
    
    public void Dispose()
    {
        // Clean up resources
        SpyTransform<int>.Reset();
        MockValidationPipeline.Reset();
    }
}
```

## Summary

| Test Type | Purpose | Frequency |
|-----------|---------|-----------|
| **Unit Tests** | Test sub-pipelines independently | High (many tests) |
| **Integration Tests** | Test pipeline hierarchy | Medium (some tests) |
| **Mock Tests** | Test structure without execution | Medium (as needed) |
| **Error Tests** | Test error handling | High (important) |
| **Context Tests** | Test context inheritance | Medium (per config) |

**Key Principles:**
- Test sub-pipelines independently first
- Use test helpers (CollectorSink, StubSource, SpyTransform)
- Follow the test pyramid
- Isolate external dependencies
- Test error scenarios thoroughly
- Use descriptive test names
- Keep tests focused and clean
