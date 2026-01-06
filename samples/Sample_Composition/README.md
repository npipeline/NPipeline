# NPipeline.Extensions.Composition Sample

This sample demonstrates how to use the NPipeline.Extensions.Composition extension to create modular, hierarchical pipelines.

## Overview

The sample shows:

- **Basic Composition**: Using composite nodes to execute sub-pipelines
- **Context Inheritance**: Controlling what data is inherited from parent pipeline
- **Nested Composition**: Composite nodes within composite nodes
- **Type Safety**: Full compile-time type checking throughout the pipeline hierarchy

## Running the Sample

```bash
cd samples/Sample_Composition
dotnet run
```

## Sample Structure

### Models

- `Customer`: Input customer record
- `ValidatedCustomer`: Validated customer record with validation results
- `EnrichedCustomer`: Enriched customer record with loyalty information

### Sub-Pipelines

- `ValidationPipeline`: Validates customer data
    - Checks for required fields (name, email)
    - Validates email format
    - Returns validation results

- `EnrichmentPipeline`: Enriches validated customer data
    - Determines loyalty tier based on customer ID
    - Calculates loyalty points
    - Adds enrichment timestamp

### Main Pipeline

- `CompositionPipeline`: Main pipeline demonstrating composition
    - Uses `ValidationPipeline` as a composite node
    - Uses `EnrichmentPipeline` as a composite node with context inheritance
    - Processes customers through validation and enrichment
    - Outputs results to console

### Transform Nodes

- `CustomerValidator`: Validates customer data
- `CustomerEnricher`: Enriches validated customer data

### Source/Sink Nodes

- `CustomerSource`: Generates sample customer data
- `ConsoleSink<T>`: Outputs processed data to console

## Key Concepts Demonstrated

### 1. Basic Composition

```csharp
var validate = builder.AddComposite<Customer, ValidatedCustomer, ValidationPipeline>(
    name: "validation",
    contextConfiguration: CompositeContextConfiguration.Default);
```

### 2. Context Inheritance

```csharp
var enrich = builder.AddComposite<ValidatedCustomer, EnrichedCustomer, EnrichmentPipeline>(
    name: "enrichment",
    contextConfiguration: CompositeContextConfiguration.InheritAll);
```

### 3. Sub-Pipeline Definition

```csharp
public class ValidationPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddSource<PipelineInputSource<Customer>, Customer>("input");
        var validate = builder.AddTransform<CustomerValidator, Customer, ValidatedCustomer>("validate");
        var output = builder.AddSink<PipelineOutputSink<ValidatedCustomer>, ValidatedCustomer>("output");

        builder.Connect(input, validate);
        builder.Connect(validate, output);
    }
}
```

## Expected Output

```text
=== NPipeline.Extensions.Composition Sample ===

Running composition pipeline...

Processed: EnrichedCustomer { ValidatedCustomer = ValidatedCustomer { OriginalCustomer = Customer { Id = 1, Name = John Doe, Email = john.doe@example.com, Phone = 555-1234 }, IsValid = True, ValidationErrors = [] }, EnrichmentTimestamp = 2026-01-06T01:00:00.0000000Z, LoyaltyTier = Bronze, LoyaltyPoints = 10 }

Processed: EnrichedCustomer { ValidatedCustomer = ValidatedCustomer { OriginalCustomer = Customer { Id = 50, Name = Jane Smith, Email = jane.smith@example.com, Phone =  }, IsValid = True, ValidationErrors = [] }, EnrichmentTimestamp = 2026-01-06T01:00:00.0000000Z, LoyaltyTier = Gold, LoyaltyPoints = 500 }

Processed: EnrichedCustomer { ValidatedCustomer = ValidatedCustomer { OriginalCustomer = Customer { Id = 200, Name = Bob Johnson, Email = bob.johnson@example.com, Phone = 555-5678 }, IsValid = True, ValidationErrors = [] }, EnrichmentTimestamp = 2026-01-06T01:00:00.0000000Z, LoyaltyTier = Silver, LoyaltyPoints = 2000 }

Processed: EnrichedCustomer { ValidatedCustomer = ValidatedCustomer { OriginalCustomer = Customer { Id = 1000, Name = Alice Williams, Email = alice.williams@example.com, Phone =  }, IsValid = True, ValidationErrors = [] }, EnrichmentTimestamp = 2026-01-06T01:00:00.0000000Z, LoyaltyTier = Bronze, LoyaltyPoints = 10000 }

Processed: EnrichedCustomer { ValidatedCustomer = ValidatedCustomer { OriginalCustomer = Customer { Id = 5, Name = Charlie Brown, Email = charlie.brown@example.com, Phone = 555-9012 }, IsValid = True, ValidationErrors = [] }, EnrichmentTimestamp = 2026-01-06T01:00:00.0000000Z, LoyaltyTier = Bronze, LoyaltyPoints = 50 }

Pipeline execution completed successfully!
```

## Learning Points

1. **Modular Design**: Each sub-pipeline has a single, well-defined responsibility
2. **Type Safety**: Compile-time type checking ensures data flows correctly through the pipeline hierarchy
3. **Context Control**: Use `CompositeContextConfiguration` to control what data is inherited
4. **Reuse**: Sub-pipelines can be reused across multiple parent pipelines
5. **Isolation**: Sub-pipelines have isolated contexts, preventing unintended side effects

## Extending the Sample

Try modifying the sample to:

1. Add more validation rules in `CustomerValidator`
2. Create additional sub-pipelines for different processing stages
3. Experiment with different context inheritance configurations
4. Add error handling to see how errors propagate through composite nodes
5. Create nested composition by adding composite nodes within sub-pipelines
