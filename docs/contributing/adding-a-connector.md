---
title: "Adding a Connector"
description: "How to create, package, and test a new NPipeline connector."
order: 3
---

# Adding a Connector

A connector is a NuGet package that provides source and/or sink nodes for a specific data system (file format, database, message queue). This guide covers the project structure, required interfaces, and packaging conventions.

## Project Structure

Follow the naming convention `NPipeline.Connectors.{Name}`. Here's the layout based on existing connectors:

```
src/NPipeline.Connectors.{Name}/
├── {Name}Configuration.cs         # Configuration record
├── {Name}SourceNode.cs            # SourceNode<T> implementation
├── {Name}SinkNode.cs              # SinkNode<T> implementation (if applicable)
├── {Name}Row.cs                   # Row/record abstraction (if applicable)
├── Mapping/                       # Optional: mapping/serialization helpers
├── NPipeline.Connectors.{Name}.csproj
└── README.md                      # Package-level README (included in NuGet)

tests/NPipeline.Connectors.{Name}.Tests/
├── {Name}SourceNodeTests.cs
├── {Name}SinkNodeTests.cs
├── {Name}ConfigurationTests.cs
├── {Name}IntegrationTests.cs
├── TestData/                      # Sample files for testing
└── NPipeline.Connectors.{Name}.Tests.csproj
```

## Step 1: Create the Project

Create a `.csproj` with multi-targeting and standard NuGet metadata:

```xml
<Project Sdk="Microsoft.NET.Sdk">
    <PropertyGroup>
        <TargetFrameworks>net8.0;net9.0;net10.0</TargetFrameworks>
        <ImplicitUsings>enable</ImplicitUsings>
        <Nullable>enable</Nullable>
        <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
        <GenerateDocumentationFile>true</GenerateDocumentationFile>

        <PackageId>NPipeline.Connectors.{Name}</PackageId>
        <Description>{Name} source and sink nodes for NPipeline...</Description>
        <PackageTags>npipeline;{name};connectors;data-format</PackageTags>
        <Authors>NPipeline contributors</Authors>
        <PackageLicenseExpression>MIT</PackageLicenseExpression>
        <IncludeSymbols>true</IncludeSymbols>
        <SymbolPackageFormat>snupkg</SymbolPackageFormat>
        <PackageIcon>icon.png</PackageIcon>
        <PackageReadmeFile>README.md</PackageReadmeFile>
    </PropertyGroup>

    <PropertyGroup>
        <WarningsNotAsErrors>$(WarningsNotAsErrors);CS1591</WarningsNotAsErrors>
    </PropertyGroup>

    <ItemGroup>
        <ProjectReference Include="..\NPipeline.Connectors\NPipeline.Connectors.csproj"/>
        <ProjectReference Include="..\NPipeline.StorageProviders\NPipeline.StorageProviders.csproj"/>
    </ItemGroup>

    <ItemGroup>
        <None Include="..\..\icon.png" Pack="true" PackagePath="\"/>
        <None Include="README.md" Pack="true" PackagePath="\"/>
    </ItemGroup>
</Project>
```

Key references:

- **`NPipeline.Connectors`** — base abstractions shared by all connectors
- **`NPipeline.StorageProviders`** — storage provider interfaces for file-based connectors (optional for database/queue connectors)

## Step 2: Define Configuration

Create a configuration class with sensible defaults:

```csharp
namespace NPipeline.Connectors.{Name};

/// <summary>Configuration for the {Name} connector.</summary>
public sealed class {Name}Configuration
{
    /// <summary>Connection string or file path.</summary>
    public required string ConnectionString { get; init; }

    /// <summary>Batch size for bulk operations. Default: 1000.</summary>
    public int BatchSize { get; init; } = 1000;
}
```

## Step 3: Implement the Source Node

Extend `SourceNode<T>` and return a streaming `IDataStream<T>`:

```csharp
namespace NPipeline.Connectors.{Name};

public sealed class {Name}SourceNode(
    {Name}Configuration configuration) : SourceNode<{Name}Row>
{
    public override IDataStream<{Name}Row> OpenStream(
        PipelineContext context, CancellationToken cancellationToken)
    {
        return new DataStream<{Name}Row>(
            ReadAsync(cancellationToken), $"{Name}-source");
    }

    private async IAsyncEnumerable<{Name}Row> ReadAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        // Stream items lazily — never materialize the full dataset
        // Forward ct to all async operations
    }
}
```

> **Tip:** The `SourceNodeStreamingAnalyzer` (NP9107) warns if `OpenStream` materializes data into a `List` or array instead of streaming.

## Step 4: Implement the Sink Node

Extend `SinkNode<T>`:

```csharp
namespace NPipeline.Connectors.{Name};

public sealed class {Name}SinkNode(
    {Name}Configuration configuration) : SinkNode<{Name}Row>
{
    public override async Task ConsumeAsync(
        IDataStream<{Name}Row> input, PipelineContext context,
        CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            // Write items — forward cancellationToken to all I/O
        }
    }
}
```

> **Tip:** The `SinkNodeInputConsumptionAnalyzer` (NP9301) errors if `ConsumeAsync` ignores the `input` parameter.

## Step 5: Add to the Solution

1. Add the project to `NPipeline.sln`.
2. Add the test project to the solution.
3. Reference the connector project from the test project.
4. Add any sample project under `samples/Sample_{Name}Connector/`.

## Step 6: Write Tests

Follow existing test conventions:

```csharp
public sealed class {Name}SourceNodeTests
{
    [Fact]
    public async Task OpenStream_ValidConfig_ReturnsExpectedItems()
    {
        // Arrange
        var config = new {Name}Configuration { ConnectionString = "..." };
        var node = new {Name}SourceNode(config);
        var context = new PipelineContext();

        // Act
        var stream = node.OpenStream(context, CancellationToken.None);
        var items = await stream.ToListAsync();

        // Assert
        items.Should().HaveCount(3);
    }

    [Fact]
    public async Task OpenStream_CancellationRequested_StopsStreaming()
    {
        // Test that cancellation is respected
    }
}
```

Use `TestData/` for sample files and `FakeItEasy` for mocking external dependencies.

## Packaging Checklist

- [ ] Multi-targets `net8.0;net9.0;net10.0`
- [ ] References `NPipeline.Connectors` (and `NPipeline.StorageProviders` if file-based)
- [ ] `TreatWarningsAsErrors=true` with `CS1591` relaxed
- [ ] Package includes `icon.png` and `README.md`
- [ ] `IncludeSymbols=true` with `snupkg` format
- [ ] XML documentation on all public types and members
- [ ] Unit tests for source, sink, and configuration
- [ ] Integration tests with realistic data
- [ ] All analyzers pass (run `dotnet build` with no warnings)

## Next Steps

- [Adding a Node Type](adding-a-node-type.md) — node implementation fundamentals
- [Coding Conventions](coding-conventions.md) — style and analyzer rules to follow
- [Contributor Guide](index.md) — build commands and PR process
