---
title: "Coding Conventions"
description: "Language settings, style rules, analyzer diagnostics, and test patterns."
order: 10
---

# Coding Conventions

This page documents the enforced coding standards across the NPipeline codebase. All rules are checked at build time — `TreatWarningsAsErrors` means violations fail the build.

## Language and Build Settings

Defined in `Directory.Build.props`:

| Setting | Value |
|---------|-------|
| `LangVersion` | `12.0` |
| `TreatWarningsAsErrors` | `true` |
| `Nullable` | `enable` |
| `ImplicitUsings` | `enable` |
| `GenerateDocumentationFile` | `true` |
| `EnableNETAnalyzers` | `true` |
| `AnalysisLevel` | `latest` |
| `TargetFrameworks` | `net8.0;net9.0;net10.0` |

## Style Rules

From `.editorconfig`:

- **`var` preferred** when the type is apparent or built-in (`csharp_style_var_*: true`)
- **System directives first** in using blocks (`dotnet_sort_system_directives_first: true`)
- **No separated import groups** (`dotnet_separate_import_directive_groups: false`)
- **StyleCop ordering** enforced: `SA1201` (element order), `SA1202` (access modifier order), `SA1203` (constants first), `SA1204` (static before instance)
- **UTF-8** encoding, **CRLF** line endings, trailing whitespace trimmed, final newline inserted

## XML Documentation

All public APIs require XML documentation (`GenerateDocumentationFile=true`). Include at minimum:

```csharp
/// <summary>Transforms a raw order into an enriched order.</summary>
/// <param name="item">The raw order to transform.</param>
/// <param name="context">The pipeline execution context.</param>
/// <param name="cancellationToken">Cancellation token.</param>
/// <returns>The enriched order.</returns>
/// <exception cref="InvalidOperationException">Thrown when the order ID is missing.</exception>
```

Connector and test projects suppress `CS1591` (missing XML docs) via `WarningsNotAsErrors`.

## Custom Analyzers

NPipeline ships 20+ Roslyn analyzers in `NPipeline.Analyzers`. They run during every build.

### Configuration & Setup

| ID | Severity | Rule |
|----|----------|------|
| NP9001 | Warning | `RestartNode` requires `ResilientExecutionStrategy`, `MaxNodeRestartAttempts > 0`, and `MaxMaterializedItems` to be set |
| NP9002 | Error | `MaxMaterializedItems` must not be null — prevents unbounded memory growth |
| NP9003 | Warning | Inappropriate parallelism configuration (too high for CPU-bound, too low for I/O) |
| NP9004 | Warning | Batch size / timeout mismatch (large batch + short timeout or vice versa) |
| NP9005 | Warning | Inappropriate timeout values (zero, negative, too short for I/O, too long for CPU) |

### Performance & Optimization

| ID | Severity | Rule |
|----|----------|------|
| NP9101 | Warning | Blocking calls in async methods (`.Result`, `.Wait()`, `Thread.Sleep()`) |
| NP9102 | Warning | Sync-over-async anti-patterns |
| NP9103 | Warning | LINQ allocations in hot-path methods (`TransformAsync`, `ConsumeAsync`, `OpenStream`) |
| NP9104 | Warning | String concatenation with `+` in loops |
| NP9105 | Warning | Anonymous object allocations in hot paths |
| NP9106 | Info | `TransformNode` uses `Task.FromResult` but could override `ExecuteValueTaskAsync` for zero-allocation |
| NP9107 | Warning | `SourceNode.OpenStream` materializes data into `List`/`Array` instead of streaming |
| NP9108 | Info | Node could benefit from a parameterless constructor for compiled-expression instantiation |

### Reliability & Error Handling

| ID | Severity | Rule |
|----|----------|------|
| NP9201 | Warning | Catch block swallows `OperationCanceledException` without re-throwing |
| NP9202 | Warning | Inefficient exception handling in hot paths |
| NP9203 | Warning | `CancellationToken` parameter not forwarded to async calls or checked in loops |

### Data Integrity & Correctness

| ID | Severity | Rule |
|----|----------|------|
| NP9301 | Error | `SinkNode.ConsumeAsync` does not consume the `input` parameter |
| NP9302 | Warning | Unsafe access on nullable `PipelineContext` properties |

### Design & Architecture

| ID | Severity | Rule |
|----|----------|------|
| NP9401 | Info | `TransformAsync` returns `IAsyncEnumerable` — consider `IStreamTransformNode` instead |
| NP9402 | Warning | `IStreamTransformNode` paired with a non-stream execution strategy |
| NP9403 | Warning | Node missing public parameterless constructor (requires DI or pre-configured instance) |
| NP9404 | Warning | DI anti-patterns in nodes (service locator, static singleton access) |

## Test Conventions

### Framework and Libraries

- **Framework:** xUnit
- **Assertions:** AwesomeAssertions (`.Should().Be(...)`, `.Should().HaveCount(...)`)
- **Mocking:** FakeItEasy (`A.Fake<T>()`, `A.CallTo(...)`)
- **Coverage:** Coverlet

### Naming

Test methods follow `MethodName_Condition_ExpectedBehavior`:

```csharp
[Fact]
public async Task TransformAsync_NullInput_ThrowsArgumentNullException()

[Fact]
public async Task OpenStream_ValidConfig_ReturnsExpectedItems()

[Fact]
public async Task ConsumeAsync_CancellationRequested_StopsProcessing()
```

### Structure

- Test project mirrors source: `tests/NPipeline.Connectors.Json.Tests/` for `src/NPipeline.Connectors.Json/`
- One test class per production class: `JsonSourceNodeTests.cs` tests `JsonSourceNode.cs`
- Integration tests in separate files: `{Feature}IntegrationTests.cs`
- Shared helpers in `tests/NPipeline.Tests.Common/` (e.g., `InMemoryDataStream`, `TransformNodeTestExtensions`)
- Test projects set `<IsPackable>false</IsPackable>`
- Test projects suppress `CA1873`, `CA1848`, `CA2253`, `CA2254`, `CA1727` (logging-related warnings)

### Pattern

```csharp
public sealed class MyNodeTests
{
    [Fact]
    public async Task TransformAsync_ValidInput_ReturnsExpected()
    {
        // Arrange
        var node = new MyNode();
        var context = new PipelineContext();

        // Act
        var result = await node.TransformAsync("input", context, CancellationToken.None);

        // Assert
        result.Should().Be("expected");
    }
}
```

## Next Steps

- [Adding a Node Type](../advanced-topics/adding-a-node-type.md) — apply these conventions in practice
- [Adding a Connector](../advanced-topics/adding-a-connector.md) — packaging conventions
- [Contributor Guide](index.md) — build commands and PR process
