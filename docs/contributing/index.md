---
title: "Contributing"
description: "How to set up the NPipeline development environment, build, test, and submit changes."
order: 13
---

# Contributing

This section is for contributors to the NPipeline source code itself. If you're using NPipeline to build pipelines, see the [Getting Started](../getting-started/installation.md) guide instead.

## Prerequisites

- **.NET SDK 10.0.100** or later (with `rollForward: latestFeature` — see `global.json`)
- **Git**
- **JetBrains Rider**, **Visual Studio 2022**, or **VS Code** with the C# extension

## Getting Started

1. Fork the repository and clone your fork:

```bash
git clone https://github.com/your-username/NPipeline.git
cd NPipeline
git remote add upstream https://github.com/NPipeline/NPipeline.git
```

1. Build and run tests:

```bash
# Mac/Linux
./build.sh

# Windows
.\build.ps1
```

The build script restores packages, builds the solution in Release mode, and runs all tests.

1. Verify the build produces no warnings (warnings are treated as errors).

## Repository Structure

```
NPipeline/
├── src/                          # Source projects (~45 packages)
│   ├── NPipeline/                # Core runtime, nodes, execution, resilience
│   ├── NPipeline.Analyzers/      # Roslyn analyzers
│   ├── NPipeline.Connectors/     # Connector base abstractions
│   ├── NPipeline.Connectors.*/   # Individual connectors (Csv, Kafka, Postgres, etc.)
│   ├── NPipeline.StorageProviders/   # Storage abstraction layer
│   ├── NPipeline.StorageProviders.*/ # Individual providers (S3, Azure, etc.)
│   └── NPipeline.Extensions.*/   # Extension packages (DI, Composition, Testing, etc.)
├── tests/                        # Test projects (~39 projects, mirrors src/)
├── samples/                      # Example projects (~50 samples)
├── benchmarks/                   # Performance benchmarks
├── docs/                         # Documentation (existing)
├── docs-new/                     # Documentation (restructured)
├── build.sh / build.ps1          # Cross-platform build scripts
├── Directory.Build.props         # Shared MSBuild properties
├── Directory.Build.targets       # Shared MSBuild targets
├── global.json                   # SDK version pinning
└── NPipeline.sln                 # Solution file
```

## Build Commands

| Command | Purpose |
|---------|---------|
| `./build.sh` | Release build + tests (Mac/Linux) |
| `./build.sh -c Debug` | Debug build |
| `./build.sh -p` | Build + create NuGet packages |
| `dotnet build` | Build only |
| `dotnet test` | Run all tests |
| `dotnet test --filter "FullyQualifiedName~MyTest"` | Run specific tests |
| `dotnet test --collect:"XPlat Code Coverage"` | Run with coverage |

## Test Framework

- **xUnit 2.9.3** — test framework (`[Fact]`, `[Theory]`)
- **FluentAssertions / AwesomeAssertions** — readable assertions
- **FakeItEasy** — mocking
- **Coverlet** — code coverage

Test naming convention: `MethodName_Should_ExpectedBehavior_When_Condition`

## Multi-Targeting

All packages target **.NET 8, 9, and 10**. Tests run against all target frameworks. Ensure your changes compile and pass on all three.

## Pull Request Process

1. Create a feature branch from `main`.
2. Make your changes with tests.
3. Ensure `./build.sh` passes with no warnings.
4. Submit a PR with a clear title and description.
5. See the root [CONTRIBUTING.md](https://github.com/NPipeline/NPipeline/blob/main/CONTRIBUTING.md) for full PR guidelines.

## In This Section

- [Coding Conventions](coding-conventions.md) — code style, naming, and test expectations

## See Also

- [Advanced Topics](../advanced-topics/index.md) — architecture deep-dives, execution model, and extensibility guides
