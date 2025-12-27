# Sample 1: Basic Pipeline

## Overview

This sample demonstrates the fundamental concepts of NPipeline through a simple "Hello World" pipeline with three core components:

- A **source** that generates data
- A **transform** that processes the data
- A **sink** that outputs the processed data

## Core Concepts

1. **Source, Transform, and Sink Nodes**
    - Data flows from source through transforms to sinks
    - Type-safe connections between nodes

2. **Pipeline Definition and Execution**
    - IPipelineDefinition implementation pattern
    - PipelineBuilder for constructing pipelines
    - Dependency injection integration

## Quick Setup and Run

### Prerequisites

- .NET 8.0, .NET 9.0 or .NET 10.0 SDK
- JetBrains Rider, Visual Studio 2022, VS Code, or .NET CLI

### Running the Sample

```bash
cd samples/Sample_BasicPipeline
dotnet restore
dotnet run
```

## Expected Output

```
=== NPipeline Basic Sample: Hello World Pipeline ===

Dependency injection configured successfully.
Starting pipeline execution...

Starting source node execution: HelloWorldSource
Generated 10 Hello World messages

Processing message #1: HELLO WORLD
[Pipeline Output] HELLO WORLD
Processing message #2: HELLO NPIPELINE
[Pipeline Output] HELLO NPIPELINE
...
ConsoleSink processed 10 messages

Pipeline execution completed successfully!
```

