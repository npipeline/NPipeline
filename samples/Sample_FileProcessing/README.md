# Sample 2: File Processing Pipeline

## Overview

This sample demonstrates file-based data processing with NPipeline through a complete file processing workflow:

- **TextFileSource**: Reads text files line by line using streaming
- **LineTransform**: Processes each line with configurable transformations
- **FileSink**: Writes processed data to output files with atomic operations

## Core Concepts

1. **File-Based Source Nodes**
    - Streaming file reading for memory efficiency
    - Line-by-line processing of large files

2. **Stream Processing**
    - Async enumerable patterns for file I/O
    - Proper resource management and disposal

3. **Line-by-Line Transformation**
    - Configurable text transformations
    - Line numbering and case conversion options

4. **Output to New File**
    - Atomic file writing with temporary files
    - Error handling and resource cleanup

## Quick Setup and Run

### Prerequisites

- .NET 8.0, .NET 9.0 or .NET 10.0 SDK
- JetBrains Rider, Visual Studio 2022, VS Code, or .NET CLI

### Running the Sample

```bash
cd samples/Sample_FileProcessing
dotnet restore
dotnet run
```

## Expected Output

### Before (Input File)

```
Welcome to the NPipeline File Processing Sample
This file contains various types of content for processing
123456789
Mixed content with numbers 42 and text
UPPERCASE LINE
lowercase line
```

### After (Output File)

```
PROCESSED: [Line 0001] Welcome to the NPipeline File Processing Sample
PROCESSED: [Line 0002] This file contains various types of content for processing
PROCESSED: [Line 0003] 123456789
PROCESSED: [Line 0004] Mixed content with numbers 42 and text
PROCESSED: [Line 0005] UPPERCASE LINE
PROCESSED: [Line 0006] lowercase line
```

## Key Code Examples

### Pipeline Definition

```csharp
public void Define(PipelineBuilder builder, PipelineContext context)
{
    // Add the source node that reads text files line by line
    var source = builder.AddSource<TextFileSource, string>("text-file-source");

    // Add the transform node that processes each line
    var transform = builder.AddTransform<LineTransform, string, string>("line-transform");

    // Add the sink node that writes processed lines to output file
    var sink = builder.AddSink<FileSink, string>("file-sink");

    // Connect the nodes in a linear flow: source -> transform -> sink
    builder.Connect(source, transform);
    builder.Connect(transform, sink);
}
```

### TextFileSource Node

```csharp
public override async Task<IDataPipe<string>> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
{
    var filePath = context.Parameters.TryGetValue("FilePath", out var contextPath)
        ? contextPath.ToString()
        : _filePath;

    // Create a streaming async enumerable that reads the file line by line
    var lineStream = ReadLinesAsync(filePath, cancellationToken);

    // Return a streaming data pipe that will process lines as they are requested
    return new StreamingDataPipe<string>(lineStream, "TextFileSource");
}
```

### LineTransform Node

```csharp
public LineTransform(string prefix = "PROCESSED: ", bool addLineNumbers = true, bool convertToUpperCase = false)
{
    _prefix = prefix ?? throw new ArgumentNullException(nameof(prefix));
    _addLineNumbers = addLineNumbers;
    _convertToUpperCase = convertToUpperCase;
    _lineNumber = 0;
}
```

### FileSink Node

```csharp
// Use a temporary file for atomic write operation
var tempFilePath = outputFilePath + ".tmp";

// Write to temporary file first, then move to final destination
using (var writer = new StreamWriter(tempFilePath, append: false, System.Text.Encoding.UTF8))
{
    await foreach (var line in input.WithCancellation(cancellationToken))
    {
        await writer.WriteLineAsync(line.AsMemory(), cancellationToken);
        linesWritten++;
    }
}

// Atomically move the temporary file to the final destination
File.Move(tempFilePath, outputFilePath, overwrite: true);
```

## Pipeline Flow

1. **TextFileSource** reads the input text file line by line using streaming
2. **LineTransform** applies configurable transformations to each line:
    - Adds a prefix ("PROCESSED: ")
    - Adds line numbers ([Line 0001], [Line 0002], etc.)
    - Optionally converts to uppercase
3. **FileSink** writes the processed lines to the output file using atomic operations

## Key Features Demonstrated

- Streaming file processing for memory efficiency
- Configurable transformation options through constructor parameters
- File path configuration through pipeline context parameters
- Atomic file writing with temporary files
- Proper resource disposal and error handling
- Type-safe node connections and data flow

This sample builds on the basic pipeline concepts from Sample 1 and demonstrates how to work with file-based data sources and sinks in NPipeline.
