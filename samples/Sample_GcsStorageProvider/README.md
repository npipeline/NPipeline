# GCS Storage Provider Sample

This sample demonstrates how to use Google Cloud Storage with NPipeline, showing a complete pipeline that reads documents from GCS, processes them, and writes the results back to GCS.

## Overview

The sample implements a document processing pipeline:

```
GcsDocumentSource → TextTransform → GcsDocumentSink
```

1. **GcsDocumentSource**: Reads text documents from a GCS bucket
2. **TextTransform**: Processes each document (converts to uppercase, adds timestamp)
3. **GcsDocumentSink**: Writes processed documents back to GCS

## Running with Docker (Recommended)

The easiest way to run this sample is using the GCS emulator via Docker.

### Prerequisites

- Docker Desktop installed and running
- .NET 10.0 SDK

### Quick Start

1. **Start the GCS emulator:**

   ```bash
   cd samples/Sample_GcsStorageProvider
   docker-compose up -d
   ```

   This starts `fake-gcs-server` on port 4443.

2. **Wait for the emulator to be ready:**

   ```bash
   curl http://localhost:4443/storage/v1/b
   ```

   You should see a JSON response with bucket information.

3. **Run the sample:**

   ```bash
   dotnet run
   ```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `NP_GCS_BUCKET` | GCS bucket name | `sample-bucket` |
| `NP_GCS_PROJECT_ID` | Google Cloud project ID | `test-project` |
| `NP_GCS_SERVICE_URL` | GCS service URL (for emulator) | (not set - uses real GCS) |

When using the emulator, the sample automatically:

- Seeds initial test data in the `input/` folder
- Runs the pipeline
- Lists the processed files in the `output/` folder

### Docker Commands

```bash
# Start the emulator
docker-compose up -d

# Check emulator status
docker-compose ps

# View emulator logs
docker-compose logs -f

# Stop the emulator
docker-compose down

# Stop and remove data
docker-compose down -v
```

## Running with Real Google Cloud Storage

To run against real GCS:

1. **Set up Google Cloud credentials:**

   ```bash
   # Option 1: Application Default Credentials
   gcloud auth application-default login

   # Option 2: Service account key
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
   ```

2. **Create a GCS bucket:**

   ```bash
   gsutil mb gs://your-bucket-name
   ```

3. **Upload test data:**

   ```bash
   echo "Hello from GCS" | gsutil cp - gs://your-bucket-name/input/document-1.txt
   ```

4. **Run the sample:**

   ```bash
   export NP_GCS_BUCKET=your-bucket-name
   export NP_GCS_PROJECT_ID=your-project-id
   # Don't set NP_GCS_SERVICE_URL to use real GCS
   
   dotnet run
   ```

## Project Structure

```
Sample_GcsStorageProvider/
├── Program.cs              # Entry point and pipeline execution
├── GcsPipeline.cs          # Pipeline definition
├── docker-compose.yml      # Docker Compose for GCS emulator
├── README.md               # This file
└── Nodes/
    ├── GcsDocumentSource.cs  # Source node - reads from GCS
    ├── TextTransform.cs       # Transform node - processes text
    └── GcsDocumentSink.cs    # Sink node - writes to GCS
```

## Key Concepts

### GcsStorageProvider

The `GcsStorageProvider` implements `IStorageProvider` and handles:

- Reading objects via `OpenReadAsync()`
- Writing objects via `OpenWriteAsync()`
- Listing objects via `ListAsync()`
- Metadata retrieval via `GetMetadataAsync()`

### Node Dependencies

Nodes that require the storage provider accept it via constructor injection:

```csharp
public class GcsDocumentSource : SourceNode<string>
{
    private readonly IStorageProvider _storageProvider;
    private readonly string _bucket;
    
    public GcsDocumentSource(
        IStorageProvider storageProvider,
        string bucket,
        string prefix = "input/")
    {
        _storageProvider = storageProvider;
        _bucket = bucket;
    }
    
    // ...
}
```

### Emulator Compatibility

The GCS provider automatically detects the emulator and uses appropriate credentials:

- With emulator: Uses a dummy access token
- With real GCS: Uses Application Default Credentials

## Troubleshooting

### Emulator Connection Refused

```bash
# Check if Docker is running
docker info

# Check if the container is healthy
docker-compose ps

# Restart the container
docker-compose restart
```

### Authentication Errors with Real GCS

```bash
# Verify credentials are set
echo $GOOGLE_APPLICATION_CREDENTIALS

# Or check ADC
gcloud auth application-default print-access-token
```

### Bucket Not Found

The sample creates buckets automatically when using the emulator. For real GCS, ensure the bucket exists:

```bash
gsutil ls gs://your-bucket-name
```

## Learn More

- [GCS Storage Provider Documentation](../../docs/storage-providers/gcs-storage-provider.md)
- [NPipeline Documentation](../../README.md)
- [fake-gcs-server GitHub](https://github.com/fsouza/fake-gcs-server)
