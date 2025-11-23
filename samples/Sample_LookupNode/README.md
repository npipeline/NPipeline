# Sample_LookupNode

IoT Sensor Data Enrichment using LookupNode

## Overview

This sample demonstrates how to use NPipeline's LookupNode for enriching IoT sensor data with device metadata.

## Features

- LookupNode implementation for external data enrichment
- IoT sensor data processing pipeline
- Device metadata lookup and validation
- Risk assessment and alerting
- Multi-sink output patterns

## Pipeline Structure

1. **SensorSource** - Generates raw sensor readings
2. **DeviceMetadataLookup** - Enriches data using LookupNode
3. **CalibrationValidationNode** - Validates sensor calibration
4. **RiskAssessmentNode** - Calculates risk levels
5. **EnrichedSink** - Outputs enriched data
6. **AlertingSink** - Handles high-priority alerts

## Running the Sample

```bash
dotnet run --project Sample_LookupNode.csproj
```

## Key Concepts Demonstrated

- LookupNode for efficient external data lookups
- Branching pipeline patterns
- IoT data modeling with records
- Async data processing
- Error handling and validation
- Multi-sink output patterns