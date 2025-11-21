# Sample 07: Streaming Analytics Pipeline

This sample demonstrates advanced stream processing concepts with NPipeline, including windowed processing, real-time aggregations, and late-arriving data
handling.

## Key Features

- **Tumbling Windows**: Non-overlapping 5-second time windows
- **Sliding Windows**: 5-second windows sliding every 2 seconds
- **Late-Arriving Data**: Handling of out-of-order data with configurable lateness tolerance
- **Real-Time Aggregations**: Statistical analysis including trend detection and anomaly detection
- **Performance Optimization**: Efficient streaming processing with minimal memory overhead

## Pipeline Architecture

```
TimeSeriesSource
├── TumblingWindowTransform → AggregationTransform → ConsoleSink
└── SlidingWindowTransform → AggregationTransform → ConsoleSink
```

### Components

1. **TimeSeriesSource**: Generates realistic sensor data with multiple sources and daily cycles
2. **TumblingWindowTransform**: Processes data in fixed 5-second non-overlapping windows
3. **SlidingWindowTransform**: Processes data in 5-second windows sliding every 2 seconds
4. **AggregationTransform**: Enriches results with trend analysis and anomaly detection
5. **ConsoleSink**: Outputs detailed windowed results with statistics

## Running the Sample

```bash
cd samples/Sample_07_StreamingAnalytics
dotnet run
```

The pipeline runs for approximately 30 seconds, generating and processing time-series data in real-time.

## Key Concepts Demonstrated

- **Windowed Processing**: Both tumbling (non-overlapping) and sliding (overlapping) windows
- **Late Data Handling**: Configurable lateness tolerance with proper data routing
- **Real-Time Analytics**: Statistical calculations, trend analysis, and anomaly detection
- **Performance Optimization**: Efficient memory management for high-throughput scenarios
- **Branching Pipelines**: Multiple processing paths from a single data source

## Configuration

- Window size: 5 seconds
- Slide interval: 2 seconds (for sliding windows)
- Allowed lateness: 10 seconds
- Data sources: 4 simulated sensors (Sensor-A, Sensor-B, Sensor-C, Sensor-D)
- Late data probability: 10%
