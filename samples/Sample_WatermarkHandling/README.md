# Sample_WatermarkHandling

Advanced Event-Time Processing for IoT Manufacturing Platform using WatermarkHandling

## Overview

This sample demonstrates sophisticated watermark handling capabilities in NPipeline for complex IoT manufacturing scenarios. It showcases how to manage
event-time processing, handle late data, and synchronize multiple sensor networks with different characteristics in a real-time manufacturing environment.

## Scenario: IoT Manufacturing Platform

In modern smart manufacturing facilities, multiple IoT sensor networks monitor different aspects of production:

- **Production Line A**: Temperature/Pressure/Vibration sensors (WiFi, high bandwidth, low latency)
- **Production Line B**: Flow Meters/Quality Cameras/Power Monitors (LoRaWAN, low bandwidth, high latency)
- **Environmental**: Humidity/Air Quality sensors (Ethernet, reliable, medium latency)

Each network has different timing characteristics, clock synchronization methods, and latency patterns, making watermark management crucial for accurate
temporal processing.

## Key Concepts Demonstrated

- **Advanced Watermark Generation**: Adaptive watermark strategies based on network conditions
- **Multi-Stream Synchronization**: Coordinating watermarks across heterogeneous sensor networks
- **Late Data Handling**: Configurable tolerance policies for out-of-order data
- **Event-Time Processing**: Proper temporal windowing with watermark-based advancement
- **Network-Aware Processing**: Adapting processing strategies to network characteristics
- **Dynamic Adjustment**: Real-time watermark adjustment based on system load and conditions

## Running the Sample

```bash
dotnet run --project Sample_WatermarkHandling.csproj
```

## Pipeline Architecture

The pipeline implements a sophisticated IoT manufacturing data processing flow:

```
Production Line A Source ──┐
                           ├── AdaptiveWatermarkGenerator ── WatermarkAligner ── LateDataFilter ── TimeWindowedAggregator ── MonitoringSink
Production Line B Source ──┤
                           │
Environmental Source ───────┘
```

### Components

1. **IoT Sensor Sources**:
    - `ProductionLineASource`: WiFi-based sensors with GPS-disciplined clocks (±1ms)
    - `ProductionLineBSource`: LoRaWAN sensors with NTP synchronization (±10ms)
    - `EnvironmentalSource`: Ethernet sensors with internal clocks and drift compensation

2. **Watermark Management**:
    - `AdaptiveWatermarkGenerator`: Dynamic watermark generation based on network conditions
    - `WatermarkAligner`: Multi-stream watermark synchronization and alignment
    - `NetworkAwareWatermarkStrategy`: Network-specific watermark generation strategies
    - `DeviceSpecificLatenessStrategy`: Per-device lateness tolerance configuration
    - `DynamicAdjustmentStrategy`: Real-time watermark adjustment based on system load

3. **Data Processing**:
    - `LateDataFilter`: Configurable late data handling with tolerance policies
    - `TimeWindowedAggregator`: Time-windowed processing with watermark-based window advancement
    - `MonitoringSink`: Output sink with comprehensive metrics and alerting

## Data Models

### SensorReading

Represents sensor data with device ID, timestamp, value, and quality indicators:

- DeviceId: Unique identifier for the sensor device
- Timestamp: Event time when the reading was captured
- Value: Numerical sensor reading value
- Unit: Unit of measurement (e.g., "°C", "kPa", "%")
- ReadingType: Type of sensor (Temperature, Pressure, Humidity, etc.)
- QualityIndicators: Data quality metrics and flags

### DeviceMetadata

Device information including network type, clock accuracy, and latency characteristics:

- DeviceId: Unique device identifier
- DeviceType: Type of sensor device
- NetworkType: WiFi, LoRaWAN, Ethernet, etc.
- ClockAccuracy: Clock synchronization precision (±1ms, ±10ms, etc.)
- LatencyCharacteristics: Expected latency patterns and variance
- Location: Physical location in the manufacturing facility
- LastCalibration: Device calibration timestamp

### WatermarkMetrics

Watermark progress tracking and performance metrics:

- CurrentWatermark: Current watermark timestamp
- ProcessingDelay: Delay between event time and processing time
- LateDataCount: Count of late data events
- WatermarkAdvanceRate: Rate of watermark advancement
- SystemLoad: Current system load indicators
- NetworkCondition: Current network health status

### LateDataRecord

Records of late data events for analysis:

- OriginalTimestamp: Original event timestamp
- ArrivalTimestamp: When the late data arrived
- LatenessDuration: How late the data was
- DeviceId: Source device identifier
- HandlingAction: What action was taken (accept, reject, side-output)
- Reason: Why the data was considered late

### ProcessingStats

Overall pipeline statistics and health indicators:

- TotalEventsProcessed: Total count of processed events
- AverageProcessingLatency: Average end-to-end processing latency
- WatermarkAccuracy: Watermark precision metrics
- SystemThroughput: Events processed per second
- ErrorRate: Error and exception rates
- ResourceUtilization: CPU, memory, and network usage

## Watermark Strategies

### Network-Aware Watermark Strategy

Adapts watermark generation based on network conditions:

- WiFi networks: Aggressive watermarks with low latency tolerance
- LoRaWAN networks: Conservative watermarks with high latency tolerance
- Ethernet networks: Balanced watermarks with medium latency tolerance
- Dynamic adjustment based on network health and congestion

### Device-Specific Lateness Strategy

Per-device lateness tolerance configuration:

- GPS-disciplined devices: Strict lateness tolerance (±100ms)
- NTP-synchronized devices: Moderate lateness tolerance (±500ms)
- Internal clock devices: Relaxed lateness tolerance (±2000ms)
- Adaptive tolerance based on historical performance

### Dynamic Adjustment Strategy

Real-time watermark adjustment based on system load:

- Load-based watermark advancement speed
- Backpressure-aware watermark generation
- Resource-constrained processing modes
- Automatic strategy switching based on conditions

## Implementation Details

### Watermark Generation

- Event-time based watermark generation
- Network condition monitoring and adaptation
- Clock drift compensation and synchronization
- Latency prediction and tolerance management
- Multi-stream watermark alignment and coordination

### Late Data Handling

- Configurable lateness tolerance windows
- Multiple handling strategies (accept, reject, side-output)
- Late data analytics and reporting
- Quality degradation handling
- Graceful degradation under high lateness

### Time Windowed Processing

- Watermark-based window advancement
- Event-time windowing with proper handling
- Late data window updates and corrections
- Window result computation and emission
- Performance optimization for high-throughput scenarios

## Performance Considerations

- High-throughput processing for thousands of sensors
- Low-latency watermark generation and propagation
- Efficient memory usage for large time windows
- Scalable multi-stream synchronization
- Adaptive processing based on system conditions
- Comprehensive monitoring and alerting

## Key Takeaways

1. **Adaptive Watermarks**: Watermark generation must adapt to network characteristics and conditions
2. **Multi-Stream Coordination**: Synchronizing watermarks across heterogeneous sensor networks is crucial
3. **Late Data Management**: Proper handling of late data ensures data quality and completeness
4. **Event-Time Processing**: Accurate temporal processing requires sophisticated watermark management
5. **Dynamic Adjustment**: Real-time adaptation to changing conditions improves system reliability

## Extending the Sample

This sample can be extended to demonstrate:

- Additional sensor types and network protocols
- More sophisticated watermark generation algorithms
- Machine learning-based latency prediction
- Real-time alerting and anomaly detection
- Historical data analysis and reporting
- Integration with external monitoring systems

## Dependencies

- NPipeline core library
- NPipeline.Extensions.DependencyInjection
- Microsoft.Extensions.Hosting
- Microsoft.Extensions.Logging

## .NET Versions

This sample supports:

- .NET 8.0
- .NET 9.0
- .NET 10.0
