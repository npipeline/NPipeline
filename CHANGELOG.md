# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-12-27

### Added

- Initial release of NPipeline core library, a high-performance streaming data pipeline framework for .NET
- Core node abstractions for building data processing workflows:
  - Source nodes for data generation and ingestion
  - Transform nodes for data processing and enrichment
  - Sink nodes for data consumption and output
- Pipeline builder with fluent API for intuitive pipeline definition and configuration
- Multiple execution strategies optimized for different scenarios:
  - Sequential processing for ordered data streams
  - Parallel processing for high-throughput scenarios
  - Batched processing for efficient bulk operations
- Comprehensive error handling and resilience features:
  - Retry strategies with exponential backoff and jitter
  - Circuit breaker pattern to prevent cascading failures
  - Dead-letter queues for handling failed data items
- Roslyn analyzers for compile-time validation and developer guidance:
  - Pipeline configuration validation
  - Type safety checks for node connections
  - Performance optimization recommendations
- Code fix providers for automated code improvements:
  - Anonymous object allocation optimizations
  - Batching configuration mismatch corrections
  - ValueTask optimization suggestions
  - Inefficient string operation fixes
  - Resilient execution configuration helpers
  - Dependency injection pattern improvements
  - Timeout configuration adjustments
  - Pipeline context access safety enhancements
  - Inappropriate parallelism configuration corrections
  - Synchronous over async operation fixes
  - Sink node input consumption corrections
  - OperationCanceledException handling improvements
  - LINQ in hot paths optimizations
  - Inefficient exception handling corrections
  - CancellationToken respect enforcement
  - Source node streaming improvements
  - Unbounded materialization configuration fixes
  - Blocking async operation corrections
- Comprehensive test suite ensuring reliability and correctness:
  - Unit tests covering all major components with high code coverage
  - Integration tests validating end-to-end pipeline scenarios
  - Performance benchmarks for throughput and latency measurement
  - Test utilities and harnesses for simplified pipeline testing
- Extension packages for enhanced functionality:
  - Dependency injection integration with Microsoft.Extensions.DependencyInjection
  - Advanced parallelism support using TPL Dataflow
  - Testing utilities with FluentAssertions and AwesomeAssertions integration
- Data connectors for external system integration:
  - CSV connector for reading and writing CSV files with configurable parsing options
  - Storage provider abstractions for building custom connectors
- Comprehensive documentation with 50+ guides covering:
  - Getting started tutorials and quick start guides
  - Core concepts and architectural patterns
  - Advanced topics including performance optimization
  - Complete API reference and troubleshooting guides
- Performance optimizations for high-throughput scenarios:
  - Streaming architecture using `IAsyncEnumerable<T>` to minimize memory allocations
  - Zero-reflection execution model with pre-compiled delegates
  - Efficient async/await patterns with minimal overhead
- Specialized node types for common data processing patterns:
  - Aggregation nodes for combining multiple data items
  - Batching nodes for processing data in groups
  - Branch nodes for splitting data flows
  - Join nodes for combining data from multiple sources
  - Time-windowed join nodes for temporal data correlation
  - Lookup nodes for data enrichment
  - Tap nodes for non-intrusive monitoring and side-channel processing
  - ValueTask transforms for high-performance scenarios
- Sample applications demonstrating common use cases:
  - Simple data transformation pipelines
  - Complex data processing with multiple sources
  - Advanced error handling patterns
  - Performance optimization techniques
  - Real-time streaming analytics
  - File processing workflows
- Support for .NET 8.0, .NET 9.0, and .NET 10.0 with zero external dependencies in the core library
