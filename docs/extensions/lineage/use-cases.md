---
title: Lineage Use Cases
description: Common use cases and practical examples for NPipeline Lineage extension.
---

# Lineage Use Cases

This guide covers common use cases and practical examples for NPipeline Lineage extension.

## Data Governance

Maintain complete audit trails for regulatory compliance:

### Compliance Tracking

Track all data transformations for GDPR, HIPAA, SOX compliance:

```csharp
services.AddNPipelineLineage<DatabaseLineageSink>();

// In pipeline
builder.EnableItemLevelLineage(options =>
{
    options.SampleEvery = 1;  // 100% for compliance
    options.DeterministicSampling = true;  // Consistent tracking
    options.RedactData = false;  // Keep data for audit
    options.MaterializationCap = int.MaxValue;  // Complete records
    options.OverflowPolicy = LineageOverflowPolicy.Materialize;
});
```

**Why This Works:**

- Complete audit trail of all data movements
- Immutable records for legal requirements
- Timestamped transformations for accountability
- Queryable lineage for audits

### Audit Report Generation

Generate compliance reports from lineage data:

```csharp
public sealed class ComplianceReportGenerator
{
    private readonly ILineageCollector _collector;

    public ComplianceReportGenerator(ILineageCollector collector)
    {
        _collector = collector;
    }

    public ComplianceReport GenerateReport(DateTime startDate, DateTime endDate)
    {
        var allLineage = _collector.GetAllLineageInfo();
        
        var report = new ComplianceReport
        {
            PeriodStart = startDate,
            PeriodEnd = endDate,
            TotalItemsProcessed = allLineage.Count,
            TransformationsApplied = allLineage
                .SelectMany(li => li.LineageHops)
                .Count(),
            UniqueNodesInvolved = allLineage
                .SelectMany(li => li.TraversalPath)
                .Distinct()
                .Count()
        };
        
        return report;
    }
}
```

### Sensitive Data Handling

Track sensitive data without storing actual values:

```csharp
builder.EnableItemLevelLineage(options =>
{
    options.RedactData = true;  // Don't store PII
    options.SampleEvery = 1;  // Track all items
});
```

**Benefits:**

- Compliance with data protection regulations
- Audit trail without exposing sensitive data
- Reduced memory footprint
- Maintains transformation history

## Debugging

Quickly identify which node introduced issues:

### Root Cause Analysis

Trace problems back to their source:

```csharp
public sealed class LineageDebugger
{
    private readonly ILineageCollector _collector;
    private readonly ILogger _logger;

    public LineageDebugger(ILineageCollector collector, ILogger<LineageDebugger> logger)
    {
        _collector = collector;
        _logger = logger;
    }

    public void DebugItem(Guid lineageId)
    {
        var lineageInfo = _collector.GetLineageInfo(lineageId);
        
        if (lineageInfo == null)
        {
            _logger.LogWarning("Lineage {LineageId} not found", lineageId);
            return;
        }

        _logger.LogInformation("=== Lineage Debug Report ===");
        _logger.LogInformation("Lineage ID: {LineageId}", lineageInfo.LineageId);
        _logger.LogInformation("Traversal Path: {Path}", 
            string.Join(" → ", lineageInfo.TraversalPath));
        
        foreach (var hop in lineageInfo.LineageHops)
        {
            _logger.LogInformation("Hop {Index}: {NodeId}", 
                lineageInfo.LineageHops.IndexOf(hop), hop.NodeId);
            _logger.LogInformation("  Outcome: {Outcome}", hop.Outcome);
            _logger.LogInformation("  Cardinality: {Cardinality}", hop.Cardinality);
            _logger.LogInformation("  Input Count: {InputCount}", hop.InputContributorCount);
            _logger.LogInformation("  Output Count: {OutputCount}", hop.OutputEmissionCount);
            
            if (hop.Outcome.HasFlag(HopDecisionFlags.Error))
            {
                _logger.LogError("  ⚠️  Issue detected at this hop!");
            }
        }
    }
}
```

### Identify Problematic Nodes

Find nodes with high failure rates:

```csharp
public sealed class NodeHealthAnalyzer
{
    private readonly ILineageCollector _collector;

    public NodeHealthAnalyzer(ILineageCollector collector)
    {
        _collector = collector;
    }

    public Dictionary<string, NodeHealth> AnalyzeNodeHealth()
    {
        var allLineage = _collector.GetAllLineageInfo();
        var nodeHealth = new Dictionary<string, NodeHealth>();
        
        foreach (var lineage in allLineage)
        {
            foreach (var hop in lineage.LineageHops)
            {
                if (!nodeHealth.ContainsKey(hop.NodeId))
                {
                    nodeHealth[hop.NodeId] = new NodeHealth
                    {
                        NodeId = hop.NodeId,
                        TotalHops = 0,
                        SuccessCount = 0,
                        FailureCount = 0
                    };
                }
                
                var health = nodeHealth[hop.NodeId];
                health.TotalHops++;
                
                // Outcome is HopDecisionFlags enum - check for Error flag
                if (hop.Outcome.HasFlag(HopDecisionFlags.Error))
                    health.FailureCount++;
                else if (hop.Outcome.HasFlag(HopDecisionFlags.Emitted))
                    health.SuccessCount++;
            }
        }
        
        // Calculate success rates
        foreach (var health in nodeHealth.Values)
        {
            health.SuccessRate = (double)health.SuccessCount / health.TotalHops;
        }
        
        return nodeHealth;
    }
}

public sealed record NodeHealth(
    string NodeId,
    int TotalHops,
    int SuccessCount,
    int FailureCount,
    double SuccessRate
);
```

### Debug Specific Data Items

Trace exact journey of a problematic item:

```csharp
// After pipeline execution
var collector = serviceProvider.GetRequiredService<ILineageCollector>();

// Find lineage for a specific item
var problematicItemId = Guid.Parse("your-item-id-here");
var lineageInfo = collector.GetLineageInfo(problematicItemId);

if (lineageInfo != null)
{
    Console.WriteLine($"Item entered at: {lineageInfo.TraversalPath[0]}");
    
    foreach (var hop in lineageInfo.LineageHops)
    {
        Console.WriteLine($"  → {hop.NodeId}");
        Console.WriteLine($"    Outcome: {hop.Outcome}");
        Console.WriteLine($"    Cardinality: {hop.Cardinality}");
        Console.WriteLine($"    Input: {hop.InputContributorCount}, Output: {hop.OutputEmissionCount}");
    }
}
```

## Impact Analysis

Understand dependencies before making changes:

### Find Affected Downstream Processes

Identify all items that passed through a specific node:

```csharp
public sealed class ImpactAnalyzer
{
    private readonly ILineageCollector _collector;

    public ImpactAnalyzer(ILineageCollector collector)
    {
        _collector = collector;
    }

    public ImpactReport AnalyzeImpact(string nodeId)
    {
        var allLineage = _collector.GetAllLineageInfo();
        
        var affectedItems = allLineage
            .Where(li => li.TraversalPath.Contains(nodeId))
            .ToList();
        
        var report = new ImpactReport
        {
            NodeId = nodeId,
            AffectedItemCount = affectedItems.Count,
            AffectedLineageIds = affectedItems.Select(li => li.LineageId).ToList(),
            DownstreamNodes = GetDownstreamNodes(nodeId, allLineage)
        };
        
        return report;
    }
    
    private List<string> GetDownstreamNodes(string nodeId, IReadOnlyList<ILineageInfo> allLineage)
    {
        var downstream = new HashSet<string>();
        
        foreach (var lineage in allLineage)
        {
            var nodeIndex = lineage.TraversalPath.IndexOf(nodeId);
            if (nodeIndex >= 0 && nodeIndex < lineage.TraversalPath.Count - 1)
            {
                downstream.Add(lineage.TraversalPath[nodeIndex + 1]);
            }
        }
        
        return downstream.ToList();
    }
}

public sealed record ImpactReport(
    string NodeId,
    int AffectedItemCount,
    List<Guid> AffectedLineageIds,
    List<string> DownstreamNodes
);
```

### Before Change Analysis

Assess impact before modifying a node:

```csharp
public class Program
{
    public static async Task Main(string[] args)
    {
        // Run pipeline with lineage
        await RunPipelineWithLineage();
        
        // Analyze impact of changing "ValidationNode"
        var analyzer = serviceProvider.GetRequiredService<ImpactAnalyzer>();
        var impact = analyzer.AnalyzeImpact("ValidationNode");
        
        Console.WriteLine($"Impact Analysis for {impact.NodeId}:");
        Console.WriteLine($"  Affected Items: {impact.AffectedItemCount}");
        Console.WriteLine($"  Downstream Nodes: {string.Join(", ", impact.DownstreamNodes)}");
        
        if (impact.AffectedItemCount > 10000)
        {
            Console.WriteLine("  ⚠️  High impact - consider careful testing");
        }
    }
}
```

### Dependency Mapping

Build a complete dependency graph:

```csharp
public sealed class DependencyMapper
{
    private readonly ILineageCollector _collector;

    public DependencyMapper(ILineageCollector collector)
    {
        _collector = collector;
    }

    public DependencyGraph BuildDependencyGraph()
    {
        var allLineage = _collector.GetAllLineageInfo();
        var graph = new DependencyGraph();
        
        foreach (var lineage in allLineage)
        {
            for (int i = 0; i < lineage.TraversalPath.Count - 1; i++)
            {
                var source = lineage.TraversalPath[i];
                var target = lineage.TraversalPath[i + 1];
                
                graph.AddEdge(source, target);
            }
        }
        
        return graph;
    }
}

public sealed class DependencyGraph
{
    private readonly Dictionary<string, HashSet<string>> _edges = new();
    
    public void AddEdge(string source, string target)
    {
        if (!_edges.ContainsKey(source))
            _edges[source] = new HashSet<string>();
        
        _edges[source].Add(target);
    }
    
    public HashSet<string> GetDownstream(string node)
    {
        return _edges.GetValueOrDefault(node, new HashSet<string>());
    }
    
    public HashSet<string> GetUpstream(string node)
    {
        var upstream = new HashSet<string>();
        
        foreach (var kvp in _edges)
        {
            if (kvp.Value.Contains(node))
                upstream.Add(kvp.Key);
        }
        
        return upstream;
    }
}
```

## Performance Monitoring

Identify bottlenecks in complex pipelines:

### Find Slow Transformations

Analyze hop durations to find performance issues:

```csharp
public sealed class PerformanceAnalyzer
{
    private readonly ILineageCollector _collector;

    public PerformanceAnalyzer(ILineageCollector collector)
    {
        _collector = collector;
    }

    public List<NodePerformance> AnalyzePerformance()
    {
        var allLineage = _collector.GetAllLineageInfo();
        var nodePerformance = new Dictionary<string, NodePerformance>();
        
        foreach (var lineage in allLineage)
        {
            foreach (var hop in lineage.LineageHops)
            {
                if (!nodePerformance.ContainsKey(hop.NodeId))
                {
                    nodePerformance[hop.NodeId] = new NodePerformance
                    {
                        NodeId = hop.NodeId,
                        TotalHops = 0,
                        TotalDurationMs = 0
                    };
                }
                
                var perf = nodePerformance[hop.NodeId];
                perf.TotalHops++;
                
                // Note: Duration would need to be tracked in LineageHop
                // This is a placeholder for the concept
                perf.TotalDurationMs += 0;  // Would be hop.DurationMs
            }
        }
        
        return nodePerformance.Values.OrderByDescending(p => p.TotalDurationMs).ToList();
    }
}

public sealed record NodePerformance(
    string NodeId,
    int TotalHops,
    double TotalDurationMs,
    double AverageDurationMs => TotalDurationMs / TotalHops
);
```

### Throughput Analysis

Measure processing rates across nodes:

```csharp
public sealed class ThroughputAnalyzer
{
    private readonly ILineageCollector _collector;

    public ThroughputAnalyzer(ILineageCollector collector)
    {
        _collector = collector;
    }

    public List<NodeThroughput> AnalyzeThroughput()
    {
        var allLineage = _collector.GetAllLineageInfo();
        var nodeThroughput = new Dictionary<string, NodeThroughput>();
        
        foreach (var lineage in allLineage)
        {
            foreach (var hop in lineage.LineageHops)
            {
                if (!nodeThroughput.ContainsKey(hop.NodeId))
                {
                    nodeThroughput[hop.NodeId] = new NodeThroughput
                    {
                        NodeId = hop.NodeId,
                        TotalInputCount = 0,
                        TotalOutputCount = 0
                    };
                }
                
                var throughput = nodeThroughput[hop.NodeId];
                throughput.TotalInputCount += hop.InputContributorCount ?? 0;
                throughput.TotalOutputCount += hop.OutputEmissionCount ?? 0;
            }
        }
        
        return nodeThroughput.Values.ToList();
    }
}

public sealed record NodeThroughput(
    string NodeId,
    long TotalInputCount,
    long TotalOutputCount,
    double FilterRatio => TotalInputCount > 0 ? (double)TotalOutputCount / TotalInputCount : 0
);
```

### Cardinality Analysis

Understand data transformation patterns:

```csharp
public sealed class CardinalityAnalyzer
{
    private readonly ILineageCollector _collector;

    public CardinalityAnalyzer(ILineageCollector collector)
    {
        _collector = collector;
    }

    public Dictionary<string, CardinalityStats> AnalyzeCardinality()
    {
        var allLineage = _collector.GetAllLineageInfo();
        var stats = new Dictionary<string, CardinalityStats>();
        
        foreach (var lineage in allLineage)
        {
            foreach (var hop in lineage.LineageHops)
            {
                if (!stats.ContainsKey(hop.NodeId))
                {
                    stats[hop.NodeId] = new CardinalityStats
                    {
                        NodeId = hop.NodeId,
                        TotalHops = 0,
                        CardinalityCounts = new Dictionary<ObservedCardinality, int>()
                    };
                }
                
                var nodeStats = stats[hop.NodeId];
                nodeStats.TotalHops++;
                
                if (!nodeStats.CardinalityCounts.ContainsKey(hop.Cardinality))
                    nodeStats.CardinalityCounts[hop.Cardinality] = 0;
                
                nodeStats.CardinalityCounts[hop.Cardinality]++;
            }
        }
        
        return stats;
    }
}

public sealed record CardinalityStats(
    string NodeId,
    int TotalHops,
    Dictionary<ObservedCardinality, int> CardinalityCounts
);

// ObservedCardinality enum (defined in NPipeline.Lineage namespace)
public enum ObservedCardinality
{
    Unknown = 0,   // Cardinality is not known
    Zero = 1,      // No items were observed
    One = 2,       // Exactly one item was observed
    Many = 3       // More than one item was observed
}
```

## Data Science and Analytics

Support reproducibility and data cataloging:

### Dataset Provenance

Document exactly how datasets were created:

```csharp
public sealed class DatasetProvenanceTracker
{
    private readonly ILineageCollector _collector;

    public DatasetProvenanceTracker(ILineageCollector collector)
    {
        _collector = collector;
    }

    public DatasetProvenance GetProvenance(string datasetName)
    {
        var allLineage = _collector.GetAllLineageInfo();
        
        var provenance = new DatasetProvenance
        {
            DatasetName = datasetName,
            CreationTimestamp = DateTime.UtcNow,
            SourceNodes = allLineage
                .Where(li => li.TraversalPath.Count > 0)
                .Select(li => li.TraversalPath[0])
                .Distinct()
                .ToList(),
            TransformationsApplied = allLineage
                .SelectMany(li => li.LineageHops)
                .Where(h => h.Outcome.HasFlag(HopDecisionFlags.Emitted))
                .GroupBy(h => h.NodeId)
                .Select(g => new TransformationSummary
                {
                    NodeId = g.Key,
                    ApplicationCount = g.Count()
                })
                .ToList(),
            TotalItemsProcessed = allLineage.Count
        };
        
        return provenance;
    }
}

public sealed record DatasetProvenance(
    string DatasetName,
    DateTime CreationTimestamp,
    List<string> SourceNodes,
    List<TransformationSummary> TransformationsApplied,
    int TotalItemsProcessed
);

public sealed record TransformationSummary(
    string NodeId,
    int ApplicationCount
);
```

### Model Training Lineage

Understand the provenance of training data:

```csharp
public sealed class ModelTrainingLineage
{
    private readonly ILineageCollector _collector;

    public ModelTrainingLineage(ILineageCollector collector)
    {
        _collector = collector;
    }

    public ModelTrainingReport GenerateReport(string modelName)
    {
        var allLineage = _collector.GetAllLineageInfo();
        
        var report = new ModelTrainingReport
        {
            ModelName = modelName,
            TrainingDataSources = allLineage
                .Where(li => li.TraversalPath.Count > 0)
                .Select(li => li.TraversalPath[0])
                .Distinct()
                .ToList(),
            DataTransformations = allLineage
                .SelectMany(li => li.LineageHops)
                .GroupBy(h => h.NodeId)
                .Select(g => new
                {
                    NodeId = g.Key,
                    TransformCount = g.Count(),
                    SuccessRate = g.Count(h => h.Outcome.HasFlag(HopDecisionFlags.Emitted)) / (double)g.Count()
                })
                .ToList(),
            SampleSize = allLineage.Count,
            DataQualityMetrics = CalculateQualityMetrics(allLineage)
        };
        
        return report;
    }
    
    private DataQualityMetrics CalculateQualityMetrics(IReadOnlyList<LineageInfo> lineage)
    {
        return new DataQualityMetrics
        {
            SuccessRate = lineage.Count(li => li.LineageHops.All(h => h.Outcome.HasFlag(HopDecisionFlags.Emitted))) / (double)lineage.Count,
            AverageHopCount = lineage.Average(li => li.LineageHops.Count)
        };
    }
}

public sealed record ModelTrainingReport(
    string ModelName,
    List<string> TrainingDataSources,
    List<object> DataTransformations,
    int SampleSize,
    DataQualityMetrics DataQualityMetrics
);

public sealed record DataQualityMetrics(
    double SuccessRate,
    double AverageHopCount
);
```

### Data Cataloging

Build a comprehensive catalog of data sources and transformations:

```csharp
public sealed class DataCatalogBuilder
{
    private readonly ILineageCollector _collector;

    public DataCatalogBuilder(ILineageCollector collector)
    {
        _collector = collector;
    }

    public DataCatalog BuildCatalog()
    {
        var allLineage = _collector.GetAllLineageInfo();
        
        var catalog = new DataCatalog
        {
            DataSources = ExtractDataSources(allLineage),
            Transformations = ExtractTransformations(allLineage),
            DataFlows = ExtractDataFlows(allLineage)
        };
        
        return catalog;
    }
    
    private List<DataSource> ExtractDataSources(IReadOnlyList<LineageInfo> lineage)
    {
        // Sources are identified by being the first node in the traversal path
        return lineage
            .Where(li => li.TraversalPath.Count > 0)
            .GroupBy(li => li.TraversalPath[0])
            .Select(g => new DataSource
            {
                NodeId = g.Key,
                UsageCount = g.Count()
            })
            .ToList();
    }
    
    private List<DataTransformation> ExtractTransformations(IReadOnlyList<LineageInfo> lineage)
    {
        return lineage
            .SelectMany(li => li.LineageHops)
            .Where(h => h.Outcome.HasFlag(HopDecisionFlags.Emitted))
            .GroupBy(h => h.NodeId)
            .Select(g => new DataTransformation
            {
                NodeId = g.Key,
                ApplicationCount = g.Count(),
                CardinalityDistribution = g.GroupBy(h => h.Cardinality)
                    .Select(cg => new { Cardinality = cg.Key, Count = cg.Count() })
                    .ToList()
            })
            .ToList();
    }
    
    private List<DataFlow> ExtractDataFlows(IReadOnlyList<LineageInfo> lineage)
    {
        return lineage
            .Select(li => new DataFlow
            {
                LineageId = li.LineageId,
                FlowPath = li.TraversalPath,
                HopCount = li.LineageHops.Count
            })
            .ToList();
    }
}

public sealed record DataCatalog(
    List<DataSource> DataSources,
    List<DataTransformation> Transformations,
    List<DataFlow> DataFlows
);

public sealed record DataSource(
    string NodeId,
    int UsageCount
);

public sealed record DataTransformation(
    string NodeId,
    int ApplicationCount,
    List<object> CardinalityDistribution
);

public sealed record DataFlow(
    Guid LineageId,
    IReadOnlyList<string> FlowPath,
    int HopCount
);
```

## Complete Examples

### Example 1: ETL Pipeline with Lineage

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Lineage;
using NPipeline.Lineage.DependencyInjection;

public class EtlPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<DatabaseSource, RawData>("source");
        var validate = builder.AddTransform<ValidationTransform, RawData, ValidatedData>("validate");
        var transform = builder.AddTransform<DataTransform, ValidatedData, ProcessedData>("transform");
        var sink = builder.AddSink<DataWarehouseSink, ProcessedData>("sink");

        builder.Connect(source, validate);
        builder.Connect(validate, transform);
        builder.Connect(transform, sink);
    }
}

public class Program
{
    public static async Task Main(string[] args)
    {
        var services = new ServiceCollection();
        
        // Add lineage with database sink for compliance
        services.AddNPipelineLineage<DatabaseLineageSink>();
        services.AddNPipeline(typeof(Program).Assembly);
        
        var serviceProvider = services.BuildServiceProvider();
        
        var builder = new PipelineBuilder("EtlPipeline");
        
        // Enable lineage for compliance
        builder.EnableItemLevelLineage(options =>
        {
            options.SampleEvery = 1;  // 100% for compliance
            options.DeterministicSampling = true;
            options.RedactData = false;  // Keep data for audit
        });
        
        var pipeline = new EtlPipeline();
        pipeline.Define(builder, new PipelineContext());
        
        await serviceProvider.RunPipelineAsync(builder.Build());
    }
}
```

### Example 2: Debugging with Lineage

```csharp
public class Program
{
    public static async Task Main(string[] args)
    {
        var services = new ServiceCollection();
        
        services.AddNPipelineLineage();  // Use logging sink
        services.AddNPipeline(typeof(Program).Assembly);
        
        var serviceProvider = services.BuildServiceProvider();
        
        var builder = new PipelineBuilder("DebugPipeline");
        
        // Enable lineage for debugging
        builder.EnableItemLevelLineage(options =>
        {
            options.SampleEvery = 1;  // Track everything
            options.RedactData = false;  // Keep data for inspection
        });
        
        var pipeline = new DebugPipeline();
        pipeline.Define(builder, new PipelineContext());
        
        // Run pipeline
        await serviceProvider.RunPipelineAsync(builder.Build());
        
        // Analyze lineage for debugging
        var collector = serviceProvider.GetRequiredService<ILineageCollector>();
        var debugger = new LineageDebugger(collector, logger);
        
        // Debug a specific problematic item
        var problematicItemId = Guid.Parse("your-item-id");
        debugger.DebugItem(problematicItemId);
    }
}
```

## Best Practices

### 1. Use Appropriate Sampling

Choose sampling rate based on use case:

| Use Case | Sampling Rate | Reasoning |
|-----------|----------------|-------------|
| Compliance | 100% | Complete audit trail required |
| Debugging | 100% | Need complete visibility |
| Monitoring | 1-10% | Representative samples sufficient |
| Analytics | 0.1-1% | Minimal overhead needed |

### 2. Enable Redaction for Sensitive Data

Always redact PII, financial data, or health records:

```csharp
options.RedactData = true;
```

### 3. Use Deterministic Sampling for Reproducibility

When you need consistent behavior:

```csharp
options.DeterministicSampling = true;
```

### 4. Implement Custom Sinks for Production

Use database or external system sinks:

```csharp
services.AddNPipelineLineage<DatabaseLineageSink>();
```

### 5. Analyze Lineage Regularly

Build tools to analyze lineage data:

```csharp
var analyzer = new ImpactAnalyzer(collector);
var impact = analyzer.AnalyzeImpact("ValidationNode");
```

## Related Topics

- **[Getting Started](./getting-started.md)** - Installation and basic setup
- **[Configuration](./configuration.md)** - Configuration options and settings
- **[Architecture](./architecture.md)** - Internal architecture and design decisions
- **[Performance](./performance.md)** - Performance characteristics and optimization
