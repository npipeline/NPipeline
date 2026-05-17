---
title: "Routing with RouteNode"
description: "Conditionally route items to named downstream outputs using RouteNode, ConnectWhen, and ConnectOtherwise."
order: 6
---

# Routing with RouteNode

> **Prerequisites:** [Defining Pipelines](defining-pipelines.md), [Branching and Merging](branching-and-merging.md)

Use `RouteNode` when you need conditional fan-out. Unlike plain fan-out (`Connect` to multiple targets), a route node evaluates predicates and only forwards each item to matching outputs.

## When to Use RouteNode

Use RouteNode when:

- downstream paths represent distinct business decisions (for example, `high-value`, `international`, `fraud-review`)
- each item should go to one or more specific paths based on conditions
- you want explicit output names that are stable for tooling and visualization

Use [Branching and Merging](branching-and-merging.md) when every item should go to every downstream path.

## Basic Conditional Routing

```csharp
public sealed class OrderRoutingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<OrderSource, Order>("orders");
        var route = builder.AddRoute<Order>("route-orders");

        var highValue = builder.AddSink<HighValueSink, Order>("high-value-sink");
        var international = builder.AddSink<InternationalSink, Order>("international-sink");
        var standard = builder.AddSink<StandardSink, Order>("standard-sink");

        builder.Connect(source, route);

        builder.ConnectWhen(route, highValue, o => o.Amount >= 1000m, "high-value");
        builder.ConnectWhen(route, international, o => o.Country != "US", "international");
        builder.ConnectOtherwise(route, standard, "standard");
    }
}
```

In this configuration:

- items matching `high-value` go to `high-value-sink`
- items matching `international` go to `international-sink`
- items matching neither go to `standard-sink`

## Match Modes

`RouteOptions<T>` supports two match modes:

| Mode | Behavior |
|------|----------|
| `RouteMatchMode.FirstMatch` (default) | Evaluates rules in registration order and routes to the first matching output only |
| `RouteMatchMode.AllMatches` | Routes to every output whose rule matches |

> **Note:** `ConnectOtherwise` fires only when no explicit rule matches, regardless of match mode. In `AllMatches` mode, an item that satisfies at least one `ConnectWhen` predicate will not reach the otherwise output.

Configure it when creating or updating the route:

```csharp
var route = builder.AddRoute<Order>(options =>
{
    options.WithMatchMode(RouteMatchMode.AllMatches);
}, "route-orders");
```

## Unmatched Item Behavior

If no route rule matches and no otherwise output is configured, unmatched items are dropped by default.

You can change this behavior:

```csharp
builder.ConfigureRoute(route, options =>
{
    options.WithNoMatchBehavior(NoRouteMatchBehavior.Throw);
});
```

| Option | Behavior |
|--------|----------|
| `NoRouteMatchBehavior.Drop` (default) | Silently drops unmatched items |
| `NoRouteMatchBehavior.Throw` | Throws to fail fast on unexpected routing gaps |

## Naming Outputs for Tooling

Route output names come from `ConnectWhen`/`ConnectOtherwise` (`sourceOutputName`). Use stable, semantic names (`high-value`, `international`, `standard`) so diagrams and tooling can render route branches consistently.

## Common Patterns

### Priority Routing (first match)

Use `FirstMatch` and declare highest-priority rules first.

### Tag-Based Multicast (all matches)

Use `AllMatches` when an item can legitimately belong to multiple categories.

### Strict Routing Contracts

Use `NoRouteMatchBehavior.Throw` in critical paths to surface missing route coverage early.

## How Route Predicates Work with Lineage

Route predicates configured via `ConnectWhen` and `AddRoute` always operate on the **payload type** `T`. You never write predicates against `LineagePacket<T>` - the framework handles the difference transparently.

When item-level lineage is enabled, stream items are wrapped in `LineagePacket<T>` at runtime. Before execution starts, `RuntimePipelineBinder` normalizes `RouteOptions<T>` to `RouteOptions<LineagePacket<T>>` once at bind time - rewrapping each predicate to delegate to `packet.Data`. This happens once per run, not once per item.

If you build a `RouteOptions<T>` outside the pipeline builder and register it manually via execution annotations, it is automatically normalized during binding as long as the lineage flag is set correctly. A type mismatch that cannot be resolved at bind time is a hard error with diagnostics.

## Sample

See [Sample_RouteNode](../samples/index.md) for a runnable example showing:

- named route outputs
- `AllMatches` routing
- `ConnectOtherwise` fallback routing

## Next Steps

- [Branching and Merging](branching-and-merging.md) - unconditional fan-out and fan-in patterns
- [Joins and Lookups](joins-and-lookups.md) - combine multiple sources after routing
- [Pipeline Validation](pipeline-validation.md) - validate graph structure before execution
- [Execution Model](../advanced-topics/execution-model.md) - how route option normalization fits into the runtime lifecycle
