# Sample_RouteNode

This sample demonstrates conditional routing with NPipeline's Route node.

## Overview

The pipeline routes order events to named outputs using `ConnectWhen` and `ConnectOtherwise`:

- `high-value`: orders where `Amount >= 1000`
- `international`: orders where `Country != "US"`
- `standard` (otherwise): all remaining orders

The sample configures `RouteMatchMode.AllMatches`, so a single order can be delivered to multiple routes when multiple conditions are true.

## Pipeline Flow

`OrderSource -> RouteNode -> (high-value, international, standard)`

## Running the Sample

```bash
cd samples/Sample_RouteNode
dotnet restore
dotnet run
```

## What to Observe

- Orders can appear in more than one sink when multiple route predicates match.
- Unmatched orders are delivered to the `otherwise` route.
- Routing is based on named route outputs, not downstream subscription order.
