---
title: "Node restart and the replay window"
description: "How a transform node restarts from its checkpoint, what the replay window holds, and what a restart guarantees."
order: 7
---

# Node restart and the replay window

NPipeline streams items one at a time through `IAsyncEnumerable<T>`. Most inputs are forward-only: once an item is
read, the input can't give it back. Node restart (L2) recovers a transform whose output stream fails mid-stream
without reading its input again from the start. It resumes the node at its *checkpoint* and processes again only the
items whose outcome wasn't delivered.

## Enable node restart

To make a transform restartable, set `NodeRestart.MaxRestarts` above zero, for the pipeline or for one node:

```csharp
builder.WithResilience(transform, options => options with
{
    NodeRestart = new NodeRestartOptions
    {
        MaxRestarts = 3,
        Backoff = RetryBackoff.Exponential(TimeSpan.FromSeconds(1), maxDelay: TimeSpan.FromSeconds(30)),
    },
});
```

When you build the pipeline, the builder wraps each transform whose options allow restarts in the restart strategy.
There's no separate call to make. A transform with `MaxRestarts = 0` isn't wrapped and pays nothing.

The node's execution strategy must be able to resume, which means it implements `IResumableExecutionStrategy`.
`SequentialExecutionStrategy` and the Parallelism extension's strategies do. If a transform has restarts configured
and a strategy that can't resume, the build fails with `NP0425`. Restarting such a node from the beginning would
deliver its items twice, so NPipeline doesn't fall back to that.

Node restart applies to transform nodes (`ITransformNode`). Stream transforms, sources, sinks, and aggregates don't
restart. For more information about recovering those, see [Error handling](index.md).

## The checkpoint

An item's *outcome* is delivered when its output has been handed to the next node, or when the item was skipped or
dead-lettered and so has no output. The checkpoint is the first input item whose outcome hasn't been delivered.

When the node's stream fails, the resilience policy's `DecideRestartAsync` receives a `StreamFailure` that includes
`Checkpoint`, the input index a restart resumes from, and `Delivered`, the outputs delivered so far. If the policy
answers `RestartNode`, the node waits for `NodeRestart.Backoff` and then runs again from the checkpoint. Items before
the checkpoint are neither read nor processed again.

A failure of the input itself, such as an upstream source losing its connection, isn't restarted. The input can't be
read again, so the failure propagates.

## The replay window

Between the checkpoint and the last item read, the node holds the items it has read but whose outcome it hasn't
delivered, so that a restart can process them again. `NodeRestart.MaxReplayWindow` (default 10,000) bounds how many it
holds. When the window is full, the node stops reading its input until the checkpoint advances.

The window is backpressure, never an error, and it doesn't limit the length of the input. A restartable node streams
an input of any length, including one that never ends, and memory stays proportional to the items in flight rather
than to the input.

For a sequential node, the window holds at most the item in progress. For a parallel node, it holds the items in
flight. In unordered mode, a failed item holds the checkpoint back until the restart, so the window also bounds how
many outputs a restart can deliver again.

## What a restart guarantees

The following table shows how outputs are delivered across a restart, by inner strategy:

| Inner strategy | Delivery across a restart |
| --- | --- |
| Sequential | Each output exactly once. A skipped or dead-lettered item isn't skipped or dead-lettered again. |
| Parallel, `PreserveOrdering = true` (the default) | Each output exactly once. An item dead-lettered while in flight can be dead-lettered again. |
| Parallel, `PreserveOrdering = false`, and the drop strategies | At least once. Outputs delivered ahead of the checkpoint are delivered again, at most `MaxReplayWindow` for each restart. |
| Any | Work inside `TransformAsync` for items in flight at the failure is done again. A restart re-runs work; it can't undo it. |

Delivery means handing an output to the next node. A restart never retracts an output.

## Long-running streams

By default, restarts count over the whole life of the stream, so a stream that runs for days with rare faults
eventually uses them up. To give a stream its restarts back once it has run cleanly for a while, set
`ResetAfterItems`:

```csharp
NodeRestart = new NodeRestartOptions
{
    MaxRestarts = 3,
    ResetAfterItems = 10_000,   // After 10,000 outputs without a failure, the restart count starts again.
}
```

## Cancellation

Cancelling the pipeline throws `OperationCanceledException` from the node's stream. Cancellation never uses a restart
and never ends the stream as if it had completed.

## Next steps

- [Resilience policies](resilience-policies.md): decide restarts in code with `DecideRestartAsync`.
- [Retry strategies](retry-strategies.md): choose the backoff between restarts.
- [Circuit breakers](circuit-breakers.md): stop calling a dependency that keeps failing.
