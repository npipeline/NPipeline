---
title: "Error Codes"
description: "Complete catalog of NPipeline error codes with descriptions and resolution guidance."
order: 4
---

# Error Codes

NPipeline error codes follow the pattern `NPxxxx` where the first two digits indicate the category.

## NP01xx - Core Pipeline Errors

| Code | Name | Description |
|------|------|-------------|
| NP0101 | PipelineRequiresAtLeastOneNode | A pipeline must have at least one node. Add at least one source, transform, or sink. |
| NP0102 | NodeMissingInputConnection | A non-source node has no input connection. Connect an upstream node to it. |
| NP0103 | CyclicDependencyDetected | The pipeline graph contains a cycle. NPipeline requires a directed acyclic graph (DAG). The message names the nodes that could not be ordered and traces one cycle through them. |
| NP0104 | NodeAlreadyAdded | A node with this ID has already been added to the builder. Use a unique ID. |
| NP0105 | NodeNameNotUnique | A node with this name already exists. Choose a different name. |

## NP02xx - Type System Errors

| Code | Name | Description |
|------|------|-------------|
| NP0201 | TypeMismatchInConnection | Output type of the source node doesn't match input type of the target node. |
| NP0202 | InputDataStreamWrongType | The input data stream is not of the expected type. Check node type parameters. |
| NP0203 | CannotRegisterMappingsAfterExecution | Mapping registrations must happen before pipeline execution begins. |
| NP0204 | RecordTypeHasNoPublicConstructor | The target record type has no public constructors. Add a public constructor. |
| NP0205 | InvalidMemberAccessExpression | The member selector expression is invalid. Use a simple property accessor. |
| NP0206 | MemberNotWritable | The target member is read-only. Use a writable property or constructor parameter. |
| NP0207 | SetterCreationFailed | Failed to create a setter for the specified member via reflection. |
| NP0208 | ValueTupleConstructorNotFound | Could not find the expected ValueTuple constructor. |
| NP0210 | CannotConcatenateStreamsTypeMismatch | Cannot concatenate data streams of different types. |
| NP0211 | NodeOutputTypeMismatch | A node produced items whose type does not match its declared output type, and no conversion applied. |
| NP0212 | OutputAdaptationUnavailable | A node's output needs converting to its declared type but no converter exists for that node kind. |
| NP0213 | InputStreamContractMismatch | A node received input items of a type it cannot consume. Check the connection's source node. |
| NP0214 | RouteNodeMissingOutputType | A route node has no output item type. Add it with `PipelineBuilder.AddRoute<T>()`, or set `OutputType` on a custom route definition. |

## NP03xx - Node Execution Errors

| Code | Name | Description |
|------|------|-------------|
| NP0301 | NodeKindNotSupported | The node kind is not supported or its execution delegate was not bound. |
| NP0302 | OutputNotFoundForSourceNode | Could not locate the output data stream for a source node. |
| NP0303 | PipelineExecutionFailedAtNode | Execution failed at a specific node. Check the `NodeId` in the exception. |
| NP0304 | PipelineExecutionFailed | The pipeline execution failed. Check the inner exception for details. |
| NP0305 | ItemFailedAfterMaxRetries | An item failed to process after all retry attempts were exhausted. |
| NP0306 | ErrorHandlingFailed | The error handler itself threw an exception. |
| NP0307 | LineageCardinalityMismatch | The number of lineage inputs doesn't match the number of outputs. Internal framework error. |
| NP0308 | FailedToExtractItemsFromInMemoryDataStream | Could not extract items from an InMemoryDataStream. Internal framework error. |
| NP0310 | *(retired)* | No longer used. It belonged to `CircuitBreakerTrippedException`, which was never thrown and has been removed. An open breaker raises `CircuitBreakerOpenException`. |
| NP0311 | RetryLimitExhausted | All retry attempts were exhausted. |

## NP04xx - Configuration and Setup Errors

| Code | Name | Description |
|------|------|-------------|
| NP0401 | ExecutionStrategyCannotBeSetForNonTransformNode | Execution strategies can only be applied to transform nodes. |
| NP0402 | NodeNotFoundInBuilder | The referenced node was not found in the pipeline builder. |
| NP0403 | *Retired* | Belonged to `WithResilience(NodeHandle)`, which node restart options replaced. Not reused. |
| NP0404 | InvalidErrorHandlerType | The error handler type doesn't implement the required interface. |
| NP0405 | PreConfiguredInstanceAlreadyAdded | A pre-configured instance for this node has already been registered. |
| NP0406 | PreConfiguredInstanceNodeNotFound | The node for the pre-configured instance was not found in the builder. |
| NP0407 | MergeStrategyNotSupported | The specified merge strategy is not supported. |
| NP0408 | NodeActivationFailed | Could not instantiate the node type. Check constructors and DI registrations. |
| NP0411 | JoinNodeRequiresTwoKeySelectorAttributes | Join nodes require exactly two `[KeySelector]` attribute declarations. |
| NP0412 | UnbatchingNodeNotSupported | UnbatchingNode cannot be executed directly; use the unbatching execution strategy. |
| NP0413 | BatchingNodeNotSupported | BatchingNode doesn't support per-item transformation; use the batching execution strategy. |
| NP0414 | CustomMergeNodeMissingInterface | The custom merge node is missing the required interface implementation. |
| NP0415 | UnbatchingExecutionStrategyMissingDeadLetterHandler | The unbatching strategy requires a dead letter handler when resilience is enabled. |
| NP0416 | LineageAdapterMissing | Internal: lineage adapter not configured. |
| NP0417 | SourceNodeLineageUnwrapMissing | Internal: source node lineage unwrap delegate not configured. |
| NP0418 | SinkNodeLineageUnwrapMissing | Internal: sink node lineage unwrap delegate not configured. |
| NP0419 | MissingTypeMetadata | Internal: node is missing type metadata. |
| NP0420 | TimeWindowAssignerCannotBeNull | The time window assigner cannot be null for time-windowed operations. |
| NP0421 | StreamTransformNodeRequiresStreamStrategy | A stream transform node was configured with a strategy that cannot execute streams. |
| NP0422 | NodeCannotSupplyExecutionStrategy | A node scheduled as a transform does not implement the transform interface. |
| NP0424 | DeadLetterSinkNotConfigured | An item was dead-lettered, but the pipeline has no dead-letter sink. A transform with `OnItemFailure = ItemFailureAction.DeadLetter` raises it before any node starts; a custom resilience policy that returns `DeadLetter` raises it when the item fails. Add a sink with `AddDeadLetterSink`, or use `Skip` or `Fail`. |
| NP0425 | NodeRestartRequiresResumableStrategy | A transform has `NodeRestart.MaxRestarts` above zero, but its execution strategy does not implement `IResumableExecutionStrategy`. Use a resumable strategy, or set `MaxRestarts` to 0 for that node. |

## NP05xx - Resource Management Errors

| Code | Name | Description |
|------|------|-------------|
| NP0501 | ContextDisposalFailed | One or more errors occurred while disposing pipeline context resources. |
| NP0502 | DeadLetterQueueCapacityExceeded | The dead letter queue has reached its capacity limit. |
| NP0503 | *Retired* | Belonged to the node restart materialization buffer, which the replay window replaced. `NodeRestart.MaxReplayWindow` applies backpressure instead of failing. Not reused. |
| NP0504 | BatchSizeMustBeGreaterThanZero | Batch size must be a positive integer. |

## Next Steps

- [Build-Time Analyzers](../analyzers/index.md) - build-time diagnostic rules
- [Common Issues](../troubleshooting/common-issues.md) - symptom-based troubleshooting
