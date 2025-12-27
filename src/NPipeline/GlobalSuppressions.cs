// GlobalSuppressions - Exceptional Architectural Cases
// This file contains ONLY truly exceptional suppressions that cannot be handled by .editorconfig
// and require specific architectural justification. All other suppressions have been moved
// to .editorconfig or should be addressed through proper code fixes.

using System.Diagnostics.CodeAnalysis;

// ============================================================================
// ARCHITECTURE & API DESIGN - Intentional design choices
// ============================================================================

// CA1034: Nested types should not be visible
// Justification: Nested fluent builder types (e.g., PipelineBuilder.NodeOptions) are 
// intentionally part of the public API surface to enable fluent chaining patterns.
// Moving these types outside would break the fluent API design and reduce discoverability.
[assembly:
    SuppressMessage("Design", "CA1034:Nested types should not be visible",
        Justification = "Nested fluent builder types are intentionally part of the public API surface to enable fluent chaining patterns.")]

// CA1711: Identifiers should not have incorrect suffix
// Justification: Delegate suffix usage (e.g., ErrorHandlerDelegate, NodeFactoryDelegate)
// is intentional for API clarity. These suffixes immediately communicate the purpose
// and usage pattern of the delegates, which is more valuable than strict naming conventions.
[assembly:
    SuppressMessage("Design", "CA1711:Identifiers should not have incorrect suffix",
        Justification =
            "Delegate suffix usage provides immediate clarity about purpose and usage patterns, which is more valuable than strict naming conventions.")]

// CA1040: Avoid empty interfaces
// Justification: INodeErrorHandler is a marker interface that enables discovery via reflection
// and dependency injection filtering. While it has no members, it serves a critical architectural
// purpose in the error handling subsystem by providing a type-safe contract for error handlers.
[assembly:
    SuppressMessage("Design", "CA1040:Avoid empty interfaces",
        Justification =
            "INodeErrorHandler is a marker interface that enables discovery via reflection and dependency injection filtering, serving a critical architectural purpose despite having no members.")]

// ============================================================================
// ERROR HANDLING & VALIDATION - Pipeline orchestration requirements
// ============================================================================

// CA1031: Do not catch general exception types
// Justification: The PipelineRunner must catch ALL exceptions to implement retry policies,
// circuit breakers, and dead-letter queue functionality. This is a fundamental requirement
// for a reliable pipeline orchestration system. The exception is properly logged and
// handled according to configured policies.
[assembly:
    SuppressMessage("Design", "CA1031:Do not catch general exception types",
        Justification =
            "PipelineRunner must catch all exceptions to implement retry policies, circuit breakers, and dead-letter queue functionality - a fundamental requirement for reliable pipeline orchestration.")]

// CA1062: Validate arguments of public methods
// Justification: Validation is handled through centralized guard methods at pipeline boundaries.
// This approach provides consistent validation behavior, better error messages, and allows
// for validation policies to be configured per pipeline. Individual parameter validation
// throughout the codebase would create duplication and inconsistency.
[assembly:
    SuppressMessage("Design", "CA1062:Validate arguments of public methods",
        Justification =
            "Validation is centralized through guard methods at pipeline boundaries for consistency, better error messages, and configurable validation policies. Individual validation would create duplication.")]

// ============================================================================
// COMPLEXITY & MAINTAINABILITY - Inherent to pipeline orchestration
// ============================================================================

// CA1502: Avoid excessive complexity
// Justification: The complexity in PipelineOrchestrator and NodeExecutor classes reflects
// the necessary branching logic for pipeline execution, error handling, retry policies,
// and resource management. Attempting to reduce this complexity through extraction would
// actually harm readability by scattering related logic across many small methods.
// The complexity is localized and well-documented.
[assembly:
    SuppressMessage("Maintainability", "CA1502:Avoid excessive complexity",
        Justification =
            "Complexity in PipelineOrchestrator and NodeExecutor reflects necessary branching logic for execution, error handling, and retry policies. Reducing complexity would scatter related logic and harm readability.")]

// CA1506: Avoid excessive class coupling
// Justification: High coupling is inherent to the pipeline node registration model where
// nodes must interact with PipelineContext, INodeFactory, and various infrastructure services.
// This coupling is intentional and provides the extensibility that is central to the pipeline
// architecture. The coupling points are well-defined through abstractions.
[assembly:
    SuppressMessage("Maintainability", "CA1506:Avoid excessive class coupling",
        Justification =
            "High coupling is inherent to the pipeline node registration model where nodes must interact with infrastructure services. This intentional coupling provides the extensibility central to the pipeline architecture.")]
