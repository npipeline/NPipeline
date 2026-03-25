using System.Runtime.CompilerServices;
using NPipeline.Observability;

// ObservabilityCollector has been moved to the core NPipeline package.
// This type-forward ensures binary compatibility for existing consumers.
[assembly: TypeForwardedTo(typeof(ObservabilityCollector))]
