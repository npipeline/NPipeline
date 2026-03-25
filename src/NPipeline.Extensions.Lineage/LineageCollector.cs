using System.Runtime.CompilerServices;
using NPipeline.Lineage;

// LineageCollector has been moved to the core NPipeline package.
// This type-forward ensures binary compatibility for existing consumers.
[assembly: TypeForwardedTo(typeof(LineageCollector))]
