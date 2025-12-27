using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_SimpleDataTransformation.Nodes;

/// <summary>
///     Transform node that filters Person objects based on conditions.
///     This node demonstrates how to create a filtering transform that applies business rules
///     by inheriting directly from TransformNode&lt;Person, Person&gt;.
/// </summary>
public class FilteringTransform : TransformNode<Person, Person>
{
    /// <summary>
    ///     Filters the input Person object based on business conditions.
    /// </summary>
    /// <param name="item">The input Person object to filter.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task containing the filtered Person object.</returns>
    public override async Task<Person> ExecuteAsync(Person item, PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine($"Filtering Person: {item.FirstName} {item.LastName} (Age: {item.Age}, City: {item.City})");

        // Filter condition 1: Age must be 18 or older
        if (item.Age < 18)
        {
            var message = $"Filtered out {item.FirstName} {item.LastName}: Under 18 years old (Age: {item.Age})";
            Console.WriteLine($"FILTER: {message}");

            // Return a placeholder Person with ID 0 to indicate filtered out
            var filteredPerson = new Person(0, "Filtered", "Out", 0, "filtered@none.com", "Filtered");
            return await Task.FromResult(filteredPerson);
        }

        // Filter condition 2: Exclude certain cities (demonstrating geographic filtering)
        var excludedCities = new[] { "Chicago", "Philadelphia" };

        if (excludedCities.Contains(item.City, StringComparer.OrdinalIgnoreCase))
        {
            var message = $"Filtered out {item.FirstName} {item.LastName}: City excluded ({item.City})";
            Console.WriteLine($"FILTER: {message}");

            // Return a placeholder Person with ID 0 to indicate filtered out
            var filteredPerson = new Person(0, "Filtered", "Out", 0, "filtered@none.com", "Filtered");
            return await Task.FromResult(filteredPerson);
        }

        // Filter condition 3: Maximum age limit for this specific pipeline
        if (item.Age > 50)
        {
            var message = $"Filtered out {item.FirstName} {item.LastName}: Age exceeds limit (Age: {item.Age})";
            Console.WriteLine($"FILTER: {message}");

            // Return a placeholder Person with ID 0 to indicate filtered out
            var filteredPerson = new Person(0, "Filtered", "Out", 0, "filtered@none.com", "Filtered");
            return await Task.FromResult(filteredPerson);
        }

        Console.WriteLine($"Person passed filtering: {item.FirstName} {item.LastName} (Age: {item.Age}, City: {item.City})");

        return await Task.FromResult(item);
    }
}
