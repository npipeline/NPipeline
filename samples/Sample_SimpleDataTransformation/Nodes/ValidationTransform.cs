using System.Text.RegularExpressions;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_SimpleDataTransformation.Nodes;

/// <summary>
///     Transform node that validates Person objects according to business rules.
///     This node demonstrates how to create a validation transform that checks data quality
///     by inheriting directly from TransformNode&lt;Person, Person&gt;.
/// </summary>
public class ValidationTransform : TransformNode<Person, Person>
{
    private static readonly Regex EmailRegex = new(@"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", RegexOptions.Compiled);

    /// <summary>
    ///     Validates the input Person object according to business rules.
    /// </summary>
    /// <param name="item">The input Person object to validate.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task containing the validated Person object.</returns>
    /// <exception cref="ArgumentException">Thrown when validation fails.</exception>
    public override async Task<Person> ExecuteAsync(Person item, PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine($"Validating Person: {item.FirstName} {item.LastName} (ID: {item.Id})");

        var validationErrors = new List<string>();

        // Validate ID
        if (item.Id <= 0)
            validationErrors.Add("ID must be a positive number");

        // Validate names
        if (string.IsNullOrWhiteSpace(item.FirstName))
            validationErrors.Add("First name is required");

        if (string.IsNullOrWhiteSpace(item.LastName))
            validationErrors.Add("Last name is required");

        // Validate age
        if (item.Age < 0 || item.Age > 150)
            validationErrors.Add("Age must be between 0 and 150");

        // Validate email
        if (string.IsNullOrWhiteSpace(item.Email))
            validationErrors.Add("Email is required");
        else if (!EmailRegex.IsMatch(item.Email))
            validationErrors.Add("Email format is invalid");

        // Validate city
        if (string.IsNullOrWhiteSpace(item.City))
            validationErrors.Add("City is required");

        if (validationErrors.Count > 0)
        {
            var errorMessage = $"Validation failed for Person {item.Id}: {string.Join(", ", validationErrors)}";
            Console.WriteLine($"ERROR: {errorMessage}");
            throw new ArgumentException(errorMessage);
        }

        Console.WriteLine($"Validation passed for Person: {item.FirstName} {item.LastName} (ID: {item.Id})");

        return await Task.FromResult(item);
    }
}
