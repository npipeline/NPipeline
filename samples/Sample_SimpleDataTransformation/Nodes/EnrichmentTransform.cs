using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_SimpleDataTransformation.Nodes;

/// <summary>
///     Transform node that enriches Person objects with additional data.
///     This node demonstrates how to create a transform that adds value to data
///     by inheriting directly from TransformNode&lt;Person, EnrichedPerson&gt;.
/// </summary>
public class EnrichmentTransform : TransformNode<Person, EnrichedPerson>
{
    /// <summary>
    ///     Processes the input Person object and returns an EnrichedPerson with additional data.
    /// </summary>
    /// <param name="item">The Person object to enrich.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task representing the enrichment operation with an EnrichedPerson result.</returns>
    public override Task<EnrichedPerson> ExecuteAsync(Person item, PipelineContext context, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Skip placeholder items (those with ID 0) by returning an empty EnrichedPerson
        if (item.Id == 0)
            return Task.FromResult(new EnrichedPerson(0, string.Empty, string.Empty, 0, string.Empty, string.Empty, string.Empty, string.Empty, false));

        // Simulate external data source for country lookup
        var country = GetCountryFromCity(item.City);

        // Determine age category
        var ageCategory = GetAgeCategory(item.Age);

        // Create enriched person
        var enrichedPerson = new EnrichedPerson(
            item.Id,
            item.FirstName,
            item.LastName,
            item.Age,
            item.City,
            item.Email,
            country,
            ageCategory,
            IsValidEmail(item.Email)
        );

        return Task.FromResult(enrichedPerson);
    }

    /// <summary>
    ///     Determines the country based on the city.
    ///     In a real application, this might call an external API or database.
    /// </summary>
    /// <param name="city">The city name.</param>
    /// <returns>The country name.</returns>
    private static string GetCountryFromCity(string city)
    {
        return city.ToLowerInvariant() switch
        {
            "new york" or "los angeles" or "chicago" => "USA",
            "london" => "UK",
            "paris" => "France",
            "tokyo" => "Japan",
            "sydney" => "Australia",
            "toronto" => "Canada",
            "berlin" => "Germany",
            "madrid" => "Spain",
            "rome" => "Italy",
            "moscow" => "Russia",
            "beijing" => "China",
            "mumbai" => "India",
            "são paulo" => "Brazil",
            "mexico city" => "Mexico",
            "cairo" => "Egypt",
            "lagos" => "Nigeria",
            "istanbul" => "Turkey",
            "singapore" => "Singapore",
            "dubai" => "UAE",
            "hong kong" => "China",
            "seoul" => "South Korea",
            _ => "Unknown",
        };
    }

    /// <summary>
    ///     Determines the age category based on the age.
    /// </summary>
    /// <param name="age">The age value.</param>
    /// <returns>The age category.</returns>
    private static string GetAgeCategory(int age)
    {
        return age switch
        {
            < 18 => "Minor",
            < 30 => "Young Adult",
            < 50 => "Adult",
            < 65 => "Middle Aged",
            _ => "Senior",
        };
    }

    /// <summary>
    ///     Validates if an email address is in a valid format.
    /// </summary>
    /// <param name="email">The email address to validate.</param>
    /// <returns>True if the email is valid, false otherwise.</returns>
    private static bool IsValidEmail(string email)
    {
        if (string.IsNullOrWhiteSpace(email))
            return false;

        // Simple email validation - in a real app, use a more sophisticated validation
        var parts = email.Split('@');

        if (parts.Length != 2)
            return false;

        return parts[0].Length > 0 && parts[1].Contains('.');
    }
}
