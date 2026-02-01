using System.Globalization;
using NPipeline.Connectors.Csv.Attributes;

namespace Sample_CsvConnector;

/// <summary>
///     Represents a customer record from the CSV file.
///     Uses CsvColumn attributes for explicit column mapping.
/// </summary>
public class Customer
{
    /// <summary>
    ///     Gets or sets the customer ID.
    /// </summary>
    [CsvColumn("Id")]
    public int Id { get; set; }

    /// <summary>
    ///     Gets or sets the customer's first name.
    /// </summary>
    [CsvColumn("FirstName")]
    public string FirstName { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the customer's last name.
    /// </summary>
    [CsvColumn("LastName")]
    public string LastName { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the customer's email address.
    /// </summary>
    [CsvColumn("Email")]
    public string Email { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the customer's age.
    /// </summary>
    [CsvColumn("Age")]
    public int Age { get; set; }

    /// <summary>
    ///     Gets or sets the registration date.
    /// </summary>
    [CsvColumn("RegistrationDate")]
    public DateTime RegistrationDate { get; set; }

    /// <summary>
    ///     Gets or sets the customer's country.
    /// </summary>
    [CsvColumn("Country")]
    public string Country { get; set; } = string.Empty;

    /// <summary>
    ///     Gets the full name of the customer.
    ///     This computed property is excluded from CSV mapping.
    /// </summary>
    [CsvIgnore]
    public string FullName => $"{FirstName} {LastName}";

    /// <summary>
    ///     Gets a value indicating whether the customer is an adult (18+).
    ///     This computed property is excluded from CSV mapping.
    /// </summary>
    [CsvIgnore]
    public bool IsAdult => Age >= 18;

    /// <summary>
    ///     Gets the age category of the customer.
    ///     This computed property is excluded from CSV mapping.
    /// </summary>
    [CsvIgnore]
    public string AgeCategory => Age switch
    {
        < 18 => "Minor",
        < 30 => "Young Adult",
        < 50 => "Adult",
        < 65 => "Middle Aged",
        _ => "Senior",
    };

    /// <summary>
    ///     Gets the normalized country name.
    ///     This computed property is excluded from CSV mapping.
    /// </summary>
    [CsvIgnore]
    public string NormalizedCountry => Country.ToUpperInvariant() switch
    {
        "USA" or "UNITED STATES" => "United States",
        "UK" or "UNITED KINGDOM" => "United Kingdom",
        _ => CultureInfo.CurrentCulture.TextInfo.ToTitleCase(Country.ToLowerInvariant()),
    };

    public override string ToString()
    {
        return $"Customer {Id}: {FullName} ({Age}, {NormalizedCountry})";
    }
}
