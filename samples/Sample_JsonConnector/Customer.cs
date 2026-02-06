using System.Globalization;
using NPipeline.Connectors.Attributes;

namespace Sample_JsonConnector;

/// <summary>
///     Represents a customer record from the JSON file.
///     Uses Column attributes for explicit property mapping.
/// </summary>
public class Customer
{
    /// <summary>
    ///     Gets or sets the customer ID.
    /// </summary>
    [Column("id")]
    public int Id { get; set; }

    /// <summary>
    ///     Gets or sets the customer's first name.
    /// </summary>
    [Column("firstName")]
    public string FirstName { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the customer's last name.
    /// </summary>
    [Column("lastName")]
    public string LastName { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the customer's email address.
    /// </summary>
    [Column("email")]
    public string Email { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the customer's age.
    /// </summary>
    [Column("age")]
    public int Age { get; set; }

    /// <summary>
    ///     Gets or sets the registration date.
    /// </summary>
    [Column("registrationDate")]
    public DateTime RegistrationDate { get; set; }

    /// <summary>
    ///     Gets or sets the customer's country.
    /// </summary>
    [Column("country")]
    public string Country { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets whether the customer is active.
    /// </summary>
    [Column("isActive")]
    public bool IsActive { get; set; }

    /// <summary>
    ///     Gets the full name of the customer.
    ///     This computed property is excluded from JSON mapping.
    /// </summary>
    [IgnoreColumn]
    public string FullName => $"{FirstName} {LastName}";

    /// <summary>
    ///     Gets a value indicating whether the customer is an adult (18+).
    ///     This computed property is excluded from JSON mapping.
    /// </summary>
    [IgnoreColumn]
    public bool IsAdult => Age >= 18;

    /// <summary>
    ///     Gets the age category of the customer.
    ///     This computed property is excluded from JSON mapping.
    /// </summary>
    [IgnoreColumn]
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
    ///     This computed property is excluded from JSON mapping.
    /// </summary>
    [IgnoreColumn]
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
