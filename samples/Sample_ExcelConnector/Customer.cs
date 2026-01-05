using System.Globalization;

namespace Sample_ExcelConnector;

/// <summary>
///     Represents a customer record from the Excel file.
///     This model demonstrates various data types that can be mapped using the Excel connector.
/// </summary>
public class Customer
{
    /// <summary>
    ///     Gets or sets the customer ID.
    /// </summary>
    public int Id { get; set; }

    /// <summary>
    ///     Gets or sets the customer's first name.
    /// </summary>
    public string FirstName { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the customer's last name.
    /// </summary>
    public string LastName { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the customer's email address.
    /// </summary>
    public string Email { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the customer's age.
    /// </summary>
    public int Age { get; set; }

    /// <summary>
    ///     Gets or sets the registration date.
    /// </summary>
    public DateTime RegistrationDate { get; set; }

    /// <summary>
    ///     Gets or sets the customer's country.
    /// </summary>
    public string Country { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the customer's account balance.
    ///     Demonstrates decimal type mapping.
    /// </summary>
    public decimal AccountBalance { get; set; }

    /// <summary>
    ///     Gets or sets whether the customer is a premium member.
    ///     Demonstrates boolean type mapping.
    /// </summary>
    public bool IsPremiumMember { get; set; }

    /// <summary>
    ///     Gets or sets the customer's discount percentage.
    ///     Demonstrates double type mapping.
    /// </summary>
    public double DiscountPercentage { get; set; }

    /// <summary>
    ///     Gets or sets the customer's loyalty points.
    ///     Demonstrates long type mapping.
    /// </summary>
    public long LoyaltyPoints { get; set; }

    /// <summary>
    ///     Gets the full name of the customer.
    /// </summary>
    public string FullName => $"{FirstName} {LastName}";

    /// <summary>
    ///     Gets a value indicating whether the customer is an adult (18+).
    /// </summary>
    public bool IsAdult => Age >= 18;

    /// <summary>
    ///     Gets the age category of the customer.
    /// </summary>
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
    /// </summary>
    public string NormalizedCountry => Country.ToUpperInvariant() switch
    {
        "USA" or "UNITED STATES" => "United States",
        "UK" or "UNITED KINGDOM" => "United Kingdom",
        _ => CultureInfo.CurrentCulture.TextInfo.ToTitleCase(Country.ToLowerInvariant()),
    };

    /// <summary>
    ///     Gets the customer status based on account balance and premium membership.
    /// </summary>
    public string CustomerStatus
    {
        get
        {
            if (IsPremiumMember && AccountBalance > 10000)
                return "VIP";

            if (IsPremiumMember)
                return "Premium";

            if (AccountBalance > 5000)
                return "Gold";

            if (AccountBalance > 1000)
                return "Silver";

            return "Standard";
        }
    }

    public override string ToString()
    {
        return $"Customer {Id}: {FullName} ({Age}, {NormalizedCountry}) - {CustomerStatus}";
    }
}
