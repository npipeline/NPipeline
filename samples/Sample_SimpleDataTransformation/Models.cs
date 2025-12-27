namespace Sample_SimpleDataTransformation;

/// <summary>
///     Represents a person record read from CSV data.
/// </summary>
/// <param name="Id">The unique identifier for the person.</param>
/// <param name="FirstName">The first name of the person.</param>
/// <param name="LastName">The last name of the person.</param>
/// <param name="Age">The age of the person.</param>
/// <param name="Email">The email address of the person.</param>
/// <param name="City">The city where the person lives.</param>
public record Person(int Id, string FirstName, string LastName, int Age, string Email, string City);

/// <summary>
///     Represents a person record enriched with additional data.
/// </summary>
/// <param name="Id">The unique identifier for the person.</param>
/// <param name="FirstName">The first name of the person.</param>
/// <param name="LastName">The last name of the person.</param>
/// <param name="Age">The age of the person.</param>
/// <param name="Email">The email address of the person.</param>
/// <param name="City">The city where the person lives.</param>
/// <param name="Country">The country where the person lives.</param>
/// <param name="AgeCategory">The age category of the person.</param>
/// <param name="IsValidEmail">Whether the email is valid.</param>
public record EnrichedPerson(
    int Id,
    string FirstName,
    string LastName,
    int Age,
    string Email,
    string City,
    string Country,
    string AgeCategory,
    bool IsValidEmail);
