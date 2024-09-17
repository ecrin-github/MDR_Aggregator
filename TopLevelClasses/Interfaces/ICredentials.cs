namespace MDR_Aggregator.TopLevelClasses.Interfaces;

public interface ICredentials
{
    string Password { get; }
    string Username { get; }

    string GetConnectionString(string database_name);
}

