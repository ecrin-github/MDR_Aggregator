namespace MDR_Aggregator;

public interface ICredentials
{
    string Password { get; }
    string Username { get; }

    string GetConnectionString(string database_name);
}

