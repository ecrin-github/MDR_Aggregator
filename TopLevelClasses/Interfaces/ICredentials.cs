namespace MDR_Aggregator;

public interface ICredentials
{
    string Host { get; set; }
    string Password { get; set; }
    string Username { get; set; }

    string GetConnectionString(string database_name);
}

