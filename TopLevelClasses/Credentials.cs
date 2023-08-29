using Microsoft.Extensions.Configuration;
using Npgsql;
namespace MDR_Aggregator;

public class Credentials : ICredentials
{
    public string Host { get; set; }
    public string Username { get; set; }
    public string Password { get; set; }

    public Credentials(IConfiguration settings)
    {
        Host = settings["host"]!;
        Username = settings["user"]!;
        Password = settings["password"]!;
    }

    public string GetConnectionString(string database_name)
    {
        NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
        builder.Host = Host;
        builder.Username = Username;
        builder.Password = Password;
        builder.Database = database_name;
        return builder.ConnectionString;
    }
}

