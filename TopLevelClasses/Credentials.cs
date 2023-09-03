using Microsoft.Extensions.Configuration;
using Npgsql;
namespace MDR_Aggregator;

public class Credentials : ICredentials
{
    private readonly string _host;
    private readonly string _username;
    private readonly string _password;
    private readonly int _port;
    
    public string Username => _username;
    public string Password => _password;
    
    public Credentials(IConfiguration settings)
    {
        // all asserted as non-null

        _host = settings["host"]!;
        _username = settings["user"]!;
        _password = settings["password"]!;
        string? PortAsString = settings["port"];
        if (string.IsNullOrWhiteSpace(PortAsString))
        {
            _port = 5432;  // default
        }
        else
        {
            _port = int.TryParse(PortAsString, out int port_num) ? port_num : 5432;
        }
    }

    public string GetConnectionString(string database_name)
    {
        NpgsqlConnectionStringBuilder builder = new()
        {
            Host = _host,
            Username = _username,
            Password = _password,
            Port = _port,
            Database = database_name,
            KeepAlive = 300,
            IncludeErrorDetail = true
        };
        return builder.ConnectionString;
    }
}

