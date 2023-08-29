using Dapper;
using Npgsql;
using Microsoft.Extensions.Configuration;

namespace MDR_Aggregator;

public class DataLayer
    {
        private readonly string? connString;
        private readonly string? mon_connString;
        private readonly string? username;
        private readonly string? password;
        private readonly string? host;

        public DataLayer(string database_name)
        {
                IConfigurationRoot settings = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json")
                .Build();

            NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
            builder.Host = settings["host"];
            builder.Username = settings["user"];
            builder.Password = settings["password"];
            builder.Database = database_name;

            connString = builder.ConnectionString;

            builder.Database = "mon";
            mon_connString = builder.ConnectionString;

            username = builder.Username;
            password = builder.Password;
            host = builder.Host;
        }

        public string ConnString => connString;
        public string monConnString => mon_connString;

        //public string Username => username;
        //public string Password => password;


        public string GetConnString(int source_id)
        {
            string sql_string = @"select database_name 
                                  from sf.source_parameters 
                                  where id = " + source_id.ToString();
            string db_name = "";
            using (var conn = new NpgsqlConnection(mon_connString))
            {
                db_name = conn.Query<string>(sql_string).FirstOrDefault();
            }

            return GetConnString(db_name);
        }


        public string GetConnString(string db_name)
        {
            NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
            builder.Host = host;
            builder.Username = username;
            builder.Password = password;
            builder.Database = db_name;
            return builder.ConnectionString;
        }

        /*
        public string SetUpTempFTW(string database_name)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                string sql_string = @"CREATE EXTENSION IF NOT EXISTS postgres_fdw
                                     schema core;";
                conn.Execute(sql_string);

                sql_string = @"CREATE SERVER IF NOT EXISTS " + database_name
                           + @" FOREIGN DATA WRAPPER postgres_fdw
                             OPTIONS (host 'localhost', dbname '" + database_name + "');";
                conn.Execute(sql_string);

                sql_string = @"CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
                     SERVER " + database_name
                     + @" OPTIONS (user '" + username + "', password '" + password + "');";
                conn.Execute(sql_string);
                string schema_name = "";
                if (database_name == "mon")
                {
                    schema_name = database_name + "_sf";
                    sql_string = @"DROP SCHEMA IF EXISTS " + schema_name + @" cascade;
                     CREATE SCHEMA " + schema_name + @";
                     IMPORT FOREIGN SCHEMA sf
                     FROM SERVER " + database_name +
                         @" INTO " + schema_name + ";";
                }
                else
                {
                    schema_name = database_name + "_ad";
                    sql_string = @"DROP SCHEMA IF EXISTS " + schema_name + @" cascade;
                     CREATE SCHEMA " + schema_name + @";
                     IMPORT FOREIGN SCHEMA ad
                     FROM SERVER " + database_name +
                         @" INTO " + schema_name + ";";
                }
                conn.Execute(sql_string);
                return schema_name;
            }
        }


        public void DropTempFTW(string database_name)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                string schema_name = "";
                if (database_name == "mon")
                {
                    schema_name = database_name + "_sf";
                }
                else
                {
                    schema_name = database_name + "_ad";
                }

                string sql_string = @"DROP USER MAPPING IF EXISTS FOR CURRENT_USER
                     SERVER " + database_name + ";";
                conn.Execute(sql_string);

                sql_string = @"DROP SERVER IF EXISTS " + database_name + " CASCADE;";
                conn.Execute(sql_string);

                sql_string = @"DROP SCHEMA IF EXISTS " + schema_name;
                conn.Execute(sql_string);
            }
        }
        */

        /*
        public void SetUpTempContextFTWs()
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                string sql_string = @"CREATE EXTENSION IF NOT EXISTS postgres_fdw
                                     schema sf;";
                conn.Execute(sql_string);

                sql_string = @"CREATE SERVER IF NOT EXISTS context
                               FOREIGN DATA WRAPPER postgres_fdw
                               OPTIONS (host 'localhost', dbname 'context');";
                conn.Execute(sql_string);

                sql_string = @"CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
                     SERVER context"
                     + @" OPTIONS (user '" + username + "', password '" + password + "');";
                conn.Execute(sql_string);

                sql_string = @"DROP SCHEMA IF EXISTS context_lup cascade;
                     CREATE SCHEMA context_lup;
                     IMPORT FOREIGN SCHEMA lup
                     FROM SERVER context 
                     INTO context_lup;";
                conn.Execute(sql_string);

                sql_string = @"DROP SCHEMA IF EXISTS context_ctx cascade;
                     CREATE SCHEMA context_ctx;
                     IMPORT FOREIGN SCHEMA ctx
                     FROM SERVER context 
                     INTO context_ctx;";
                conn.Execute(sql_string);
            }
        }


        public void DropTempContextFTWs()
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                string sql_string = @"DROP USER MAPPING IF EXISTS FOR CURRENT_USER
                     SERVER context;";
                conn.Execute(sql_string);

                sql_string = @"DROP SERVER IF EXISTS context CASCADE;";
                conn.Execute(sql_string);

                sql_string = @"DROP SCHEMA IF EXISTS context_lup;";
                conn.Execute(sql_string);
                sql_string = @"DROP SCHEMA IF EXISTS context_ctx;";
                conn.Execute(sql_string);
            }
        }
        */
    }

