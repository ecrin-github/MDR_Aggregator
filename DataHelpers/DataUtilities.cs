using Dapper;
using Npgsql;
namespace MDR_Aggregator;

public class DBUtilities
{
    string connstring;
    ILoggingHelper _loggingHelper;

    public DBUtilities(string _connstring, ILoggingHelper logginghelper)
    {
        connstring = _connstring;
        _loggingHelper = logginghelper;
    }

    public int ExecuteSQL(string sql_string)
    {
        using (var conn = new NpgsqlConnection(connstring))
        {
            try
            {
                return conn.Execute(sql_string);
            }
            catch (Exception e)
            {
                _loggingHelper.LogError("In ExecuteSQL; " + e.Message + ", \nSQL was: " + sql_string);
                return 0;
            }
        }
    }
    
    public int GetMaxId(string schema_name, string table_name)
    {
        string sql_string = @"select max(id) from " + schema_name + "." + table_name;
        using (var conn = new NpgsqlConnection(connstring))
        {
           return conn.ExecuteScalar<int>(sql_string);
        }
    }


    public int GetAggMinId(string full_table_name)
    {
        string sql_string = @"select min(id) from " + full_table_name;
        using (var conn = new NpgsqlConnection(connstring))
        {
            return conn.ExecuteScalar<int>(sql_string);
        }
    }

    public int GetAggMaxId(string full_table_name)
    {
        string sql_string = @"select max(id) from " + full_table_name;
        using (var conn = new NpgsqlConnection(connstring))
        {
            return conn.ExecuteScalar<int>(sql_string);
        }
    }


    public void Update_SourceTable_ExportDate(string schema_name, string table_name)
    {
        try
        {
            int rec_count = GetMaxId(schema_name, table_name);
            int rec_batch = 50000;
            string sql_string = @"UPDATE " + schema_name + "." + table_name + @" s
                                  SET exported_on = CURRENT_TIMESTAMP ";

            if (rec_count > rec_batch)
            {
                for (int r = 1; r <= rec_count; r += rec_batch)
                {
                    string batch_sql_string = sql_string + " where s.id >= " + r.ToString() + " and s.id < " + (r + rec_batch).ToString();
                    ExecuteSQL(batch_sql_string);
                    string feedback = "Updated " + schema_name + "." + table_name + " export date, " + r.ToString() + " to ";
                    feedback += (r + rec_batch < rec_count) ? (r + rec_batch - 1).ToString() : rec_count.ToString();
                    _loggingHelper.LogLine(feedback);
                }
            }
            else
            {
                ExecuteSQL(sql_string);
                _loggingHelper.LogLine("Updated " + schema_name + "." + table_name + " export date, as a single batch");
            }
        }
        catch (Exception e)
        {
            string res = e.Message;
            _loggingHelper.LogError("In update export date (" + schema_name + "." + table_name + ") to aggregate table: " + res);
        }
    }


    public int ExecuteTransferSQL(string sql_string, string schema_name, string table_name, string context)
    {
        try
        {
            int transferred = 0;
            int rec_count = GetMaxId(schema_name, table_name);
            int rec_batch = 50000;
            // int rec_batch = 10000;  // for testing 
            if (rec_count > rec_batch)
            {
                for (int r = 1; r <= rec_count; r += rec_batch)
                {
                    string batch_sql_string = sql_string + " and s.id >= " + r.ToString() + " and s.id < " + (r + rec_batch).ToString();
                    transferred += ExecuteSQL(batch_sql_string);

                    string feedback = "Transferred " + schema_name + "." + table_name + " (" + context + ") data, " + r.ToString() + " to ";
                    feedback += (r + rec_batch < rec_count) ? (r + rec_batch - 1).ToString() : rec_count.ToString();
                    _loggingHelper.LogLine(feedback);
                }
            }
            else
            {
                transferred = ExecuteSQL(sql_string);
                _loggingHelper.LogLine("Transferred " + schema_name + "." + table_name + " (" + context + ") data, as a single batch");
            }
            return transferred;
        }

        catch (Exception e)
        {
            string res = e.Message;
            _loggingHelper.LogError("In data transfer (" + schema_name + "." + table_name + "(" + context + ")) to aggregate table: " + res);
            return 0;
        }
    }


    public int  ExecuteCoreTransferSQL(string sql_string, string full_table_name)
    {
        try
        {
            int transferred = 0;
            int min_id = GetAggMinId(full_table_name);
            int max_id = GetAggMaxId(full_table_name);
            int rec_batch = 50000;
            // int rec_batch = 10000;  // for testing 
            if (max_id - min_id > rec_batch)
            {
                for (int r = min_id; r <= max_id; r += rec_batch)
                {
                    string batch_sql_string = sql_string + " WHERE id >= " + r.ToString() + " and id < " + (r + rec_batch).ToString();
                    transferred += ExecuteSQL(batch_sql_string);

                    string feedback = "Transferred " + full_table_name + " data, " + r.ToString() + " to ";
                    feedback += (r + rec_batch < max_id) ? (r + rec_batch - 1).ToString() : max_id.ToString();
                    _loggingHelper.LogLine(feedback);
                }
            }
            else
            {
                transferred = ExecuteSQL(sql_string);
                _loggingHelper.LogLine("Transferred " + full_table_name + " data, as a single batch");
            }
            return transferred;
        }

        catch (Exception e)
        {
            string res = e.Message;
            _loggingHelper.LogError("In data transfer (" + full_table_name + " to core table: " + res);
            return 0;
        }
    }


    public int ExecuteProvenanceSQL(string sql_string, string full_table_name)
    {
        try
        {
            int transferred = 0;
            int min_id = GetAggMinId(full_table_name);
            int max_id = GetAggMaxId(full_table_name);
            int rec_batch = 50000;
            if (max_id - min_id > rec_batch)
            {
                for (int r = min_id; r <= max_id; r += rec_batch)
                {
                    string batch_sql_string = sql_string + " AND s.id >= " + r.ToString() + " and s.id < " + (r + rec_batch).ToString();
                    transferred += ExecuteSQL(batch_sql_string);

                    string feedback = "Updated " + full_table_name + " with provenance data, " + r.ToString() + " to ";
                    feedback += (r + rec_batch < max_id) ? (r + rec_batch - 1).ToString() : max_id.ToString();
                    _loggingHelper.LogLine(feedback);
                }
            }
            else
            {
                transferred = ExecuteSQL(sql_string);
                _loggingHelper.LogLine("Updated " + full_table_name + " with provenance data, as a single batch");
            }
            return transferred;
        }
        catch (Exception e)
        {
            string res = e.Message;
            _loggingHelper.LogError("In updating provenance data in " + full_table_name + ": " + res);
            return 0;
        }
    }
}