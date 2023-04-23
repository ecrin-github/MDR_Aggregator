using Dapper;
using Npgsql;
namespace MDR_Aggregator;

public class DBUtilities
{
    readonly string connstring;
    readonly ILoggingHelper _loggingHelper;

    public DBUtilities(string _connstring, ILoggingHelper logginghelper)
    {
        connstring = _connstring;
        _loggingHelper = logginghelper;
    }
    
    
    public int ExecuteSQL(string sql_string)
    {
        using var conn = new NpgsqlConnection(connstring);
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
    
    public int GetMaxId(string schema_name, string table_name)
    {
        string sql_string = $@"select max(id) from {schema_name}.{table_name}";
        using var conn = new NpgsqlConnection(connstring);
        return conn.ExecuteScalar<int>(sql_string);
    }


    public int GetAggMinId(string full_table_name)
    {
        string sql_string = $"select min(id) from {full_table_name}";
        using var conn = new NpgsqlConnection(connstring);
        return conn.ExecuteScalar<int>(sql_string);
    }

    
    public int GetAggMaxId(string full_table_name)
    {
        string sql_string = $"select max(id) from {full_table_name}";
        using var conn = new NpgsqlConnection(connstring);
        return conn.ExecuteScalar<int>(sql_string);
    }

    
    public int GetCount(string full_table_name)
    {
        string sql_string = $"SELECT COUNT(*) FROM {full_table_name}";
        using var conn = new NpgsqlConnection(connstring);
        return conn.ExecuteScalar<int>(sql_string);
    }


    public int Update_UsingTempTable(string index_table_name, string updated_table_name, 
                                     string sql_string, string conditional, int batch_size)
    {
        try
        {
            int max_id = GetCount(index_table_name);
            int updated = 0; 
            if (max_id > batch_size)
            {
                sql_string += conditional;
                for (int r = 1; r <= max_id; r += batch_size)
                {
                    string batch_sql_string = sql_string + $" t.id >= {r} and t.id < {r + batch_size} ";
                    updated += ExecuteSQL(batch_sql_string);
                    int e = r + batch_size < max_id ? r + batch_size - 1 : max_id;
                    string feedback = $"Updated {updated_table_name}, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                }
            }
            else
            {
                updated = ExecuteSQL(sql_string);
                _loggingHelper.LogLine($"Updated {updated_table_name} as a single batch");
            }
            return updated;
        } 
        catch (Exception e)
        {
            _loggingHelper.LogError($"In update of {updated_table_name}: {e.Message}");
            return 0;
        }
    }

    public int ExecuteTransferSQL(string sql_string, string schema_name, string table_name, 
                                  string qualifier, string context)
    {
        try
        {
            int transferred = 0;
            int max_id = GetMaxId(schema_name, table_name);
            int rec_batch = 50000;
            // int rec_batch = 10000;  // for testing 
            if (max_id > rec_batch)
            {
                sql_string += qualifier;
                for (int r = 1; r <= max_id; r += rec_batch)
                {
                    string batch_sql_string = sql_string + $" s.id >= {r} and s.id < {r + rec_batch} ";
                    transferred += ExecuteSQL(batch_sql_string);
                    int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                    string feedback = $"Transferred {schema_name}.{table_name} ({context}) data, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                }
            }
            else
            {
                transferred = ExecuteSQL(sql_string);
                _loggingHelper.LogLine($"Transferred {schema_name}.{table_name} ({context}) data, as a single batch");
            }
            return transferred;
        }
        catch (Exception e)
        {
            string feedback =
                $"In data transfer ({schema_name}.{table_name} ({context})) to aggregate table: {e.Message}";
            _loggingHelper.LogError(feedback);
            return 0;
        }
    }

    public int TransferFeatureData(string sql_string, string data_type, int min_id, int max_id)
    {
        try
        {
            int transferred = 0;
            int rec_batch = 50000;
            for (int r = min_id; r <= max_id; r += rec_batch)
            {
                string batch_sql_string = sql_string + $" and s.id >= {r} and s.id < {r + rec_batch} ";
                transferred += ExecuteSQL(batch_sql_string);
                int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                string feedback = $"Updated study_search table with {data_type} data, ids {r} to {e}";
                _loggingHelper.LogLine(feedback);
            }
            return transferred;
        }
        catch (Exception e)
        {
            string res = e.Message;
            _loggingHelper.LogError("In study search update (" + data_type + "): " + res);
            return 0;
        }
    }


    public int TransferObjectData(string where_string, string object_type)
    {
        try
        {
            int transferred = 0;
            int rec_batch = 50000;

            string setup_sql = @"TRUNCATE TABLE core.temp_searchobjects RESTART IDENTITY;
                        INSERT INTO core.temp_searchobjects(study_id)
                        SELECT DISTINCT k.study_id from core.study_object_links k
                        inner join core.data_objects b
                        on k.object_id = b.id
                        where " + where_string;
            ExecuteSQL(setup_sql);

            string sql_string = @"UPDATE core.study_search ss
                        SET has_" + object_type + @" = true
                        FROM core.temp_searchobjects d
                        WHERE ss.id = d.study_id ";

            int max_id = GetCount("core.temp_searchobjects");
            if (max_id > rec_batch)
            {
                for (int r = 1; r <= max_id; r += rec_batch)
                {
                    string batch_sql_string = sql_string + $" and d.id >= {r} and d.id < {r + rec_batch} ";
                    transferred += ExecuteSQL(batch_sql_string);
                    int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                    string feedback = $"Updated study_search table with has_{object_type} data, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                }
            }
            else
            {
                transferred = ExecuteSQL(sql_string);
                _loggingHelper.LogLine($"Updated study_search table with has_{object_type} data, as a single batch");
            }
            return transferred;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In study search update (has_{object_type} to core table: {e.Message}");
            return 0;
        }
    }

    public int SearchUpdateSQL(string sql_string, string data_type, int min_id, int max_id)
    {
        try
        {
            int transferred = 0;
            int rec_batch = 20000;
            for (int r = min_id; r <= max_id; r += rec_batch)
            {
                string batch_sql_string = sql_string + $" where s.id >= {r} and s.id < {r + rec_batch} ";
                transferred += ExecuteSQL(batch_sql_string);
                int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                string feedback = $"Updated {data_type} data, ids {r} to {e}";
                _loggingHelper.LogLine(feedback);
            }
            return transferred;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In study search update ({data_type}): {e.Message}");
            return 0;
        }
    }


    public int SearchStudyUpdateSQL(string sql_string, string data_type, int min_id, int max_id)
    {
        try
        {
            int transferred = 0;
            int rec_batch = 20000;
            for (int r = min_id; r <= max_id; r += rec_batch)
            {
                string batch_sql_string = sql_string + $" where s.study_id >= {r} and s.study_id < {r + rec_batch} ";
                batch_sql_string += " group by study_id";
                transferred += ExecuteSQL(batch_sql_string);
                int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                string feedback = $"Created {data_type}, ids {r} to {e}";
                _loggingHelper.LogLine(feedback);
            }
            return transferred;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In study search update ({data_type}): { e.Message}");
            return 0;
        }
    }


    public int TransferSearchDataByStudy(string sql_string, string data_type, int min_id, int max_id)
    {
        // uses study id to go through records because records must be grouped by study

        try
        {
            int transferred = 0;
            int rec_batch = 10000;
            for (int r = min_id; r <= max_id; r += rec_batch)
            {
                string batch_sql_string = sql_string + $" and ss.id >= {r} and ss.id < {r + rec_batch} ";
                transferred += ExecuteSQL(batch_sql_string);
                int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                string feedback = $"Updated study_search table with {data_type} data, ids {r} to {e}";
                _loggingHelper.LogLine(feedback);
            }
            return transferred;
        } 
        catch (Exception e)
        {
            _loggingHelper.LogError($"In study search update ({data_type}): { e.Message}");
            return 0;
        }
    }

    
    public int ExecuteCoreTransferSQL(string sql_string, string qualifier, string full_table_name)
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
                sql_string += qualifier;
                for (int r = min_id; r <= max_id; r += rec_batch)
                {
                    string batch_sql_string = sql_string + $" id >= {r} and id < {r + rec_batch} ";
                    transferred += ExecuteSQL(batch_sql_string);
                    int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                    string feedback = $"Transferred {full_table_name} data, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                }
            }
            else
            {
                transferred = ExecuteSQL(sql_string);
                _loggingHelper.LogLine($"Transferred {full_table_name} data, as a single batch");
            }
            return transferred;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In data transfer ({full_table_name} to core table: {e.Message}");
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
                    string batch_sql_string = sql_string + $" and s.id >= {r} and s.id < {r + rec_batch} ";
                    transferred += ExecuteSQL(batch_sql_string);
                    int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                    string feedback = $"Updated {full_table_name} with provenance data, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                }
            }
            else
            {
                transferred = ExecuteSQL(sql_string);
                _loggingHelper.LogLine($"Updated {full_table_name} with provenance data, as a single batch");
            }
            return transferred;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In updating provenance data in {full_table_name}: {e.Message}");
            return 0;
        }
    }

}