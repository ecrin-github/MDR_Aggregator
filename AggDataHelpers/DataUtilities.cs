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
    
    public int GetMaxId(string ftw_schema_name, string table_name)
    {
        string sql_string = $@"select max(id) from {ftw_schema_name}.{table_name}";
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
    
    public int GetMinStudyId(string full_table_name)
    {
        string sql_string = $"select min(study_id) from {full_table_name}";
        using var conn = new NpgsqlConnection(connstring);
        return conn.ExecuteScalar<int>(sql_string);
    }

    
    public int GetMaxStudyId(string full_table_name)
    {
        string sql_string = $"select max(study_id) from {full_table_name}";
        using var conn = new NpgsqlConnection(connstring);
        return conn.ExecuteScalar<int>(sql_string);
    }


    
    public int GetCount(string full_table_name)
    {
        string sql_string = $"SELECT COUNT(*) FROM {full_table_name}";
        using var conn = new NpgsqlConnection(connstring);
        return conn.ExecuteScalar<int>(sql_string);
    }

    // Used by the Study Transfer Helper and Object Transfer Helper to process ids, 
    // e.g. when identifying new and existing studies and objects, (4 calls in each)
    // and extensively by the Pubmed Transfer process - 11 calls
    
    public int Update_UsingTempTable(string index_table_name, string updated_table_name, 
                                     string sql_string, string conditional, int batch_size, 
                                     string feedback_addition)
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
                    string feedback = $"Updating {updated_table_name}{feedback_addition}, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                }
            }
            else
            {
                updated = ExecuteSQL(sql_string);
                _loggingHelper.LogLine($"Updating {updated_table_name}{feedback_addition} as a single batch");
            }
            return updated;
        } 
        catch (Exception e)
        {
            _loggingHelper.LogError($"In update of {updated_table_name}: {e.Message}");
            return 0;
        }
    }

    // The main routine used when transferring data from the source databases to the aggs database.
    // 15 calls involved in transferring object data, 23 calls for study data
    
    public int ExecuteTransferSQL(string sql_string, string ftw_schema_name, string table_name, 
                                  string qualifier, string context)
    {
        try
        {
            int transferred = 0;
            int max_id = GetMaxId(ftw_schema_name, table_name);
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
                    string feedback = $"Transferred {ftw_schema_name}.{table_name} ({context}) data, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                }
            }
            else
            {
                transferred = ExecuteSQL(sql_string);
                _loggingHelper.LogLine($"Transferred {ftw_schema_name}.{table_name} ({context}) data, as a single batch");
            }
            return transferred;
        }
        catch (Exception e)
        {
            string feedback =
                $"In data transfer ({ftw_schema_name}.{table_name} ({context})) to aggregate table: {e.Message}";
            _loggingHelper.LogError(feedback);
            return 0;
        }
    }
    
    // The main routine for transferring data from the aggs database to the core database
    // 24 calls within this process, plus 4 calls when setting up tables of temp titles,
    // topics and conditions data, and search_lexemes table, in the indexing process. 
    
    public int ExecuteCoreTransferSQL(string sql_string, string qualifier, string full_table_name)
    {
        try
        {
            int transferred = 0;
            int min_id = GetAggMinId(full_table_name);
            int max_id = GetAggMaxId(full_table_name);
            int rec_batch = 50000;
            string fbc = $"records of {full_table_name} data";
            if (max_id - min_id > rec_batch)
            {
                sql_string += qualifier;
                for (int r = min_id; r <= max_id; r += rec_batch)
                {
                    string batch_sql_string = sql_string + $" id >= {r} and id < {r + rec_batch} ";
                    int res = ExecuteSQL(batch_sql_string);
                    if (res > 0)
                    {
                        int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                        string feedback = $"Transferred {res} {fbc}, ids {r} to {e}";
                        _loggingHelper.LogLine(feedback);
                        transferred += res;
                    }
                }
            }
            else
            {
                transferred = ExecuteSQL(sql_string);
                _loggingHelper.LogLine($"Transferred {transferred} {fbc}, as a single batch");
            }
            return transferred;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In data transfer ({full_table_name} to core table: {e.Message}");
            return 0;
        }
    }
    
    // Used just for transferring study condition icd data to the icd table
    
    public int TransferICDSQL(string sql_string, string full_table_name)
    {
        try
        {
            int transferred = 0;
            int min_id = GetMinStudyId(full_table_name);
            int max_id = GetMaxStudyId(full_table_name);
            int rec_batch = 100000;
            string fbc = $"ICD data records from {full_table_name} data";
            for (int r = min_id; r <= max_id; r += rec_batch)
            {
                string batch_sql_string = sql_string + $" study_id >= {r} and study_id < {r + rec_batch} ";
                int res = ExecuteSQL(batch_sql_string);
                if (res > 0)
                {
                    int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                    string feedback = $"Transferred {res} {fbc}, study ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                    transferred += res;
                }
            }
            return transferred;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In data transfer ({full_table_name} to core table: {e.Message}");
            return 0;
        }
    }

    // Used once for studies and twice for data objects (pubmed and non pubmed) to create and 
    // insert the provenance strings for the data in the core schema.

    public int ExecuteProvenanceSQL(string sql_string, string full_table_name, string type_qualifier)
    {
        string feedback_name = full_table_name + type_qualifier;
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
                    string feedback = $"Updated {feedback_name} with provenance data, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                }
            }
            else
            {
                transferred = ExecuteSQL(sql_string);
                _loggingHelper.LogLine($"Updated {feedback_name} with provenance data, as a single batch");
            }
            return transferred;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In updating provenance data in {feedback_name}: {e.Message}");
            return 0;
        }
    }

    
    
    // Used twice in the search setup process, to recreate the base search_studies
    // and search_objects tables.
    
    public int SearchTableTransfer(string top_sql, string bottom_sql, string id_field,
                           int min_id, int max_id, string table_name, int rec_batch)
    {
        try
        {
            int transferred = 0;
            for (int r = min_id; r <= max_id; r += rec_batch)
            {
                string batch_sql_string = top_sql
                                          + $" where s.{id_field} >= {r} and s.{id_field} < {r + rec_batch} "
                                          + bottom_sql;
                int res = ExecuteSQL(batch_sql_string);
                if (res > 0)
                {
                    int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                    string feedback = $"Transferred {res} records to {table_name}, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                    transferred += res;
                }
            }
            return transferred;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In data transfer to ({table_name}: {e.Message}");
            return 0;
        }
    }

    // Used 5 times in updating the search_studies table with text decodes for study parameter ids.

    public int UpdateSearchStudyData(string sql_string, string data_type, int min_id, int max_id)
    {
        try
        {
            int updated = 0;
            int rec_batch = 20000;
            for (int r = min_id; r <= max_id; r += rec_batch)
            {
                string batch_sql_string = sql_string + $" and ss.study_id >= {r} and ss.study_id < {r + rec_batch} ";
                int res = ExecuteSQL(batch_sql_string);
                if (res > 0)
                {
                    int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                    string feedback = $"Updated {res} {data_type} fields, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                    updated += res;
                }
            }
            return updated;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In study search update ({data_type}): { e.Message}");
            return 0;
        }
    }

    // Used twice times in updating the search_studies table with feature data.
    
    public int UpdateSearchFeatureData(string sql_string, string data_type, int min_id, int max_id)
    {
        try
        {
            int transferred = 0;
            int rec_batch = 20000;
            for (int r = min_id; r <= max_id; r += rec_batch)
            {
                string batch_sql_string = sql_string + $" and ss.study_id >= {r} and ss.study_id < {r + rec_batch} ";
                int res = ExecuteSQL(batch_sql_string);
                if (res > 0)
                {
                    int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                    string feedback = $"Updated {res} records in study_search table with {data_type} data, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                    transferred += res;
                }
            }
            return transferred;
        }
        catch (Exception e)
        {
            string res = e.Message;
            _loggingHelper.LogError($"In study search update ({data_type}): {res}");
            return 0;
        }
    }

    // Used in the search setup process to collect the 'has an object of type X' data.
    // Called 16 times, i.e. for each object type
    
    public int CollectHasObjectData(string where_string, int bit_pos, string object_type)
    {
        try
        {
            string setup_sql = $@"INSERT INTO core.temp_searchobjects(study_id, bit_pos)
                        SELECT DISTINCT k.study_id, {bit_pos}
                        from core.study_object_links k
                        inner join core.data_objects b
                        on k.object_id = b.id
                        where " + where_string;
            return ExecuteSQL(setup_sql);
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In collecting object data (has_{object_type}) : {e.Message}");
            return 0;
        }
    }
    
    // Used in the search setup process to transfer the 'has an object of type X' data to
    // a bitmap. Called 16 times, for each object type, from within a single loop.
    
    public int UpdateBitMap(string sql_string, int n, int min_id, int max_id )
    {
        try
        {
            int updated = 0;
            int rec_batch = 20000;
            for (int r = min_id; r <= max_id; r += rec_batch)
            {
                string batch_sql_string = sql_string + $" and ss.study_id >= {r} and ss.study_id < {r + rec_batch} ";
                int res = ExecuteSQL(batch_sql_string);
                if (res > 0)
                {
                    int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                    updated += res;
                }
            }
            return updated;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In bit map update (n = {n}): { e.Message}");
            return 0;
        }
    }

    // Used to update the search_studies table with lists of countries and conditions,
    // as two separate sets of calls.
    
    public int UpdateListData(string top_sql, string bottom_sql, int min_id, int max_id, string list_type )
    {
        try
        { 
            int updated = 0;
            int rec_batch = 20000;
            for (int r = min_id; r <= max_id; r += rec_batch)
            {
                string batch_sql_string = top_sql 
                                          + $" where sc.study_id >= {r} and sc.study_id < {r + rec_batch} " +
                                          bottom_sql;
                int res = ExecuteSQL(batch_sql_string);
                if (res > 0)
                {
                    int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                    string feedback = $"Updated {res} {list_type} fields, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                    updated += res;
                }
            }
            return updated;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In {list_type} update: { e.Message}");
            return 0;
        }
    }

    // Used to create the data in the search_idents table
    
    public int CreateSearchIdentsData(string top_sql, string bottom_sql, int min_id, int max_id, string list_type )
    {
        try
        { 
            int created = 0;
            int rec_batch = 50000;
            for (int r = min_id; r <= max_id; r += rec_batch)
            {
                string batch_sql_string = top_sql 
                                          + $" and si.study_id >= {r} and si.study_id < {r + rec_batch} " +
                                          bottom_sql;
                int res = ExecuteSQL(batch_sql_string);
                if (res > 0)
                {
                    int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                    string feedback = $"Updated {res} {list_type} fields, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                    created += res;
                }
            }
            return created;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In {list_type} update: { e.Message}");
            return 0;
        }
    }
    
    public int CreateSearchCountriesData(string top_sql, int min_id, int max_id, string list_type )
    {
        try
        { 
            int created = 0;
            int rec_batch = 50000;
            for (int r = min_id; r <= max_id; r += rec_batch)
            {
                string batch_sql_string = top_sql
                                          + $" and sc.study_id >= {r} and sc.study_id < {r + rec_batch} ";
                int res = ExecuteSQL(batch_sql_string);
                if (res > 0)
                {
                    int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                    string feedback = $"Updated {res} {list_type} fields, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                    created += res;
                }
            }
            return created;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In {list_type} update: { e.Message}");
            return 0;
        }
    }
    
    
    // Used during construction of object search data. Called 3 times.
    
    public int UpdateObjectSearchData(string sql_string, int min_id, int max_id, string qualifier, string field_type )
    {
        try
        {
            int updated = 0;
            int rec_batch = 20000;
            for (int r = min_id; r <= max_id; r += rec_batch)
            {
                string batch_sql_string = sql_string + qualifier
                            + $" so.study_id >= {r} and so.study_id < {r + rec_batch} ";
                int res = ExecuteSQL(batch_sql_string);
                if (res > 0)
                {
                    int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                    string feedback = $"Updated {res} {field_type} fields, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                    updated += res;
                }
            }
            return updated;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In {field_type} update: { e.Message}");
            return 0;
        }
    }

    // Used (3 times) within the search setup process for the titles and topic indexing process.
    
    public int CreateLexSQL(string sql_string, string data_type, string full_table_name)
    {
        try
        {
            int created = 0;
            int min_id = GetAggMinId(full_table_name);
            int max_id = GetAggMaxId(full_table_name);
            int rec_batch = 25000;
            for (int r = min_id; r <= max_id; r += rec_batch)
            {
                string batch_sql_string = sql_string + $" where s.id >= {r} and s.id < {r + rec_batch} ";
                created += ExecuteSQL(batch_sql_string);
                int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                string feedback = $"Updated {data_type} data, ids {r} to {e}";
                _loggingHelper.LogLine(feedback);
            }
            return created;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In create lexemes ({data_type}): {e.Message}");
            return 0;
        }
    }
   
    // Used (3 times) within the search setup process for the titles and topic indexing process.
    
    public int AggregateLexDataByStudy(string sql_string, string full_table_name, int min_id, int max_id)
    {
        try
        {
            int transferred = 0;
            int rec_batch = 25000;
            // int rec_batch = 10000;  // for testing 
            
            if (max_id - min_id > rec_batch)
            {
                for (int r = min_id; r <= max_id; r += rec_batch)
                {
                    string batch_sql_string = sql_string 
                                    + $" where study_id >= {r} and study_id < {r + rec_batch} ";
                    batch_sql_string += " group by study_id";
                    transferred += ExecuteSQL(batch_sql_string);
                    int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                    string feedback = $"Updated {full_table_name} data, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                }
            }
            else
            {
                transferred = ExecuteSQL(sql_string);
                _loggingHelper.LogLine($"Updated {full_table_name} data, as a single batch");
            }
            return transferred;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In AggregateLexDataByStudy ({full_table_name} in search table: {e.Message}");
            return 0;
        }
    }
    
    
    // Used 2 times in the set up process for titles, topics and conditions indexing
    // Uses study id to go through records because records must be grouped by study
    
    public int TransferSearchDataByStudy(string sql_string, string data_type, int min_id, int max_id)
    {
        try
        {
            int transferred = 0;
            int rec_batch = 20000;
            for (int r = min_id; r <= max_id; r += rec_batch)
            {
                string batch_sql_string = sql_string + $" and s.study_id >= {r} and s.study_id < {r + rec_batch} ";
                int res = ExecuteSQL(batch_sql_string);
                if (res > 0)
                {
                    int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                    string feedback = $"Updated search_lexemes table with {data_type} data, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                    transferred += res;
                }
            }
            return transferred;
        } 
        catch (Exception e)
        {
            _loggingHelper.LogError($"In TransferSearchDataByStudy ({data_type}): { e.Message}");
            return 0;
        }
    }
    
    
    public int UpdateStudyFeatureList(string sql_string,  int min_id, int max_id, string data_type)
    {
        try
        {
            int transferred = 0;
            int rec_batch = 20000;
            for (int r = min_id; r <= max_id; r += rec_batch)
            {
                string batch_sql_string = sql_string + $" and ss.study_id >= {r} and ss.study_id < {r + rec_batch} ";
                int res = ExecuteSQL(batch_sql_string);
                if (res > 0)
                {
                    int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                    string feedback = $"Updated search_studies table with {data_type} data, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                    transferred += res;
                }
            }
            return transferred;
        } 
        catch (Exception e)
        {
            _loggingHelper.LogError($"In UpdateStudyFeatureList ({data_type}s): { e.Message}");
            return 0;
        }
    }
    
    /*
    public int UpdateSearchStudyObjectJson(int min_id, int max_id, string list_type )
    {
        try
        { 
            int updated = 0;
            int rec_batch = 20000;
            for (int r = min_id; r <= max_id; r += rec_batch)
            {
                string batch_sql_string = $@"Update search.studies ss
                   set object_json = b.obj_json
                from
                    (select study_id, json_agg(object_json) as obj_json
                    from search.objects 
                    where study_id >= {r}
                    and study_id < {r + rec_batch} 
                    group by study_id) b
                    where ss.study_id = b.study_id ";

                int res = ExecuteSQL(batch_sql_string);
                if (res > 0)
                {
                    int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                    string feedback = $"Updated {res} {list_type} fields, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                    updated += res;
                }
            }
            return updated;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In {list_type} update: { e.Message}");
            return 0;
        }
    }
    */
    
    public int UpdateStudyJson(string sql_string,  int min_id, int max_id, string data_type)
    {
        try
        {
            int transferred = 0;
            int rec_batch = 20000;
            for (int r = min_id; r <= max_id; r += rec_batch)
            {
                string batch_sql_string = sql_string + $" where ss.study_id >= {r} and ss.study_id < {r + rec_batch} ";
                int res = ExecuteSQL(batch_sql_string);
                if (res > 0)
                {
                    int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                    string feedback = $"Updated search_studies table with {data_type} data, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                    transferred += res;
                }
            }
            return transferred;
        } 
        catch (Exception e)
        {
            _loggingHelper.LogError($"In UpdateStudyFeatureList ({data_type}s): { e.Message}");
            return 0;
        }
    }
    
    public int TransferStudyJson(string sql_string,  int min_id, int max_id, string data_type)
    {
        try
        {
            int transferred = 0;
            int rec_batch = 10000;
            for (int r = min_id; r <= max_id; r += rec_batch)
            {
                string batch_sql_string = sql_string + $" and s.study_id >= {r} and s.study_id < {r + rec_batch} ";
                int res = ExecuteSQL(batch_sql_string);
                if (res > 0)
                {
                    int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                    string feedback = $"Updated {data_type} table with study json data, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                    transferred += res;
                }
            }
            return transferred;
        } 
        catch (Exception e)
        {
            _loggingHelper.LogError($"In UpdateStudyFeatureList ({data_type}s): { e.Message}");
            return 0;
        }
    }
    
}