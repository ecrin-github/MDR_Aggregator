using Dapper;
using MDR_Aggregator.LoggingHelpers.Interfaces;
using Npgsql;

namespace MDR_Aggregator.AggDataHelpers;

public class DbUtilities
{
    private readonly string _connString;
    readonly ILoggingHelper _loggingHelper;

    public DbUtilities(string connString, ILoggingHelper loggingHelper)
    {
        _connString = connString;
        _loggingHelper = loggingHelper;
    }


    public int ExecuteSql(string sqlString)
    {
        using var conn = new NpgsqlConnection(_connString);
        try
        {
            return conn.Execute(sqlString);
        }
        catch (Exception e)
        {
            _loggingHelper.LogError("In ExecuteSQL; " + e.Message + ", \nSQL was: " + sqlString);
            return 0;
        }
    }

    private int GetMaxId(string ftwSchemaName, string tableName)
    {
        var sql_string = $"select max(id) from {ftwSchemaName}.{tableName}";
        using var conn = new NpgsqlConnection(_connString);
        return conn.ExecuteScalar<int>(sql_string);
    }


    public int GetAggMinId(string fullTableName)
    {
        var sql_string = $"select min(id) from {fullTableName}";
        using var conn = new NpgsqlConnection(_connString);
        return conn.ExecuteScalar<int>(sql_string);
    }


    public int GetAggMaxId(string fullTableName)
    {
        var sql_string = $"select max(id) from {fullTableName}";
        using var conn = new NpgsqlConnection(_connString);
        return conn.ExecuteScalar<int>(sql_string);
    }

    private int GetMinStudyId(string fullTableName)
    {
        var sql_string = $"select min(study_id) from {fullTableName}";
        using var conn = new NpgsqlConnection(_connString);
        return conn.ExecuteScalar<int>(sql_string);
    }


    private int GetMaxStudyId(string fullTableName)
    {
        var sql_string = $"select max(study_id) from {fullTableName}";
        using var conn = new NpgsqlConnection(_connString);
        return conn.ExecuteScalar<int>(sql_string);
    }



    public int GetCount(string fullTableName)
    {
        var sql_string = $"SELECT COUNT(*) FROM {fullTableName}";
        using var conn = new NpgsqlConnection(_connString);
        return conn.ExecuteScalar<int>(sql_string);
    }

    // Used by the Study Transfer Helper and Object Transfer Helper to process ids, 
    // e.g. when identifying new and existing studies and objects, (4 calls in each)
    // and extensively by the Pubmed Transfer process - 11 calls

    public int Update_UsingTempTable(string indexTableName, string updatedTableName,
                                     string sqlString, string conditional, int batchSize,
                                     string feedbackAddition)
    {
        try
        {
            var max_id = GetCount(indexTableName);
            var updated = 0;
            if (max_id > batchSize)
            {
                sqlString += conditional;
                for (var r = 1; r <= max_id; r += batchSize)
                {
                    var batch_sql_string = sqlString + $" t.id >= {r} and t.id < {r + batchSize} ";
                    updated += ExecuteSql(batch_sql_string);
                    var e = r + batchSize < max_id ? r + batchSize - 1 : max_id;
                    var feedback = $"Updating {updatedTableName}{feedbackAddition}, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                }
            }
            else
            {
                updated = ExecuteSql(sqlString);
                _loggingHelper.LogLine($"Updating {updatedTableName}{feedbackAddition} as a single batch");
            }
            return updated;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In update of {updatedTableName}: {e.Message}");
            return 0;
        }
    }

    // The main routine used when transferring data from the source databases to the aggs database.
    // 15 calls involved in transferring object data, 23 calls for study data

    public int ExecuteTransferSql(string sqlString, string ftwSchemaName, string tableName,
                                  string qualifier, string context)
    {
        try
        {
            var transferred = 0;
            var max_id = GetMaxId(ftwSchemaName, tableName);
            const int recBatch = 50000;
            // int rec_batch = 10000;  // for testing 
            if (max_id > recBatch)
            {
                sqlString += qualifier;
                for (var r = 1; r <= max_id; r += recBatch)
                {
                    var batch_sql_string = sqlString + $" s.id >= {r} and s.id < {r + recBatch} ";
                    transferred += ExecuteSql(batch_sql_string);
                    var e = r + recBatch < max_id ? r + recBatch - 1 : max_id;
                    var feedback = $"Transferred {ftwSchemaName}.{tableName} ({context}) data, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                }
            }
            else
            {
                transferred = ExecuteSql(sqlString);
                _loggingHelper.LogLine($"Transferred {ftwSchemaName}.{tableName} ({context}) data, as a single batch");
            }
            return transferred;
        }
        catch (Exception e)
        {
            var feedback =
                $"In data transfer ({ftwSchemaName}.{tableName} ({context})) to aggregate table: {e.Message}";
            _loggingHelper.LogError(feedback);
            return 0;
        }
    }

    // The main routine for transferring data from the aggs database to the core database
    // 24 calls within this process, plus 4 calls when setting up tables of temp titles,
    // topics and conditions data, and search_lexemes table, in the indexing process. 

    public int ExecuteCoreTransferSql(string sqlString, string qualifier, string fullTableName)
    {
        try
        {
            var transferred = 0;
            var min_id = GetAggMinId(fullTableName);
            var max_id = GetAggMaxId(fullTableName);
            const int recBatch = 50000;
            var fbc = $"records of {fullTableName} data";
            if (max_id - min_id > recBatch)
            {
                sqlString += qualifier;
                for (var r = min_id; r <= max_id; r += recBatch)
                {
                    var batch_sql_string = sqlString + $" id >= {r} and id < {r + recBatch} ";
                    var res = ExecuteSql(batch_sql_string);
                    if (res <= 0) continue;
                    var e = r + recBatch < max_id ? r + recBatch - 1 : max_id;
                    var feedback = $"Transferred {res} {fbc}, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                    transferred += res;
                }
            }
            else
            {
                transferred = ExecuteSql(sqlString);
                _loggingHelper.LogLine($"Transferred {transferred} {fbc}, as a single batch");
            }
            return transferred;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In data transfer ({fullTableName} to core table: {e.Message}");
            return 0;
        }
    }

    // Used just for transferring study condition icd data to the icd table

    public int TransferIcdSql(string sqlString, string fullTableName)
    {
        try
        {
            var transferred = 0;
            var min_id = GetMinStudyId(fullTableName);
            var max_id = GetMaxStudyId(fullTableName);
            const int recBatch = 100000;
            var fbc = $"ICD data records from {fullTableName} data";
            for (var r = min_id; r <= max_id; r += recBatch)
            {
                var batch_sql_string = sqlString + $" study_id >= {r} and study_id < {r + recBatch} ";
                var res = ExecuteSql(batch_sql_string);
                if (res <= 0) continue;
                var e = r + recBatch < max_id ? r + recBatch - 1 : max_id;
                var feedback = $"Transferred {res} {fbc}, study ids {r} to {e}";
                _loggingHelper.LogLine(feedback);
                transferred += res;
            }
            return transferred;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In data transfer ({fullTableName} to core table: {e.Message}");
            return 0;
        }
    }

    // Used once for studies and twice for data objects (pubmed and non pubmed) to create and 
    // insert the provenance strings for the data in the core schema.

    public int ExecuteProvenanceSql(string sqlString, string fullTableName, string typeQualifier)
    {
        var feedback_name = fullTableName + typeQualifier;
        try
        {
            var transferred = 0;
            var min_id = GetAggMinId(fullTableName);
            var max_id = GetAggMaxId(fullTableName);
            const int recBatch = 50000;
            if (max_id - min_id > recBatch)
            {
                for (var r = min_id; r <= max_id; r += recBatch)
                {
                    var batch_sql_string = sqlString + $" and s.id >= {r} and s.id < {r + recBatch} ";
                    transferred += ExecuteSql(batch_sql_string);
                    var e = r + recBatch < max_id ? r + recBatch - 1 : max_id;
                    var feedback = $"Updated {feedback_name} with provenance data, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                }
            }
            else
            {
                transferred = ExecuteSql(sqlString);
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

    public int SearchTableTransfer(string topSql, string bottomSql, string idField,
                           int minId, int maxId, string tableName, int recBatch)
    {
        try
        {
            var transferred = 0;
            for (var r = minId; r <= maxId; r += recBatch)
            {
                var batch_sql_string = topSql
                                       + $" where s.{idField} >= {r} and s.{idField} < {r + recBatch} "
                                       + bottomSql;
                var res = ExecuteSql(batch_sql_string);
                if (res <= 0) continue;
                var e = r + recBatch < maxId ? r + recBatch - 1 : maxId;
                var feedback = $"Transferred {res} records to {tableName}, ids {r} to {e}";
                _loggingHelper.LogLine(feedback);
                transferred += res;
            }
            return transferred;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In data transfer to ({tableName}: {e.Message}");
            return 0;
        }
    }

    // Used 5 times in updating the search_studies table with text decodes for study parameter ids.

    public int UpdateSearchStudyData(string sqlString, string dataType, int minId, int maxId)
    {
        try
        {
            var updated = 0;
            const int recBatch = 20000;
            for (var r = minId; r <= maxId; r += recBatch)
            {
                var batch_sql_string = sqlString + $" and ss.study_id >= {r} and ss.study_id < {r + recBatch} ";
                var res = ExecuteSql(batch_sql_string);
                if (res <= 0) continue;
                var e = r + recBatch < maxId ? r + recBatch - 1 : maxId;
                var feedback = $"Updated {res} {dataType} fields, ids {r} to {e}";
                _loggingHelper.LogLine(feedback);
                updated += res;
            }
            return updated;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In study search update ({dataType}): {e.Message}");
            return 0;
        }
    }

    // Used twice times in updating the search_studies table with feature data.

    public int UpdateSearchFeatureData(string sqlString, string dataType, int minId, int maxId)
    {
        try
        {
            var transferred = 0;
            const int recBatch = 20000;
            for (var r = minId; r <= maxId; r += recBatch)
            {
                var batch_sql_string = sqlString + $" and ss.study_id >= {r} and ss.study_id < {r + recBatch} ";
                var res = ExecuteSql(batch_sql_string);
                if (res <= 0) continue;
                var e = r + recBatch < maxId ? r + recBatch - 1 : maxId;
                var feedback = $"Updated {res} records in study_search table with {dataType} data, ids {r} to {e}";
                _loggingHelper.LogLine(feedback);
                transferred += res;
            }
            return transferred;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In study search update ({dataType}): {e.Message}");
            return 0;
        }
    }

    // Used in the search setup process to collect the 'has an object of type X' data.
    // Called 16 times, i.e. for each object type

    public int CollectHasObjectData(string whereString, int bitPos, string objectType)
    {
        try
        {
            var setup_sql = $"""
                             INSERT INTO core.temp_searchobjects(study_id, bit_pos)
                                                     SELECT DISTINCT k.study_id, {bitPos}
                                                     from core.study_object_links k
                                                     inner join core.data_objects b
                                                     on k.object_id = b.id
                                                     where 
                             """ + whereString;
            return ExecuteSql(setup_sql);
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In collecting object data (has_{objectType}) : {e.Message}");
            return 0;
        }
    }

    // Used in the search setup process to transfer the 'has an object of type X' data to
    // a bitmap. Called 16 times, for each object type, from within a single loop.

    public int UpdateBitMap(string sqlString, int n, int minId, int maxId)
    {
        try
        {
            var updated = 0;
            const int recBatch = 20000;
            for (var r = minId; r <= maxId; r += recBatch)
            {
                var batch_sql_string = sqlString + $" and ss.study_id >= {r} and ss.study_id < {r + recBatch} ";
                var res = ExecuteSql(batch_sql_string);
                if (res <= 0) continue;
                // var e = r + recBatch < maxId ? r + recBatch - 1 : maxId;
                updated += res;
            }
            return updated;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In bit map update (n = {n}): {e.Message}");
            return 0;
        }
    }

    // Used to update the search_studies table with lists of countries and conditions,
    // as two separate sets of calls.

    public int UpdateListData(string topSql, string bottomSql, int minId, int maxId, string listType)
    {
        try
        {
            var updated = 0;
            const int recBatch = 20000;
            for (var r = minId; r <= maxId; r += recBatch)
            {
                var batch_sql_string = topSql
                                       + $" where sc.study_id >= {r} and sc.study_id < {r + recBatch} " +
                                       bottomSql;
                var res = ExecuteSql(batch_sql_string);
                if (res <= 0) continue;
                var e = r + recBatch < maxId ? r + recBatch - 1 : maxId;
                var feedback = $"Updated {res} {listType} fields, ids {r} to {e}";
                _loggingHelper.LogLine(feedback);
                updated += res;
            }
            return updated;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In {listType} update: {e.Message}");
            return 0;
        }
    }

    // Used to create the data in the search_idents table

    public int CreateSearchIdentsData(string topSql, string bottomSql, int minId, int maxId, string listType)
    {
        try
        {
            var created = 0;
            const int recBatch = 50000;
            for (var r = minId; r <= maxId; r += recBatch)
            {
                var batch_sql_string = topSql
                                       + $" and si.study_id >= {r} and si.study_id < {r + recBatch} " +
                                       bottomSql;
                var res = ExecuteSql(batch_sql_string);
                if (res <= 0) continue;
                var e = r + recBatch < maxId ? r + recBatch - 1 : maxId;
                var feedback = $"Updated {res} {listType} fields, ids {r} to {e}";
                _loggingHelper.LogLine(feedback);
                created += res;
            }
            return created;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In {listType} update: {e.Message}");
            return 0;
        }
    }

    public int CreateSearchCountriesData(string topSql, int minId, int maxId, string listType)
    {
        try
        {
            var created = 0;
            const int recBatch = 50000;
            for (var r = minId; r <= maxId; r += recBatch)
            {
                var batch_sql_string = topSql
                                       + $" and sc.study_id >= {r} and sc.study_id < {r + recBatch} ";
                var res = ExecuteSql(batch_sql_string);
                if (res <= 0) continue;
                var e = r + recBatch < maxId ? r + recBatch - 1 : maxId;
                var feedback = $"Updated {res} {listType} fields, ids {r} to {e}";
                _loggingHelper.LogLine(feedback);
                created += res;
            }
            return created;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In {listType} update: {e.Message}");
            return 0;
        }
    }


    // Used during construction of object search data. Called 3 times.

    public int UpdateObjectSearchData(string sqlString, int minId, int maxId, string qualifier, string fieldType)
    {
        try
        {
            var updated = 0;
            const int recBatch = 20000;
            for (var r = minId; r <= maxId; r += recBatch)
            {
                var batch_sql_string = sqlString + qualifier
                                                  + $" so.study_id >= {r} and so.study_id < {r + recBatch} ";
                var res = ExecuteSql(batch_sql_string);
                if (res <= 0) continue;
                var e = r + recBatch < maxId ? r + recBatch - 1 : maxId;
                var feedback = $"Updated {res} {fieldType} fields, ids {r} to {e}";
                _loggingHelper.LogLine(feedback);
                updated += res;
            }
            return updated;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In {fieldType} update: {e.Message}");
            return 0;
        }
    }

    // Used (3 times) within the search setup process for the titles and topic indexing process.

    public int CreateLexSql(string sqlString, string dataType, string fullTableName)
    {
        try
        {
            var created = 0;
            var min_id = GetAggMinId(fullTableName);
            var max_id = GetAggMaxId(fullTableName);
            const int recBatch = 25000;
            for (var r = min_id; r <= max_id; r += recBatch)
            {
                var batch_sql_string = sqlString + $" where s.id >= {r} and s.id < {r + recBatch} ";
                created += ExecuteSql(batch_sql_string);
                var e = r + recBatch < max_id ? r + recBatch - 1 : max_id;
                var feedback = $"Updated {dataType} data, ids {r} to {e}";
                _loggingHelper.LogLine(feedback);
            }
            return created;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In create lexemes ({dataType}): {e.Message}");
            return 0;
        }
    }

    // Used (3 times) within the search setup process for the titles and topic indexing process.
    public int AggregateLexDataByStudy(string sqlString, string fullTableName, int minId, int maxId)
    {
        try
        {
            var transferred = 0;
            const int recBatch = 25000;
            // int rec_batch = 10000;  // for testing 
            if (maxId - minId > recBatch)
            {
                for (var r = minId; r <= maxId; r += recBatch)
                {
                    var batch_sql_string = sqlString
                                           + $" where study_id >= {r} and study_id < {r + recBatch} ";
                    batch_sql_string += " group by study_id";
                    transferred += ExecuteSql(batch_sql_string);
                    var e = r + recBatch < maxId ? r + recBatch - 1 : maxId;
                    var feedback = $"Updated {fullTableName} data, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                }
            }
            else
            {
                transferred = ExecuteSql(sqlString);
                _loggingHelper.LogLine($"Updated {fullTableName} data, as a single batch");
            }
            return transferred;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In AggregateLexDataByStudy ({fullTableName} in search table: {e.Message}");
            return 0;
        }
    }


    // Used 2 times in the set up process for titles, topics and conditions indexing
    // Uses study id to go through records because records must be grouped by study
    public int TransferSearchDataByStudy(string sqlString, string dataType, int minId, int maxId)
    {
        try
        {
            var transferred = 0;
            const int recBatch = 20000;
            for (var r = minId; r <= maxId; r += recBatch)
            {
                var batch_sql_string = sqlString + $" and s.study_id >= {r} and s.study_id < {r + recBatch} ";
                var res = ExecuteSql(batch_sql_string);
                if (res <= 0) continue;
                var e = r + recBatch < maxId ? r + recBatch - 1 : maxId;
                var feedback = $"Updated search_lexemes table with {dataType} data, ids {r} to {e}";
                _loggingHelper.LogLine(feedback);
                transferred += res;
            }
            return transferred;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In TransferSearchDataByStudy ({dataType}): {e.Message}");
            return 0;
        }
    }


    public int UpdateStudyFeatureList(string sqlString, int minId, int maxId, string dataType)
    {
        try
        {
            var transferred = 0;
            const int recBatch = 20000;
            for (var r = minId; r <= maxId; r += recBatch)
            {
                var batch_sql_string = sqlString + $" and ss.study_id >= {r} and ss.study_id < {r + recBatch} ";
                var res = ExecuteSql(batch_sql_string);
                if (res <= 0) continue;
                var e = r + recBatch < maxId ? r + recBatch - 1 : maxId;
                var feedback = $"Updated search_studies table with {dataType} data, ids {r} to {e}";
                _loggingHelper.LogLine(feedback);
                transferred += res;
            }
            return transferred;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In UpdateStudyFeatureList ({dataType}s): {e.Message}");
            return 0;
        }
    }


    public int UpdateSearchStudyObjectJson(int minId, int maxId, string listType)
    {
        try
        {
            var updated = 0;
            const int recBatch = 20000;
            for (var r = minId; r <= maxId; r += recBatch)
            {
                var batch_sql_string = $"""
                                        Update core.search_studies ss
                                                           set object_json = b.obj_json
                                                        from
                                                            (select study_id, json_agg(object_json) as obj_json
                                                            from core.search_objects 
                                                            where study_id >= {r}
                                                            and study_id < {r + recBatch} 
                                                            group by study_id) b
                                                            where ss.study_id = b.study_id 
                                        """;

                var res = ExecuteSql(batch_sql_string);
                if (res <= 0) continue;
                var e = r + recBatch < maxId ? r + recBatch - 1 : maxId;
                var feedback = $"Updated {res} {listType} fields, ids {r} to {e}";
                _loggingHelper.LogLine(feedback);
                updated += res;
            }
            return updated;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In {listType} update: {e.Message}");
            return 0;
        }
    }

    public int UpdateStudyJson(string sqlString, int minId, int maxId, string dataType)
    {
        try
        {
            var transferred = 0;
            const int recBatch = 20000;
            for (var r = minId; r <= maxId; r += recBatch)
            {
                var batch_sql_string = sqlString + $" where ss.study_id >= {r} and ss.study_id < {r + recBatch} ";
                var res = ExecuteSql(batch_sql_string);
                if (res <= 0) continue;
                var e = r + recBatch < maxId ? r + recBatch - 1 : maxId;
                var feedback = $"Updated search_studies table with {dataType} data, ids {r} to {e}";
                _loggingHelper.LogLine(feedback);
                transferred += res;
            }
            return transferred;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In UpdateStudyFeatureList ({dataType}s): {e.Message}");
            return 0;
        }
    }

    public int TransferStudyJson(string sqlString, int minId, int maxId, string dataType)
    {
        try
        {
            var transferred = 0;
            const int recBatch = 10000;
            for (var r = minId; r <= maxId; r += recBatch)
            {
                var batch_sql_string = sqlString + $" and s.study_id >= {r} and s.study_id < {r + recBatch} ";
                var res = ExecuteSql(batch_sql_string);
                if (res <= 0) continue;
                var e = r + recBatch < maxId ? r + recBatch - 1 : maxId;
                var feedback = $"Updated {dataType} table with study json data, ids {r} to {e}";
                _loggingHelper.LogLine(feedback);
                transferred += res;
            }
            return transferred;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In UpdateStudyFeatureList ({dataType}s): {e.Message}");
            return 0;
        }
    }
}