﻿using Dapper;
using Dapper.Contrib.Extensions;
using Npgsql;
using PostgreSQLCopyHelper;

namespace MDR_Aggregator;

public class MonDataLayer : IMonDataLayer
{
    private readonly ICredentials _credentials;
    private readonly string monConnString;
    private Source? source;

    public MonDataLayer(ICredentials credentials)
    {
        _credentials = credentials;
        monConnString = credentials.GetConnectionString("mon", false);
    }

    public ICredentials Credentials => _credentials;
    
    public Source FetchSourceParameters(int source_id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(monConnString);
        source = Conn.Get<Source>(source_id);
        return source;
    }

    public string GetConnectionString(string databaseName, bool testing)
    {
        return _credentials.GetConnectionString(databaseName, testing);
    }
    
    
    public List<string> SetUpTempFTWs(ICredentials credentials, string dbConnString, string fdw_schema, 
                                      string source_db, List<string> source_schemas)
    {
        using var conn = new NpgsqlConnection(dbConnString);
        string username = credentials.Username;
        string password = credentials.Password;

        string sql_string = $"CREATE EXTENSION IF NOT EXISTS postgres_fdw schema {fdw_schema};";
        conn.Execute(sql_string);

        sql_string = $@"CREATE SERVER IF NOT EXISTS {source_db}
                        FOREIGN DATA WRAPPER postgres_fdw
                        OPTIONS (host 'localhost', dbname {source_db});";
        conn.Execute(sql_string);

        sql_string = $@"CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
                        SERVER {source_db} 
                        OPTIONS (user '{username}', password '{password}');";
        conn.Execute(sql_string);

        List<string> schema_names = new();
        foreach(string schema in source_schemas)
        {
            string schema_name = $"{source_db}_{schema}";
            sql_string = $@"DROP SCHEMA IF EXISTS {schema_name} cascade;
                             CREATE SCHEMA {schema_name};
                             IMPORT FOREIGN SCHEMA {schema}
                             FROM SERVER {source_db} 
                             INTO {schema_name};";
            conn.Execute(sql_string);
            schema_names.Add(schema_name);
        }
        return schema_names;
    }
    
    
    public void DropTempFTWs(string dbConnString, string source_db, List<string> source_schemas)
    {
        using var conn = new NpgsqlConnection(dbConnString);
        string sql_string = $"DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER {source_db};";
        conn.Execute(sql_string);

        sql_string = $@"DROP SERVER IF EXISTS {source_db} CASCADE;";
        conn.Execute(sql_string);

        foreach(string schema in source_schemas)
        {
            string schema_name = $"{source_db}_{schema}";
            sql_string = $@"DROP SCHEMA IF EXISTS {schema_name};";
            conn.Execute(sql_string);
        }
    }
    
    
    
    public void SetUpTempContextFTWs(ICredentials credentials, string dbConnString)
    {
        using var conn = new NpgsqlConnection(dbConnString);
        string username = credentials.Username;
        string password = credentials.Password;

        string sql_string = @"CREATE EXTENSION IF NOT EXISTS postgres_fdw
                                 schema sf;";
        conn.Execute(sql_string);

        sql_string = @"CREATE SERVER IF NOT EXISTS context
                           FOREIGN DATA WRAPPER postgres_fdw
                           OPTIONS (host 'localhost', dbname 'context');";
        conn.Execute(sql_string);

        sql_string = $@"CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
                 SERVER context 
                 OPTIONS (user '{username}', password '{password}');";
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


    public void DropTempContextFTWs(string dbConnString)
    {
        using var conn = new NpgsqlConnection(dbConnString);
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


    public string SetUpTempCoreFTW(ICredentials credentials, string database_name, string dbConnString)
    {
        using var conn = new NpgsqlConnection(dbConnString);
        string username = credentials.Username;
        string password = credentials.Password;     
            
        string sql_string = $@"CREATE EXTENSION IF NOT EXISTS postgres_fdw
                                 schema core;";
        conn.Execute(sql_string);

        sql_string = $@"CREATE SERVER IF NOT EXISTS {database_name} FOREIGN DATA WRAPPER postgres_fdw
                         OPTIONS (host 'localhost', dbname '{database_name}');";
        conn.Execute(sql_string);

        sql_string = $@"CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
                 SERVER {database_name} OPTIONS (user '{username}', password '{password}');";
        conn.Execute(sql_string);
        string schema_name;
        if (database_name == "mon")
        {
            schema_name = database_name + "_sf";
            sql_string = $@"DROP SCHEMA IF EXISTS {schema_name} cascade;
                 CREATE SCHEMA {schema_name};
                 IMPORT FOREIGN SCHEMA sf
                 FROM SERVER {database_name} INTO {schema_name};";
        }
        else if (database_name == "aggs")
        {
            schema_name = database_name + "_nk";
            sql_string = $@"DROP SCHEMA IF EXISTS {schema_name} cascade;
                 CREATE SCHEMA {schema_name};
                 IMPORT FOREIGN SCHEMA nk
                 FROM SERVER {database_name} INTO {schema_name};";
        }
        else
        {
            schema_name = database_name + "_ad";
            sql_string = $@"DROP SCHEMA IF EXISTS {schema_name} cascade;
                 CREATE SCHEMA {schema_name};
                 IMPORT FOREIGN SCHEMA ad
                 FROM SERVER {database_name} INTO {schema_name};";
        }
        conn.Execute(sql_string);
        return schema_name;
    }

    public string SetUpTempIECFTW(ICredentials credentials, string database_name, string dbConnString)
    {
        using var conn = new NpgsqlConnection(dbConnString);
        string username = credentials.Username;
        string password = credentials.Password;     
            
        string sql_string = $@"CREATE EXTENSION IF NOT EXISTS postgres_fdw
                                 schema ad;";
        conn.Execute(sql_string);

        sql_string = $@"CREATE SERVER IF NOT EXISTS {database_name} FOREIGN DATA WRAPPER postgres_fdw
                         OPTIONS (host 'localhost', dbname '{database_name}');";
        conn.Execute(sql_string);

        sql_string = $@"CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
                 SERVER {database_name} OPTIONS (user '{username}', password '{password}');";
        conn.Execute(sql_string);
        string schema_name;
        if (database_name == "mon")
        {
            schema_name = database_name + "_sf";
            sql_string = $@"DROP SCHEMA IF EXISTS {schema_name} cascade;
                 CREATE SCHEMA {schema_name};
                 IMPORT FOREIGN SCHEMA sf
                 FROM SERVER {database_name} INTO {schema_name};";
        }
        else if (database_name == "aggs")
        {
            schema_name = database_name + "_nk";
            sql_string = $@"DROP SCHEMA IF EXISTS {schema_name} cascade;
                 CREATE SCHEMA {schema_name};
                 IMPORT FOREIGN SCHEMA nk
                 FROM SERVER {database_name} INTO {schema_name};";
        }
        else
        {
            schema_name = database_name + "_ad";
            sql_string = $@"DROP SCHEMA IF EXISTS {schema_name} cascade;
                 CREATE SCHEMA {schema_name};
                 IMPORT FOREIGN SCHEMA ad
                 FROM SERVER {database_name} INTO {schema_name};";
        }
        conn.Execute(sql_string);
        return schema_name;
    }
    
    public void DropTempIECFTW(string database_name, string dbConnString)
    {
        using var conn = new NpgsqlConnection(dbConnString);
        string schema_name;
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
    
    
    public void DropTempCoreFTW(string database_name, string dbConnString)
    {
        using var conn = new NpgsqlConnection(dbConnString);
        string schema_name;
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


    public void SetUpTempAggsFTW(ICredentials credentials, string dbConnString)
    {
        using var conn = new NpgsqlConnection(dbConnString);
        string username = credentials.Username;
        string password = credentials.Password;     
            
        string sql_string = @"CREATE EXTENSION IF NOT EXISTS postgres_fdw
                                 schema core;";
        conn.Execute(sql_string);

        sql_string = @"CREATE SERVER IF NOT EXISTS aggs  FOREIGN DATA WRAPPER postgres_fdw
                         OPTIONS (host 'localhost', dbname 'aggs');";
        conn.Execute(sql_string);

        sql_string = $@"CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
                 SERVER aggs OPTIONS (user '{username}', password '{password}');";
        conn.Execute(sql_string);
        string schema_name;
        
        schema_name = "aggs_st";
        sql_string = $@"DROP SCHEMA IF EXISTS {schema_name} cascade;
             CREATE SCHEMA {schema_name};
             IMPORT FOREIGN SCHEMA st
             FROM SERVER aggs INTO {schema_name};";
        conn.Execute(sql_string);
        
        schema_name = "aggs_ob";
        sql_string =$@"DROP SCHEMA IF EXISTS {schema_name} cascade;
             CREATE SCHEMA {schema_name};
             IMPORT FOREIGN SCHEMA ob
             FROM SERVER aggs INTO {schema_name};";
        conn.Execute(sql_string);
        
        schema_name = "aggs_nk";
        sql_string = $@"DROP SCHEMA IF EXISTS {schema_name} cascade;
             CREATE SCHEMA {schema_name};
             IMPORT FOREIGN SCHEMA nk
             FROM SERVER aggs INTO {schema_name};";
        conn.Execute(sql_string);
    }

    public void DropTempAggsFTW(string dbConnString)
    {
        using var conn = new NpgsqlConnection(dbConnString);
        
        string sql_string = @"DROP USER MAPPING IF EXISTS FOR CURRENT_USER
                 SERVER aggs;";
        conn.Execute(sql_string);

        sql_string = @"DROP SERVER IF EXISTS aggs CASCADE;";
        conn.Execute(sql_string);

        sql_string = @"DROP SCHEMA IF EXISTS aggs_st;";
        conn.Execute(sql_string);
        
        sql_string = @"DROP SCHEMA IF EXISTS aggs_ob;";
        conn.Execute(sql_string);
        
        sql_string = @"DROP SCHEMA IF EXISTS aggs_nk;";
        conn.Execute(sql_string);
    }
    
    
    public IEnumerable<Source> RetrieveDataSources()
    {
        string sql_string = @"select id, preference_rating, database_name, study_iec_storage_type,
                              has_study_tables,	has_study_topics, has_study_conditions, has_study_features,
                              has_study_people, has_study_organisations, 
                              has_study_references, has_study_relationships,
                              has_study_countries, has_study_locations,
                              has_object_datasets, has_object_dates, has_object_rights,
                              has_object_relationships, has_object_pubmed_set 
                            from sf.source_parameters
                            where is_current_agg_source = true
                            order by preference_rating;";

        using var conn = new NpgsqlConnection(monConnString);
        return conn.Query<Source>(sql_string);
    }

    public IEnumerable<Source> RetrieveIECDataSources()
    {
        string sql_string = @"select id, preference_rating, database_name, study_iec_storage_type,
                              has_study_tables,	has_study_topics, has_study_conditions, has_study_features,
                              has_study_people, has_study_organisations, 
                              has_study_references, has_study_relationships,
                              has_study_countries, has_study_locations,
                              has_object_datasets, has_object_dates, has_object_rights,
                              has_object_relationships, has_object_pubmed_set 
                            from sf.source_parameters
                            where study_iec_storage_type <> 'n/a';";
        
        using var conn = new NpgsqlConnection(monConnString);
        return conn.Query<Source>(sql_string);
    }
    
    public int GetRecNum(string table_name, string source_conn_string)
    {
        string sql_string = "SELECT count(*) from ad." + table_name;
        using var conn = new NpgsqlConnection(source_conn_string);
        var rec_num = conn.ExecuteScalar<int?>(sql_string);
        return rec_num ?? 0;
    }
    
    public int GetNextAggEventId()
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(monConnString);
        string sql_string = "select max(id) from sf.aggregation_events ";
        int? last_id = Conn.ExecuteScalar<int?>(sql_string);
        return (last_id == null) ? 100001 : (int)last_id + 1;
    }
    
    public int GetNextIECAggEventId()
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(monConnString);
        string sql_string = "select max(id) from sf.iec_agg_events ";
        int? last_id = Conn.ExecuteScalar<int?>(sql_string);
        return (last_id == null) ? 100001 : (int)last_id + 1;
    }

    public int GetLastAggEventId()
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(monConnString);
        string sql_string = "select max(id) from sf.aggregation_events ";
        int? last_id = Conn.ExecuteScalar<int?>(sql_string);
        return last_id ?? 0;
    }

    public CoreSummary? GetLatestCoreSummary()
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(monConnString);
        string sql_string = "select max(aggregation_event_id) from sf.aggregation_summaries ";
        int? last_id = Conn.ExecuteScalar<int?>(sql_string);
        if (last_id.HasValue)
        {
            sql_string = $@"select * from sf.aggregation_summaries 
                               where aggregation_event_id = {last_id}";
            return Conn.Query<CoreSummary?>(sql_string).FirstOrDefault();
        }
        return null;  // as a fallback

    }

    public List<AggregationObjectNum>? GetLatestObjectNumbers()
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(monConnString);
        string sql_string = "select max(aggregation_event_id) from sf.aggregation_object_numbers ";
        int? last_id = Conn.ExecuteScalar<int?>(sql_string);
        if (last_id.HasValue)
        {
            sql_string = $@"select * from sf.aggregation_object_numbers
                               where aggregation_event_id = {last_id}
                               order by number_of_type desc";
            return Conn.Query<AggregationObjectNum>(sql_string)?.ToList();
        }
        return null;  // as a fallback
    }
    
    public int StoreAggregationEvent(AggregationEvent aggregation)
    {
        aggregation.time_ended = DateTime.Now;
        using var conn = new NpgsqlConnection(monConnString);
        return (int)conn.Insert(aggregation);
    }
    
    public int StoreIECAggregationEvent(IECAggregationEvent iec_agg)
    {
        iec_agg.time_ended = DateTime.Now;
        using var conn = new NpgsqlConnection(monConnString);
        return (int)conn.Insert(iec_agg);
    }

    public void StoreSourceIECData(int iec_agg_id, Source source, Int64 res)
    {
        IECAggregationSourceNum summ_rec = new(iec_agg_id, source.id, source.database_name, res);
        using var conn = new NpgsqlConnection(monConnString);
        conn.Insert(summ_rec);
    }

    public void UpdateIECAggregationEvent(IECAggregationEvent iec_agg_event, string iec_conn_string)
    {
        iec_agg_event.iec_null_recs = GetRecNum("iec_null_recs", iec_conn_string);
        iec_agg_event.iec_pre06_recs = GetRecNum("iec_null_recs", iec_conn_string);
        iec_agg_event.iec_0608_recs = GetRecNum("iec_null_recs", iec_conn_string);
        iec_agg_event.iec_0910_recs = GetRecNum("iec_null_recs", iec_conn_string);
        iec_agg_event.iec_1112_recs = GetRecNum("iec_null_recs", iec_conn_string);
        iec_agg_event.iec_1314_recs = GetRecNum("iec_null_recs", iec_conn_string);
        iec_agg_event.iec_15_recs = GetRecNum("iec_null_recs", iec_conn_string);
        iec_agg_event.iec_16_recs = GetRecNum("iec_null_recs", iec_conn_string);
        iec_agg_event.iec_17_recs = GetRecNum("iec_null_recs", iec_conn_string);
        iec_agg_event.iec_18_recs = GetRecNum("iec_null_recs", iec_conn_string);
        iec_agg_event.iec_19_recs = GetRecNum("iec_null_recs", iec_conn_string);
        iec_agg_event.iec_20_recs = GetRecNum("iec_null_recs", iec_conn_string);
        iec_agg_event.iec_21_recs = GetRecNum("iec_null_recs", iec_conn_string);
        iec_agg_event.iec_22_recs = GetRecNum("iec_null_recs", iec_conn_string);
        iec_agg_event.iec_23_recs = GetRecNum("iec_null_recs", iec_conn_string);
        iec_agg_event.iec_24_recs = GetRecNum("iec_null_recs", iec_conn_string);
        iec_agg_event.iec_25_recs = GetRecNum("iec_null_recs", iec_conn_string);
        iec_agg_event.iec_26_recs = GetRecNum("iec_null_recs", iec_conn_string);
        iec_agg_event.iec_27_recs = GetRecNum("iec_null_recs", iec_conn_string);
        iec_agg_event.iec_28_recs = GetRecNum("iec_null_recs", iec_conn_string);
        iec_agg_event.iec_29_recs = GetRecNum("iec_null_recs", iec_conn_string);
        iec_agg_event.iec_30_recs = GetRecNum("iec_null_recs", iec_conn_string);
        
        iec_agg_event.total_records_imported = (iec_agg_event.iec_null_recs ?? 0) +
            (iec_agg_event.iec_pre06_recs ?? 0) + (iec_agg_event.iec_0608_recs ?? 0) + 
            (iec_agg_event.iec_0910_recs ?? 0) + (iec_agg_event.iec_1112_recs ?? 0) +
            (iec_agg_event.iec_1314_recs ?? 0) + (iec_agg_event.iec_15_recs ?? 0) +
            (iec_agg_event.iec_16_recs ?? 0) + (iec_agg_event.iec_17_recs ?? 0) +
            (iec_agg_event.iec_18_recs ?? 0) + (iec_agg_event.iec_19_recs ?? 0) +
            (iec_agg_event.iec_20_recs ?? 0) + (iec_agg_event.iec_21_recs ?? 0) +
            (iec_agg_event.iec_22_recs ?? 0) + (iec_agg_event.iec_23_recs ?? 0)+
            (iec_agg_event.iec_24_recs ?? 0) + (iec_agg_event.iec_25_recs ?? 0) +
            (iec_agg_event.iec_26_recs ?? 0) + (iec_agg_event.iec_27_recs ?? 0) +
            (iec_agg_event.iec_28_recs ?? 0) + (iec_agg_event.iec_29_recs ?? 0) +
            (iec_agg_event.iec_30_recs ?? 0);
    }
    

    public void DeleteSameEventDBStats(int agg_event_id)
    {
        string sql_string = $@"DELETE from sf.source_summaries 
                              where aggregation_event_id = {agg_event_id}";
        using var conn = new NpgsqlConnection(monConnString);
        conn.Execute(sql_string);
    }

    
    public void DeleteSameEventSummaryStats(int agg_event_id)
    {
        string sql_string = $@"DELETE from sf.aggregation_summaries 
                               where aggregation_event_id = {agg_event_id}";
        using var conn = new NpgsqlConnection(monConnString);
        conn.Execute(sql_string);
    }


    public int GetAggregateRecNum(string table_name, string schema_name, string source_conn_string)
    {
        string sql_string = "SELECT count(*) from " + schema_name + "." + table_name;
        using var conn = new NpgsqlConnection(source_conn_string);
        return conn.ExecuteScalar<int?>(sql_string) ?? 0;
    }


    public void StoreSourceSummary(SourceSummary sm)
    {
        using var conn = new NpgsqlConnection(monConnString);
        conn.Insert(sm);
    }


    public void StoreCoreSummary(CoreSummary asm)
    {
        using var conn = new NpgsqlConnection(monConnString);
        conn.Insert(asm);
    }


    public void DeleteSameEventObjectStats(int agg_event_id)
    {
        string sql_string = $@"DELETE from sf.aggregation_object_numbers 
                            where aggregation_event_id = {agg_event_id}";
        using var conn = new NpgsqlConnection(monConnString);
        conn.Execute(sql_string);
    }


    public List<AggregationObjectNum> GetObjectTypes(int aggregation_event_id, string dest_conn_string)
    {
        string sql_string = $@"SELECT {aggregation_event_id} as aggregation_event_id, 
                d.object_type_id, 
                t.name as object_type_name,
                count(d.id) as number_of_type
                from core.data_objects d
                inner join context_lup.object_types t
                on d.object_type_id = t.id
                group by object_type_id, t.name
                order by count(d.id) desc";

        using var conn = new NpgsqlConnection(dest_conn_string);
        return conn.Query<AggregationObjectNum>(sql_string).ToList();
    }


    public void RecreateStudyStudyLinksTable()
    {
        using var conn = new NpgsqlConnection(monConnString);
        string sql_string = "DROP TABLE IF EXISTS sf.study_study_link_data ";
        conn.Execute(sql_string);

        sql_string = @"CREATE TABLE sf.study_study_link_data
            (
               id                   int NOT NULL GENERATED BY DEFAULT AS IDENTITY(INCREMENT 1 START 10000001)
             , source_id            int
             , source_name          varchar
             , other_source_id      int
             , other_source_name    varchar
             , number_in_other_source int
            )";
        conn.Execute(sql_string);
    }

    public List<StudyStudyLinkData> GetStudyStudyLinkData(int aggregation_event_id, string dest_conn_string)
    {
        string sql_string = @"SELECT 
                k.source_id, 
                d1.default_name as source_name,
                k.preferred_source_id as other_source_id,
                d2.default_name as other_source_name,
                count(preferred_sd_sid) as number_in_other_source
                from aggs_nk.study_study_links k
                inner join context_ctx.data_sources d1
                on k.source_id = d1.id
                inner join context_ctx.data_sources d2
                on k.preferred_source_id = d2.id
                group by source_id, preferred_source_id, d1.default_name, d2.default_name;";

        using var conn = new NpgsqlConnection(dest_conn_string);
        return conn.Query<StudyStudyLinkData>(sql_string).ToList();
    }

    public List<StudyStudyLinkData> GetStudyStudyLinkData2(int aggregation_event_id, string dest_conn_string)
    {
        string sql_string = @"SELECT 
              k.preferred_source_id as source_id, 
              d2.default_name as source_name,
              k.source_id as other_source_id,
              d1.default_name as other_source_name,
              count(sd_sid) as number_in_other_source
              from aggs_nk.study_study_links k
              inner join context_ctx.data_sources d1
              on k.source_id = d1.id
              inner join context_ctx.data_sources d2
              on k.preferred_source_id = d2.id
             group by preferred_source_id, source_id, d2.default_name, d1.default_name;";

        using var conn = new NpgsqlConnection(dest_conn_string);
        return conn.Query<StudyStudyLinkData>(sql_string).ToList();
    }



    public ulong StoreObjectNumbers(PostgreSQLCopyHelper<AggregationObjectNum> copyHelper, 
                                     IEnumerable<AggregationObjectNum> entities)
    {
        // stores the study id data in a temporary table
        using var conn = new NpgsqlConnection(monConnString);
        conn.Open();
        return copyHelper.SaveAll(conn, entities);
    }


    public ulong StoreStudyLinkNumbers(PostgreSQLCopyHelper<StudyStudyLinkData> copyHelper,
                                    IEnumerable<StudyStudyLinkData> entities)
    {
        // stores the study id data in a temporary table
        using var conn = new NpgsqlConnection(monConnString);
        conn.Open();
        return copyHelper.SaveAll(conn, entities);
    }
}

