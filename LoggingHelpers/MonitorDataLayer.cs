using Dapper;
using Dapper.Contrib.Extensions;
using Npgsql;
using PostgreSQLCopyHelper;

namespace MDR_Aggregator;

public class MonDataLayer : IMonDataLayer
{
    private readonly ICredentials _credentials;
    private readonly string monConnString;
    private readonly string aggConnString;
    
    public MonDataLayer(ICredentials credentials)
    {
        _credentials = credentials;
        monConnString = credentials.GetConnectionString("mon");
        aggConnString = credentials.GetConnectionString("aggs");
    }

    public ICredentials Credentials => _credentials;
    
    public Source FetchSourceParameters(int source_id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(monConnString);
        return Conn.Get<Source>(source_id);
    }

    public string GetConnectionString(string databaseName)
    {
        return _credentials.GetConnectionString(databaseName);
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
                        OPTIONS (host 'localhost', dbname '{source_db}');";
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
    
    
    public IEnumerable<Source> RetrieveDataSources()
    {
        string sql_string = @"select id, preference_rating, database_name, repo_name, study_iec_storage_type,
                              has_study_tables,	has_study_topics, has_study_conditions, has_study_features,
                              has_study_people, has_study_organisations, 
                              has_study_references, has_study_relationships,
                              has_study_countries, has_study_locations,
                              has_object_datasets, has_object_instances, has_object_dates,
                              has_object_descriptions, has_object_identifiers, 
                              has_object_people, has_object_organisations, has_object_topics,
                              has_object_rights, has_object_relationships
                            from sf.source_parameters
                            where is_current_agg_source = true
                            order by preference_rating;";

        using var conn = new NpgsqlConnection(monConnString);
        return conn.Query<Source>(sql_string);
    }

    public IEnumerable<Source> RetrieveIECDataSources()
    {
        string sql_string = @"select id, preference_rating, database_name, repo_name, study_iec_storage_type,
                              has_study_tables,	has_study_topics, has_study_conditions, has_study_features,
                              has_study_people, has_study_organisations, 
                              has_study_references, has_study_relationships,
                              has_study_countries, has_study_locations,
                              has_object_datasets, has_object_instances, has_object_dates,
                              has_object_descriptions, has_object_identifiers, 
                              has_object_people, has_object_organisations, has_object_topics,
                              has_object_rights, has_object_relationships
                            from sf.source_parameters
                            where study_iec_storage_type <> 'n/a' 
                              and is_current_agg_source = true;";
        
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
        string sql_string = "select max(id) from sf.agg_events ";
        int? last_id = Conn.ExecuteScalar<int?>(sql_string);
        return (last_id == null) ? 100001 : (int)last_id + 1;
    }
    
    public int GetNextIECAggEventId()
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(monConnString);
        string sql_string = "select max(id) from sf.agg_iec_events ";
        int? last_id = Conn.ExecuteScalar<int?>(sql_string);
        return (last_id == null) ? 100001 : (int)last_id + 1;
    }

    public int GetLastAggEventId()
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(monConnString);
        string sql_string = "select max(id) from sf.agg_events ";
        int? last_id = Conn.ExecuteScalar<int?>(sql_string);
        return last_id ?? 0;
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
        iec_agg_event.iec_null_recs = GetRecNum("study_iec_null", iec_conn_string);
        iec_agg_event.iec_pre06_recs = GetRecNum("study_iec_pre06", iec_conn_string);
        iec_agg_event.iec_0608_recs = GetRecNum("study_iec_0608", iec_conn_string);
        iec_agg_event.iec_0910_recs = GetRecNum("study_iec_0910", iec_conn_string);
        iec_agg_event.iec_1112_recs = GetRecNum("study_iec_1112", iec_conn_string);
        iec_agg_event.iec_1314_recs = GetRecNum("study_iec_1314", iec_conn_string);
        iec_agg_event.iec_15_recs = GetRecNum("study_iec_15", iec_conn_string);
        iec_agg_event.iec_16_recs = GetRecNum("study_iec_16", iec_conn_string);
        iec_agg_event.iec_17_recs = GetRecNum("study_iec_17", iec_conn_string);
        iec_agg_event.iec_18_recs = GetRecNum("study_iec_18", iec_conn_string);
        iec_agg_event.iec_19_recs = GetRecNum("study_iec_19", iec_conn_string);
        iec_agg_event.iec_20_recs = GetRecNum("study_iec_20", iec_conn_string);
        iec_agg_event.iec_21_recs = GetRecNum("study_iec_21", iec_conn_string);
        iec_agg_event.iec_22_recs = GetRecNum("study_iec_22", iec_conn_string);
        iec_agg_event.iec_23_recs = GetRecNum("study_iec_23", iec_conn_string);
        iec_agg_event.iec_24_recs = GetRecNum("study_iec_24", iec_conn_string);
        iec_agg_event.iec_25_recs = GetRecNum("study_iec_25", iec_conn_string);
        iec_agg_event.iec_26_recs = GetRecNum("study_iec_26", iec_conn_string);
        iec_agg_event.iec_27_recs = GetRecNum("study_iec_27", iec_conn_string);
        iec_agg_event.iec_28_recs = GetRecNum("study_iec_28", iec_conn_string);
        iec_agg_event.iec_29_recs = GetRecNum("study_iec_29", iec_conn_string);
        iec_agg_event.iec_30_recs = GetRecNum("study_iec_30", iec_conn_string);
        
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
        string sql_string = $@"DELETE from sf.agg_source_summaries 
                              where agg_event_id = {agg_event_id}";
        using var conn = new NpgsqlConnection(monConnString);
        conn.Execute(sql_string);
    }
    
    public void DeleteSameEventSummaryStats(int agg_event_id)
    {
        string sql_string = $@"DELETE from sf.agg_summaries 
                               where agg_event_id = {agg_event_id}";
        using var conn = new NpgsqlConnection(monConnString);
        conn.Execute(sql_string);
    }
    
    public void DeleteSameEventObjectStats(int agg_event_id)
    {
        string sql_string = $@"DELETE from sf.agg_object_numbers 
                            where agg_event_id = {agg_event_id}";
        using var conn = new NpgsqlConnection(monConnString);
        conn.Execute(sql_string);
    }

    public void DeleteSameEventStudy1to1LinkData(int agg_event_id)
    {
        string sql_string = $@"DELETE from sf.agg_study_1to1_link_data 
                               where agg_event_id = {agg_event_id}";
        using var conn = new NpgsqlConnection(monConnString);
        conn.Execute(sql_string);
    }
    
    public void DeleteSameEventStudy1toNLinkData(int agg_event_id)
    {
        string sql_string = $@"DELETE from sf.agg_study_1ton_link_data 
                               where agg_event_id = {agg_event_id}";
        using var conn = new NpgsqlConnection(monConnString);
        conn.Execute(sql_string);
    }


    public int GetAggregateRecNum(string table_name, string schema_name)
    {
        string sql_string = "SELECT count(*) from " + schema_name + "." + table_name;
        using var conn = new NpgsqlConnection(aggConnString);
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

    public List<AggregationObjectNum> GetObjectTypes(int agg_event_id, string dest_conn_string)
    {
        string sql_string = $@"SELECT {agg_event_id} as agg_event_id, 
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

    public void DeleteSameEventStudyStudyLinkData(int agg_event_id)
    {
        string sql_string = $@"DELETE from sf.agg_object_numbers 
                            where agg_event_id = {agg_event_id}";
        using var conn = new NpgsqlConnection(monConnString);
        conn.Execute(sql_string);
    }

    public List<Study1To1LinkData> FetchStudy1to1LinkData(int last_agg_event_id)
    {
        string sql_string = $@"SELECT 
                {last_agg_event_id} as agg_event_id,
                k.source_id, 
                d1.default_name as source_name,
                k.preferred_source_id as other_source_id,
                d2.default_name as other_source_name,
                count(preferred_sd_sid) as number_in_other_source
                from nk.study_study_links k
                inner join context_ctx.data_sources d1
                on k.source_id = d1.id
                inner join context_ctx.data_sources d2
                on k.preferred_source_id = d2.id
                group by source_id, preferred_source_id, d1.default_name, d2.default_name;";

        using var conn = new NpgsqlConnection(aggConnString);
        return conn.Query<Study1To1LinkData>(sql_string).ToList();
    }

    public List<Study1To1LinkData> FetchStudy1to1LinkData2(int last_agg_event_id)
    {
        string sql_string = $@"SELECT 
              {last_agg_event_id} as agg_event_id,
              k.preferred_source_id as source_id, 
              d2.default_name as source_name,
              k.source_id as other_source_id,
              d1.default_name as other_source_name,
              count(sd_sid) as number_in_other_source
              from nk.study_study_links k
              inner join context_ctx.data_sources d1
              on k.source_id = d1.id
              inner join context_ctx.data_sources d2
              on k.preferred_source_id = d2.id
              group by preferred_source_id, source_id, d2.default_name, d1.default_name;";

        using var conn = new NpgsqlConnection(aggConnString);
        return conn.Query<Study1To1LinkData>(sql_string).ToList();
    }
    
    public List<Study1ToNLinkData> FetchStudy1toNLinkData(int last_agg_event_id)
    {
        string sql_string = $@"SELECT 
              {last_agg_event_id} as agg_event_id,
              k.source_id, d1.default_name as source_name,
              k.relationship_id, srt.name as relationship,
              k.target_source_id, d2.default_name as target_source_name,
              count(target_sd_sid) as number_in_other_source
              from nk.linked_study_groups k
              inner join context_ctx.data_sources d1
              on k.source_id = d1.id
              inner join context_ctx.data_sources d2
              on k.target_source_id = d2.id
              inner join context_lup.study_relationship_types srt
              on k.relationship_id = srt.id
              group by relationship_id, relationship, source_id, target_source_id,
              d1.default_name, d2.default_name";

        using var conn = new NpgsqlConnection(aggConnString);
        return conn.Query<Study1ToNLinkData>(sql_string).ToList();
    }
    
    public ulong StoreObjectNumbers(PostgreSQLCopyHelper<AggregationObjectNum> copyHelper, 
                                     IEnumerable<AggregationObjectNum> entities)
    {
        // stores the study id data in a temporary table
        using var conn = new NpgsqlConnection(monConnString);
        conn.Open();
        return copyHelper.SaveAll(conn, entities);
    }

    
    public ulong Store1to1LinkNumbers(PostgreSQLCopyHelper<Study1To1LinkData> copyHelper, 
        IEnumerable<Study1To1LinkData> entities)
    {
        // stores the study id data in a temporary table
        using var conn = new NpgsqlConnection(monConnString);
        conn.Open();
        return copyHelper.SaveAll(conn, entities);
    }


    public ulong Store1toNLinkNumbers(PostgreSQLCopyHelper<Study1ToNLinkData> copyHelper,
        IEnumerable<Study1ToNLinkData> entities)
    {
        // stores the study id data in a temporary table
        using var conn = new NpgsqlConnection(monConnString);
        conn.Open();
        return copyHelper.SaveAll(conn, entities);
    }
    
    // Used in obtaining data for the statistics builder to write out
    
    public CoreSummary? GetLatestCoreSummary()
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(monConnString);
        string sql_string = "select max(agg_event_id) from sf.agg_summaries ";
        int? last_id = Conn.ExecuteScalar<int?>(sql_string);
        if (last_id.HasValue)
        {
            sql_string = $@"select * from sf.agg_summaries 
                               where agg_event_id = {last_id}";
            return Conn.Query<CoreSummary?>(sql_string).FirstOrDefault();
        }
        return null;  // as a fallback
    }

    public List<AggregationObjectNum>? GetLatestObjectNumbers()
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(monConnString);
        string sql_string = "select max(agg_event_id) from sf.agg_object_numbers ";
        int? last_id = Conn.ExecuteScalar<int?>(sql_string);
        if (last_id.HasValue)
        {
            sql_string = $@"select * from sf.agg_object_numbers
                               where agg_event_id = {last_id}
                               order by number_of_type desc";
            return Conn.Query<AggregationObjectNum>(sql_string)?.ToList();
        }
        return null;  // as a fallback
    }

    public List<Study1To1LinkData>? GetLatestStudy1to1LinkData()
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(monConnString);
        string sql_string = "select max(agg_event_id) from sf.agg_study_1to1_link_data ";
        int? last_id = Conn.ExecuteScalar<int?>(sql_string);
        if (last_id.HasValue)
        {
            sql_string = $@"select source_id, source_name, 
                       other_source_id, other_source_name, number_in_other_source  
                       from sf.agg_study_1to1_link_data
                       where agg_event_id = {last_id}
                       order by source_name, other_source_name ";
            return Conn.Query<Study1To1LinkData>(sql_string)?.ToList();
        }
        return null;  // as a fallback
    }

    public List<Study1ToNLinkData>? GetLatestStudy1toNLinkData()
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(monConnString);
        string sql_string = "select max(agg_event_id) from sf.agg_study_1ton_link_data ";
        int? last_id = Conn.ExecuteScalar<int?>(sql_string);
        if (last_id.HasValue)
        {
            sql_string = $@"select source_id, source_name, relationship_id, relationship,
                       target_source_id, target_source_name, number_in_other_source  
                       from sf.agg_study_1ton_link_data
                       where agg_event_id = {last_id}
                       order by relationship_id, source_name";
            return Conn.Query<Study1ToNLinkData>(sql_string)?.ToList();
        }
        return null;  // as a fallback
    }

    public SourceSummary? RetrieveSourceSummary(int agg_event_id, string database_name)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(monConnString);
        string sql_string = $@"select * from sf.agg_source_summaries
                            where agg_event_id = {agg_event_id} 
                            and database_name = '{database_name}'";
        return Conn.Query<SourceSummary>(sql_string)?.FirstOrDefault();
    }
}

