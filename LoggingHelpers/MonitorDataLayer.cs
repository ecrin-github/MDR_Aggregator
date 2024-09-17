using Dapper;
using Dapper.Contrib.Extensions;
using MDR_Aggregator.LoggingHelpers.Interfaces;
using MDR_Aggregator.TopLevelClasses.Interfaces;
using Npgsql;
using PostgreSQLCopyHelper;

namespace MDR_Aggregator.LoggingHelpers;

public class MonDataLayer : IMonDataLayer
{
    private readonly ICredentials _credentials;
    private readonly string _monConnString;
    private readonly string _aggConnString;
    
    public MonDataLayer(ICredentials credentials)
    {
        _credentials = credentials;
        _monConnString = credentials.GetConnectionString("mon");
        _aggConnString = credentials.GetConnectionString("aggs");
    }

    public ICredentials Credentials => _credentials;
    
    public Source FetchSourceParameters(int sourceId)
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_monConnString);
        return conn.Get<Source>(sourceId);
    }

    public string GetConnectionString(string databaseName)
    {
        return _credentials.GetConnectionString(databaseName);
    }
    
    public List<string> SetUpTempFTWs(ICredentials credentials, string dbConnString, string fdwSchema, 
                                      string sourceDb, List<string> sourceSchemas)
    {
        using var conn = new NpgsqlConnection(dbConnString);
        string username = credentials.Username;
        string password = credentials.Password;

        string sql_string = $"CREATE EXTENSION IF NOT EXISTS postgres_fdw schema {fdwSchema};";
        conn.Execute(sql_string);

        sql_string = $@"CREATE SERVER IF NOT EXISTS {sourceDb}
                        FOREIGN DATA WRAPPER postgres_fdw
                        OPTIONS (host 'localhost', dbname '{sourceDb}');";
        conn.Execute(sql_string);

        sql_string = $@"CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
                        SERVER {sourceDb} 
                        OPTIONS (user '{username}', password '{password}');";
        conn.Execute(sql_string);

        List<string> schema_names = new();
        foreach(string schema in sourceSchemas)
        {
            string schema_name = $"{sourceDb}_{schema}";
            sql_string = $@"DROP SCHEMA IF EXISTS {schema_name} cascade;
                             CREATE SCHEMA {schema_name};
                             IMPORT FOREIGN SCHEMA {schema}
                             FROM SERVER {sourceDb} 
                             INTO {schema_name};";
            conn.Execute(sql_string);
            schema_names.Add(schema_name);
        }
        return schema_names;
    }
    
    
    public void DropTempFTWs(string dbConnString, string sourceDb, List<string> sourceSchemas)
    {
        using var conn = new NpgsqlConnection(dbConnString);
        string sql_string = $"DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER {sourceDb};";
        conn.Execute(sql_string);

        sql_string = $@"DROP SERVER IF EXISTS {sourceDb} CASCADE;";
        conn.Execute(sql_string);

        foreach (var schema_name in sourceSchemas.Select(schema => $"{sourceDb}_{schema}"))
        {
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
                              has_study_links, has_study_ipd_available,
                              has_object_datasets, has_object_instances, has_object_dates,
                              has_object_descriptions, has_object_identifiers, 
                              has_object_people, has_object_organisations, has_object_topics,
                              has_object_rights, has_object_relationships,
                              has_object_comments, has_object_db_links, 
                              has_journal_details, has_object_publication_types
                            from sf.source_parameters
                            where is_current_agg_source = true
                            order by preference_rating;";

        using var conn = new NpgsqlConnection(_monConnString);
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
        
        using var conn = new NpgsqlConnection(_monConnString);
        return conn.Query<Source>(sql_string);
    }
    
    public int GetRecNum(string tableName, string sourceConnString)
    {
        string sql_string = "SELECT count(*) from ad." + tableName;
        using var conn = new NpgsqlConnection(sourceConnString);
        var rec_num = conn.ExecuteScalar<int?>(sql_string);
        return rec_num ?? 0;
    }
    
    public int GetNextAggEventId()
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_monConnString);
        string sql_string = "select max(id) from sf.agg_events ";
        int? last_id = conn.ExecuteScalar<int?>(sql_string);
        return (last_id == null) ? 100001 : (int)last_id + 1;
    }
    
    public int GetNextIECAggEventId()
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_monConnString);
        string sql_string = "select max(id) from sf.agg_iec_events ";
        int? last_id = conn.ExecuteScalar<int?>(sql_string);
        return (last_id == null) ? 100001 : (int)last_id + 1;
    }
    
    public int GetNextAuditId()
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_monConnString);
        string sql_string = "select max(id) from sf.source_ad_summaries ";
        int? last_id = conn.ExecuteScalar<int?>(sql_string);
        return (last_id == null) ? 100001 : (int)last_id + 1;
    }

    public int GetLastAggEventId()
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_monConnString);
        string sql_string = "select max(id) from sf.agg_events ";
        int? last_id = conn.ExecuteScalar<int?>(sql_string);
        return last_id ?? 0;
    }
    
    public int StoreAggregationEvent(AggregationEvent aggregation)
    {
        aggregation.time_ended = DateTime.Now;
        using var conn = new NpgsqlConnection(_monConnString);
        return (int)conn.Insert(aggregation);
    }
    
    public int StoreIECAggregationEvent(IECAggregationEvent iecAgg)
    {
        iecAgg.time_ended = DateTime.Now;
        using var conn = new NpgsqlConnection(_monConnString);
        return (int)conn.Insert(iecAgg);
    }
    
    public void StoreSourceIECData(int iecAggId, Source source, Int64 res)
    {
        IECAggregationSourceNum summ_rec = new(iecAggId, source.id, source.database_name, res);
        using var conn = new NpgsqlConnection(_monConnString);
        conn.Insert(summ_rec);
    }

    public void UpdateIECAggregationEvent(IECAggregationEvent iecAggEvent, string iecConnString)
    {
        iecAggEvent.iec_null_recs = GetRecNum("study_iec_null", iecConnString);
        iecAggEvent.iec_pre06_recs = GetRecNum("study_iec_pre06", iecConnString);
        iecAggEvent.iec_0608_recs = GetRecNum("study_iec_0608", iecConnString);
        iecAggEvent.iec_0910_recs = GetRecNum("study_iec_0910", iecConnString);
        iecAggEvent.iec_1112_recs = GetRecNum("study_iec_1112", iecConnString);
        iecAggEvent.iec_1314_recs = GetRecNum("study_iec_1314", iecConnString);
        iecAggEvent.iec_15_recs = GetRecNum("study_iec_15", iecConnString);
        iecAggEvent.iec_16_recs = GetRecNum("study_iec_16", iecConnString);
        iecAggEvent.iec_17_recs = GetRecNum("study_iec_17", iecConnString);
        iecAggEvent.iec_18_recs = GetRecNum("study_iec_18", iecConnString);
        iecAggEvent.iec_19_recs = GetRecNum("study_iec_19", iecConnString);
        iecAggEvent.iec_20_recs = GetRecNum("study_iec_20", iecConnString);
        iecAggEvent.iec_21_recs = GetRecNum("study_iec_21", iecConnString);
        iecAggEvent.iec_22_recs = GetRecNum("study_iec_22", iecConnString);
        iecAggEvent.iec_23_recs = GetRecNum("study_iec_23", iecConnString);
        iecAggEvent.iec_24_recs = GetRecNum("study_iec_24", iecConnString);
        iecAggEvent.iec_25_recs = GetRecNum("study_iec_25", iecConnString);
        iecAggEvent.iec_26_recs = GetRecNum("study_iec_26", iecConnString);
        iecAggEvent.iec_27_recs = GetRecNum("study_iec_27", iecConnString);
        iecAggEvent.iec_28_recs = GetRecNum("study_iec_28", iecConnString);
        iecAggEvent.iec_29_recs = GetRecNum("study_iec_29", iecConnString);
        iecAggEvent.iec_30_recs = GetRecNum("study_iec_30", iecConnString);
        
        iecAggEvent.total_records_imported = (iecAggEvent.iec_null_recs ?? 0) +
            (iecAggEvent.iec_pre06_recs ?? 0) + (iecAggEvent.iec_0608_recs ?? 0) + 
            (iecAggEvent.iec_0910_recs ?? 0) + (iecAggEvent.iec_1112_recs ?? 0) +
            (iecAggEvent.iec_1314_recs ?? 0) + (iecAggEvent.iec_15_recs ?? 0) +
            (iecAggEvent.iec_16_recs ?? 0) + (iecAggEvent.iec_17_recs ?? 0) +
            (iecAggEvent.iec_18_recs ?? 0) + (iecAggEvent.iec_19_recs ?? 0) +
            (iecAggEvent.iec_20_recs ?? 0) + (iecAggEvent.iec_21_recs ?? 0) +
            (iecAggEvent.iec_22_recs ?? 0) + (iecAggEvent.iec_23_recs ?? 0)+
            (iecAggEvent.iec_24_recs ?? 0) + (iecAggEvent.iec_25_recs ?? 0) +
            (iecAggEvent.iec_26_recs ?? 0) + (iecAggEvent.iec_27_recs ?? 0) +
            (iecAggEvent.iec_28_recs ?? 0) + (iecAggEvent.iec_29_recs ?? 0) +
            (iecAggEvent.iec_30_recs ?? 0);
    }
    

    public void DeleteSameEventDBStats(int aggEventId)
    {
        string sql_string = $@"DELETE from sf.agg_source_summaries 
                              where agg_event_id = {aggEventId}";
        using var conn = new NpgsqlConnection(_monConnString);
        conn.Execute(sql_string);
    }
    
    public void DeleteSameEventSummaryStats(int aggEventId)
    {
        string sql_string = $@"DELETE from sf.agg_summaries 
                               where agg_event_id = {aggEventId}";
        using var conn = new NpgsqlConnection(_monConnString);
        conn.Execute(sql_string);
    }
    
    public void DeleteSameEventObjectStats(int aggEventId)
    {
        string sql_string = $@"DELETE from sf.agg_object_numbers 
                            where agg_event_id = {aggEventId}";
        using var conn = new NpgsqlConnection(_monConnString);
        conn.Execute(sql_string);
    }

    public void DeleteSameEventStudy1to1LinkData(int aggEventId)
    {
        string sql_string = $@"DELETE from sf.agg_study_1to1_link_data 
                               where agg_event_id = {aggEventId}";
        using var conn = new NpgsqlConnection(_monConnString);
        conn.Execute(sql_string);
    }
    
    public void DeleteSameEventStudy1toNLinkData(int aggEventId)
    {
        string sql_string = $@"DELETE from sf.agg_study_1ton_link_data 
                               where agg_event_id = {aggEventId}";
        using var conn = new NpgsqlConnection(_monConnString);
        conn.Execute(sql_string);
    }


    public int GetAggregateRecNum(string tableName, string schemaName)
    {
        string sql_string = "SELECT count(*) from " + schemaName + "." + tableName;
        using var conn = new NpgsqlConnection(_aggConnString);
        return conn.ExecuteScalar<int?>(sql_string) ?? 0;
    }

    public void StoreSourceSummary(SourceSummary sm)
    {
        using var conn = new NpgsqlConnection(_monConnString);
        conn.Insert(sm);
    }

    public void StoreCoreSummary(CoreSummary asm)
    {
        using var conn = new NpgsqlConnection(_monConnString);
        conn.Insert(asm);
    }
    
    public void StoreAdSummary(SourceADSummary sad)
    {
        using var conn = new NpgsqlConnection(_monConnString);
        conn.Insert(sad);
    }

    public List<AggregationObjectNum> GetObjectTypes(int aggEventId, string destConnString)
    {
        string sql_string = $@"SELECT {aggEventId} as agg_event_id, 
                d.object_type_id, 
                t.name as object_type_name,
                count(d.id) as number_of_type
                from core.data_objects d
                inner join context_lup.object_types t
                on d.object_type_id = t.id
                group by object_type_id, t.name
                order by count(d.id) desc";

        using var conn = new NpgsqlConnection(destConnString);
        return conn.Query<AggregationObjectNum>(sql_string).ToList();
    }

    public void DeleteSameEventStudyStudyLinkData(int aggEventId)
    {
        string sql_string = $@"DELETE from sf.agg_object_numbers 
                            where agg_event_id = {aggEventId}";
        using var conn = new NpgsqlConnection(_monConnString);
        conn.Execute(sql_string);
    }

    public List<Study1To1LinkData> FetchStudy1to1LinkData(int lastAggEventId)
    {
        string sql_string = $@"SELECT 
                {lastAggEventId} as agg_event_id,
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

        using var conn = new NpgsqlConnection(_aggConnString);
        return conn.Query<Study1To1LinkData>(sql_string).ToList();
    }

    public List<Study1To1LinkData> FetchStudy1to1LinkData2(int lastAggEventId)
    {
        string sql_string = $@"SELECT 
              {lastAggEventId} as agg_event_id,
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

        using var conn = new NpgsqlConnection(_aggConnString);
        return conn.Query<Study1To1LinkData>(sql_string).ToList();
    }
    
    public List<Study1ToNLinkData> FetchStudy1toNLinkData(int lastAggEventId)
    {
        string sql_string = $@"SELECT 
              {lastAggEventId} as agg_event_id,
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

        using var conn = new NpgsqlConnection(_aggConnString);
        return conn.Query<Study1ToNLinkData>(sql_string).ToList();
    }
    
    public ulong StoreObjectNumbers(PostgreSQLCopyHelper<AggregationObjectNum> copyHelper, 
                                     IEnumerable<AggregationObjectNum> entities)
    {
        // stores the study id data in a temporary table
        using var conn = new NpgsqlConnection(_monConnString);
        conn.Open();
        return copyHelper.SaveAll(conn, entities);
    }

    
    public ulong Store1to1LinkNumbers(PostgreSQLCopyHelper<Study1To1LinkData> copyHelper, 
        IEnumerable<Study1To1LinkData> entities)
    {
        // stores the study id data in a temporary table
        using var conn = new NpgsqlConnection(_monConnString);
        conn.Open();
        return copyHelper.SaveAll(conn, entities);
    }


    public ulong Store1toNLinkNumbers(PostgreSQLCopyHelper<Study1ToNLinkData> copyHelper,
        IEnumerable<Study1ToNLinkData> entities)
    {
        // stores the study id data in a temporary table
        using var conn = new NpgsqlConnection(_monConnString);
        conn.Open();
        return copyHelper.SaveAll(conn, entities);
    }
    
    // Used in obtaining data for the statistics builder to write out
    
    public CoreSummary? GetLatestCoreSummary()
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_monConnString);
        string sql_string = "select max(agg_event_id) from sf.agg_summaries ";
        int? last_id = conn.ExecuteScalar<int?>(sql_string);
        if (!last_id.HasValue) return null; // as a fallback
        sql_string = $@"select * from sf.agg_summaries 
                               where agg_event_id = {last_id}";
        return conn.Query<CoreSummary?>(sql_string).FirstOrDefault();
    }

    public List<AggregationObjectNum>? GetLatestObjectNumbers()
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_monConnString);
        string sql_string = "select max(agg_event_id) from sf.agg_object_numbers ";
        int? last_id = conn.ExecuteScalar<int?>(sql_string);
        if (!last_id.HasValue) return null; // as a fallback
        sql_string = $@"select * from sf.agg_object_numbers
                               where agg_event_id = {last_id}
                               order by number_of_type desc";
        return conn.Query<AggregationObjectNum>(sql_string)?.ToList();
    }

    public List<Study1To1LinkData>? GetLatestStudy1to1LinkData()
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_monConnString);
        string sql_string = "select max(agg_event_id) from sf.agg_study_1to1_link_data ";
        int? last_id = conn.ExecuteScalar<int?>(sql_string);
        if (!last_id.HasValue) return null; // as a fallback
        sql_string = $@"select source_id, source_name, 
                       other_source_id, other_source_name, number_in_other_source  
                       from sf.agg_study_1to1_link_data
                       where agg_event_id = {last_id}
                       order by source_name, other_source_name ";
        return conn.Query<Study1To1LinkData>(sql_string)?.ToList();
    }

    public List<Study1ToNLinkData>? GetLatestStudy1toNLinkData()
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_monConnString);
        string sql_string = "select max(agg_event_id) from sf.agg_study_1ton_link_data ";
        int? last_id = conn.ExecuteScalar<int?>(sql_string);
        if (!last_id.HasValue) return null; // as a fallback
        sql_string = $@"select source_id, source_name, relationship_id, relationship,
                       target_source_id, target_source_name, number_in_other_source  
                       from sf.agg_study_1ton_link_data
                       where agg_event_id = {last_id}
                       order by relationship_id, source_name";
        return conn.Query<Study1ToNLinkData>(sql_string)?.ToList();
    }

    public SourceSummary? RetrieveSourceSummary(int aggEventId, string databaseName)
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_monConnString);
        string sql_string = $@"select * from sf.agg_source_summaries
                            where agg_event_id = {aggEventId} 
                            and database_name = '{databaseName}'";
        return conn.Query<SourceSummary>(sql_string)?.FirstOrDefault();
    }
}

