using PostgreSQLCopyHelper;

namespace MDR_Aggregator;

public interface IMonDataLayer
{
    // Set up and utility functions
    
    ICredentials Credentials { get; }
    string GetConnectionString(string databaseName);

    IEnumerable<Source> RetrieveDataSources();
    IEnumerable<Source> RetrieveIECDataSources();
    
    List<string> SetUpTempFTWs(ICredentials credentials, string dbConnString, string fdw_schema,
        string source_db, List<string> source_schemas);
    void DropTempFTWs(string dbConnString, string source_db, List<string>  source_schemas);

    int GetNextAggEventId();
    int GetNextIECAggEventId();
    int GetLastAggEventId();

    // Used in removing prior results data with the same Agg Id
    // (e.g. after test runs that do not fully complete)
    
    void DeleteSameEventDBStats(int agg_event_id);
    void DeleteSameEventSummaryStats(int agg_event_id);    
    void DeleteSameEventObjectStats(int agg_event_id);
    void DeleteSameEventStudy1to1LinkData(int agg_event_id);
    void DeleteSameEventStudy1toNLinkData(int agg_event_id);
    
    // Used in generating and storing results and statistics
    
    void UpdateIECAggregationEvent(IECAggregationEvent iec_agg_event, string iec_conn_string);
    int GetRecNum(string table_name, string source_conn_string);
    int GetAggregateRecNum(string table_name, string schema_name);
    
    List<AggregationObjectNum> GetObjectTypes(int aggregation_event_id, string dest_conn_string);
    List<Study1To1LinkData>? FetchStudy1to1LinkData(int last_agg_event_id);
    List<Study1To1LinkData>? FetchStudy1to1LinkData2(int last_agg_event_id);
    List<Study1ToNLinkData>? FetchStudy1toNLinkData(int last_agg_event_id);
    
    int StoreAggregationEvent(AggregationEvent aggregation);
    int StoreIECAggregationEvent(IECAggregationEvent iec_agg);
    void StoreSourceSummary(SourceSummary sm);
    void StoreSourceIECData(int iec_agg_id, Source source, Int64 res);    
    void StoreCoreSummary(CoreSummary asm);
    ulong StoreObjectNumbers(PostgreSQLCopyHelper<AggregationObjectNum> copyHelper,
                             IEnumerable<AggregationObjectNum> entities);
    ulong Store1to1LinkNumbers(PostgreSQLCopyHelper<Study1To1LinkData> copyHelper,
                             IEnumerable<Study1To1LinkData> entities);
    ulong Store1toNLinkNumbers(PostgreSQLCopyHelper<Study1ToNLinkData> copyHelper,
                             IEnumerable<Study1ToNLinkData> entities);
    
    // Used in obtaining data for the statistics builder to write out
    
    CoreSummary? GetLatestCoreSummary();
    List<AggregationObjectNum>? GetLatestObjectNumbers();    
    List<Study1To1LinkData>? GetLatestStudy1to1LinkData();
    List<Study1ToNLinkData>? GetLatestStudy1toNLinkData();
    SourceSummary? RetrieveSourceSummary(int last_agg_event_id, string database_name);
}
