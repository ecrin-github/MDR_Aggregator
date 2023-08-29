using PostgreSQLCopyHelper;

namespace MDR_Aggregator;

public interface IMonDataLayer
{
    ICredentials Credentials { get; }
    string GetConnectionString(string databaseName);

    IEnumerable<Source> RetrieveDataSources();
    IEnumerable<Source> RetrieveIECDataSources();
    
    List<string> SetUpTempFTWs(ICredentials credentials, string dbConnString, string fdw_schema,
        string source_db, List<string> source_schemas);
    void DropTempFTWs(string dbConnString, string source_db, List<string>  source_schemas);
    
    Source FetchSourceParameters(int source_id);

    int GetNextAggEventId();
    int GetNextIECAggEventId();
    
    int GetLastAggEventId();
    List<AggregationObjectNum>? GetLatestObjectNumbers();
    CoreSummary? GetLatestCoreSummary();

    int StoreAggregationEvent(AggregationEvent aggregation);
    int StoreIECAggregationEvent(IECAggregationEvent iec_agg);
    void StoreSourceIECData(int iec_agg_id, Source source, Int64 res);
    void UpdateIECAggregationEvent(IECAggregationEvent iec_agg_event, string iec_conn_string);
    
    void DeleteSameEventDBStats(int agg_event_id);
    int GetRecNum(string table_name, string source_conn_string);
    void DeleteSameEventSummaryStats(int agg_event_id);
    int GetAggregateRecNum(string table_name, string schema_name, string source_conn_string);
    void StoreSourceSummary(SourceSummary sm);
    void StoreCoreSummary(CoreSummary asm);
    void DeleteSameEventObjectStats(int agg_event_id);
    List<AggregationObjectNum> GetObjectTypes(int aggregation_event_id, string dest_conn_string);
    void RecreateStudyStudyLinksTable();

    List<StudyStudyLinkData> GetStudyStudyLinkData(int aggregation_event_id, string dest_conn_string);
    List<StudyStudyLinkData> GetStudyStudyLinkData2(int aggregation_event_id, string dest_conn_string);

    ulong StoreObjectNumbers(PostgreSQLCopyHelper<AggregationObjectNum> copyHelper,
                                     IEnumerable<AggregationObjectNum> entities);
    ulong StoreStudyLinkNumbers(PostgreSQLCopyHelper<StudyStudyLinkData> copyHelper,
                                    IEnumerable<StudyStudyLinkData> entities);
    
}
