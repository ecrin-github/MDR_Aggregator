using MDR_Aggregator.TopLevelClasses.Interfaces;
using PostgreSQLCopyHelper;

namespace MDR_Aggregator.LoggingHelpers.Interfaces;

public interface IMonDataLayer
{
    // Set up and utility functions
    
    ICredentials Credentials { get; }
    string GetConnectionString(string databaseName);

    IEnumerable<Source> RetrieveDataSources();
    IEnumerable<Source> RetrieveIECDataSources();
    
    List<string> SetUpTempFTWs(ICredentials credentials, string dbConnString, string fdwSchema,
        string sourceDb, List<string> sourceSchemas);
    void DropTempFTWs(string dbConnString, string sourceDb, List<string>  sourceSchemas);

    int GetNextAggEventId();
    int GetNextIECAggEventId();
    int GetNextAuditId();
    int GetLastAggEventId();

    // Used in removing prior results data with the same Agg Id
    // (e.g. after test runs that do not fully complete)
    
    void DeleteSameEventDBStats(int aggEventId);
    void DeleteSameEventSummaryStats(int aggEventId);    
    void DeleteSameEventObjectStats(int aggEventId);
    void DeleteSameEventStudy1to1LinkData(int aggEventId);
    void DeleteSameEventStudy1toNLinkData(int aggEventId);
    
    // Used in generating and storing results and statistics
    
    void UpdateIECAggregationEvent(IECAggregationEvent iecAggEvent, string iecConnString);
    int GetRecNum(string tableName, string sourceConnString);
    int GetAggregateRecNum(string tableName, string schemaName);
    
    List<AggregationObjectNum> GetObjectTypes(int aggregationEventId, string destConnString);
    List<Study1To1LinkData>? FetchStudy1to1LinkData(int lastAggEventId);
    List<Study1To1LinkData>? FetchStudy1to1LinkData2(int lastAggEventId);
    List<Study1ToNLinkData>? FetchStudy1toNLinkData(int lastAggEventId);
    
    int StoreAggregationEvent(AggregationEvent aggregation);
    int StoreIECAggregationEvent(IECAggregationEvent iecAgg);
    void StoreSourceSummary(SourceSummary sm);
    void StoreAdSummary(SourceADSummary sad);
    void StoreSourceIECData(int iecAggId, Source source, Int64 res);    
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
    SourceSummary? RetrieveSourceSummary(int lastAggEventId, string databaseName);
}
