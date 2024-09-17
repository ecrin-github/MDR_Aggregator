using MDR_Aggregator.AggDataHelpers;
using MDR_Aggregator.LoggingHelpers;
using MDR_Aggregator.LoggingHelpers.Interfaces;

namespace MDR_Aggregator.CoreDBBuilders;

public class CoreTransferBuilder
{
    private readonly ILoggingHelper _loggingHelper;
    private readonly IMonDataLayer _monDatalayer;
    private readonly string _connString;
    private readonly CoreDataTransferrer _coreTr;
    private readonly string _studySchema, _objectSchema, _linkSchema;
    
    public CoreTransferBuilder(string connString, IMonDataLayer monDatalayer, ILoggingHelper logginghelper)
    {
        _connString = connString;
        _loggingHelper = logginghelper;
        _monDatalayer = monDatalayer;
        _coreTr = new CoreDataTransferrer(connString, _loggingHelper);
        
        _studySchema = "aggs_st";
        _objectSchema= "aggs_ob";
        _linkSchema = "aggs_nk";
        
    }

    public void TransferCoreStudyData(CoreSummary coreSumm)
    {
        int res = _coreTr.LoadCoreStudyData(_studySchema);
        coreSumm.study_recs = res;
        _loggingHelper.LogLine($"{res} core studies transferred");
        
        res = _coreTr.LoadCoreStudyIdentifiers(_studySchema);
        coreSumm.study_identifiers_recs = res;
        _loggingHelper.LogLine($"{res} core study identifiers transferred");
        
        res = _coreTr.LoadCoreStudyTitles(_studySchema);
        coreSumm.study_titles_recs = res;
        _loggingHelper.LogLine($"{res} core study titles transferred");
        
        res = _coreTr.LoadCoreStudyPeople(_studySchema);
        coreSumm.study_people_recs = res;
        _loggingHelper.LogLine($"{res} core study people transferred");
        
        res = _coreTr.LoadCoreStudyOrganisations(_studySchema);
        coreSumm.study_organisations_recs = res;
        _loggingHelper.LogLine($"{res} core study organisations transferred");
        
        res = _coreTr.LoadCoreStudyTopics(_studySchema);
        coreSumm.study_topics_recs = res;
        _loggingHelper.LogLine($"{res} core study topics transferred");
        
        res = _coreTr.LoadCoreStudyConditions(_studySchema);
        coreSumm.study_conditions_recs = res;
        _loggingHelper.LogLine($"{res} core study conditions transferred");
        
        res = _coreTr.LoadCoreStudyICDs(_studySchema);
        coreSumm.study_icd_recs = res;
        _loggingHelper.LogLine($"{res} core study conditions transferred");
        
        res = _coreTr.LoadCoreStudyFeatures(_studySchema);
        coreSumm.study_features_recs = res;
        _loggingHelper.LogLine($"{res} core study features transferred");
        
        res = _coreTr.LoadCoreStudyRelationShips(_studySchema);
        coreSumm.study_relationships_recs = res;
        _loggingHelper.LogLine($"{res} core study relationships transferred");
        
        res = _coreTr.LoadCoreStudyCountries(_studySchema);
        coreSumm.study_countries_recs = res;
        _loggingHelper.LogLine($"{res} core study countries transferred");
        
        res = _coreTr.LoadCoreStudyLocations(_studySchema);
        coreSumm.study_locations_recs = res;
        _loggingHelper.LogLine($"{res} core study locations transferred");
    }


    public void TransferCoreObjectData(CoreSummary coreSumm)
    {
        int res = _coreTr.LoadCoreDataObjects(_objectSchema);
        coreSumm.data_object_recs = res;
        _loggingHelper.LogLine($"{res} core data objects transferred");
        
        res = _coreTr.LoadCoreObjectDatasets(_objectSchema);
        coreSumm.object_datasets_recs = res;
        _loggingHelper.LogLine($"{res} core object datasets transferred");
        
        res = _coreTr.LoadCoreObjectInstances(_objectSchema);
        coreSumm.object_instances_recs = res;
        _loggingHelper.LogLine($"{res} core object instances transferred");
        
        res = _coreTr.LoadCoreObjectTitles(_objectSchema);
        coreSumm.object_titles_recs = res;
        _loggingHelper.LogLine($"{res} core object titles transferred");
        
        res = _coreTr.LoadCoreObjectDates(_objectSchema);
        coreSumm.object_dates_recs = res;
        _loggingHelper.LogLine($"{res} core object dates transferred");
        
        res = _coreTr.LoadCoreObjectPeople(_objectSchema);
        coreSumm.object_people_recs = res;
        _loggingHelper.LogLine($"{res} core object people transferred");
        
        res = _coreTr.LoadCoreObjectOrganisations(_objectSchema);
        coreSumm.object_organisations_recs = res;
        _loggingHelper.LogLine($"{res} core object organisations transferred");
        
        res = _coreTr.LoadCoreObjectTopics(_objectSchema);
        coreSumm.object_topics_recs = res;
        _loggingHelper.LogLine($"{res} core object topics transferred");
        
        res = _coreTr.LoadCoreObjectDescriptions(_objectSchema);
        coreSumm.object_descriptions_recs = res;
        _loggingHelper.LogLine($"{res} core object descriptions transferred");
        
        res = _coreTr.LoadCoreObjectIdentifiers(_objectSchema);
        coreSumm.object_identifiers_recs = res;
        _loggingHelper.LogLine($"{res} core object identifiers transferred");
        
        res = _coreTr.LoadCoreObjectRelationships(_objectSchema);
        coreSumm.object_relationships_recs = res;
        _loggingHelper.LogLine($"{res} core object relationships transferred");
        
        res = _coreTr.LoadCoreObjectRights(_objectSchema);
        coreSumm.object_rights_recs = res;
        _loggingHelper.LogLine($"{res} core object rights transferred");
    }

    public void TransferCoreLinkData(CoreSummary coreSumm)
    {
        int res = _coreTr.LoadStudyObjectLinks(_linkSchema);
        coreSumm.study_object_link_recs = res;
        _loggingHelper.LogLine($"{res} core link data transferred");
    }


    public void GenerateProvenanceData()
    {
        _coreTr.GenerateStudyProvenanceData();
        _loggingHelper.LogLine("Core study provenance data created");
        _coreTr.GenerateObjectProvenanceData();
        _loggingHelper.LogLine("Core object provenance data created");
    }
   
    public void StoreCoreSummaryStatistics(CoreSummary coreSumm)
    {
        _monDatalayer.DeleteSameEventSummaryStats(coreSumm.agg_event_id);
        _monDatalayer.StoreCoreSummary(coreSumm);
    }
    
    public void StoreDataObjectStatistics(int lastAggEventId)
    {
        _monDatalayer.DeleteSameEventObjectStats(lastAggEventId);
        List<AggregationObjectNum> object_numbers = _monDatalayer.GetObjectTypes(lastAggEventId, _connString);
        _monDatalayer.StoreObjectNumbers(CopyHelpers.object_numbers_helper, object_numbers);
        _loggingHelper.LogLine("Statistics created for different data objects");
    }

}

