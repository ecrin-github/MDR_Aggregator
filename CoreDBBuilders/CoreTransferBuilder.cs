namespace MDR_Aggregator;

public class CoreTransferBuilder
{
    private readonly ILoggingHelper _loggingHelper;
    private readonly IMonDataLayer _monDatalayer;
    private readonly string _connString;
    private readonly CoreDataTransferrer core_tr;
    private readonly string study_schema, object_schema, link_schema;
    
    public CoreTransferBuilder(string connString, IMonDataLayer monDatalayer, ILoggingHelper logginghelper)
    {
        _connString = connString;
        _loggingHelper = logginghelper;
        _monDatalayer = monDatalayer;
        core_tr = new CoreDataTransferrer(connString, _loggingHelper);
        
        study_schema = "aggs_st";
        object_schema= "aggs_ob";
        link_schema = "aggs_nk";
        
    }

    public void TransferCoreStudyData(CoreSummary core_summ)
    {
        int res = core_tr.LoadCoreStudyData(study_schema);
        core_summ.study_recs = res;
        _loggingHelper.LogLine($"{res} core studies transferred");
        
        res = core_tr.LoadCoreStudyIdentifiers(study_schema);
        core_summ.study_identifiers_recs = res;
        _loggingHelper.LogLine($"{res} core study identifiers transferred");
        
        res = core_tr.LoadCoreStudyTitles(study_schema);
        core_summ.study_titles_recs = res;
        _loggingHelper.LogLine($"{res} core study titles transferred");
        
        res = core_tr.LoadCoreStudyPeople(study_schema);
        core_summ.study_people_recs = res;
        _loggingHelper.LogLine($"{res} core study people transferred");
        
        res = core_tr.LoadCoreStudyOrganisations(study_schema);
        core_summ.study_organisations_recs = res;
        _loggingHelper.LogLine($"{res} core study organisations transferred");
        
        res = core_tr.LoadCoreStudyTopics(study_schema);
        core_summ.study_topics_recs = res;
        _loggingHelper.LogLine($"{res} core study topics transferred");
        
        res = core_tr.LoadCoreStudyConditions(study_schema);
        core_summ.study_conditions_recs = res;
        _loggingHelper.LogLine($"{res} core study conditions transferred");
        
        res = core_tr.LoadCoreStudyFeatures(study_schema);
        core_summ.study_features_recs = res;
        _loggingHelper.LogLine($"{res} core study features transferred");
        
        res = core_tr.LoadCoreStudyRelationShips(study_schema);
        core_summ.study_relationships_recs = res;
        _loggingHelper.LogLine($"{res} core study relationships transferred");
        
        res = core_tr.LoadCoreStudyCountries(study_schema);
        core_summ.study_countries_recs = res;
        _loggingHelper.LogLine($"{res} core study countries transferred");
        
        res = core_tr.LoadCoreStudyLocations(study_schema);
        core_summ.study_locations_recs = res;
        _loggingHelper.LogLine($"{res} core study locations transferred");
    }


    public void TransferCoreObjectData(CoreSummary core_summ)
    {
        int res = core_tr.LoadCoreDataObjects(object_schema);
        core_summ.data_object_recs = res;
        _loggingHelper.LogLine($"{res} core data objects transferred");
        
        res = core_tr.LoadCoreObjectDatasets(object_schema);
        core_summ.object_datasets_recs = res;
        _loggingHelper.LogLine($"{res} core object datasets transferred");
        
        res = core_tr.LoadCoreObjectInstances(object_schema);
        core_summ.object_instances_recs = res;
        _loggingHelper.LogLine($"{res} core object instances transferred");
        
        res = core_tr.LoadCoreObjectTitles(object_schema);
        core_summ.object_titles_recs = res;
        _loggingHelper.LogLine($"{res} core object titles transferred");
        
        res = core_tr.LoadCoreObjectDates(object_schema);
        core_summ.object_dates_recs = res;
        _loggingHelper.LogLine($"{res} core object dates transferred");
        
        res = core_tr.LoadCoreObjectPeople(object_schema);
        core_summ.object_people_recs = res;
        _loggingHelper.LogLine($"{res} core object people transferred");
        
        res = core_tr.LoadCoreObjectOrganisations(object_schema);
        core_summ.object_organisations_recs = res;
        _loggingHelper.LogLine($"{res} core object organisations transferred");
        
        res = core_tr.LoadCoreObjectTopics(object_schema);
        core_summ.object_topics_recs = res;
        _loggingHelper.LogLine($"{res} core object topics transferred");
        
        res = core_tr.LoadCoreObjectDescriptions(object_schema);
        core_summ.object_descriptions_recs = res;
        _loggingHelper.LogLine($"{res} core object descriptions transferred");
        
        res = core_tr.LoadCoreObjectIdentifiers(object_schema);
        core_summ.object_identifiers_recs = res;
        _loggingHelper.LogLine($"{res} core object identifiers transferred");
        
        res = core_tr.LoadCoreObjectRelationships(object_schema);
        core_summ.object_relationships_recs = res;
        _loggingHelper.LogLine($"{res} core object relationships transferred");
        
        res = core_tr.LoadCoreObjectRights(object_schema);
        core_summ.object_rights_recs = res;
        _loggingHelper.LogLine($"{res} core object rights transferred");
    }

    public void TransferCoreLinkData(CoreSummary core_summ)
    {
        int res = core_tr.LoadStudyObjectLinks(link_schema);
        core_summ.study_object_link_recs = res;
        _loggingHelper.LogLine($"{res} core link data transferred");
    }


    public void GenerateProvenanceData()
    {
        core_tr.GenerateStudyProvenanceData();
        _loggingHelper.LogLine("Core study provenance data created");
        core_tr.GenerateObjectProvenanceData();
        _loggingHelper.LogLine("Core object provenance data created");
    }
    
    public void StoreSourceSummaryStatistics()
    {
        //_monDatalayer.StoreCoreSummary();
        // 
    }
    

    public void StoreCoreSummaryStatistics(CoreSummary core_summ)
    {
        _monDatalayer.StoreCoreSummary(core_summ);
    }
    
    public void StoreDataObjectStatistics(int last_agg_event_id)
    {
        _monDatalayer.DeleteSameEventObjectStats(last_agg_event_id);
        List<AggregationObjectNum> object_numbers = _monDatalayer.GetObjectTypes(last_agg_event_id, _connString);
        _monDatalayer.StoreObjectNumbers(CopyHelpers.object_numbers_helper, object_numbers);
        _loggingHelper.LogLine("Statistics done for different data objects");
    }
    
    public void StoreStudyStudyLinkStatistics(int last_agg_event_id)
    {
        _monDatalayer.RecreateStudyStudyLinksTable();
        List<StudyStudyLinkData> study_link_numbers = _monDatalayer.GetStudyStudyLinkData(last_agg_event_id, _connString);
        _monDatalayer.StoreStudyLinkNumbers(CopyHelpers.study_link_numbers_helper, study_link_numbers);
        study_link_numbers = _monDatalayer.GetStudyStudyLinkData2(last_agg_event_id, _connString);
        _monDatalayer.StoreStudyLinkNumbers(CopyHelpers.study_link_numbers_helper, study_link_numbers);
        _loggingHelper.LogLine("Statistics done for study-study links");
    }
    
    

}

