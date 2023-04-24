namespace MDR_Aggregator;

public class CoreTransferBuilder
{
    private readonly ILoggingHelper _loggingHelper;
    private readonly CoreDataTransferrer core_tr;
    private readonly string study_schema, object_schema, link_schema;
    public CoreTransferBuilder(string connString, ILoggingHelper logginghelper)
    {
        _loggingHelper = logginghelper;
        core_tr = new CoreDataTransferrer(connString, _loggingHelper);
        study_schema = "aggs_st";
        object_schema= "aggs_ob";
        link_schema = "aggs_nk";
    }

    public void TransferCoreStudyData()
    {
        int res = core_tr.LoadCoreStudyData(study_schema);
        _loggingHelper.LogLine($"{res} core studies transferred");
        res = core_tr.LoadCoreStudyIdentifiers(study_schema);
        _loggingHelper.LogLine($"{res} core study identifiers transferred");
        res = core_tr.LoadCoreStudyTitles(study_schema);
        _loggingHelper.LogLine($"{res} core study titles transferred");
        res = core_tr.LoadCoreStudyPeople(study_schema);
        _loggingHelper.LogLine($"{res} core study people transferred");
        res = core_tr.LoadCoreStudyOrganisations(study_schema);
        _loggingHelper.LogLine($"{res} core study organisations transferred");
        res = core_tr.LoadCoreStudyTopics(study_schema);
        _loggingHelper.LogLine($"{res} core study topics transferred");
        res = core_tr.LoadCoreStudyConditions(study_schema);
        _loggingHelper.LogLine($"{res} core study conditions transferred");
        res = core_tr.LoadCoreStudyFeatures(study_schema);
        _loggingHelper.LogLine($"{res} core study features transferred");
        res = core_tr.LoadCoreStudyRelationShips(study_schema);
        _loggingHelper.LogLine($"{res} core study relationships transferred");
        res = core_tr.LoadCoreStudyCountries(study_schema);
        _loggingHelper.LogLine($"{res} core study countries transferred");
        res = core_tr.LoadCoreStudyLocations(study_schema);
        _loggingHelper.LogLine($"{res} core study locations transferred");
    }


    public void TransferCoreObjectData()
    {
        int res = core_tr.LoadCoreDataObjects(object_schema);
        _loggingHelper.LogLine($"{res} core data objects transferred");
        res = core_tr.LoadCoreObjectDatasets(object_schema);
        _loggingHelper.LogLine($"{res} core object datasets transferred");
        res = core_tr.LoadCoreObjectInstances(object_schema);
        _loggingHelper.LogLine($"{res} core object instances transferred");
        res = core_tr.LoadCoreObjectTitles(object_schema);
        _loggingHelper.LogLine($"{res} core object titles transferred");
        res = core_tr.LoadCoreObjectDates(object_schema);
        _loggingHelper.LogLine($"{res} core object dates transferred");
        res = core_tr.LoadCoreObjectPeople(object_schema);
        _loggingHelper.LogLine($"{res} core object people transferred");
        res = core_tr.LoadCoreObjectOrganisations(object_schema);
        _loggingHelper.LogLine($"{res} core object organisations transferred");
        res = core_tr.LoadCoreObjectTopics(object_schema);
        _loggingHelper.LogLine($"{res} core object topics transferred");
        res = core_tr.LoadCoreObjectDescriptions(object_schema);
        _loggingHelper.LogLine($"{res} core object descriptions transferred");
        res = core_tr.LoadCoreObjectIdentifiers(object_schema);
        _loggingHelper.LogLine($"{res} core object identifiers transferred");
        res = core_tr.LoadCoreObjectRelationships(object_schema);
        _loggingHelper.LogLine($"{res} core object relationships transferred");
        res = core_tr.LoadCoreObjectRights(object_schema);
        _loggingHelper.LogLine($"{res} core object rights transferred");
    }

    public void TransferCoreLinkData()
    {
        int res = core_tr.LoadStudyObjectLinks(link_schema);
        _loggingHelper.LogLine($"{res} core link data transferred");
    }


    public void GenerateProvenanceData()
    {
        core_tr.GenerateStudyProvenanceData();
        _loggingHelper.LogLine("Core study provenance data created");
        core_tr.GenerateObjectProvenanceData();
        _loggingHelper.LogLine("Core object provenance data created");
    }

}

