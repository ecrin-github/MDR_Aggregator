namespace MDR_Aggregator;

public class CoreTransferBuilder
{
    readonly ILoggingHelper _loggingHelper;
    readonly CoreDataTransferrer core_tr;

    public CoreTransferBuilder(string connString, ILoggingHelper logginghelper)
    {
        _loggingHelper = logginghelper;
        core_tr = new CoreDataTransferrer(connString, _loggingHelper);
    }

    public void TransferCoreStudyData()
    {
        int res = core_tr.LoadCoreStudyData();
        _loggingHelper.LogLine(res + " core studies transferred");
        res = core_tr.LoadCoreStudyIdentifiers();
        _loggingHelper.LogLine(res + " core study identifiers transferred");
        res = core_tr.LoadCoreStudyTitles();
        _loggingHelper.LogLine(res + " core study titles transferred");
        res = core_tr.LoadCoreStudyPeople();
        _loggingHelper.LogLine(res + " core study people transferred");
        res = core_tr.LoadCoreStudyOrganisations();
        _loggingHelper.LogLine(res + " core study organisations transferred");
        res = core_tr.LoadCoreStudyTopics();
        _loggingHelper.LogLine(res + " core study topics transferred");
        res = core_tr.LoadCoreStudyConditions();
        _loggingHelper.LogLine(res + " core study conditions transferred");
        res = core_tr.LoadCoreStudyFeatures();
        _loggingHelper.LogLine(res + " core study features transferred");
        res = core_tr.LoadCoreStudyRelationShips();
        _loggingHelper.LogLine(res + " core study relationships transferred");
        res = core_tr.LoadCoreStudyCountries();
        _loggingHelper.LogLine(res.ToString() + " core study countries transferred");
        res = core_tr.LoadCoreStudyLocations();
        _loggingHelper.LogLine(res.ToString() + " core study locations transferred");
    }


    public void TransferCoreObjectData()
    {
        int res = core_tr.LoadCoreDataObjects();
        _loggingHelper.LogLine(res + " core data objects transferred");
        res = core_tr.LoadCoreObjectDatasets();
        _loggingHelper.LogLine(res + " core object datasets transferred");
        res = core_tr.LoadCoreObjectInstances();
        _loggingHelper.LogLine(res + " core object instances transferred");
        res = core_tr.LoadCoreObjectTitles();
        _loggingHelper.LogLine(res + " core object titles transferred");
        res = core_tr.LoadCoreObjectDates();
        _loggingHelper.LogLine(res + " core object dates transferred");
        res = core_tr.LoadCoreObjectPeople();
        _loggingHelper.LogLine(res + " core object people transferred");
        res = core_tr.LoadCoreObjectOrganisations();
        _loggingHelper.LogLine(res + " core object organisations transferred");
        res = core_tr.LoadCoreObjectTopics();
        _loggingHelper.LogLine(res + " core object topics transferred");
        res = core_tr.LoadCoreObjectDescriptions();
        _loggingHelper.LogLine(res + " core object descriptions transferred");
        res = core_tr.LoadCoreObjectIdentifiers();
        _loggingHelper.LogLine(res + " core object identifiers transferred");
        res = core_tr.LoadCoreObjectRelationships();
        _loggingHelper.LogLine(res + " core object relationships transferred");
        res = core_tr.LoadCoreObjectRights();
        _loggingHelper.LogLine(res + " core object rights transferred");
    }

    public void TransferCoreLinkData()
    {
        int res = core_tr.LoadStudyObjectLinks();
        _loggingHelper.LogLine(res + " core link data transferred");
    }


    public void GenerateProvenanceData()
    {
        core_tr.GenerateStudyProvenanceData();
        _loggingHelper.LogLine("Core study provenance data created");
        core_tr.GenerateObjectProvenanceData();
        _loggingHelper.LogLine("Core object provenance data created");
    }

}

