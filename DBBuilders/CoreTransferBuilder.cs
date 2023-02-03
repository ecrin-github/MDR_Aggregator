
namespace MDR_Aggregator
{
    public class CoreTransferBuilder
    {
        string _connString;
        ILoggingHelper _loggingHelper;
        CoreDataTransferrer core_tr;

        public CoreTransferBuilder(string connString, ILoggingHelper logginghelper)
        {
            _loggingHelper = logginghelper;
            _connString = connString;
            core_tr = new CoreDataTransferrer(_connString, _loggingHelper);
        }

        public void TransferCoreStudyData()
        {
            int res;
            res = core_tr.LoadCoreStudyData();
            _loggingHelper.LogLine(res.ToString() + " core studies transferred");
            res = core_tr.LoadCoreStudyIdentifiers();
            _loggingHelper.LogLine(res.ToString() + " core study identifiers transferred");
            res = core_tr.LoadCoreStudyTitles();
            _loggingHelper.LogLine(res.ToString() + " core study titles transferred");
            res = core_tr.LoadCoreStudyContributors();
            _loggingHelper.LogLine(res.ToString() + " core study contributors transferred");
            res = core_tr.LoadCoreStudyTopics();
            _loggingHelper.LogLine(res.ToString() + " core study topics transferred");
            res = core_tr.LoadCoreStudyFeatures();
            _loggingHelper.LogLine(res.ToString() + " core study features transferred");
            res = core_tr.LoadCoreStudyRelationShips();
            _loggingHelper.LogLine(res.ToString() + " core study relationships transferred");
        }


        public void TransferCoreObjectData()
        {
            int res;
            res = core_tr.LoadCoreDataObjects();
            _loggingHelper.LogLine(res.ToString() + " core data objects transferred");
            res = core_tr.LoadCoreObjectDatasets();
            _loggingHelper.LogLine(res.ToString() + " core object datasets transferred");
            res = core_tr.LoadCoreObjectInstances();
            _loggingHelper.LogLine(res.ToString() + " core object instances transferred");
            res = core_tr.LoadCoreObjectTitles();
            _loggingHelper.LogLine(res.ToString() + " core object titles transferred");
            res = core_tr.LoadCoreObjectDates();
            _loggingHelper.LogLine(res.ToString() + " core object dates transferred");
            res = core_tr.LoadCoreObjectContributors();
            _loggingHelper.LogLine(res.ToString() + " core object contributors transferred");
            res = core_tr.LoadCoreObjectTopics();
            _loggingHelper.LogLine(res.ToString() + " core object topics transferred");
            res = core_tr.LoadCoreObjectDescriptions();
            _loggingHelper.LogLine(res.ToString() + " core object descriptions transferred");
            res = core_tr.LoadCoreObjectIdentifiers();
            _loggingHelper.LogLine(res.ToString() + " core object identifiers transferred");
            res = core_tr.LoadCoreObjectRelationships();
            _loggingHelper.LogLine(res.ToString() + " core object relationships transferred");
            res = core_tr.LoadCoreObjectRights();
            _loggingHelper.LogLine(res.ToString() + " core object rights transferred");
        }

        public void TransferCoreLinkData()
        {
            int res;
            res = core_tr.LoadStudyObjectLinks();
            _loggingHelper.LogLine(res.ToString() + " core link data transferred");
        }


        public void GenerateProvenanceData()
        {
            core_tr.GenerateStudyProvenanceData();
            _loggingHelper.LogLine("Core study provenance data created");
            core_tr.GenerateObjectProvenanceData();
            _loggingHelper.LogLine("Core object provenance data created");
        }

    }
}
