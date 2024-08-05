using MDR_Aggregator.AggDataHelpers;
using MDR_Aggregator.LoggingHelpers;
using MDR_Aggregator.LoggingHelpers.Interfaces;
using MDR_Aggregator.SourceSpecific.PubMed;

namespace MDR_Aggregator.AggDBBuilders;

public class DataTransferBuilder
{
    private readonly Source _source;
    private readonly ILoggingHelper _loggingHelper;
    private readonly IMonDataLayer _monDatalayer;
    private readonly string _ftwSchemaName;
    private readonly string _sourceConnString;
    private readonly string _destConnString;

    private readonly StudyDataTransferrer _stTr;
    private readonly ObjectDataTransferrer _obTr;
    private readonly int _sourceId;

    public DataTransferBuilder(Source source, string ftwSchemaName, string destConnString, 
                               IMonDataLayer monDatalayer, ILoggingHelper loggingHelper)
    {
        _source = source;
        _sourceId = source.id;
        _sourceConnString = source.db_conn!;   
        
        _loggingHelper = loggingHelper;
        _monDatalayer = monDatalayer;
        _ftwSchemaName = ftwSchemaName;
        _destConnString = destConnString;

        _stTr = new StudyDataTransferrer(_destConnString, _loggingHelper);
        _obTr = new ObjectDataTransferrer(_destConnString, _loggingHelper);
    }

    
    public void ProcessStudyIds()
    {
        // Get the new study data as a set of study records using the ad database as the source.
        // Set up a temporary table that holds the sd_sid for all studies, and then fill it.

        _stTr.SetUpTempStudyIdsTable();
        ulong res = _stTr.FetchStudyIds(_source.id, _sourceConnString);
        _loggingHelper.LogLine($"{res} Study Ids obtained");
        
        // Match existing studies, then do the check of the temp table ids against the study_study links.
        // Change the table to mark the 'preferred' Ids and back load the correct study ids into the temporary table.

        _stTr.MatchExistingStudyIds(_sourceId);
        _stTr.IdentifyNewLinkedStudyIds();
        _stTr.AddNewStudyIds(_sourceId);
        _stTr.CreateTempStudyIdTables(_sourceId);
        _loggingHelper.LogLine("Study Ids matched or added new");
    }
    

    public int TransferStudyData(SourceSummary srceSummary)
    {
        int study_number = _stTr.LoadStudies(_ftwSchemaName);
        srceSummary.study_recs = study_number;
        srceSummary.study_identifiers_recs = _stTr.LoadStudyIdentifiers(_ftwSchemaName);
        srceSummary.study_titles_recs = _stTr.LoadStudyTitles(_ftwSchemaName);
        if (_source.has_study_people is true)
        {
            srceSummary.study_people_recs = _stTr.LoadStudyPeople(_ftwSchemaName);
        }
        if (_source.has_study_organisations is true)
        {
            srceSummary.study_organisations_recs = _stTr.LoadStudyOrganisations(_ftwSchemaName);
        }
        if (_source.has_study_topics is true)
        {
            srceSummary.study_topics_recs = _stTr.LoadStudyTopics(_ftwSchemaName);
        }
        if (_source.has_study_conditions is true)
        {
            srceSummary.study_conditions_recs = _stTr.LoadStudyConditions(_ftwSchemaName);
        }
        if (_source.has_study_features is true)
        {
            srceSummary.study_features_recs = _stTr.LoadStudyFeatures(_ftwSchemaName);
        }
        if (_source.has_study_relationships is true)
        {
            srceSummary.study_relationships_recs = _stTr.LoadStudyRelationShips(_ftwSchemaName, _sourceId);
        }
        if (_source.has_study_countries is true)
        {
            srceSummary.study_countries_recs = _stTr.LoadStudyCountries(_ftwSchemaName);
        }
        if (_source.has_study_locations is true)
        {
            srceSummary.study_locations_recs = _stTr.LoadStudyLocations(_ftwSchemaName);
        }
        _stTr.DropTempStudyIdsTable();
        return study_number;
    }
    
    
    public void ProcessStudyObjectIds()
    {
        // Set up temp tables and fill the first with the sd_oids, parent sd_sids,
        // dates of data fetch, of the objects in the source database.

        _obTr.SetUpTempObjectIdsTables();
        ulong res = _obTr.FetchObjectIds(_source.id, _sourceConnString);
        _loggingHelper.LogLine($"{res} Object Ids obtained");
        _loggingHelper.LogBlank();
        
        // Update the object parent ids against the study_ids table.

        _obTr.MatchExistingObjectIds(_sourceId);
        _obTr.UpdateNewObjectsWithStudyIds(_sourceId);
        _obTr.AddNewObjectsToIdentifiersTable(_sourceId);
        _loggingHelper.LogBlank();
        
        // Carry out a check for (currently very rare) duplicate objects (i.e. that have been imported
        // before with the data from another source). 
        
        _obTr.CheckNewObjectsForDuplicateTitles(_sourceId);
        _obTr.CheckNewObjectsForDuplicateUrLs(_sourceId, _ftwSchemaName);
        _obTr.CompleteNewObjectsStatuses(_sourceId);
        _loggingHelper.LogLine("Object Ids updated");
        _loggingHelper.LogBlank();

        // Update all objects ids table and derive a small table that lists the object Ids for all objects,
        // and one that lists the ids of possible duplicate objects, to check.

        _obTr.FillObjectsToAddTables(_sourceId);
        _loggingHelper.LogLine("Object Ids processed");
        _loggingHelper.LogBlank();
    }
 
    
    public void ProcessStandaloneObjectIds()
    {
        // process the data using available object-study links (may be multiple study links per object).
        // Exact process likely to differ with different object sources - at present only PubMed in this category

        if (_sourceId == 100135)  // PubMed publication data
        {
            // Get the source-study-pmid link data. A table of PMID bank data was created during
            // data download, but this may have been date limited (probably was) so the total of records 
            // in the ad tables needs to be used. This needs to be combined with the references in those sources 
            // that contain study_reference tables.

            PubmedTransferHelper pm_tr = new PubmedTransferHelper(_ftwSchemaName, _destConnString, _loggingHelper);
            pm_tr.SetupTempPMIDTables();
            
            int res = pm_tr.FetchBankReferences(_sourceId, _ftwSchemaName);
            _loggingHelper.LogLine($"{res} PMID Ids obtained from PMID 'bank' data");
            
            // study ids referenced in PubMed data often poorly formed and need cleaning

            pm_tr.CleanPMIDsdsidData();
            _loggingHelper.LogLine("Study Ids in 'Bank' PMID records cleaned");
            
            // This needs to be combined with the pmid references from those sources that contain them.
            // A table of DB references data was created during pubmed data download, (mn.dbrefs_all).
            // This holds all known trial registry sourced references, most of which have PMIDs. There is no 
            // to recapture it - it should always reflect the state of the DBs during the most recent download.
            
            ulong res2 = pm_tr.FetchSourceReferences(_sourceId, _sourceConnString);
            _loggingHelper.LogLine($"{res2} PMID Ids obtained from DB sources");
            _loggingHelper.LogBlank();
            
            // Transfer data to 'standard' data_object_identifiers table and insert the 
            // 'correct' study_ids against the sd_sid (all are known as studies already added).

            pm_tr.TransferPMIDLinksToTempObjectIds();
            pm_tr.UpdateTempObjectIdsWithStudyDetails();  
            _loggingHelper.LogBlank();

            // Duplication of PMIDs is from
            // a) The same study-PMID combination in both trial registry record and Pubmed record
            // b) The same study-PMID combination in different versions of the study records
            // c) The same PMID being used for multiple studies
            // To remove a) and b) a select distinct is done on the current set of unmatched PMID-Study combinations

            pm_tr.FillDistinctTempObjectsTable();

            // Table now has all study id - PMID combinations
            // Match against existing records here and update status and date-time of data fetch

            pm_tr.MatchExistingPMIDLinks();
            _loggingHelper.LogBlank();
            
            // New, unmatched combinations of PMID and studies may have PMIDs completely new to the system, or 
            // new PMID-study combinations for existing PMIDs

            pm_tr.IdentifyNewPMIDLinks();
            pm_tr.AddNewPMIDStudyLinks();
            pm_tr.AddCompletelyNewPMIDs();
            pm_tr.IdentifyPMIDDataForImport(_sourceId);
            _loggingHelper.LogBlank();
            
            pm_tr.DropTempPMIDTables();
        }

        if (_sourceId == 110426) // BBMRI sample data
        {
            // This is far simpler than the pubmed data above.
            // The links between the objects and studies are already established, because identifying such a link
            // is the basis for identifying the data. The known sd_sid has been used to construct an sd_oid. 
            // The data objects table includes a reference to the parent sd_sid, (but not the parent source id).
            // The data objects table is also assumed to have a value for the datetime_of_data_fetch.
            
            // In general, only a trial registry id will have been provided. The source trial registry will
            // therefore need to be identified, as well as an sd_oid constructed. These steps can occur 
            // as part of the preceding download and / or harvest phases. The current retrospective data has
            // been imported directly into the ad tables, so by-passing the import process, but can (and has
            // been) coded there. At the moment, the source id can be derived from the sd_sid.
            
            // N.B. It is assumed that the data_object and object_index data will only have distinct
            // study-sample combinations, so there is no need to do preliminary 'select distinct' operations.
            
            // Establish the helper class and use it to first identify existing known sample-study links. 
            // These are matched in the nk.data_object_ids table. Datetime of data fetch is updated,
            // but little else is required.
            
            BbmriTransferHelper bb_tr = new BbmriTransferHelper(_ftwSchemaName, _destConnString, _loggingHelper);
            bb_tr.MatchExistingBBMRILinks();
            _loggingHelper.LogBlank();
            
            // Non-matched sample set records may be completely new to the system, or represent new
            // sample-study combinations for existing samples. The functions below identify these
            // different types of samples and code and add them accordingly.

            bb_tr.IdentifyNewBBMRILinks();
            bb_tr.AddNewBBMRIStudyLinks();
            bb_tr.AddCompletelyNewBBMRIObjects();
            bb_tr.IdentifyBBMRIDataForImport(_sourceId);
            _loggingHelper.LogBlank();
            
            bb_tr.DropTempBBMRITables();
        }
    }


    public int TransferObjectData(SourceSummary srceSummary)
    {
        // Add new records where status indicates they are new.
        
        int object_number = _obTr.LoadDataObjects(_ftwSchemaName);
        srceSummary.data_object_recs = object_number;
        srceSummary.object_titles_recs = _obTr.LoadObjectTitles(_ftwSchemaName);
        
        if (_source.has_object_datasets is true)
        {
            srceSummary.object_datasets_recs = _obTr.LoadObjectDatasets(_ftwSchemaName);
        }
        if (_source.has_object_instances is true)
        {
            srceSummary.object_instances_recs = _obTr.LoadObjectInstances(_ftwSchemaName);
        }
        if (_source.has_object_dates is true)
        {
            srceSummary.object_dates_recs = _obTr.LoadObjectDates(_ftwSchemaName);
        }
        if (_source.has_object_rights is true)
        {
            srceSummary.object_rights_recs = _obTr.LoadObjectRights(_ftwSchemaName);
        }
        if (_source.has_object_relationships is true)
        {
            srceSummary.object_relationships_recs = _obTr.LoadObjectRelationships(_ftwSchemaName);
        }
        if (_source.has_object_descriptions is true)
        {
            srceSummary.object_descriptions_recs = _obTr.LoadObjectDescriptions(_ftwSchemaName);
        }
        if (_source.has_object_identifiers is true)
        {
            srceSummary.object_identifiers_recs = _obTr.LoadObjectIdentifiers(_ftwSchemaName);
        }
        if (_source.has_object_people is true)
        {
            srceSummary.object_people_recs = _obTr.LoadObjectPeople(_ftwSchemaName);
        }
        if (_source.has_object_organisations is true)
        {
            srceSummary.object_organisations_recs = _obTr.LoadObjectOrganisations(_ftwSchemaName);
        }
        if (_source.has_object_topics is true)
        {
            srceSummary.object_topics_recs = _obTr.LoadObjectTopics(_ftwSchemaName);
        }
        _obTr.DropTempObjectIdsTable();
        return object_number;
    }
    
    public void StoreSourceSummaryStatistics(SourceSummary srceSummary)
    {
        _monDatalayer.StoreSourceSummary(srceSummary);
    }
    
}

