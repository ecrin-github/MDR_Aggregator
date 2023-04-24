namespace MDR_Aggregator;

public class DataTransferBuilder
{
    private readonly Source _source;
    private readonly ILoggingHelper _loggingHelper;
    private readonly string _ftw_schema_name;
    private readonly string _source_conn_string;
    private readonly string _dest_conn_string;

    private readonly StudyDataTransferrer st_tr;
    private readonly ObjectDataTransferrer ob_tr;
    private readonly int source_id;

    public DataTransferBuilder(Source source, string ftw_schema_name, string dest_conn_string, 
                               ILoggingHelper logginghelper)
    {
        _source = source;
        source_id = _source.id;
        
        _loggingHelper = logginghelper;
        _ftw_schema_name = ftw_schema_name;
        _source_conn_string = source.db_conn!;
        _dest_conn_string = dest_conn_string;

        st_tr = new StudyDataTransferrer(_dest_conn_string, _loggingHelper);
        ob_tr = new ObjectDataTransferrer(_dest_conn_string, _loggingHelper);
    }

    
    public void ProcessStudyIds()
    {
        // Get the new study data as a set of study records using the ad database as the source.
        // Set up a temporary table that holds the sd_sid for all studies, and then fill it.

        st_tr.SetUpTempStudyIdsTable();
        ulong res = st_tr.FetchStudyIds(_source.id, _source_conn_string);
        _loggingHelper.LogLine($"{res} Study Ids obtained");
        
        // Match existing studies, then do the check of the temp table ids against the study_study links.
        // Change the table to mark the 'preferred' Ids and back load the correct study ids into the temporary table.

        st_tr.MatchExistingStudyIds();
        st_tr.IdentifyNewLinkedStudyIds();
        st_tr.AddNewStudyIds(source_id);
        st_tr.CreateTempStudyIdTables(source_id);
        _loggingHelper.LogLine("Study Ids matched or added new");
    }
    

    public int TransferStudyData()
    {
        int study_number = st_tr.LoadStudies(_ftw_schema_name);
        st_tr.LoadStudyIdentifiers(_ftw_schema_name);
        st_tr.LoadStudyTitles(_ftw_schema_name);
        
        if (_source.has_study_people is true) st_tr.LoadStudyPeople(_ftw_schema_name);
        if (_source.has_study_organisations is true) st_tr.LoadStudyOrganisations(_ftw_schema_name);
        if (_source.has_study_topics is true) st_tr.LoadStudyTopics(_ftw_schema_name);
        if (_source.has_study_conditions is true) st_tr.LoadStudyConditions(_ftw_schema_name);
        if (_source.has_study_features is true) st_tr.LoadStudyFeatures(_ftw_schema_name);
        if (_source.has_study_relationships is true) st_tr.LoadStudyRelationShips(_ftw_schema_name);
        if (_source.has_study_countries is true) st_tr.LoadStudyCountries(_ftw_schema_name);
        if (_source.has_study_locations is true) st_tr.LoadStudyLocations(_ftw_schema_name);
        
        st_tr.DropTempStudyIdsTable();
        return study_number;
    }

    
    public void ProcessStudyObjectIds()
    {
        // Set up temp tables and fill the first with the sd_oids, parent sd_sids,
        // dates of data fetch, of the objects in the source database.

        ob_tr.SetUpTempObjectIdsTables();
        ulong res = ob_tr.FetchObjectIds(_source.id, _source_conn_string);
        _loggingHelper.LogLine($"{res} Object Ids obtained");
        _loggingHelper.LogLine("");
        
        // Update the object parent ids against the all_ids_studies table.

        ob_tr.MatchExistingObjectIds(source_id);
        ob_tr.UpdateNewObjectsWithStudyIds(source_id);
        ob_tr.AddNewObjectsToIdentifiersTable(source_id);
        _loggingHelper.LogLine("");
        
        // Carry out a check for (currently very rare) duplicate objects (i.e. that have been imported
        // before with the data from another source). [RARE - TO IMPLEMENT]
        
        ob_tr.CheckNewObjectsForDuplicateTitles(source_id);
        ob_tr.CheckNewObjectsForDuplicateURLs(source_id, _ftw_schema_name);
        ob_tr.CompleteNewObjectsStatuses(source_id);
        _loggingHelper.LogLine("Object Ids updated");
        _loggingHelper.LogLine("");

        // Update all objects ids table and derive a small table that lists the object Ids for all objects,
        // and one that lists the ids of possible duplicate objects, to check.

        ob_tr.FillObjectsToAddTables(source_id);
        _loggingHelper.LogLine("Object Ids processed");
        _loggingHelper.LogLine("");
    }
 
    
    public void ProcessStandaloneObjectIds()
    {
        ob_tr.SetUpTempObjectIdsTables();

        // process the data using available object-study links (may be multiple study links per object).
        // Exact process likely to differ with different object sources - at present only PubMed in this category

        if (source_id == 100135)
        {
            // Get the source-study-pmid link data. A table of PMID bank data was created during
            // data download, but this may have been date limited (probably was) so the total of records 
            // in the ad tables needs to be used. This needs to be combined with the references in those sources 
            // that contain study_reference tables.

            PubmedTransferHelper pm_tr = new PubmedTransferHelper(_ftw_schema_name, _dest_conn_string, _loggingHelper);
            pm_tr.SetupTempPMIDTables();
            
            int res = pm_tr.FetchBankReferences(_source.id, _ftw_schema_name);
            _loggingHelper.LogLine($"{res} PMID Ids obtained from PMID 'bank' data");
            
            // study ids referenced in PubMed data often poorly formed and need cleaning

            pm_tr.CleanPMIDsdsidData();
            _loggingHelper.LogLine("Study Ids in 'Bank' PMID records cleaned");
            
            // This needs to be combined with the pmid references from those sources that contain them.
            // A table of DB references data was created during pubmed data download, (mn.dbrefs_all).
            // This holds all known trial registry sourced references, most of which have PMIDs. There is no 
            // to recapture it - it should always reflect the state of the DBs during the most recent download.

            ulong res2 = pm_tr.FetchSourceReferences(_source.id, _source_conn_string);
            _loggingHelper.LogLine($"{res2} PMID Ids obtained from DB sources");
            _loggingHelper.LogLine("");
            
            // Transfer data to 'standard' data_object_identifiers table and insert the 
            // 'correct' study_ids against the sd_sid (all are known as studies already added).

            pm_tr.TransferPMIDLinksToTempObjectIds();
            pm_tr.UpdateTempObjectIdsWithStudyDetails();  
            _loggingHelper.LogLine("");

            // Duplication of PMIDs is from
            // a) The same study-PMID combination in both trial registry record and Pubmed record
            // b) The same study-PMID combination in different versions of the study records
            // c) The same PMID being used for multiple studies
            // To remove a) and b) a select distinct is done on the current set of unmatched PMID-Study combinations

            pm_tr.FillDistinctTempObjectsTable();

            // Table now has all study id - PMID combinations
            // Match against existing records here and update status and date-time of data fetch

            pm_tr.MatchExistingPMIDLinks();
            _loggingHelper.LogLine("");
            
            // New, unmatched combinations of PMID and studies may have PMIDs completely new to the system, or 
            // new PMID-study combinations for existing PMIDs

            pm_tr.IdentifyNewPMIDLinks();
            pm_tr.AddNewPMIDStudyLinks();
            pm_tr.AddCompletelyNewPMIDs();
            pm_tr.IdentifyPMIDDataForImport(source_id);
            _loggingHelper.LogLine("");
            
            pm_tr.DropTempPMIDTables();
        }
    }


    public int TransferObjectData()
    {
        // Add new records where status indicates they are new.
        
        int object_number = ob_tr.LoadDataObjects(_ftw_schema_name);
        if (_source.has_object_datasets is true) ob_tr.LoadObjectDatasets(_ftw_schema_name);
        ob_tr.LoadObjectInstances(_ftw_schema_name);
        ob_tr.LoadObjectTitles(_ftw_schema_name);
        
        if (_source.has_object_dates is true) ob_tr.LoadObjectDates(_ftw_schema_name);
        if (_source.has_object_rights is true) ob_tr.LoadObjectRights(_ftw_schema_name);
        if (_source.has_object_relationships is true) ob_tr.LoadObjectRelationships(_ftw_schema_name);
        if (_source.has_object_pubmed_set is true)
        {
            ob_tr.LoadObjectPeople(_ftw_schema_name);
            ob_tr.LoadObjectOrganisations(_ftw_schema_name);
            ob_tr.LoadObjectTopics(_ftw_schema_name);
            ob_tr.LoadObjectDescriptions(_ftw_schema_name);
            ob_tr.LoadObjectIdentifiers(_ftw_schema_name);
        }
        ob_tr.DropTempObjectIdsTable();
        return object_number;
    }
}

