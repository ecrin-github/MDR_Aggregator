namespace MDR_Aggregator;

public class DataTransferBuilder
{
    readonly Source _source;
    readonly ILoggingHelper _loggingHelper;
    readonly string _schema_name;
    readonly string _source_conn_string;
    readonly string _dest_conn_string;

    readonly StudyDataTransferrer st_tr;
    readonly ObjectDataTransferrer ob_tr;

    readonly int source_id;

    public DataTransferBuilder(Source source, string schema_name, string dest_conn_string, ILoggingHelper logginghelper)
    {
        _source = source;
        _loggingHelper = logginghelper;
        _schema_name = schema_name;
        _source_conn_string = source.db_conn!;
        _dest_conn_string = dest_conn_string;

        source_id = _source.id;
        st_tr = new StudyDataTransferrer(_dest_conn_string, _loggingHelper);
        ob_tr = new ObjectDataTransferrer(_dest_conn_string, _loggingHelper);
    }

    
    public void ProcessStudyIds()
    {
        // Get the new study data as a set of study records using the ad database as the source.
        // Set up a temporary table that holds the sd_sid for all studies, and then fill it.

        st_tr.SetUpTempStudyIdsTable();
        IEnumerable<StudyId> study_ids = st_tr.FetchStudyIds(source_id, _source_conn_string);
        _loggingHelper.LogLine("Study Ids obtained");
        
        st_tr.StoreStudyIds(CopyHelpers.study_ids_helper, study_ids);
        _loggingHelper.LogLine("Study Ids stored");

        // Do the check of the temp table ids against the study_study links. Change the table to reflect
        // the 'preferred' Ids. Back load the correct study ids into the temporary table.

        // Match existing studies, then do the check of the temp table ids against the study_study links.
        // Change the table to mark the 'preferred' Ids and back load the correct study ids into the temporary table.

        st_tr.MatchExistingStudyIds();
        st_tr.IdentifyNewLinkedStudyIds();
        st_tr.AddNewStudyIds(source_id);
        _loggingHelper.LogLine("Study Ids checked");
        st_tr.CreateTempStudyIdTables(source_id);
        _loggingHelper.LogLine("Study Ids processed");
        
        /*
        st_tr.CheckStudyLinks();
        _loggingHelper.LogLine("Study Ids checked");
        st_tr.UpdateAllStudyIdsTable(source_id);
        _loggingHelper.LogLine("Study Ids processed");
        */
    }
    

    public int TransferStudyData()
    {
        int study_number = st_tr.LoadStudies(_schema_name);
        st_tr.LoadStudyIdentifiers(_schema_name);
        st_tr.LoadStudyTitles(_schema_name);
        
        if (_source.has_study_people is true) st_tr.LoadStudyPeople(_schema_name);
        if (_source.has_study_organisations is true) st_tr.LoadStudyOrganisations(_schema_name);
        if (_source.has_study_topics is true) st_tr.LoadStudyTopics(_schema_name);
        if (_source.has_study_conditions is true) st_tr.LoadStudyConditions(_schema_name);
        if (_source.has_study_features is true) st_tr.LoadStudyFeatures(_schema_name);
        if (_source.has_study_relationships is true) st_tr.LoadStudyRelationShips(_schema_name);
        if (_source.has_study_countries is true) st_tr.LoadStudyCountries(_schema_name);
        if (_source.has_study_locations is true) st_tr.LoadStudyLocations(_schema_name);
        
        st_tr.DropTempStudyIdsTable();
        return study_number;
    }

    
    public void ProcessStudyObjectIds()
    {
        // Set up temp tables and fill the first with the sd_oids, parent sd_sids,
        // dates of data fetch, of the objects in the source database.

        ob_tr.SetUpTempObjectIdsTables();
        IEnumerable<ObjectId> object_ids = ob_tr.FetchObjectIds(source_id, _source_conn_string);
        _loggingHelper.LogLine("Object Ids obtained");
        
        ob_tr.StoreObjectIds(CopyHelpers.object_ids_helper, object_ids);
        _loggingHelper.LogLine("Object Ids stored");

        // Update the object parent ids against the all_ids_studies table.

        ob_tr.MatchExistingObjectIds(source_id);
        ob_tr.UpdateNewObjectsWithStudyIds(source_id);
        ob_tr.AddNewObjectsToIdentifiersTable(source_id);

        // Carry out a check for (currently very rare) duplicate objects (i.e. that have been imported
        // before with the data from another source). [RARE - TO IMPLEMENT]
        
        ob_tr.CheckNewObjectsForDuplicateTitles(source_id);
        ob_tr.CheckNewObjectsForDuplicateURLs(source_id, _schema_name);
        ob_tr.CompleteNewObjectsStatuses(source_id);
        _loggingHelper.LogLine("Object Ids updated");

        // Update all objects ids table and derive a small table that lists the object Ids for all objects,
        // and one that lists the ids of possible duplicate objects, to check.

        ob_tr.FillObjectsToAddTables(source_id);
        _loggingHelper.LogLine("Object Ids processed");
    }

    
    public void ProcessStandaloneObjectIds(IEnumerable<Source> sources, ICredentials credentials, bool testing)
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

            PubmedTransferHelper pm_tr = new PubmedTransferHelper(_schema_name, _dest_conn_string, _loggingHelper);
            pm_tr.SetupTempPMIDTables();
            
            IEnumerable<PMIDLink> bank_object_ids = pm_tr.FetchBankPMIDs();
            ulong res = pm_tr.StorePMIDLinks(CopyHelpers.pmid_links_helper, bank_object_ids);
            _loggingHelper.LogLine(res.ToString() + " study Ids obtained from PMID 'bank' data");
            
            // study ids referenced in PubMed data often poorly formed and need cleaning

            pm_tr.CleanPMIDsdsidData();
            _loggingHelper.LogLine("Study Ids in 'Bank' PMID records cleaned");
            
            // This needs to be combined with the pmid references from those sources that contain them.
            // A table of DB references data was created during pubmed data download, (mn.dbrefs_all).
            // This holds all known trial registry sourced references, most of which have PMIDs. There is no 
            // to recapture it - it should always reflect the state of the DBs during the most recent download.
            
            // *************************
            // suitable code here to transfer the data from this table
            // *************************
            
            // Transfer data to 'standard' data_object_identifiers table.
            // and insert the 'correct' study_ids against the sd_sid
            // (all are known as studies already added)

            pm_tr.TransferPMIDLinksToTempObjectIds();
            pm_tr.UpdateTempObjectIdsWithStudyDetails();  

            // Duplication of PMIDs is from
            // a) The same study-PMID combination in both trial registry record and Pubmed record
            // b) The same study-PMID combination in different versions of the study record
            // c) The same PMID beiung used for multiple studies
            // To remove a) and b) a select distinct is done on the current set of unmatched PMID-Study combinations

            pm_tr.FillDistinctTempObjectsTable();

            // Table now has all study id - PMID combinations
            // Match against existing records here and update status and date-time of data fetch

            pm_tr.MatchExistingPMIDLinks();

            // New, unmatched combinations of PMID and studies may have PMIDs completely new to the system, or 
            // new PMID - study combinations for existing PMIDs

            pm_tr.IdentifyNewPMIDLinks();
            pm_tr.AddNewPMIDStudyLinks();
            pm_tr.AddCompletelyNewPMIDs();
            pm_tr.IdentifyPMIDDataForImport(source_id);

            pm_tr.DropTempPMIDTables();
            
            /*
            _loggingHelper.LogLine("PMID source object Ids obtained");

            pm_tr.FillDistinctPMIDsTable();
            pm_tr.DropTempPMIDTable();

            // Try and tidy some of the worst data anomalies before updating the data to the permanent tables.

            pm_tr.CleanPMIDsdsidData1();
            pm_tr.CleanPMIDsdsidData2();
            pm_tr.CleanPMIDsdsidData3();
            pm_tr.CleanPMIDsdsidData4();
            _loggingHelper.LogLine("PMID Ids cleaned");

            // Transfer data to all_ids_data_objects table.

            pm_tr.TransferPMIDLinksToObjectIds();
            ob_tr.UpdateObjectsWithStudyIds(source_id);
            _loggingHelper.LogLine("Object Ids matched to study ids");

            // Use study-study link table to get preferred sd_sid then drop any duplicates from study-pmid table.
            
            pm_tr.InputPreferredSDSIDS();

            // add in study-pmid links to all_ids_objects.
            
            ob_tr.UpdateAllObjectIdsTable(source_id);
            _loggingHelper.LogLine("PMID Ids added to table");

            // Use min of ids to set all object ids the same for the same pmid.
            
            pm_tr.ResetIdsOfDuplicatedPMIDs();
            _loggingHelper.LogLine("PMID Ids deduplicated");

            // Make new table of distinct pmids to add.
            
            ob_tr.FillObjectsToAddTable(source_id);
            _loggingHelper.LogLine("PMID Ids processed");
            */
        }
    }


    public int TransferObjectData()
    {
        // Add new records where status indicates they are new.
        
        int object_number = ob_tr.LoadDataObjects(_schema_name);
        if (_source.has_object_datasets is true) ob_tr.LoadObjectDatasets(_schema_name);
        ob_tr.LoadObjectInstances(_schema_name);
        ob_tr.LoadObjectTitles(_schema_name);
        
        if (_source.has_object_dates is true) ob_tr.LoadObjectDates(_schema_name);
        if (_source.has_object_rights is true) ob_tr.LoadObjectRights(_schema_name);
        if (_source.has_object_relationships is true) ob_tr.LoadObjectRelationships(_schema_name);
        if (_source.has_object_pubmed_set is true)
        {
            ob_tr.LoadObjectPeople(_schema_name);
            ob_tr.LoadObjectOrganisations(_schema_name);
            ob_tr.LoadObjectTopics(_schema_name);
            ob_tr.LoadObjectDescriptions(_schema_name);
            ob_tr.LoadObjectIdentifiers(_schema_name);
        }
        ob_tr.DropTempObjectIdsTable();
        return object_number;
    }
}

