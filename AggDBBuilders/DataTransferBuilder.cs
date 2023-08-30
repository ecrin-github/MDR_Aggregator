namespace MDR_Aggregator;

public class DataTransferBuilder
{
    private readonly Source _source;
    private readonly ILoggingHelper _loggingHelper;
    private readonly IMonDataLayer _monDatalayer;
    private readonly string _ftw_schema_name;
    private readonly string _source_conn_string;
    private readonly string _dest_conn_string;

    private readonly StudyDataTransferrer st_tr;
    private readonly ObjectDataTransferrer ob_tr;
    private readonly int _source_id;

    public DataTransferBuilder(Source source, string ftw_schema_name, string dest_conn_string, 
                               IMonDataLayer monDatalayer, ILoggingHelper logginghelper)
    {
        _source = source;
        _source_id = source.id;
        _source_conn_string = source.db_conn!;   
        
        _loggingHelper = logginghelper;
        _monDatalayer = monDatalayer;
        _ftw_schema_name = ftw_schema_name;
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

        st_tr.MatchExistingStudyIds(_source_id);
        st_tr.IdentifyNewLinkedStudyIds();
        st_tr.AddNewStudyIds(_source_id);
        st_tr.CreateTempStudyIdTables(_source_id);
        _loggingHelper.LogLine("Study Ids matched or added new");
    }
    

    public int TransferStudyData(SourceSummary srce_summ)
    {
        int study_number = st_tr.LoadStudies(_ftw_schema_name);
        srce_summ.study_recs = study_number;
        srce_summ.study_identifiers_recs = st_tr.LoadStudyIdentifiers(_ftw_schema_name);
        srce_summ.study_titles_recs = st_tr.LoadStudyTitles(_ftw_schema_name);
        if (_source.has_study_people is true)
        {
            srce_summ.study_people_recs = st_tr.LoadStudyPeople(_ftw_schema_name);
        }
        if (_source.has_study_organisations is true)
        {
            srce_summ.study_organisations_recs = st_tr.LoadStudyOrganisations(_ftw_schema_name);
        }
        if (_source.has_study_topics is true)
        {
            srce_summ.study_topics_recs = st_tr.LoadStudyTopics(_ftw_schema_name);
        }
        if (_source.has_study_conditions is true)
        {
            srce_summ.study_conditions_recs = st_tr.LoadStudyConditions(_ftw_schema_name);
        }
        if (_source.has_study_features is true)
        {
            srce_summ.study_features_recs = st_tr.LoadStudyFeatures(_ftw_schema_name);
        }
        if (_source.has_study_relationships is true)
        {
            srce_summ.study_relationships_recs = st_tr.LoadStudyRelationShips(_ftw_schema_name, _source_id);
        }
        if (_source.has_study_countries is true)
        {
            srce_summ.study_countries_recs = st_tr.LoadStudyCountries(_ftw_schema_name);
        }
        if (_source.has_study_locations is true)
        {
            srce_summ.study_locations_recs = st_tr.LoadStudyLocations(_ftw_schema_name);
        }
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
        
        // Update the object parent ids against the study_ids table.

        ob_tr.MatchExistingObjectIds(_source_id);
        ob_tr.UpdateNewObjectsWithStudyIds(_source_id);
        ob_tr.AddNewObjectsToIdentifiersTable(_source_id);
        _loggingHelper.LogLine("");
        
        // Carry out a check for (currently very rare) duplicate objects (i.e. that have been imported
        // before with the data from another source). 
        
        ob_tr.CheckNewObjectsForDuplicateTitles(_source_id);
        ob_tr.CheckNewObjectsForDuplicateURLs(_source_id, _ftw_schema_name);
        ob_tr.CompleteNewObjectsStatuses(_source_id);
        _loggingHelper.LogLine("Object Ids updated");
        _loggingHelper.LogLine("");

        // Update all objects ids table and derive a small table that lists the object Ids for all objects,
        // and one that lists the ids of possible duplicate objects, to check.

        ob_tr.FillObjectsToAddTables(_source_id);
        _loggingHelper.LogLine("Object Ids processed");
        _loggingHelper.LogLine("");
    }
 
    
    public void ProcessStandaloneObjectIds()
    {
        ob_tr.SetUpTempObjectIdsTables();

        // process the data using available object-study links (may be multiple study links per object).
        // Exact process likely to differ with different object sources - at present only PubMed in this category

        if (_source_id == 100135)
        {
            // Get the source-study-pmid link data. A table of PMID bank data was created during
            // data download, but this may have been date limited (probably was) so the total of records 
            // in the ad tables needs to be used. This needs to be combined with the references in those sources 
            // that contain study_reference tables.

            PubmedTransferHelper pm_tr = new PubmedTransferHelper(_ftw_schema_name, _dest_conn_string, _loggingHelper);
            pm_tr.SetupTempPMIDTables();
            
            int res = pm_tr.FetchBankReferences(_source_id, _ftw_schema_name);
            _loggingHelper.LogLine($"{res} PMID Ids obtained from PMID 'bank' data");
            
            // study ids referenced in PubMed data often poorly formed and need cleaning

            pm_tr.CleanPMIDsdsidData();
            _loggingHelper.LogLine("Study Ids in 'Bank' PMID records cleaned");
            
            // This needs to be combined with the pmid references from those sources that contain them.
            // A table of DB references data was created during pubmed data download, (mn.dbrefs_all).
            // This holds all known trial registry sourced references, most of which have PMIDs. There is no 
            // to recapture it - it should always reflect the state of the DBs during the most recent download.

            
            ulong res2 = pm_tr.FetchSourceReferences(_source_id, _source_conn_string);
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
            pm_tr.IdentifyPMIDDataForImport(_source_id);
            _loggingHelper.LogLine("");
            
            pm_tr.DropTempPMIDTables();
        }
    }


    public int TransferObjectData(SourceSummary srce_summ)
    {
        // Add new records where status indicates they are new.
        
        int object_number = ob_tr.LoadDataObjects(_ftw_schema_name);
        srce_summ.data_object_recs = object_number;
        if (_source.has_object_datasets is true)
        {
            srce_summ.object_datasets_recs = ob_tr.LoadObjectDatasets(_ftw_schema_name);
        }
        srce_summ.object_instances_recs = ob_tr.LoadObjectInstances(_ftw_schema_name);
        srce_summ.object_titles_recs = ob_tr.LoadObjectTitles(_ftw_schema_name);
        if (_source.has_object_dates is true)
        {
            srce_summ.object_dates_recs = ob_tr.LoadObjectDates(_ftw_schema_name);
        }
        if (_source.has_object_rights is true)
        {
            srce_summ.object_rights_recs = ob_tr.LoadObjectRights(_ftw_schema_name);
        }
        if (_source.has_object_relationships is true)
        {
            srce_summ.object_relationships_recs = ob_tr.LoadObjectRelationships(_ftw_schema_name);
        }
        if (_source.has_object_pubmed_set is true)
        {
            srce_summ.object_people_recs = ob_tr.LoadObjectPeople(_ftw_schema_name);
            srce_summ.object_organisations_recs = ob_tr.LoadObjectOrganisations(_ftw_schema_name);
            srce_summ.object_topics_recs = ob_tr.LoadObjectTopics(_ftw_schema_name);
            srce_summ.object_descriptions_recs = ob_tr.LoadObjectDescriptions(_ftw_schema_name);
            srce_summ.object_identifiers_recs = ob_tr.LoadObjectIdentifiers(_ftw_schema_name);
        }
        ob_tr.DropTempObjectIdsTable();
        return object_number;
    }
    
    public void StoreSourceSummaryStatistics(SourceSummary srce_summ)
    {
        _monDatalayer.StoreSourceSummary(srce_summ);
    }
    
}

