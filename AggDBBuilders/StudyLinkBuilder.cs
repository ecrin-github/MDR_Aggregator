namespace MDR_Aggregator;

public class StudyLinkBuilder
{
    private readonly IMonDataLayer _monDatalayer;
    private readonly ILoggingHelper _loggingHelper;
    private readonly LinksDataHelper slh;
    private readonly ICredentials _credentials;

    public StudyLinkBuilder(IMonDataLayer monDatalayer, ILoggingHelper loggingHelper, ICredentials credentials, 
           string aggs_connString)
    {
        _monDatalayer = monDatalayer;
        _loggingHelper = loggingHelper;
        _credentials = credentials;
        slh = new LinksDataHelper(aggs_connString, _loggingHelper);
    }
    
    // There is a permanent nk.study_study_links table, which holds all the links found so far.
    // Most of these links come from examining the lists of 'secondary ids' that are registry Ids,
    // and this is done on each aggregation. Some also come from examining sponsor and other Ids
    // that are identical (and given from the same source) in different registries. These are 
    // using the aggregated data from the previous aggregation - but once identified cannot be 
    // identified again, because on the next aggregation the records will already have the same study Id.
    // A few links may even be added manually after unrelated work using the data set.
    
    // Because of this the set of study-study links identified at each aggregation may be smaller than the 
    // total number known to the system. That is why a separate permanent table is used to hold 
    // this data, which is augmented by any new links each aggregation.
    
    public void CollectStudyStudyLinks(List<Source> sources)
    {
        // Establish temp tables and then loop through each source.
        // Sources are called in 'preference order' starting with clinical trials.gov.

        slh.SetUpTempPreferencesTable(sources);
        slh.SetUpTempLinkCollectorTable();
        slh.SetUpTempLinkSortedTable();

        foreach (Source source in sources)
        {
            if (source.has_study_tables is true)
            {
                // Fetch the study-study links and store them in the initial Collector table
                // (assuming the source has study data).
                
                string source_conn_string = _credentials.GetConnectionString(source.database_name!);
                IEnumerable<StudyLink> links = slh.FetchLinks(source.id, source_conn_string);
                ulong num = slh.StoreLinksInTempTable(CopyHelpers.links_helper, links);
                _loggingHelper.LogLine($"{num} secondary id records transferred from {source.database_name!} ");
                
                // for 2 databases , also establish a table of equivalent old / new identifiers, for later checking

                if (source.id == 100120)
                {
                    IEnumerable<OldNewLink> ctg_ids = slh.GetOldAndNewIds(source_conn_string, 44);
                    ulong ctg_num = slh.StoreLinksInCTGLInksTable(CopyHelpers.oldnewlink_ctg_helper, ctg_ids);
                    _loggingHelper.LogLine($"{ctg_num} old / new id pairs transferred from CTG database");
                }

                if (source.id == 100132)
                {
                    IEnumerable<OldNewLink> dutch_ids = slh.GetOldAndNewIds(source_conn_string, 45);
                    ulong dutch_num = slh.StoreLinksInDutchLinksTable(CopyHelpers.oldnewlink_ntr_helper, dutch_ids);
                    _loggingHelper.LogLine($"{dutch_num} old / new id pairs transferred from NTR database");
                }
            }
        }
        
        // All links data has been collected together, though many will be repeated (A -> B and B -> A).
        // A further problem is that the secondary link held in other systems may refer to old, obsolete IDs
        // This is especially the case for references to the Dutch registry but may affect NCT numbers as well
                     
        slh.CleanDutchSecIds();
        slh.CleanCGTSecIds();

        // Tidy up common format errors.

        slh.TidyIds1();
        slh.TidyIds2();
        slh.TidyIds3();
        _loggingHelper.LogLine($"Common format errors in secondary Ids corrected");
        
        // Add in additional pairs found by comparing non-registry ids from the previous aggregation.
        // They must have the same value, from the same source, and be at least 4 characters long, but 
        // have been cited in different registries, and not identified as the same study using registry Ids.
        
        slh.AddAdditionalLinksUsingIdenticalSponsorIds();
    }

    
    public void CheckStudyStudyLinks(IEnumerable<Source> sources)
    {
        // Create a table with the distinct values obtained from the aggregation process.

        slh.TransferLinksToSortedTable();
        slh.CreateDistinctSourceLinksTable();

        // Despite earlier cleaning there remains a small number of secondary registry Ids that are
        // referenced as 'other Ids' but which are errors, i.e. which do not correspond to any real studies
        // in the system. These need to be removed, on a source by source basis.

        foreach (Source source in sources)
        {
            if (source.has_study_tables is true)
            {
                string source_conn_string = _credentials.GetConnectionString(source.database_name!);
                slh.ObtainStudyIds(source.id, source_conn_string, CopyHelpers.studyids_checker); 
                slh.CheckIdsAgainstSourceStudyIds(source.id);
            }
        }
        slh.DeleteInvalidLinks();
    }
    
    
    public void ProcessStudyStudyLinks()
    {
        // Identify and remove studies that have links to more than 1 study in another registry -
        // these form study-study relationships and are moved to the study_relationships table, 
        // rather than simple 1-to-1 study links. These linkages need to be identified and removed 
        // from this process, and dealt with separately at the end.
        
        slh.CreateGroupedStudiesTable();
        slh.ProcessGroupedStudies();

        // Identify and repair missing 'link cascade' steps. Then cascade 'preferred' studies so that the 
        // most preferred always appears on the RHS.

        slh.AddMissingLinks();
        slh.CascadeLinks();
                        
        // Again, identify and remove studies that have links to more than 1 study in another registry.
        // Repeated because a small number (about 30) are formed by the cascade process above. Also repeat
        // the missing link / cascade process.

        slh.ProcessGroupedStudies();
        slh.AddMissingLinks();
        slh.CascadeLinks();
        
        // Transfer the resultant set into the main links table and tidy up

        slh.TransferLinksToPermanentTable();
        
        // Keep the is_preferred true / false status in the study_ids table in sync with the 
        // links data. Links may not just be simple added - they may be differently classified
        // with additional data, e.g. become a 1 to N or N to N relationship, and thus drop out
        // of the links table. The study_ids table needs to reflect the current state of the links
        // (at least for those where the sd_sid is already in the study_ids table).
        
        slh.UpdateLinksWithStudyIds();
        
        slh.DropTempTables();
    }

    // Creates the ICD data (rather than being about study linking!)
    // This function places here to make it more easily visible to the calling Aggregator function
    // The normal data transfer helpers are instantiated within and using the loop for each source.
    
    public void LoadStudyICDs(string agg_conn_string)
    {
        StudyDataTransferrer st_tr = new StudyDataTransferrer(agg_conn_string, _loggingHelper);
        int res = st_tr.LoadStudyICDs();
        _loggingHelper.LogLine($"Created {res} study ICD records, from study conditions");
    }

    
    public void AddStudyStudyRelationshipRecords()
    {
        // Adds the study relationship records previously created.
        // But first use the study_ids table to insert the correct study Ids for the linked sources / sd_sids.
        
        slh.AddStudyStudyRelationshipRecords();
    }

    public void StoreStudyLinkStatistics(int agg_event_id)
    {
        // ensure no existing data for this agg_event_id 
        // for these two types of data
        
        _monDatalayer.DeleteSameEventStudy1to1LinkData(agg_event_id);
        _monDatalayer.DeleteSameEventStudy1toNLinkData(agg_event_id);
        
        // get data for 1-to-1 study links and store using copy helpers in appropriate table
        
        ulong res = 0;
        List<Study1To1LinkData>? study_1to1_link_numbers = _monDatalayer.FetchStudy1to1LinkData(agg_event_id);
        if (study_1to1_link_numbers is not null)
        {
            res = _monDatalayer.Store1to1LinkNumbers(CopyHelpers.study_1to1_link_numbers_helper, study_1to1_link_numbers);
        }
        study_1to1_link_numbers = _monDatalayer.FetchStudy1to1LinkData2(agg_event_id);
        if (study_1to1_link_numbers is not null)
        {
            res += _monDatalayer.Store1to1LinkNumbers(CopyHelpers.study_1to1_link_numbers_helper, study_1to1_link_numbers);
           _loggingHelper.LogLine($"Statistics created for 1-to-1 study links ({res} records)");  
        }

        // get data for 1-to-n study links and store using copy helpers in appropriate table
        
        List<Study1ToNLinkData>? study_1ton_link_numbers = _monDatalayer.FetchStudy1toNLinkData(agg_event_id);
        if (study_1ton_link_numbers is not null)
        {
            res = _monDatalayer.Store1toNLinkNumbers(CopyHelpers.study_1ton_link_numbers_helper, study_1ton_link_numbers);
            _loggingHelper.LogLine($"Statistics created for 1-to-n study links ({res} records)");
        }
    }
}
