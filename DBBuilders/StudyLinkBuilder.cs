namespace MDR_Aggregator;

public class StudyLinkBuilder
{
    private readonly ILoggingHelper _loggingHelper;
    private readonly LinksDataHelper slh;
    private readonly ICredentials _credentials;
    private readonly bool _testing;

    public StudyLinkBuilder(ILoggingHelper logginghelper, ICredentials credentials, 
           string aggs_connString, bool testing)
    {
        _loggingHelper = logginghelper;
        _credentials = credentials;
        _testing = testing;
        slh = new LinksDataHelper(aggs_connString, _loggingHelper);
    }
        
    public void CollectStudyStudyLinks(List<Source> sources)
    {
        // Establish temp tables and then loop through each source.
        // Sources are called in 'preference order' starting with clinical trials.gov.

        slh.SetUpTempPreferencesTable(sources);
        slh.SetUpTempLinkCollectorTable();
        slh.SetUpTempLinkSortedTable();

        foreach (Source source in sources)
        {
            if (_testing)
            {
                // need to populate the ad tables in a test situation with the relevant data,
                // as the source_conn_string will always point to 'test' -
                // at least get the study identifiers data - but this needs further consideration.
                
                slh.TransferTestIdentifiers(source.id);
            }
            else if (source.has_study_tables is true)
            {
                // Fetch the study-study links and store them in the initial Collector table
                // (assuming the source has study data).
                
                string source_conn_string = _credentials.GetConnectionString(source.database_name!, _testing);
                IEnumerable<StudyLink> links = slh.FetchLinks(source.id, source_conn_string);
                ulong num = slh.StoreLinksInTempTable(CopyHelpers.links_helper, links);
                _loggingHelper.LogLine($"{num} secondary id records transferred from {source.database_name!} ");
                
                // for 2 databases , also establish a table of equivalent old / new identifiers, for later checking

                if (source.id == 100120)
                {
                    IEnumerable<OldNewLink> ctg_ids = slh.GetOldAndNewids(source.id, 
                                                      source_conn_string, 44);
                    ulong ctg_num = slh.StoreLinksInCTGLInksTable(CopyHelpers.oldnewlink_ctg_helper, ctg_ids);
                    _loggingHelper.LogLine($"{ctg_num} old / new id pairs transferred from CTG database");
                }

                if (source.id == 100132)
                {
                    IEnumerable<OldNewLink> dutch_ids = slh.GetOldAndNewids(source.id, 
                                                      source_conn_string, 45);
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
            if (_testing)
            {
                // do anything? - slh.TransferTestIdentifiers(source.id);
            }

            if (source.has_study_tables is true)
            {
                string source_conn_string = _credentials.GetConnectionString(source.database_name!, _testing);
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

        slh.ProcessGroupedStudies();

        // Identify and repair missing 'link cascade' steps. Then cascade 'preferred' studies so that the 
        // most preferred always appears on the RHS.

        slh.AddMissingLinks();
        slh.CascadeLinks();
                        
        // Again, identify and remove studies that have links to more than 1 study in another registry.
        // Repeated because a small number (about 30) are formed by the cascade process above

        slh.ProcessGroupedStudies();

        // Transfer the resultant set into the main links table and tidy up

        slh.TransferNewLinksToDataTable();
        slh.UpdateLinksWithStudyIds();
        slh.DropTempTables();
    }


    public void AddStudyStudyRelationshipRecords()
    {
        // Adds the study relationship records previously created.
        // But first use the study_all_ids to insert the correct study Ids for the linked sources / sd_sids.
        
        slh.AddStudyStudyRelationshipRecords();
    }
}
