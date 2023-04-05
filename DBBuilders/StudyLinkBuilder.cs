namespace MDR_Aggregator;

public class StudyLinkBuilder
{
    LinksDataHelper slh;
    string _mdr_connString;
    ICredentials _credentials;
    bool _testing;

    public StudyLinkBuilder(ICredentials credentials, string mdr_connString, bool testing)
    {
        _credentials = credentials;
        _mdr_connString = mdr_connString;
        _testing = testing;

        slh = new LinksDataHelper(_mdr_connString);
    }
        
    public void CollectStudyStudyLinks(IEnumerable<Source> sources)
    {
        // Loop through it calling the link helper functions
        // sources are called in 'preference order' starting
        // with clinical trials.gov.

        slh.SetUpTempPreferencesTable(sources);
        slh.SetUpTempLinkCollectorTable();
        slh.SetUpTempLinkSortedTable();

        foreach (Source source in sources)
        {
            // need to populate the ad tables in a test situation with 
            // the relevant data, as the source_conn_string will always 
            // point to 'test' - at least get the study identifiers data!

            if (_testing)
            {
                slh.TransferTestIdentifiers(source.id);
            }

            // Fetch the study-study links and store them
            // in the Collector table (asuming the source has study data)
            if (source.has_study_tables is true)
            {
                string source_conn_string = _credentials.GetConnectionString(source.database_name!, _testing);
                IEnumerable<StudyLink> links = slh.FetchLinks(source.id, source_conn_string);
                slh.StoreLinksInTempTable(CopyHelpers.links_helper, links);
            }
        }

        // Tidy up common format errors.

        slh.TidyIds1();
        slh.TidyIds2();
        slh.TidyIds3();
    }


    public void ProcessStudyStudyLinks()
    {
        // Create a table with the distinct values obtained 
        // from the aggregation process.

        slh.TransferLinksToSortedTable();
        slh.CreateDistinctSourceLinksTable();

        // Identify and remove studies that have links to more than 1
        // study in another registry - these form study-study relationships
        // rather than simple 1-to-1 study links (though the target links may 
        // need to be updated at the end of the process)

        slh.IdentifyGroupedStudies();
        slh.ExtractGroupedStudiess();
        slh.DeleteGroupedStudyLinkRecords();

        // Cascade 'preferred' studies so that the 
        // most preferred always appears on the RHS
        // Identify and repair missing cascade steps
        // then 're-cascade' links.

        //slh.CascadeLinksInDistinctLinksTable();
        slh.ManageIncompleteLinks();
        slh.CascadeLinksInDistinctLinksTable();

        // Transfer the (distinct) resultant set into the 
        // main links table and tidy up
        slh.TransferNewLinksToDataTable();
        slh.DropTempTables();
    }


    public void CreateStudyGroupRecords()
    {
        // Select* from nk.linked_study_groups 
        // Use the study_all_ids to insert the study Ids
        // for the linked sources / sd_sids
        slh.AddStudyStudyRelationshipRecords();
    }
}
