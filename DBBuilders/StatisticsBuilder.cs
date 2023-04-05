namespace MDR_Aggregator;

public class StatisticsBuilder
{
    private readonly int _agg_event_id;
    private readonly IMonDataLayer _monDatalayer;
    private readonly ILoggingHelper _loggingHelper;
    private readonly bool _testing;

    public StatisticsBuilder(int agg_event_id, IMonDataLayer monDatalayer, ILoggingHelper logginghelper, bool testing)
    {
        _agg_event_id = agg_event_id;
        _monDatalayer = monDatalayer;
        _loggingHelper = logginghelper;
        _testing = testing;
    }


    public void GetStatisticsBySource()
    {
        // get the list of sources
        IEnumerable<Source> sources = _monDatalayer.RetrieveDataSources();
        // Loop through and...
        // derive a connection string for each source,
        // then get the records contained in each ad table
        // and store it in the database.
        _monDatalayer.DeleteSameEventDBStats(_agg_event_id);

        _loggingHelper.LogLine("");
        _loggingHelper.LogLine("STATISTICS FOR EACH SOURCE DATABASE");
        _loggingHelper.LogLine("");

        foreach (Source s in sources)
        {
            string conn_string = _monDatalayer.GetConnectionString(s.database_name!, false);
            SourceSummary sm = new(_agg_event_id, s.database_name!);

            sm.study_recs = _monDatalayer.GetRecNum("studies", conn_string);
            sm.study_identifiers_recs = _monDatalayer.GetRecNum("study_identifiers", conn_string);
            sm.study_titles_recs = _monDatalayer.GetRecNum("study_titles", conn_string);
            sm.study_contributors_recs = _monDatalayer.GetRecNum("study_contributors", conn_string);
            sm.study_topics_recs = _monDatalayer.GetRecNum("study_topics", conn_string);
            sm.study_features_recs = _monDatalayer.GetRecNum("study_features", conn_string);
            sm.study_references_recs = _monDatalayer.GetRecNum("study_references", conn_string);
            sm.study_relationships_recs = _monDatalayer.GetRecNum("study_relationships", conn_string);
            
            sm.data_object_recs = _monDatalayer.GetRecNum("data_objects", conn_string);
            sm.object_datasets_recs = _monDatalayer.GetRecNum("object_datasets", conn_string);
            sm.object_instances_recs = _monDatalayer.GetRecNum("object_instances", conn_string);
            sm.object_titles_recs = _monDatalayer.GetRecNum("object_titles", conn_string);
            sm.object_dates_recs = _monDatalayer.GetRecNum("object_dates", conn_string);
            sm.object_contributors_recs = _monDatalayer.GetRecNum("object_contributors", conn_string);
            sm.object_topics_recs = _monDatalayer.GetRecNum("object_topics", conn_string);
            sm.object_identifiers_recs = _monDatalayer.GetRecNum("object_identifiers", conn_string);
            sm.object_descriptions_recs = _monDatalayer.GetRecNum("object_descriptions", conn_string);
            sm.object_rights_recs = _monDatalayer.GetRecNum("object_rights", conn_string);
            sm.object_relationships_recs = _monDatalayer.GetRecNum("object_relationships", conn_string);

            _monDatalayer.StoreSourceSummary(sm);
            _loggingHelper.LogLine("Summary stats generated for " + s.database_name + " tables");
        }
    }


    public void GetSummaryStatistics()
    {
        // Obtains figures for aggrgeate tables
        string conn_string = _monDatalayer.GetConnectionString("mdr", _testing);
        AggregationSummary sm = new AggregationSummary(_agg_event_id);

        _loggingHelper.LogLine("");
        _loggingHelper.LogLine("STATISTICS FOR CORE TABLES");
        _loggingHelper.LogLine("");

        _monDatalayer.DeleteSameEventSummaryStats(_agg_event_id);

        sm.study_recs = _monDatalayer.GetAggregateRecNum("studies", "st", conn_string);
        sm.study_identifiers_recs = _monDatalayer.GetAggregateRecNum("study_identifiers", "st", conn_string);
        sm.study_titles_recs = _monDatalayer.GetAggregateRecNum("study_titles", "st", conn_string);
        sm.study_contributors_recs = _monDatalayer.GetAggregateRecNum("study_contributors", "st", conn_string);
        sm.study_topics_recs = _monDatalayer.GetAggregateRecNum("study_topics", "st", conn_string);
        sm.study_features_recs = _monDatalayer.GetAggregateRecNum("study_features", "st", conn_string);
        sm.study_relationships_recs = _monDatalayer.GetAggregateRecNum("study_relationships", "st", conn_string);

        sm.data_object_recs = _monDatalayer.GetAggregateRecNum("data_objects", "ob", conn_string);
        sm.object_datasets_recs = _monDatalayer.GetAggregateRecNum("object_datasets", "ob", conn_string);
        sm.object_instances_recs = _monDatalayer.GetAggregateRecNum("object_instances", "ob", conn_string);
        sm.object_titles_recs = _monDatalayer.GetAggregateRecNum("object_titles", "ob", conn_string);
        sm.object_dates_recs = _monDatalayer.GetAggregateRecNum("object_dates", "ob", conn_string);
        sm.object_contributors_recs = _monDatalayer.GetAggregateRecNum("object_contributors", "ob", conn_string);
        sm.object_topics_recs = _monDatalayer.GetAggregateRecNum("object_topics", "ob", conn_string);
        sm.object_identifiers_recs = _monDatalayer.GetAggregateRecNum("object_identifiers", "ob", conn_string);
        sm.object_descriptions_recs = _monDatalayer.GetAggregateRecNum("object_descriptions", "ob", conn_string);
        sm.object_rights_recs = _monDatalayer.GetAggregateRecNum("object_rights", "ob", conn_string);
        sm.object_relationships_recs = _monDatalayer.GetAggregateRecNum("object_relationships", "ob", conn_string);
        sm.study_object_link_recs = _monDatalayer.GetAggregateRecNum("all_ids_data_objects", "nk", conn_string);
        _monDatalayer.StoreAggregationSummary(sm);
        _loggingHelper.LogLine("Statistics done for mdr central schemas");

        _loggingHelper.LogLine("");
        _loggingHelper.LogLine("SUMMARY OBJECT AND STUDY STATS");
        _loggingHelper.LogLine("");

        // get and store data object types
        _monDatalayer.DeleteSameEventObjectStats(_agg_event_id);
        List<AggregationObjectNum> object_numbers = _monDatalayer.GetObjectTypes(_agg_event_id, conn_string);
        _monDatalayer.StoreObjectNumbers(CopyHelpers.object_numbers_helper, object_numbers);
        _loggingHelper.LogLine("Statistics done for different data objects");

        // get study-study linkage
        _monDatalayer.RecreateStudyStudyLinksTable();
        List<StudyStudyLinkData> study_link_numbers = _monDatalayer.GetStudyStudyLinkData(_agg_event_id, conn_string);
        _monDatalayer.StoreStudyLinkNumbers(CopyHelpers.study_link_numbers_helper, study_link_numbers);
        study_link_numbers = _monDatalayer.GetStudyStudyLinkData2(_agg_event_id, conn_string);
        _monDatalayer.StoreStudyLinkNumbers(CopyHelpers.study_link_numbers_helper, study_link_numbers);
        _loggingHelper.LogLine("Statistics done for study-study links");
    }
}

