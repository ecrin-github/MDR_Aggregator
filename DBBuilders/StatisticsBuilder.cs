using System.Collections.Generic;


namespace MDR_Aggregator
{
    public class StatisticsBuilder
    {
        int _agg_event_id;
        IMonitorDataLayer _mon_repo;
        ILoggingHelper _loggingHelper;
        ICredentials _credentials;
        bool _testing;

        public StatisticsBuilder(int agg_event_id, ICredentials credentials, 
            IMonitorDataLayer mon_repo, ILoggingHelper logginghelper, bool testing)
        {
            _agg_event_id = agg_event_id;
            _credentials = credentials;
            _mon_repo = mon_repo;
            _loggingHelper = logginghelper;
            _testing = testing;
        }


        public void GetStatisticsBySource()
        {
            // get the list of sources
            IEnumerable<Source> sources = _mon_repo.RetrieveDataSources();
            // Loop through and...
            // derive a connection string for each source,
            // then get the records contained in each ad table
            // and store it in the database.
            _mon_repo.DeleteSameEventDBStats(_agg_event_id);

            _loggingHelper.LogLine("");
            _loggingHelper.LogLine("STATISTICS FOR EACH SOURCE DATABASE");
            _loggingHelper.LogLine("");

            foreach (Source s in sources)
            {
                string conn_string = _credentials.GetConnectionString(s.database_name, false);
                SourceSummary sm = new SourceSummary(_agg_event_id, s.database_name);

                sm.study_recs = _mon_repo.GetRecNum("studies", conn_string);
                sm.study_identifiers_recs = _mon_repo.GetRecNum("study_identifiers", conn_string);
                sm.study_titles_recs = _mon_repo.GetRecNum("study_titles", conn_string);
                sm.study_contributors_recs = _mon_repo.GetRecNum("study_contributors", conn_string);
                sm.study_topics_recs = _mon_repo.GetRecNum("study_topics", conn_string);
                sm.study_features_recs = _mon_repo.GetRecNum("study_features", conn_string);
                sm.study_references_recs = _mon_repo.GetRecNum("study_references", conn_string);
                sm.study_relationships_recs = _mon_repo.GetRecNum("study_relationships", conn_string);
                
                sm.data_object_recs = _mon_repo.GetRecNum("data_objects", conn_string);
                sm.object_datasets_recs = _mon_repo.GetRecNum("object_datasets", conn_string);
                sm.object_instances_recs = _mon_repo.GetRecNum("object_instances", conn_string);
                sm.object_titles_recs = _mon_repo.GetRecNum("object_titles", conn_string);
                sm.object_dates_recs = _mon_repo.GetRecNum("object_dates", conn_string);
                sm.object_contributors_recs = _mon_repo.GetRecNum("object_contributors", conn_string);
                sm.object_topics_recs = _mon_repo.GetRecNum("object_topics", conn_string);
                sm.object_identifiers_recs = _mon_repo.GetRecNum("object_identifiers", conn_string);
                sm.object_descriptions_recs = _mon_repo.GetRecNum("object_descriptions", conn_string);
                sm.object_rights_recs = _mon_repo.GetRecNum("object_rights", conn_string);
                sm.object_relationships_recs = _mon_repo.GetRecNum("object_relationships", conn_string);

                _mon_repo.StoreSourceSummary(sm);
                _loggingHelper.LogLine("Summary stats generated for " + s.database_name + " tables");
            }
        }


        public void GetSummaryStatistics()
        {
            // Obtains figures for aggrgeate tables
            string conn_string = _credentials.GetConnectionString("mdr", _testing);
            AggregationSummary sm = new AggregationSummary(_agg_event_id);

            _loggingHelper.LogLine("");
            _loggingHelper.LogLine("STATISTICS FOR CORE TABLES");
            _loggingHelper.LogLine("");

            _mon_repo.DeleteSameEventSummaryStats(_agg_event_id);

            sm.study_recs = _mon_repo.GetAggregateRecNum("studies", "st", conn_string);
            sm.study_identifiers_recs = _mon_repo.GetAggregateRecNum("study_identifiers", "st", conn_string);
            sm.study_titles_recs = _mon_repo.GetAggregateRecNum("study_titles", "st", conn_string);
            sm.study_contributors_recs = _mon_repo.GetAggregateRecNum("study_contributors", "st", conn_string);
            sm.study_topics_recs = _mon_repo.GetAggregateRecNum("study_topics", "st", conn_string);
            sm.study_features_recs = _mon_repo.GetAggregateRecNum("study_features", "st", conn_string);
            sm.study_relationships_recs = _mon_repo.GetAggregateRecNum("study_relationships", "st", conn_string);

            sm.data_object_recs = _mon_repo.GetAggregateRecNum("data_objects", "ob", conn_string);
            sm.object_datasets_recs = _mon_repo.GetAggregateRecNum("object_datasets", "ob", conn_string);
            sm.object_instances_recs = _mon_repo.GetAggregateRecNum("object_instances", "ob", conn_string);
            sm.object_titles_recs = _mon_repo.GetAggregateRecNum("object_titles", "ob", conn_string);
            sm.object_dates_recs = _mon_repo.GetAggregateRecNum("object_dates", "ob", conn_string);
            sm.object_contributors_recs = _mon_repo.GetAggregateRecNum("object_contributors", "ob", conn_string);
            sm.object_topics_recs = _mon_repo.GetAggregateRecNum("object_topics", "ob", conn_string);
            sm.object_identifiers_recs = _mon_repo.GetAggregateRecNum("object_identifiers", "ob", conn_string);
            sm.object_descriptions_recs = _mon_repo.GetAggregateRecNum("object_descriptions", "ob", conn_string);
            sm.object_rights_recs = _mon_repo.GetAggregateRecNum("object_rights", "ob", conn_string);
            sm.object_relationships_recs = _mon_repo.GetAggregateRecNum("object_relationships", "ob", conn_string);
            sm.study_object_link_recs = _mon_repo.GetAggregateRecNum("all_ids_data_objects", "nk", conn_string);
            _mon_repo.StoreAggregationSummary(sm);
            _loggingHelper.LogLine("Statistics done for mdr central schemas");

            _loggingHelper.LogLine("");
            _loggingHelper.LogLine("SUMMARY OBJECT AND STUDY STATS");
            _loggingHelper.LogLine("");

            // get and store data object types
            _mon_repo.DeleteSameEventObjectStats(_agg_event_id);
            List<AggregationObjectNum> object_numbers = _mon_repo.GetObjectTypes(_agg_event_id, conn_string);
            _mon_repo.StoreObjectNumbers(CopyHelpers.object_numbers_helper, object_numbers);
            _loggingHelper.LogLine("Statistics done for different data objects");

            // get study-study linkage
            _mon_repo.RecreateStudyStudyLinksTable();
            List<StudyStudyLinkData> study_link_numbers = _mon_repo.GetStudyStudyLinkData(_agg_event_id, conn_string);
            _mon_repo.StoreStudyLinkNumbers(CopyHelpers.study_link_numbers_helper, study_link_numbers);
            study_link_numbers = _mon_repo.GetStudyStudyLinkData2(_agg_event_id, conn_string);
            _mon_repo.StoreStudyLinkNumbers(CopyHelpers.study_link_numbers_helper, study_link_numbers);
            _loggingHelper.LogLine("Statistics done for study-study links");
        }
    }
}

