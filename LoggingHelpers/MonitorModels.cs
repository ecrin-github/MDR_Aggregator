using Dapper.Contrib.Extensions;

namespace MDR_Aggregator
{
    [Table("sf.source_parameters")]
    public class Source
    {
        public int id { get; }
        public string? source_type { get; }
        public int? preference_rating { get; }
        public string? database_name { get; }
        public string? db_conn { get; set; }
        public int? default_harvest_type_id { get; }
        public bool? requires_file_name { get; }
        public bool? uses_who_harvest { get; }
        public int? harvest_chunk { get; }
        public string? local_folder { get; }
        public bool? local_files_grouped { get; }
        public int? grouping_range_by_id { get; }
        public string? local_file_prefix { get; }
        public bool? has_study_tables { get; }
        public bool? has_study_topics { get; }
        public bool? has_study_conditions { get; }
        public bool? has_study_features { get; }
        public bool? has_study_iec{ get; }
        public bool? has_study_contributors { get; }
        public bool? has_study_references { get; }
        public bool? has_study_relationships { get; }
        public bool? has_study_countries { get; }
        public bool? has_study_locations { get; }
        public bool? has_study_links { get; }
        public bool? has_object_datasets { get; }
        public bool? has_study_ipd_available { get; }
        public bool? has_object_dates { get; }
        public bool? has_object_rights { get; }
        public bool? has_object_relationships { get; }
        public bool? has_object_pubmed_set { get; }
    }


    [Table("sf.source_summaries")]
    public class SourceSummary	
    {
        [Key]
        public int id { get; set; }
        public int aggregation_event_id { get; set; }
        public DateTime agregation_datetime { get; set; }
        public string database_name { get; set; }
        public int study_recs { get; set; }
        public int study_identifiers_recs { get; set; }
        public int study_titles_recs { get; set; }
        public int study_contributors_recs { get; set; }
        public int study_topics_recs { get; set; }
        public int study_features_recs { get; set; }
        public int study_references_recs { get; set; }
        public int study_relationships_recs { get; set; }
        public int data_object_recs { get; set; }
        public int object_datasets_recs { get; set; }
        public int object_instances_recs { get; set; }
        public int object_titles_recs { get; set; }
        public int object_dates_recs { get; set; }
        public int object_contributors_recs { get; set; }
        public int object_topics_recs { get; set; }
        public int object_identifiers_recs { get; set; }
        public int object_descriptions_recs { get; set; }
        public int object_rights_recs { get; set; }
        public int object_relationships_recs { get; set; }

        public SourceSummary(int _aggregation_event_id, string _database_name)
        {
            aggregation_event_id = _aggregation_event_id;
            agregation_datetime = DateTime.Now;
            database_name = _database_name;
        }
    }

    [Table("sf.aggregation_summaries")]
    public class AggregationSummary
    {
        [Key]
        public int id { get; set; }
        public int aggregation_event_id { get; set; }
        public DateTime agregation_datetime { get; set; }
        public int study_recs { get; set; }
        public int study_identifiers_recs { get; set; }
        public int study_titles_recs { get; set; }
        public int study_contributors_recs { get; set; }
        public int study_topics_recs { get; set; }
        public int study_features_recs { get; set; }
        public int study_relationships_recs { get; set; }

        public int data_object_recs { get; set; }
        public int object_datasets_recs { get; set; }
        public int object_instances_recs { get; set; }
        public int object_titles_recs { get; set; }
        public int object_dates_recs { get; set; }
        public int object_contributors_recs { get; set; }
        public int object_topics_recs { get; set; }
        public int object_identifiers_recs { get; set; }
        public int object_descriptions_recs { get; set; }
        public int object_rights_recs { get; set; }
        public int object_relationships_recs { get; set; }

        public int study_object_link_recs { get; set; }

        public AggregationSummary(int _aggregation_event_id)
        {
            aggregation_event_id = _aggregation_event_id;
            agregation_datetime = DateTime.Now;
        }
    }

    [Table("sf.aggregation_object_numbers")]
    public class AggregationObjectNum
    {
        [Key]
        public int id { get; set; }
        public int aggregation_event_id { get; set; }
        public int object_type_id { get; set; }
        public string object_type_name { get; set; }
        public int number_of_type { get; set; }
    }


    [Table("sf.study_study_link_data")]
    public class StudyStudyLinkData
    {
        [Key]
        public int id { get; set; }
        public int source_id { get; set; }
        public string source_name { get; set; }
        public int other_source_id { get; set; }
        public string other_source_name { get; set; }
        public int number_in_other_source { get; set; }
    }


    [Table("sf.aggregation_events")]
    public class AggregationEvent
    {
        [ExplicitKey]
        public int id { get; set; }
        public DateTime? time_started { get; set; }
        public DateTime? time_ended { get; set; }
        public int? num_studies_imported { get; set; }
        public int? num_objects_imported { get; set; }
        public int? num_total_studies { get; set; }
        public int? num_total_objects { get; set; }
        public int? num_total_study_object_links { get; set; }
        public string comments { get; set; }

        public AggregationEvent(int _id)
        {
            id = _id;
            time_started = DateTime.Now;
        }

        public AggregationEvent() { }
    }


    public class DataSource
    {
        public int id { get; set; }
        public int? preference_rating { get; set; }
        public string database_name { get; set; }

        public DataSource(int _id, int? _preference_rating, string _database_name)
        {
            id = _id;
            preference_rating = _preference_rating;
            database_name = _database_name;

        }
    }

}
