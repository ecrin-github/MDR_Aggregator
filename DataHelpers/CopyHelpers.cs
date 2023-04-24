using PostgreSQLCopyHelper;
namespace MDR_Aggregator;

public static class CopyHelpers
{
    public static readonly PostgreSQLCopyHelper<StudyLink> links_helper =
         new PostgreSQLCopyHelper<StudyLink>("nk", "temp_study_links_collector")
             .MapInteger("source_1", x => x.source_1)
             .MapVarchar("sd_sid_1", x => x.sd_sid_1)
             .MapVarchar("sd_sid_2", x => x.sd_sid_2)
             .MapInteger("source_2", x => x.source_2);

    public static readonly PostgreSQLCopyHelper<DataSource> prefs_helper =
         new PostgreSQLCopyHelper<DataSource>("nk", "temp_preferences")
             .MapInteger("id", x => x.id)
             .MapInteger("preference_rating", x => x.preference_rating)
             .MapVarchar("database_name", x => x.database_name);
    
    public static readonly PostgreSQLCopyHelper<ComplexLink> complex_links_helper =
        new PostgreSQLCopyHelper<ComplexLink>("nk", "linked_study_groups")
            .MapInteger("source_id", x => x.srce)
            .MapVarchar("sd_sid", x => x.sdsid)
            .MapInteger("relationship_id", x => x.relationship_id)
            .MapInteger("target_source_id", x => x.target_srce)
            .MapVarchar("target_sd_sid", x => x.target_sdsid);

    public static readonly PostgreSQLCopyHelper<IdChecker> studyids_checker =
        new PostgreSQLCopyHelper<IdChecker>("nk", "temp_id_checker")
            .MapVarchar("sd_sid", x => x.sd_sid);
    
    public static readonly PostgreSQLCopyHelper<StudyId> study_ids_helper =
         new PostgreSQLCopyHelper<StudyId>("nk", "temp_study_ids")
             .MapInteger("source_id", x => x.source_id)
             .MapVarchar("sd_sid", x => x.sd_sid)
             .MapTimeStampTz("datetime_of_data_fetch", x => x.datetime_of_data_fetch);

    public static readonly PostgreSQLCopyHelper<ObjectId> object_ids_helper =
         new PostgreSQLCopyHelper<ObjectId>("nk", "temp_object_ids")
             .MapInteger("source_id", x => x.source_id)
             .MapVarchar("sd_oid", x => x.sd_oid)
             .MapInteger("parent_study_source_id", x => x.parent_study_source_id)
             .MapVarchar("parent_study_sd_sid", x => x.parent_study_sd_sid)
             .MapTimeStampTz("datetime_of_data_fetch", x => x.datetime_of_data_fetch);

    public static readonly PostgreSQLCopyHelper<PMIDLink> pmid_links_helper =
         new PostgreSQLCopyHelper<PMIDLink>("nk", "temp_pmids")
             .MapInteger("source_id", x => x.source_id)
             .MapVarchar("sd_oid", x => x.sd_oid)
             .MapInteger("parent_study_source_id", x => x.parent_study_source_id)
             .MapVarchar("parent_study_sd_sid", x => x.parent_study_sd_sid)
             .MapInteger("type_id", x => x.type_id)
             .MapTimeStampTz("datetime_of_data_fetch", x => x.datetime_of_data_fetch);

    public static readonly PostgreSQLCopyHelper<AggregationObjectNum> object_numbers_helper =
         new PostgreSQLCopyHelper<AggregationObjectNum>("sf", "aggregation_object_numbers")
             .MapInteger("aggregation_event_id", x => x.aggregation_event_id)
             .MapInteger("object_type_id", x =>  x.object_type_id)
             .MapVarchar("object_type_name", x => x.object_type_name)
             .MapInteger("number_of_type", x => x.number_of_type);

    public static readonly PostgreSQLCopyHelper<StudyStudyLinkData> study_link_numbers_helper =
         new PostgreSQLCopyHelper<StudyStudyLinkData>("sf", "study_study_link_data")
             .MapInteger("source_id", x => x.source_id)
             .MapVarchar("source_name", x => x.source_name)
             .MapInteger("other_source_id", x => x.other_source_id)
             .MapVarchar("other_source_name", x => x.other_source_name)
             .MapInteger("number_in_other_source", x => x.number_in_other_source);

    public static readonly PostgreSQLCopyHelper<OldNewLink> oldnewlink_ctg_helper =
        new PostgreSQLCopyHelper<OldNewLink>("nk", "ctg_id_checker")
            .MapVarchar("new_id", x => x.new_id)
            .MapVarchar("old_id", x => x.old_id);
    
    public static readonly PostgreSQLCopyHelper<OldNewLink> oldnewlink_ntr_helper =
        new PostgreSQLCopyHelper<OldNewLink>("nk", "dutch_id_checker")
            .MapVarchar("new_id", x => x.new_id)
            .MapVarchar("old_id", x => x.old_id);
}