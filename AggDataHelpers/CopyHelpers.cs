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
             .MapInteger("parent_study_source_id", x => x.parent_study_source_id)
             .MapVarchar("sd_oid", x => x.sd_oid)
             .MapInteger("object_type_id", x => x.object_type_id)
             .MapVarchar("title", x => x.title)
             .MapVarchar("parent_study_sd_sid", x => x.parent_study_sd_sid)
             .MapTimeStampTz("datetime_of_data_fetch", x => x.datetime_of_data_fetch);
    
    public static readonly PostgreSQLCopyHelper<PMIDLink> pmid_links_helper =
         new PostgreSQLCopyHelper<PMIDLink>("nk", "temp_collected_pmids")
             .MapInteger("source_id", x => x.source_id)
             .MapVarchar("sd_oid", x => x.sd_oid)
             .MapInteger("parent_study_source_id", x => x.parent_study_source_id)
             .MapVarchar("parent_study_sd_sid", x => x.parent_study_sd_sid)
             .MapInteger("type_id", x => x.type_id)
             .MapTimeStampTz("datetime_of_data_fetch", x => x.datetime_of_data_fetch);

    public static readonly PostgreSQLCopyHelper<AggregationObjectNum> object_numbers_helper =
         new PostgreSQLCopyHelper<AggregationObjectNum>("sf", "agg_object_numbers")
             .MapInteger("agg_event_id", x => x.agg_event_id)
             .MapInteger("object_type_id", x =>  x.object_type_id)
             .MapVarchar("object_type_name", x => x.object_type_name)
             .MapInteger("number_of_type", x => x.number_of_type);

    public static readonly PostgreSQLCopyHelper<Study1To1LinkData> study_1to1_link_numbers_helper =
         new PostgreSQLCopyHelper<Study1To1LinkData>("sf", "agg_study_1to1_link_data")
             .MapInteger("agg_event_id", x => x.agg_event_id)
             .MapInteger("source_id", x => x.source_id)
             .MapVarchar("source_name", x => x.source_name)
             .MapInteger("other_source_id", x => x.other_source_id)
             .MapVarchar("other_source_name", x => x.other_source_name)
             .MapInteger("number_in_other_source", x => x.number_in_other_source);

    public static readonly PostgreSQLCopyHelper<Study1ToNLinkData> study_1ton_link_numbers_helper =
        new PostgreSQLCopyHelper<Study1ToNLinkData>("sf", "agg_study_1ton_link_data")
            .MapInteger("agg_event_id", x => x.agg_event_id)
            .MapInteger("source_id", x => x.source_id)
            .MapVarchar("source_name", x => x.source_name)
            .MapInteger("relationship_id", x => x.relationship_id)
            .MapVarchar("relationship", x => x.relationship)
            .MapInteger("target_source_id", x => x.target_source_id)
            .MapVarchar("target_source_name", x => x.target_source_name)
            .MapInteger("number_in_other_source", x => x.number_in_other_source);
    
    public static readonly PostgreSQLCopyHelper<OldNewLink> oldnewlink_ctg_helper =
        new PostgreSQLCopyHelper<OldNewLink>("nk", "ctg_id_checker")
            .MapVarchar("new_id", x => x.new_id)
            .MapVarchar("old_id", x => x.old_id);
    
    public static readonly PostgreSQLCopyHelper<OldNewLink> oldnewlink_ntr_helper =
        new PostgreSQLCopyHelper<OldNewLink>("nk", "dutch_id_checker")
            .MapVarchar("new_id", x => x.new_id)
            .MapVarchar("old_id", x => x.old_id);
    
    public static readonly PostgreSQLCopyHelper<IECStudyDetails> iec_study_helper =
        new PostgreSQLCopyHelper<IECStudyDetails>("ad", "studies")
            .MapInteger("study_id", x => x.study_id)
            .MapInteger("source_id", x => x.source_id)
            .MapVarchar("sd_sid", x => x.sd_sid)
            .MapBoolean("is_preferred", x=> x.is_preferred)
            .MapVarchar("display_title", x => x.display_title)
            .MapVarchar("brief_description", x => x.brief_description)
            .MapInteger("iec_level_id", x => x.study_id)
            .MapInteger("study_start_year", x => x.source_id)
            .MapInteger("study_start_month", x => x.study_id)
            .MapInteger("study_type_id", x => x.study_type_id)
            .MapVarchar("study_enrolment", x => x.study_enrolment)
            .MapInteger("study_gender_elig_id", x => x.study_gender_elig_id)
            .MapInteger("min_age", x => x.min_age)
            .MapInteger("min_age_units_id", x => x.min_age_units_id)
            .MapInteger("max_age", x => x.max_age)
            .MapInteger("max_age_units_id", x => x.max_age_units_id)
            .MapTimeStampTz("datetime_of_data_fetch", x => x.datetime_of_data_fetch);
    
    public static readonly PostgreSQLCopyHelper<LexemeBase> lexeme_base_helper =
        new PostgreSQLCopyHelper<LexemeBase>("search", "new_lexemes")
            .MapInteger("study_id", x => x.study_id)
            .MapVarchar("study_name", x => x.study_name)
            .MapVarchar("tt", x => x.tt)
            .MapVarchar("conditions", x => x.conditions);
    
    public static readonly PostgreSQLCopyHelper<JSONSearchResObject> object_search_helper =
        new PostgreSQLCopyHelper<JSONSearchResObject>("search", "new_objects")
            .MapInteger("oid", x => x.oid)
            .MapVarchar("ob_name", x => x.ob_name)
            .MapInteger("typeid", x => x.typeid)
            .MapVarchar("typename", x => x.typename)            
            .MapVarchar("url", x => x.url)   
            .MapInteger("res_type_id", x => x.res_type_id)
            .MapVarchar("res_icon", x => x.res_icon)
            .MapVarchar("year_pub", x => x.year_pub)
            .MapVarchar("acc_icon", x => x.acc_icon)
            .MapVarchar("prov", x => x.prov);

}