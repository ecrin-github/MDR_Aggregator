namespace MDR_Aggregator;

public class CoreDataTransferrer
{
    string _connString;
    DBUtilities db;
    ILoggingHelper _loggingHelper;


    public CoreDataTransferrer(string connString, ILoggingHelper logginghelper)
    {
        _loggingHelper = logginghelper;
        _connString = connString;
        db = new DBUtilities(_connString, _loggingHelper);
    }

    public int LoadCoreStudyData()
    {
        string sql_string = @"INSERT INTO core.studies(id, 
                display_title, title_lang_code, brief_description, data_sharing_statement,
                study_start_year, study_start_month, study_type_id, study_status_id,
                study_enrolment, study_gender_elig_id, min_age, min_age_units_id,
                max_age, max_age_units_id)
                SELECT id,
                display_title, title_lang_code, brief_description, data_sharing_statement,
                study_start_year, study_start_month, study_type_id, study_status_id,
                study_enrolment, study_gender_elig_id, min_age, min_age_units_id,
                max_age, max_age_units_id
                FROM st.studies";

        return db.ExecuteCoreTransferSQL(sql_string, "st.studies");
    }


    public int LoadCoreStudyIdentifiers()
    {
        string sql_string = @"INSERT INTO core.study_identifiers(id, study_id, 
                identifier_value, identifier_type_id, 
                identifier_org_id, identifier_org,
                identifier_org_ror_id,
                identifier_date, identifier_link)
                SELECT id, study_id,
                identifier_value, identifier_type_id, 
                identifier_org_id, identifier_org,
                identifier_org_ror_id,
                identifier_date, identifier_link
                FROM st.study_identifiers";

        return db.ExecuteCoreTransferSQL(sql_string, "st.study_identifiers");
    }


    public int LoadCoreStudyTitles()
    {
        string sql_string = @"INSERT INTO core.study_titles(id, study_id, 
                title_type_id, title_text, lang_code, 
                lang_usage_id, is_default, comments)
                SELECT id, study_id,
                title_type_id, title_text, lang_code, 
                lang_usage_id, is_default, comments 
                FROM st.study_titles";

        return db.ExecuteCoreTransferSQL(sql_string, "st.study_titles");
    }


    public int LoadCoreStudyContributors()
    {
        string sql_string = @"INSERT INTO core.study_contributors(id, study_id, 
                contrib_type_id, is_individual, 
                person_id, person_given_name, person_family_name, person_full_name,
                orcid_id, person_affiliation, 
                organisation_id, organisation_name, organisation_ror_id)
                SELECT id, study_id,
                contrib_type_id, is_individual, 
                person_id, person_given_name, person_family_name, person_full_name,
                orcid_id, person_affiliation, 
                organisation_id, organisation_name, organisation_ror_id
                FROM st.study_contributors";

        return db.ExecuteCoreTransferSQL(sql_string, "st.study_contributors");
    }


    public int LoadCoreStudyTopics()
    {
        string sql_string = @"INSERT INTO core.study_topics(id, study_id, 
                topic_type_id, mesh_coded, mesh_code, mesh_value, 
                original_ct_id, original_ct_code, original_value)
                SELECT id, study_id,
                topic_type_id, mesh_coded, mesh_code, mesh_value, 
                original_ct_id, original_ct_code, original_value
                FROM st.study_topics";

        return db.ExecuteCoreTransferSQL(sql_string, "st.study_topics");
    }


    public int LoadCoreStudyFeatures()
    {
        string sql_string = @"INSERT INTO core.study_features(id, study_id, 
                feature_type_id, feature_value_id)
                SELECT id, study_id,
                feature_type_id, feature_value_id 
                FROM st.study_features";

        return db.ExecuteCoreTransferSQL(sql_string, "st.study_features");
    }


    public int LoadCoreStudyRelationShips()
    {
        string sql_string = @"INSERT INTO core.study_relationships(id, study_id, 
                relationship_type_id, target_study_id)
                SELECT id, study_id,
                relationship_type_id, target_study_id
                FROM st.study_relationships";

        return db.ExecuteCoreTransferSQL(sql_string, "st.study_relationships");
    }


    public int LoadCoreDataObjects()
    {
        string sql_string = @"INSERT INTO core.data_objects(id,
                display_title, version, doi, doi_status_id, publication_year,
                object_class_id, object_type_id, managing_org_id, 
                managing_org, managing_org_ror_id,
                lang_code, access_type_id, access_details, access_details_url,
                url_last_checked, eosc_category, add_study_contribs, 
                add_study_topics)
                SELECT id, 
                display_title, version, doi, doi_status_id, publication_year,
                object_class_id, object_type_id, managing_org_id, 
                managing_org, managing_org_ror_id,
                lang_code, access_type_id, access_details, access_details_url,
                url_last_checked, eosc_category, add_study_contribs, 
                add_study_topics
                FROM ob.data_objects";

        return db.ExecuteCoreTransferSQL(sql_string, "ob.data_objects");

    }


    public int LoadCoreObjectDatasets()
    {
        string sql_string = @"INSERT INTO core.object_datasets(id, object_id, 
        record_keys_type_id, record_keys_details, 
        deident_type_id, deident_direct, deident_hipaa,
        deident_dates, deident_nonarr, deident_kanon, deident_details,
        consent_type_id, consent_noncommercial, consent_geog_restrict,
        consent_research_type, consent_genetic_only, consent_no_methods, consent_details)
        SELECT id, object_id, 
        record_keys_type_id, record_keys_details, 
        deident_type_id, deident_direct, deident_hipaa,
        deident_dates, deident_nonarr, deident_kanon, deident_details,
        consent_type_id, consent_noncommercial, consent_geog_restrict,
        consent_research_type, consent_genetic_only, consent_no_methods, consent_details
        FROM ob.object_datasets";

        return db.ExecuteCoreTransferSQL(sql_string, "ob.object_datasets");
    }


    public int LoadCoreObjectInstances()
    {
        string sql_string = @"INSERT INTO core.object_instances(id, object_id,  
        instance_type_id, repository_org_id, repository_org,
        url, url_accessible, url_last_checked, resource_type_id,
        resource_size, resource_size_units, resource_comments)
        SELECT id, object_id, 
        instance_type_id, repository_org_id, repository_org,
        url, url_accessible, url_last_checked, resource_type_id,
        resource_size, resource_size_units, resource_comments
        FROM ob.object_instances";

        return db.ExecuteCoreTransferSQL(sql_string, "ob.object_instances");
    }


    public int LoadCoreObjectTitles()
    {
        string sql_string = @"INSERT INTO core.object_titles(id, object_id, 
        title_type_id, title_text, lang_code,
        lang_usage_id, is_default, comments)
        SELECT id, object_id, 
        title_type_id, title_text, lang_code,
        lang_usage_id, is_default, comments
        FROM ob.object_titles";

        return db.ExecuteCoreTransferSQL(sql_string, "ob.object_titles");
    }


    public int LoadCoreObjectDates()
    {
        string sql_string = @"INSERT INTO core.object_dates(id, object_id,  
        date_type_id, date_is_range, date_as_string, start_year, 
        start_month, start_day, end_year, end_month, end_day, details)
        SELECT id, object_id, 
        date_type_id, date_is_range, date_as_string, start_year, 
        start_month, start_day, end_year, end_month, end_day, details
        FROM ob.object_dates";

        return db.ExecuteCoreTransferSQL(sql_string, "ob.object_dates");
    }


    public int LoadCoreObjectContributors()
    {
        string sql_string = @"INSERT INTO core.object_contributors(id, object_id, 
        contrib_type_id, is_individual, 
        person_id, person_given_name, 
        person_family_name, person_full_name,
        orcid_id, person_affiliation, 
        organisation_id, organisation_name, organisation_ror_id)
        SELECT id, object_id, 
        contrib_type_id, is_individual, 
        person_id, person_given_name, 
        person_family_name, person_full_name,
        orcid_id, person_affiliation, 
        organisation_id, organisation_name, organisation_ror_id
        FROM ob.object_contributors";

        return db.ExecuteCoreTransferSQL(sql_string, "ob.object_contributors");
    }


    public int LoadCoreObjectTopics()
    {
        string sql_string = @"INSERT INTO core.object_topics(id, object_id,  
        topic_type_id, mesh_coded, mesh_code, mesh_value, 
        original_ct_id, original_ct_code, original_value)
        SELECT id, object_id, 
        topic_type_id, mesh_coded, mesh_code, mesh_value, 
        original_ct_id, original_ct_code, original_value
        FROM ob.object_topics";

        return db.ExecuteCoreTransferSQL(sql_string, "ob.object_topics");
    }


    public int LoadCoreObjectDescriptions()
    {
        string sql_string = @"INSERT INTO core.object_descriptions(id, object_id, 
        description_type_id, label, description_text, lang_code)
        SELECT id, object_id, 
        description_type_id, label, description_text, lang_code
        FROM ob.object_descriptions";

        return db.ExecuteCoreTransferSQL(sql_string, "ob.object_descriptions");
    }


    public int LoadCoreObjectIdentifiers()
    {
        string sql_string = @"INSERT INTO core.object_identifiers(id, object_id, 
        identifier_value, identifier_type_id, identifier_org_id, 
        identifier_org, identifier_org_ror_id,
        identifier_date)
        SELECT id, object_id, 
        identifier_value, identifier_type_id, identifier_org_id,
        identifier_org, identifier_org_ror_id,
        identifier_date
        FROM ob.object_identifiers";

        return db.ExecuteCoreTransferSQL(sql_string, "ob.object_identifiers");
    }


    public int LoadCoreObjectRelationships()
    {
        string sql_string = @"INSERT INTO core.object_relationships(id, object_id,   
        relationship_type_id, target_object_id)
        SELECT id, object_id, 
        relationship_type_id, target_object_id
        FROM ob.object_relationships";

        return db.ExecuteCoreTransferSQL(sql_string, "ob.object_relationships");
    }


    public int LoadCoreObjectRights()
    {
        string sql_string = @"INSERT INTO core.object_rights(id, object_id,  
        rights_name, rights_uri, comments)
        SELECT id, object_id,
        rights_name, rights_uri, comments
        FROM ob.object_rights";

        return db.ExecuteCoreTransferSQL(sql_string, "ob.object_rights");
    }


    public int LoadStudyObjectLinks()
    {
        string sql_string = @"INSERT INTO core.study_object_links(id, 
        study_id, object_id)
        SELECT id, parent_study_id, object_id
        FROM nk.all_ids_data_objects";

        return db.ExecuteCoreTransferSQL(sql_string, "nk.all_ids_data_objects");
    }


    public void GenerateStudyProvenanceData()
    {
        string sql_string = "";
        sql_string = @"DROP TABLE IF EXISTS nk.temp_study_provenance;
            CREATE table nk.temp_study_provenance
                 as
                 select s.study_id, 
                 'Data retrieved from ' || string_agg(d.repo_name || ' at ' || to_char(s.datetime_of_data_fetch, 'HH24:MI, dd Mon yyyy'), ', ' ORDER BY s.datetime_of_data_fetch) as provenance
                 from nk.all_ids_studies s
                 inner join
                    (select t.id,
                      case 
                        when p.uses_who_harvest = true then t.default_name || ' (via WHO ICTRP)'
                        else t.default_name
                      end as repo_name 
                     from context_ctx.data_sources t
                     inner join mon_sf.source_parameters p
                     on t.id = p.id) d
                 on s.source_id = d.id
                 group by study_id ";
        db.ExecuteSQL(sql_string);

        sql_string = @"update core.studies s
                set provenance_string = tt.provenance
                from nk.temp_study_provenance tt
                where s.id = tt.study_id ";
        db.ExecuteProvenanceSQL(sql_string, "core.studies");

        sql_string = @"drop table nk.temp_study_provenance;";
        db.ExecuteSQL(sql_string);
    }


    public void GenerateObjectProvenanceData()
    {
        string sql_string = "";
        sql_string = @"DROP TABLE IF EXISTS nk.temp_object_provenance;
            create table nk.temp_object_provenance
                 as
                 select s.object_id, 
                 'Data retrieved from ' || string_agg(d.repo_name || ' at ' || to_char(s.datetime_of_data_fetch, 'HH24:MI, dd Mon yyyy'), ', ' ORDER BY s.datetime_of_data_fetch) as provenance
                 from nk.all_ids_data_objects s
                 inner join
                    (select t.id,
                      case 
                        when p.uses_who_harvest = true then t.default_name || ' (via WHO ICTRP)'
                        else t.default_name
                      end as repo_name 
                     from context_ctx.data_sources t
                     inner join mon_sf.source_parameters p
                     on t.id = p.id) d
                on s.source_id = d.id
                where s.source_id <> 100135
                group by object_id ";
        db.ExecuteSQL(sql_string);

        // PubMed objects need a different approach
        sql_string = @"create table nk.temp_pubmed_object_provenance
                 as select s.sd_oid,
                 'Data retrieved from Pubmed at ' || TO_CHAR(max(s.datetime_of_data_fetch), 'HH24:MI, dd Mon yyyy') as provenance
                 from nk.all_ids_data_objects s
                 inner
                 join
              (select t.id,
                     t.default_name as repo_name
                     from context_ctx.data_sources t
                     ) d
                on s.source_id = d.id
                where s.source_id = 100135
                group by s.sd_oid ";
        db.ExecuteSQL(sql_string);

        // update non pubmed objects
        sql_string = @"update core.data_objects s
                set provenance_string = tt.provenance
                from nk.temp_object_provenance tt
                where s.id = tt.object_id ";
        db.ExecuteProvenanceSQL(sql_string, "core.data_objects");

        // update pubmed objects
        sql_string = @"update core.data_objects s
                set provenance_string = tt.provenance
                from nk.temp_pubmed_object_provenance tt
                inner join nk.all_ids_data_objects k
                on tt.sd_oid = k.sd_oid
                where s.id = k.object_id ";
        db.ExecuteProvenanceSQL(sql_string, "core.data_objects");

        sql_string = @"drop table nk.temp_object_provenance;
        drop table nk.temp_pubmed_object_provenance;";
        db.ExecuteSQL(sql_string);
    }
}