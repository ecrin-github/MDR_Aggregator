using MDR_Aggregator.AggDataHelpers;
using MDR_Aggregator.LoggingHelpers.Interfaces;

namespace MDR_Aggregator.CoreDBBuilders;

public class CoreDataTransferrer
{
    readonly DbUtilities _db;

    public CoreDataTransferrer(string connString, ILoggingHelper loggingHelper)
    {
        _db = new DbUtilities(connString, loggingHelper);
    }
    
    private readonly Dictionary<string, string> _addFields = new() 
    {
        { "studies", @"id, 
                display_title, title_lang_code, brief_description, data_sharing_statement,
                study_start_year, study_start_month, study_type_id, study_status_id,
                study_enrolment, study_gender_elig_id, min_age, min_age_units_id,
                max_age, max_age_units_id, iec_level" },
        { "study_identifiers", @"id, study_id, identifier_value, identifier_type_id, 
                source_id, source, source_ror_id, identifier_date, identifier_link" },
        { "study_titles", @"id, study_id, 
                title_type_id, title_text, lang_code, 
                lang_usage_id, is_default, comments " },
        { "study_references", @"sd_sid, pmid, citation, doi, type_id, comments" },
        { "study_people", @"id, study_id, contrib_type_id, person_given_name, person_family_name, 
                person_full_name, orcid_id, person_affiliation, 
                organisation_id, organisation_name, organisation_ror_id" },
        { "study_organisations", @"id, study_id, contrib_type_id,  
                organisation_id, organisation_name, organisation_ror_id" },
        { "study_topics", @"id, study_id, 
                topic_type_id, original_value, original_ct_type_id, 
                original_ct_code, mesh_code, mesh_value " },
        { "study_relationships", @"id, study_id, 
                relationship_type_id, target_study_id " },
        { "study_features", @"id, study_id, 
                feature_type_id, feature_value_id " },
        { "study_countries", @"id, study_id, country_id, country_name, status_id " },
        { "study_locations", @"id, study_id, facility_org_id, facility, facility_ror_id,
        city_id, city_name, country_id, country_name, status_id " },
        { "study_conditions", @"id, study_id, original_value, original_ct_type_id, original_ct_code, 
        icd_code, icd_name " },
        { "study_icd", @"id, study_id, icd_code, icd_name " },
        { "study_iec", @"id, study_id, seq_num, iec_type_id, split_type, leader, indent_level,
          sequence_string, iec_text " }
    };
    

    public int LoadCoreStudyData(string schemaName)
    {
        string field_string = _addFields["studies"];
        string sql_string = $@"INSERT INTO core.studies({field_string})
                SELECT {field_string}
                FROM {schemaName}.studies";
        return _db.ExecuteCoreTransferSql(sql_string, " where ", "aggs_st.studies");
    }

    public int LoadCoreStudyIdentifiers(string schemaName)
    {
        string field_string = _addFields["study_identifiers"];
        string sql_string = $@"INSERT INTO core.study_identifiers({field_string})
                SELECT {field_string}
                FROM {schemaName}.study_identifiers";
        return _db.ExecuteCoreTransferSql(sql_string, " where ", "aggs_st.study_identifiers");
    }

    public int LoadCoreStudyTitles(string schemaName)
    {
        string field_string = _addFields["study_titles"];
        string sql_string = $@"INSERT INTO core.study_titles({field_string})
                SELECT {field_string}
                FROM {schemaName}.study_titles";
        return _db.ExecuteCoreTransferSql(sql_string, " where ", "aggs_st.study_titles");
    }
    
    public int LoadCoreStudyPeople(string schemaName)
    {
        string field_string = _addFields["study_people"];
        string sql_string = $@"INSERT INTO core.study_people({field_string})
                SELECT {field_string}
                FROM {schemaName}.study_people";
        return _db.ExecuteCoreTransferSql(sql_string, " where ", "aggs_st.study_people");
    }
    
    public int LoadCoreStudyOrganisations(string schemaName)
    {
        string field_string = _addFields["study_organisations"];
        string sql_string = $@"INSERT INTO core.study_organisations({field_string})
                SELECT {field_string}
                FROM {schemaName}.study_organisations";
        return _db.ExecuteCoreTransferSql(sql_string, " where ", "aggs_st.study_organisations");
    }

    public int LoadCoreStudyTopics(string schemaName)
    {
        string field_string = _addFields["study_topics"];
        string sql_string = $@"INSERT INTO core.study_topics({field_string})
                SELECT {field_string}
                FROM {schemaName}.study_topics";
        return _db.ExecuteCoreTransferSql(sql_string, " where ", "aggs_st.study_topics");
    }

    public int LoadCoreStudyFeatures(string schemaName)
    {
        string field_string = _addFields["study_features"];
        string sql_string = $@"INSERT INTO core.study_features({field_string})
                SELECT {field_string}
                FROM {schemaName}.study_features";
        return _db.ExecuteCoreTransferSql(sql_string, " where ", "aggs_st.study_features");
    }

    public int LoadCoreStudyRelationShips(string schemaName)
    {
        string field_string = _addFields["study_relationships"];
        string sql_string = $@"INSERT INTO core.study_relationships({field_string})
                SELECT {field_string}
                FROM {schemaName}.study_relationships";
        return _db.ExecuteCoreTransferSql(sql_string, " where ", "aggs_st.study_relationships");
    }

    public int LoadCoreStudyConditions(string schemaName)
    {
        string field_string = _addFields["study_conditions"];
        string sql_string = $@"INSERT INTO core.study_conditions({field_string})
                SELECT {field_string}
                FROM {schemaName}.study_conditions";
        return _db.ExecuteCoreTransferSql(sql_string, " where ", "aggs_st.study_conditions");
    }
    
    public int LoadCoreStudyICDs(string schemaName)
    {
        string field_string = _addFields["study_icd"];
        string sql_string = $@"INSERT INTO core.study_icd({field_string})
                SELECT {field_string}
                FROM {schemaName}.study_icd";
        return _db.ExecuteCoreTransferSql(sql_string, " where ", "aggs_st.study_icd");
    }
    
    public int LoadCoreStudyCountries(string schemaName)
    {
        string field_string = _addFields["study_countries"];
        string sql_string = $@"INSERT INTO core.study_countries({field_string})
                SELECT {field_string}
                FROM {schemaName}.study_countries";
        return _db.ExecuteCoreTransferSql(sql_string, " where ", "aggs_st.study_countries");
    }
    
    public int LoadCoreStudyLocations(string schemaName)
    {
        string field_string = _addFields["study_locations"];
        string sql_string = $@"INSERT INTO core.study_locations({field_string})
                SELECT {field_string}
                FROM {schemaName}.study_locations";
        return _db.ExecuteCoreTransferSql(sql_string, " where ", "aggs_st.study_locations");
    }
    
    private readonly Dictionary<string, string> _objectFields = new() 
    {
        { "data_objects", @"id, title, version, display_title, 
         doi, doi_status_id, publication_year, object_class_id, object_type_id, 
         managing_org_id, managing_org, managing_org_ror_id, lang_code, access_type_id, 
         access_details, access_details_url, url_last_checked, eosc_category, 
         add_study_contribs, add_study_topics" },
        { "object_datasets", @"id, object_id, record_keys_type_id, record_keys_details, 
         deident_type_id, deident_direct, deident_hipaa, deident_dates, deident_nonarr, 
         deident_kanon, deident_details, consent_type_id, consent_noncommercial, consent_geog_restrict,
         consent_research_type, consent_genetic_only, consent_no_methods, consent_details" },
        { "object_instances", @"id, object_id,  
         system_id, system, url, url_accessible, url_last_checked, 
         resource_type_id, resource_size, resource_size_units, resource_comments" },
        { "object_titles", @"id, object_id, title_type_id, title_text, lang_code,
         lang_usage_id, is_default, comments" },
        { "object_dates", @"id, object_id, date_type_id, date_is_range, date_as_string, start_year, 
         start_month, start_day, end_year, end_month, end_day, details" },
        { "object_people", @"id, object_id, contrib_type_id, person_given_name, person_family_name, 
         person_full_name, orcid_id, person_affiliation, 
         organisation_id, organisation_name, organisation_ror_id" },
        { "object_organisations", @"id, object_id, 
         contrib_type_id, organisation_id, organisation_name, organisation_ror_id" },
        { "object_topics", @"id, object_id, topic_type_id, original_value, original_ct_type_id,
         original_ct_code, mesh_code, mesh_value" },
        { "object_comments", @"id, object_id, ref_type, ref_source, pmid, pmid_version, notes" },
        { "object_descriptions", @"id, object_id, description_type_id, label, description_text, lang_code" },
        { "object_identifiers", @"id, object_id, identifier_value, identifier_type_id, 
         source_id, source, source_ror_id, identifier_date" },
        { "object_rights", @"id, object_id, rights_name, rights_uri, comments" },
        { "object_relationships", @"id, object_id, relationship_type_id, target_object_id" },
        { "study_object_links", @"id, study_id, object_id" },
    };
    
    public int LoadCoreDataObjects(string schemaName)
    {
        string field_string = _objectFields["data_objects"];
        string sql_string = $@"INSERT INTO core.data_objects({field_string})
                SELECT {field_string}
                FROM {schemaName}.data_objects";
        return _db.ExecuteCoreTransferSql(sql_string, " where ", "aggs_ob.data_objects");
    }

    public int LoadCoreObjectDatasets(string schemaName)
    {
        string field_string = _objectFields["object_datasets"];
        string sql_string = $@"INSERT INTO core.object_datasets({field_string})
                SELECT {field_string}
                FROM {schemaName}.object_datasets";
        return _db.ExecuteCoreTransferSql(sql_string, " where ", "aggs_ob.object_datasets");
    }

    public int LoadCoreObjectInstances(string schemaName)
    {
        string field_string = _objectFields["object_instances"];
        string sql_string = $@"INSERT INTO core.object_instances({field_string})
                SELECT {field_string}
                FROM {schemaName}.object_instances";
        return _db.ExecuteCoreTransferSql(sql_string, " where ", "aggs_ob.object_instances");
    }

    public int LoadCoreObjectTitles(string schemaName)
    {
        string field_string = _objectFields["object_titles"];
        string sql_string = $@"INSERT INTO core.object_titles({field_string})
                SELECT {field_string}
                FROM {schemaName}.object_titles";
        return _db.ExecuteCoreTransferSql(sql_string, " where ", "aggs_ob.object_titles");
    }

    public int LoadCoreObjectDates(string schemaName)
    {
        string field_string = _objectFields["object_dates"];
        string sql_string = $@"INSERT INTO core.object_dates({field_string})
                SELECT {field_string}
                FROM {schemaName}.object_dates";
        return _db.ExecuteCoreTransferSql(sql_string, " where ", "aggs_ob.object_dates");
    }

    public int LoadCoreObjectPeople(string schemaName)
    {
        string field_string = _objectFields["object_people"];
        string sql_string = $@"INSERT INTO core.object_people({field_string})
                SELECT {field_string}
                FROM {schemaName}.object_people";
        return _db.ExecuteCoreTransferSql(sql_string, " where ", "aggs_ob.object_people");
    }
    
    public int LoadCoreObjectOrganisations(string schemaName)
    {
        string field_string = _objectFields["object_organisations"];
        string sql_string = $@"INSERT INTO core.object_organisations({field_string})
                SELECT {field_string}
                FROM {schemaName}.object_organisations";
        return _db.ExecuteCoreTransferSql(sql_string, " where ", "aggs_ob.object_organisations");
    }

    public int LoadCoreObjectTopics(string schemaName)
    {string field_string = _objectFields["object_topics"];
        string sql_string = $@"INSERT INTO core.object_topics({field_string})
                SELECT {field_string}
                FROM {schemaName}.object_topics ";
        return _db.ExecuteCoreTransferSql(sql_string, " where ", "aggs_ob.object_topics");
    }

    public int LoadCoreObjectDescriptions(string schemaName)
    {string field_string = _objectFields["object_descriptions"];
        string sql_string = $@"INSERT INTO core.object_descriptions({field_string})
                SELECT {field_string}
                FROM {schemaName}.object_descriptions ";
        return _db.ExecuteCoreTransferSql(sql_string, " where ", "aggs_ob.object_descriptions");
    }

    public int LoadCoreObjectIdentifiers(string schemaName)
    {
        string field_string = _objectFields["object_identifiers"];
        string sql_string = $@"INSERT INTO core.object_identifiers({field_string})
                SELECT {field_string}
                FROM {schemaName}.object_identifiers ";
        return _db.ExecuteCoreTransferSql(sql_string, " where ", "aggs_ob.object_identifiers");
    }

    public int LoadCoreObjectRelationships(string schemaName)
    {
        string field_string = _objectFields["object_relationships"];
        string sql_string = $@"INSERT INTO core.object_relationships({field_string})
                SELECT {field_string}
                FROM {schemaName}.object_relationships ";
        return _db.ExecuteCoreTransferSql(sql_string, " where ", "aggs_ob.object_relationships");
    }

    public int LoadCoreObjectRights(string schemaName)
    {
        string field_string = _objectFields["object_rights"];
        string sql_string = $@"INSERT INTO core.object_rights({field_string})
                SELECT {field_string}
                FROM {schemaName}.object_rights ";
        return _db.ExecuteCoreTransferSql(sql_string, " where ", "aggs_ob.object_rights");
    }

    public int LoadStudyObjectLinks(string schemaName)
    {
        string sql_string = $@"INSERT INTO core.study_object_links(id, 
            study_id, object_id)
            SELECT  id, parent_study_id, object_id
            FROM {schemaName}.data_object_ids
            where is_valid_link = true ";

        return _db.ExecuteCoreTransferSql(sql_string, " and ", "aggs_nk.data_object_ids");
    }


    public void GenerateStudyProvenanceData()
    {
        string sql_string = "";
        sql_string = @"DROP TABLE IF EXISTS core.temp_study_provenance;
                CREATE table core.temp_study_provenance
                     as
                     select s.study_id, 
                     'Data retrieved from ' || string_agg(d.r_name || ' at ' || to_char(s.datetime_of_data_fetch, 'HH24:MI, dd Mon yyyy'), ', ' ORDER BY s.datetime_of_data_fetch) as provenance
                     from aggs_nk.study_ids s
                     inner join
                        (select id,
                          case 
                            when uses_who_harvest = true then repo_name || ' (via WHO ICTRP)'
                            else repo_name
                          end as r_name 
                         from mon_sf.source_parameters) d
                     on s.source_id = d.id
                     group by study_id ";
        _db.ExecuteSql(sql_string);

        sql_string = @"update core.studies s
                    set provenance_string = tt.provenance
                    from core.temp_study_provenance tt
                    where s.id = tt.study_id ";
        _db.ExecuteProvenanceSql(sql_string, "core.studies", "");

        sql_string = @"drop table core.temp_study_provenance;";
        _db.ExecuteSql(sql_string);
    }


    public void GenerateObjectProvenanceData()
    {
        string sql_string = "";
            sql_string = @"DROP TABLE IF EXISTS core.temp_object_provenance;
                create table core.temp_object_provenance
                     as
                     select s.object_id, 
                     'Data retrieved from ' || string_agg(d.r_name || ' at ' || to_char(s.datetime_of_data_fetch, 'HH24:MI, dd Mon yyyy'), ', ' ORDER BY s.datetime_of_data_fetch) as provenance
                     from aggs_nk.data_object_ids s
                     inner join
                        (select id,
                          case 
                            when uses_who_harvest = true then repo_name || ' (via WHO ICTRP)'
                            else repo_name
                          end as r_name 
                         from mon_sf.source_parameters) d
                    on s.source_id = d.id
                    where s.source_id <> 100135
                    group by object_id ";
            _db.ExecuteSql(sql_string);

            // PubMed objects need a different approach

            sql_string = @"DROP TABLE IF EXISTS core.temp_pubmed_object_provenance;
                 create table core.temp_pubmed_object_provenance
                     as 
                     select s.sd_oid,
                     'Data retrieved from Pubmed at ' || TO_CHAR(max(s.datetime_of_data_fetch), 'HH24:MI, dd Mon yyyy') as provenance
                     from aggs_nk.data_object_ids s
                     inner join
                         (select id, repo_name
                         from mon_sf.source_parameters) d
                     on s.source_id = d.id
                     where s.source_id = 100135
                     group by s.sd_oid ";
            _db.ExecuteSql(sql_string);

            // update non pubmed objects
            sql_string = @"update core.data_objects s
                    set provenance_string = tt.provenance
                    from core.temp_object_provenance tt
                    where s.id = tt.object_id ";
            _db.ExecuteProvenanceSql(sql_string, "core.data_objects", " (non pubmed records) ");

            // update pubmed objects
            sql_string = @"update core.data_objects s
                    set provenance_string = tt.provenance
                    from core.temp_pubmed_object_provenance tt
                    inner join aggs_nk.data_object_ids k
                    on tt.sd_oid = k.sd_oid
                    where s.id = k.object_id ";
            _db.ExecuteProvenanceSql(sql_string, "core.data_objects", " (pubmed records) ");

            sql_string = @"drop table core.temp_object_provenance;
            drop table core.temp_pubmed_object_provenance;";
            _db.ExecuteSql(sql_string);
    }
}