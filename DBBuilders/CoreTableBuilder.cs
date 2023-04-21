using Dapper;
using Npgsql;
namespace MDR_Aggregator;

public class CoreTableBuilder
{
    readonly string db_conn;
    ILoggingHelper _loggingHelper;

    public CoreTableBuilder(string _db_conn, ILoggingHelper loggingHelper)
    {
        db_conn = _db_conn;
        _loggingHelper = loggingHelper;
    }
  
    private int ExecuteSQL(string sql_string)
    {
        using var conn = new NpgsqlConnection(db_conn);
        try
        {
            return conn.Execute(sql_string);
        }
        catch (Exception e)
        {
            _loggingHelper.LogError("In ExecuteSQL; " + e.Message + ", \nSQL was: " + sql_string);
            return 0;
        }
    }

    public void create_table_studies()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.studies;
        CREATE TABLE core.studies(
            id                     INT             NOT NULL PRIMARY KEY
          , display_title          VARCHAR         NULL
          , title_lang_code        VARCHAR         NULL
          , brief_description      VARCHAR         NULL
          , data_sharing_statement VARCHAR         NULL
          , study_start_year       INT             NULL
          , study_start_month      INT             NULL
          , study_type_id          INT             NULL
          , study_status_id        INT             NULL
          , study_enrolment        VARCHAR         NULL
          , study_gender_elig_id   INT             NULL
          , min_age                INT             NULL
          , min_age_units_id       INT             NULL
          , max_age                INT             NULL
          , max_age_units_id       INT             NULL
          , iec_level              INT             NULL
          , provenance_string      VARCHAR         NULL
        );";

        ExecuteSQL(sql_string);
    }


    public void create_table_study_identifiers()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.study_identifiers;
        CREATE TABLE core.study_identifiers(
            id                     INT             NOT NULL PRIMARY KEY
          , study_id               INT             NOT NULL
          , identifier_value       VARCHAR         NULL
          , identifier_type_id     INT             NULL
          , source_id              INT             NULL
          , source                 VARCHAR         NULL
          , source_ror_id          VARCHAR         NULL
          , identifier_date        VARCHAR         NULL
          , identifier_link        VARCHAR         NULL
        );
        CREATE INDEX study_identifiers_study_id ON core.study_identifiers(study_id);";

        ExecuteSQL(sql_string);
    }


    public void create_table_study_titles()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.study_titles;
        CREATE TABLE core.study_titles(
            id                     INT             NOT NULL PRIMARY KEY
          , study_id               INT             NOT NULL
          , title_type_id          INT             NULL
          , title_text             VARCHAR         NULL
          , lang_code              VARCHAR         NULL
          , lang_usage_id          INT             NULL
          , is_default             BOOLEAN         NULL
          , comments               VARCHAR         NULL
        );
        CREATE INDEX study_titles_study_id ON core.study_titles(study_id);";

        ExecuteSQL(sql_string);
    }


    public void create_table_study_people()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.study_people;
        CREATE TABLE core.study_people(
            id                     INT             NOT NULL PRIMARY KEY
          , study_id               INT             NOT NULL
          , contrib_type_id        INT             NULL
          , person_given_name      VARCHAR         NULL
          , person_family_name     VARCHAR         NULL
          , person_full_name       VARCHAR         NULL
          , orcid_id               VARCHAR         NULL
          , person_affiliation     VARCHAR         NULL
          , organisation_id        INT             NULL
          , organisation_name      VARCHAR         NULL
          , organisation_ror_id    VARCHAR         NULL
        );
        CREATE INDEX study_people_study_id ON core.study_people(study_id);";

        ExecuteSQL(sql_string);
    }

    public void create_table_study_organisations()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.study_organisations;
        CREATE TABLE core.study_organisations(
            id                     INT             NOT NULL PRIMARY KEY
          , study_id               INT             NOT NULL
          , contrib_type_id        INT             NULL
          , organisation_id        INT             NULL
          , organisation_name      VARCHAR         NULL
          , organisation_ror_id    VARCHAR         NULL
        );
        CREATE INDEX study_organisations_study_id ON core.study_organisations(study_id);";

        ExecuteSQL(sql_string);
    }
    
    
    public void create_table_study_topics()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.study_topics;
        CREATE TABLE core.study_topics(
            id                     INT             NOT NULL PRIMARY KEY
          , study_id               INT             NOT NULL
          , topic_type_id          INT             NULL
          , original_value         VARCHAR         NULL
          , original_ct_type_id    INT             NULL
          , original_ct_code       VARCHAR         NULL                                    
          , mesh_code              VARCHAR         NULL
          , mesh_value             VARCHAR         NULL
        );
        CREATE INDEX study_topics_study_id ON core.study_topics(study_id);";

        ExecuteSQL(sql_string);
    }
    
    
    public void create_table_study_conditions()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.study_conditions;
        CREATE TABLE core.study_conditions(
            id                     INT             NOT NULL PRIMARY KEY
          , study_id               INT             NOT NULL
          , original_value         VARCHAR         NULL
          , original_ct_type_id    INT             NULL
          , original_ct_code       VARCHAR         NULL                 
          , icd_code               VARCHAR         NULL
          , icd_name               VARCHAR         NULL
        );
        CREATE INDEX study_conditions_study_id ON core.study_conditions(study_id);";

        ExecuteSQL(sql_string);
    }

    
    public void create_table_study_features()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.study_features;
        CREATE TABLE core.study_features(
            id                     INT             NOT NULL PRIMARY KEY
          , study_id               INT             NOT NULL
          , feature_type_id        INT             NULL
          , feature_value_id       INT             NULL
        );
        CREATE INDEX study_features_study_id ON core.study_features(study_id);";

        ExecuteSQL(sql_string);
    }
  

    public void create_table_study_relationships()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.study_relationships;
        CREATE TABLE core.study_relationships(
            id                     INT             NOT NULL PRIMARY KEY
          , study_id               INT             NOT NULL
          , relationship_type_id   INT             NULL
          , target_study_id        VARCHAR         NULL
        );
        CREATE INDEX study_relationships_study_id ON core.study_relationships(study_id);
        CREATE INDEX study_relationships_target_study_id ON core.study_relationships(target_study_id);";

        ExecuteSQL(sql_string);
    }

    
    public void create_table_study_countries()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.study_countries;
        CREATE TABLE core.study_countries(
            id                     INT             NOT NULL PRIMARY KEY
          , study_id               INT             NOT NULL
          , country_id             INT             NULL
          , country_name           VARCHAR         NULL
          , status_id              INT             NULL
        );
        CREATE INDEX study_countries_study_id ON core.study_countries(study_id);";

        ExecuteSQL(sql_string);
    }
    
    
    public void create_table_study_locations()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.study_locations;
        CREATE TABLE core.study_locations(
            id                     INT             NOT NULL PRIMARY KEY
          , study_id               INT             NOT NULL
          , facility_org_id        INT             NULL
          , facility               VARCHAR         NULL
          , facility_ror_id        VARCHAR         NULL
          , city_id                INT             NULL
          , city_name              VARCHAR         NULL
          , country_id             INT             NULL
          , country_name           VARCHAR         NULL
          , status_id              INT             NULL
        );
        CREATE INDEX study_locations_study_id ON core.study_locations(study_id);";

        ExecuteSQL(sql_string);
    }
    
    
    public void create_table_data_objects()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.data_objects;
        CREATE TABLE core.data_objects(
            id                     INT             NOT NULL PRIMARY KEY
          , title                  VARCHAR         NULL
          , version                VARCHAR         NULL
          , display_title          VARCHAR         NULL
          , doi                    VARCHAR         NULL 
          , doi_status_id          INT             NULL
          , publication_year       INT             NULL
          , object_class_id        INT             NULL
          , object_type_id         INT             NULL
          , managing_org_id        INT             NULL
          , managing_org_ror_id    VARCHAR         NULL
          , managing_org           VARCHAR         NULL
          , lang_code              VARCHAR         NULL
          , access_type_id         INT             NULL
          , access_details         VARCHAR         NULL
          , access_details_url     VARCHAR         NULL
          , url_last_checked       DATE            NULL
          , eosc_category          INT             NULL
          , add_study_contribs     BOOLEAN         NULL
          , add_study_topics       BOOLEAN         NULL
          , provenance_string      VARCHAR         NULL
        );";

        ExecuteSQL(sql_string);
    }


    public void create_table_object_datasets()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.object_datasets;
        CREATE TABLE core.object_datasets(
            id                     INT             NOT NULL PRIMARY KEY
          , object_id              INT             NOT NULL
          , record_keys_type_id    INT             NULL 
          , record_keys_details    VARCHAR         NULL    
          , deident_type_id        INT             NULL  
          , deident_direct 	       BOOLEAN         NULL   
          , deident_hipaa 	       BOOLEAN         NULL   
          , deident_dates 	       BOOLEAN         NULL   
          , deident_nonarr 	       BOOLEAN         NULL   
          , deident_kanon	       BOOLEAN         NULL   
          , deident_details        VARCHAR         NULL    
          , consent_type_id        INT             NULL  
          , consent_noncommercial  BOOLEAN         NULL
          , consent_geog_restrict  BOOLEAN         NULL
          , consent_research_type  BOOLEAN         NULL
          , consent_genetic_only   BOOLEAN         NULL
          , consent_no_methods     BOOLEAN         NULL
          , consent_details        VARCHAR         NULL 
        );
        CREATE INDEX object_datasets_object_id ON core.object_datasets(object_id);";

        ExecuteSQL(sql_string);
    }


    public void create_table_object_dates()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.object_dates;
        CREATE TABLE core.object_dates(
            id                     INT             NOT NULL PRIMARY KEY
          , object_id              INT             NOT NULL
          , date_type_id           INT             NULL
          , date_is_range          BOOLEAN         NULL
          , date_as_string         VARCHAR         NULL
          , start_year             INT             NULL
          , start_month            INT             NULL
          , start_day              INT             NULL
          , end_year               INT             NULL
          , end_month              INT             NULL
          , end_day                INT             NULL
          , details                VARCHAR         NULL
        );
        CREATE INDEX object_dates_object_id ON core.object_dates(object_id);";

        ExecuteSQL(sql_string);
    }


    public void create_table_object_instances()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.object_instances;
        CREATE TABLE core.object_instances(
            id                     INT             NOT NULL PRIMARY KEY
          , object_id              INT             NOT NULL
          , system_id              INT             NULL
          , system                 VARCHAR         NULL
          , url                    VARCHAR         NULL
          , url_accessible         BOOLEAN         NULL
          , url_last_checked       DATE            NULL
          , resource_type_id       INT             NULL
          , resource_size          VARCHAR         NULL
          , resource_size_units    VARCHAR         NULL
          , resource_comments      VARCHAR         NULL
        );
        CREATE INDEX object_instances_object_id ON core.object_instances(object_id);";

        ExecuteSQL(sql_string);
    }


    public void create_table_object_people()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.object_people;
        CREATE TABLE core.object_people(
            id                     INT             NOT NULL PRIMARY KEY
          , object_id              INT             NOT NULL
          , contrib_type_id        INT             NULL
          , person_given_name      VARCHAR         NULL
          , person_family_name     VARCHAR         NULL
          , person_full_name       VARCHAR         NULL
          , orcid_id               VARCHAR         NULL
          , person_affiliation     VARCHAR         NULL
          , organisation_id        INT             NULL
          , organisation_name      VARCHAR         NULL
          , organisation_ror_id    VARCHAR         NULL

        );
        CREATE INDEX object_people_object_id ON core.object_people(object_id);";

        ExecuteSQL(sql_string);
    }

    public void create_table_object_organisations()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.object_organisations;
        CREATE TABLE core.object_organisations(
            id                     INT             NOT NULL PRIMARY KEY
          , object_id              INT             NOT NULL
          , contrib_type_id        INT             NULL
          , organisation_id        INT             NULL
          , organisation_name      VARCHAR         NULL
          , organisation_ror_id    VARCHAR         NULL

        );
        CREATE INDEX object_organisations_object_id ON core.object_organisations(object_id);";

        ExecuteSQL(sql_string);
    }
    
    
    public void create_table_object_titles()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.object_titles;
        CREATE TABLE core.object_titles(
            id                     INT             NOT NULL PRIMARY KEY
          , object_id              INT             NOT NULL
          , title_type_id          INT             NULL
          , title_text             VARCHAR         NULL
          , lang_code              VARCHAR         NULL
          , lang_usage_id          INT             NOT NULL default 11
          , is_default             BOOLEAN         NULL
          , comments               VARCHAR         NULL
        );
        CREATE INDEX object_titles_object_id ON core.object_titles(object_id);";

        ExecuteSQL(sql_string);
    }


    public void create_table_object_topics()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.object_topics;
        CREATE TABLE core.object_topics(
            id                     INT             NOT NULL PRIMARY KEY
          , object_id              INT             NOT NULL
          , topic_type_id          INT             NULL
          , original_value         VARCHAR         NULL
          , original_ct_type_id    INT             NULL
          , original_ct_code       VARCHAR         NULL                                    
          , mesh_code              VARCHAR         NULL
          , mesh_value             VARCHAR         NULL
        );
        CREATE INDEX object_topics_object_id ON core.object_topics(object_id);";

        ExecuteSQL(sql_string);
    }


    public void create_table_object_descriptions()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.object_descriptions;
        CREATE TABLE core.object_descriptions(
            id                     INT             NOT NULL PRIMARY KEY
          , object_id              INT             NOT NULL
          , description_type_id    INT             NULL
          , label                  VARCHAR         NULL
          , description_text       VARCHAR         NULL
          , lang_code              VARCHAR         NULL
        );
        CREATE INDEX object_descriptions_object_id ON core.object_descriptions(object_id);";

        ExecuteSQL(sql_string);
    }


    public void create_table_object_identifiers()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.object_identifiers;
        CREATE TABLE core.object_identifiers(
            id                     INT             NOT NULL PRIMARY KEY
          , object_id              INT             NOT NULL
          , identifier_value       VARCHAR         NULL
          , identifier_type_id     INT             NULL
          , source_id              INT             NULL
          , source                 VARCHAR         NULL
          , source_ror_id          VARCHAR         NULL
          , identifier_date        VARCHAR         NULL
        );
        CREATE INDEX object_identifiers_object_id ON core.object_identifiers(object_id);";

        ExecuteSQL(sql_string);
    }


    public void create_table_object_relationships()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.object_relationships;
        CREATE TABLE core.object_relationships(
            id                     INT             NOT NULL PRIMARY KEY
          , object_id              INT             NOT NULL
          , relationship_type_id   INT             NULL
          , target_object_id       INT             NULL
        );
        CREATE INDEX object_relationships_object_id ON core.object_relationships(object_id);";

        ExecuteSQL(sql_string);
    }


    public void create_table_object_rights()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.object_rights;
        CREATE TABLE core.object_rights(
            id                     INT             NOT NULL PRIMARY KEY
          , object_id              INT             NOT NULL
          , rights_name            VARCHAR         NULL
          , rights_uri             VARCHAR         NULL
          , comments               VARCHAR         NULL
        );
        CREATE INDEX object_rights_object_id ON core.object_rights(object_id);";

        ExecuteSQL(sql_string);
    }


    public void create_table_study_object_links()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.study_object_links;
        CREATE TABLE core.study_object_links(
            id                     INT             NOT NULL PRIMARY KEY
          , study_id               INT             NOT NULL
          , object_id              INT             NOT NULL
        );
        CREATE INDEX study_object_links_studyid ON core.study_object_links(study_id);
        CREATE INDEX study_object_links_objectid ON core.study_object_links(object_id);";

        ExecuteSQL(sql_string);
    }
    
    public void create_table_study_search()
        {
            string sql_string = @"DROP TABLE IF EXISTS core.study_search;
            CREATE TABLE core.study_search(
                id                     INT             NOT NULL PRIMARY KEY
              , display_title          VARCHAR         NULL
              , title_lexemes          VARCHAR         NULL
              , topic_lexemes          VARCHAR         NULL
              , study_start_year       INT             NULL
              , study_start_month      INT             NULL
              , study_type_id          INT             NULL DEFAULT 0
              , study_status_id        INT             NULL DEFAULT 0
              , study_gender_elig_id   INT             NULL DEFAULT 915
              , min_age                INT             NULL
              , min_age_units_id       INT             NULL DEFAULT 0
              , max_age                INT             NULL
              , max_age_units_id       INT             NULL DEFAULT 0
              , phase_value            INT             NULL DEFAULT 140
              , purpose_value          INT             NULL DEFAULT 215
              , allocation_value       INT             NULL DEFAULT 325
              , intervention_value     INT             NULL DEFAULT 445
              , masking_value          INT             NULL DEFAULT 525
              , obsmodel_value         INT             NULL DEFAULT 635
              , timepersp_value        INT             NULL DEFAULT 720
              , biospec_value          INT             NULL DEFAULT 815
              , has_reg_entry          BOOL            NULL
              , has_reg_results        BOOL            NULL
              , has_article            BOOL            NULL
              , has_protocol           BOOL            NULL
              , has_overview           BOOL            NULL
              , has_pif                BOOL            NULL
              , has_ecrfs              BOOL            NULL
              , has_manual             BOOL            NULL
              , has_sap                BOOL            NULL
              , has_csr                BOOL            NULL
              , has_data_desc          BOOL            NULL
              , has_ipd                BOOL            NULL
              , has_agg_data           BOOL            NULL
              , has_other_studyres     BOOL            NULL
              , has_conf_material      BOOL            NULL
              , has_other_article      BOOL            NULL
              , has_chapter            BOOL            NULL
              , has_other_info         BOOL            NULL
              , has_website            BOOL            NULL
              , has_software           BOOL            NULL
              , has_other              BOOL            NULL
 
           );
           CREATE INDEX study_search_id ON core.study_search(id);";

           ExecuteSQL(sql_string);

        }
}