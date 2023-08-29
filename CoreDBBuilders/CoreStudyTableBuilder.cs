using Dapper;
using Npgsql;
namespace MDR_Aggregator;

public class CoreStudyTableBuilder
{
    private readonly string db_conn;
    private readonly ILoggingHelper _loggingHelper;

    public CoreStudyTableBuilder(string _db_conn, ILoggingHelper loggingHelper)
    {
        db_conn = _db_conn;
        _loggingHelper = loggingHelper;
    }
  
    private void ExecuteSQL(string sql_string)
    {
        using var conn = new NpgsqlConnection(db_conn);
        try
        {
            conn.Execute(sql_string);
        }
        catch (Exception e)
        {
            _loggingHelper.LogError("In ExecuteSQL; " + e.Message + ", \nSQL was: " + sql_string);
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
    
    
    public void create_table_study_icd()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.study_icd;
        CREATE TABLE core.study_icd(
            id                     INT             NOT NULL PRIMARY KEY
          , study_id               INT             NOT NULL
          , icd_code               VARCHAR         NULL
          , icd_name               VARCHAR         NULL
        );
        CREATE INDEX study_study_icd_study_id ON core.study_icd(study_id);";

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