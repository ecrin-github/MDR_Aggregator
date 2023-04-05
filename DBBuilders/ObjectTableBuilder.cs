using Dapper;
using Npgsql;
namespace MDR_Aggregator;

public class ObjectTableBuilder
{
    string db_conn;

    public ObjectTableBuilder(string _db_conn)
    {
        db_conn = _db_conn;
    }

    public void drop_table(string table_name)
    {
        string sql_string = @"DROP TABLE IF EXISTS ob." + table_name;
        using (var conn = new NpgsqlConnection(db_conn))
        {
            conn.Execute(sql_string);
        }
    }


    public void create_table_data_objects()
    {
        string sql_string = @"CREATE TABLE ob.data_objects(
            id                     INT             NOT NULL PRIMARY KEY
          , display_title          VARCHAR         NULL
          , version                VARCHAR         NULL
          , doi                    VARCHAR         NULL 
          , doi_status_id          INT             NULL
          , publication_year       INT             NULL
          , object_class_id        INT             NULL
          , object_type_id         INT             NULL
          , managing_org_id        INT             NULL
          , managing_org           VARCHAR         NULL
          , managing_org_ror_id    VARCHAR         NULL
          , lang_code              VARCHAR         NULL
          , access_type_id         INT             NULL
          , access_details         VARCHAR         NULL
          , access_details_url     VARCHAR         NULL
          , url_last_checked       DATE            NULL
          , eosc_category          INT             NULL
          , add_study_contribs     BOOLEAN         NULL
          , add_study_topics       BOOLEAN         NULL
          , aggregated_on          TIMESTAMPTZ     NOT NULL DEFAULT Now()
        );";

        using (var conn = new NpgsqlConnection(db_conn))
        {
            conn.Execute(sql_string);
        }
    }


    public void create_table_object_datasets()
    {
        string sql_string = @"CREATE TABLE ob.object_datasets(
            id                     INT             GENERATED ALWAYS AS IDENTITY (START WITH 40000001 INCREMENT BY 1) PRIMARY KEY
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
          , aggregated_on          TIMESTAMPTZ     NOT NULL DEFAULT Now()
        );
        CREATE INDEX object_datasets_object_id ON ob.object_datasets(object_id);";

        using (var conn = new NpgsqlConnection(db_conn))
        {
            conn.Execute(sql_string);
        }
    }


    public void create_table_object_dates()
    {
        string sql_string = @"CREATE TABLE ob.object_dates(
            id                     INT             GENERATED ALWAYS AS IDENTITY (START WITH 40000001 INCREMENT BY 1) PRIMARY KEY
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
          , aggregated_on          TIMESTAMPTZ     NOT NULL DEFAULT Now()
        );
        CREATE INDEX object_dates_object_id ON ob.object_dates(object_id);";

        using (var conn = new NpgsqlConnection(db_conn))
        {
            conn.Execute(sql_string);
        }
    }


    public void create_table_object_instances()
    {
        string sql_string = @"CREATE TABLE ob.object_instances(
            id                     INT             GENERATED ALWAYS AS IDENTITY (START WITH 40000001 INCREMENT BY 1) PRIMARY KEY
          , object_id              INT             NOT NULL
          , instance_type_id       INT             NULL 
          , repository_org_id      INT             NULL
          , repository_org         VARCHAR         NULL
          , url                    VARCHAR         NULL
          , url_accessible         BOOLEAN         NULL
          , url_last_checked       DATE            NULL
          , resource_type_id       INT             NULL
          , resource_size          VARCHAR         NULL
          , resource_size_units    VARCHAR         NULL
          , resource_comments      VARCHAR         NULL
          , aggregated_on          TIMESTAMPTZ     NOT NULL DEFAULT Now()
        );
        CREATE INDEX object_instances_object_id ON ob.object_instances(object_id);";

        using (var conn = new NpgsqlConnection(db_conn))
        {
            conn.Execute(sql_string);
        }
    }


    public void create_table_object_contributors()
    {
        string sql_string = @"CREATE TABLE ob.object_contributors(
            id                     INT             GENERATED ALWAYS AS IDENTITY (START WITH 40000001 INCREMENT BY 1) PRIMARY KEY
          , object_id              INT             NOT NULL
          , contrib_type_id        INT             NULL
          , is_individual          BOOLEAN         NULL
          , person_id              INT             NULL
          , person_given_name      VARCHAR         NULL
          , person_family_name     VARCHAR         NULL
          , person_full_name       VARCHAR         NULL
          , orcid_id               VARCHAR         NULL
          , person_affiliation     VARCHAR         NULL
          , organisation_id        INT             NULL
          , organisation_name      VARCHAR         NULL
          , organisation_ror_id    VARCHAR         NULL
          , aggregated_on          TIMESTAMPTZ     NOT NULL DEFAULT Now()
        );
        CREATE INDEX object_contributors_object_id ON ob.object_contributors(object_id);";

        using (var conn = new NpgsqlConnection(db_conn))
        {
            conn.Execute(sql_string);
        }
    }


    public void create_table_object_titles()
    {
        string sql_string = @"CREATE TABLE ob.object_titles(
            id                     INT             GENERATED ALWAYS AS IDENTITY (START WITH 40000001 INCREMENT BY 1) PRIMARY KEY
          , object_id              INT             NOT NULL
          , title_type_id          INT             NULL
          , title_text             VARCHAR         NULL
          , lang_code              VARCHAR         NULL
          , lang_usage_id          INT             NOT NULL default 11
          , is_default             BOOLEAN         NULL
          , comments               VARCHAR         NULL
          , aggregated_on          TIMESTAMPTZ     NOT NULL DEFAULT Now()
        );
        CREATE INDEX object_titles_object_id ON ob.object_titles(object_id);";

        using (var conn = new NpgsqlConnection(db_conn))
        {
            conn.Execute(sql_string);
        }
    }


    public void create_table_object_topics()
    {
        string sql_string = @"CREATE TABLE ob.object_topics(
            id                     INT             GENERATED ALWAYS AS IDENTITY (START WITH 40000001 INCREMENT BY 1) PRIMARY KEY
          , object_id              INT             NOT NULL
          , topic_type_id          INT             NULL
          , mesh_coded             BOOLEAN         NULL
          , mesh_code              VARCHAR         NULL
          , mesh_value             VARCHAR         NULL
          , original_ct_id         INT             NULL
          , original_ct_code       VARCHAR         NULL
          , original_value         VARCHAR         NULL
          , aggregated_on          TIMESTAMPTZ     NOT NULL DEFAULT Now()
        );
        CREATE INDEX object_topics_object_id ON ob.object_topics(object_id);";

        using (var conn = new NpgsqlConnection(db_conn))
        {
            conn.Execute(sql_string);
        }
    }


    public void create_table_object_descriptions()
    {
        string sql_string = @"CREATE TABLE ob.object_descriptions(
            id                     INT             GENERATED ALWAYS AS IDENTITY (START WITH 40000001 INCREMENT BY 1) PRIMARY KEY
          , object_id              INT             NOT NULL
          , description_type_id    INT             NULL
          , label                  VARCHAR         NULL
          , description_text       VARCHAR         NULL
          , lang_code              VARCHAR         NULL
          , aggregated_on          TIMESTAMPTZ     NOT NULL DEFAULT Now()
        );
        CREATE INDEX object_descriptions_object_id ON ob.object_descriptions(object_id);";

        using (var conn = new NpgsqlConnection(db_conn))
        {
            conn.Execute(sql_string);
        }
    }


    public void create_table_object_identifiers()
    {
        string sql_string = @"CREATE TABLE ob.object_identifiers(
            id                     INT             GENERATED ALWAYS AS IDENTITY (START WITH 40000001 INCREMENT BY 1) PRIMARY KEY
          , object_id              INT             NOT NULL
          , identifier_value       VARCHAR         NULL
          , identifier_type_id     INT             NULL
          , identifier_org_id      INT             NULL
          , identifier_org         VARCHAR         NULL
          , identifier_org_ror_id  VARCHAR         NULL
          , identifier_date        VARCHAR         NULL
          , aggregated_on          TIMESTAMPTZ     NOT NULL DEFAULT Now()
        );
        CREATE INDEX object_identifiers_object_id ON ob.object_identifiers(object_id);";

        using (var conn = new NpgsqlConnection(db_conn))
        {
            conn.Execute(sql_string);
        }
    }


    public void create_table_object_relationships()
    {
        string sql_string = @"CREATE TABLE ob.object_relationships(
            id                     INT             GENERATED ALWAYS AS IDENTITY (START WITH 40000001 INCREMENT BY 1) PRIMARY KEY
          , object_id              INT             NOT NULL
          , relationship_type_id   INT             NULL
          , target_object_id       INT             NULL
          , aggregated_on          TIMESTAMPTZ     NOT NULL DEFAULT Now()
        );
        CREATE INDEX object_relationships_object_id ON ob.object_relationships(object_id);";

        using (var conn = new NpgsqlConnection(db_conn))
        {
            conn.Execute(sql_string);
        }
    }


    public void create_table_object_rights()
    {
        string sql_string = @"CREATE TABLE ob.object_rights(
            id                     INT             GENERATED ALWAYS AS IDENTITY (START WITH 40000001 INCREMENT BY 1) PRIMARY KEY
          , object_id              INT             NOT NULL
          , rights_name            VARCHAR         NULL
          , rights_uri             VARCHAR         NULL
          , comments               VARCHAR         NULL
          , aggregated_on          TIMESTAMPTZ     NOT NULL DEFAULT Now()
        );
        CREATE INDEX object_rights_object_id ON ob.object_rights(object_id);";

        using (var conn = new NpgsqlConnection(db_conn))
        {
            conn.Execute(sql_string);
        }
    }

}