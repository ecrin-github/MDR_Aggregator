using Dapper;
using Npgsql;
namespace MDR_Aggregator;

public class LinkTableBuilder
{
    readonly string db_conn;

    public LinkTableBuilder(string _db_conn)
    {
        db_conn = _db_conn;
    }
    
    private void ExecuteSQL(string sql_string)
    {
        using var conn = new NpgsqlConnection(db_conn);
        conn.Execute(sql_string);
    }

    public void create_table_all_ids_studies()
    {
        string sql_string = @"DROP TABLE IF EXISTS nk.all_ids_studies;
        CREATE TABLE nk.all_ids_studies(
            id                       INT             NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 3000001 INCREMENT BY 1) PRIMARY KEY
          , study_id                 INT             NULL
          , source_id                INT             NULL
          , sd_sid                   VARCHAR         NULL
          , is_preferred             BOOLEAN         NULL
          , datetime_of_data_fetch   TIMESTAMPTZ     NULL
         );
        CREATE INDEX study_all_ids_studyid ON nk.all_ids_studies(study_id);
        CREATE INDEX study_all_ids_sdsidsource ON nk.all_ids_studies(source_id, sd_sid);";

        ExecuteSQL(sql_string);
    }


    public void create_table_all_ids_data_objects()
    {
        string sql_string = @"DROP TABLE IF EXISTS nk.all_ids_data_objects;
        CREATE TABLE nk.all_ids_data_objects(
            id                       INT             NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 10000001 INCREMENT BY 1) PRIMARY KEY
          , object_id                INT             NULL
          , source_id                INT             NOT NULL
          , sd_oid                   VARCHAR         NULL
          , parent_study_source_id   INT             NULL
          , parent_study_sd_sid      VARCHAR         NULL
          , parent_study_id          INT             NULL
          , is_preferred_study       BOOLEAN         NULL
          , datetime_of_data_fetch   TIMESTAMPTZ     NULL
        );
        CREATE INDEX object_all_ids_objectid ON nk.all_ids_data_objects(object_id);
        CREATE INDEX object_all_ids_sdidsource ON nk.all_ids_data_objects(source_id, sd_oid);";

        ExecuteSQL(sql_string);
    }


    public void create_table_all_links()
    {
        string sql_string = @"DROP TABLE IF EXISTS nk.all_links;
        CREATE TABLE nk.all_links(
            id                       INT             NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 10000001 INCREMENT BY 1) PRIMARY KEY
          , study_id                 INT             NULL
          , study_source_id          INT             NOT NULL
          , study_sd_sid             VARCHAR         NOT NULL
          , use_link                 INT             NOT NULL DEFAULT 1
          , object_sd_oid            VARCHAR         NOT NULL
          , object_source_id         INT             NOT NULL
          , object_id                INT             NULL
          );
       CREATE INDEX all_links_objectid ON nk.all_links(object_id);
       CREATE INDEX all_links_studyid ON nk.all_links(study_id);";

       ExecuteSQL(sql_string);
    }


    public void create_table_linked_study_groups()
    {
        string sql_string = @"DROP TABLE IF EXISTS nk.linked_study_groups;
        CREATE TABLE nk.linked_study_groups(
            source_id                INT             NULL
          , sd_sid                   VARCHAR         NULL
          , relationship_id          INT             NULL
          , target_sd_sid            VARCHAR         NULL
          , target_source_id         INT             NULL
        );";

        ExecuteSQL(sql_string);
    }


    public void create_table_study_object_links()
    {
        string sql_string = @"DROP TABLE IF EXISTS nk.study_object_links;
        CREATE TABLE nk.study_object_links(
            id                       INT             NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 10000001 INCREMENT BY 1) PRIMARY KEY
          , study_id                 INT             NOT NULL
          , object_id                INT             NOT NULL
    );
    CREATE INDEX study_object_links_objectid ON nk.study_object_links(object_id);
    CREATE INDEX study_object_links_studyid ON nk.study_object_links(study_id);";

        ExecuteSQL(sql_string);
    }


    public void create_table_study_study_links()
    {
        string sql_string = @"DROP TABLE IF EXISTS nk.study_study_links;
        CREATE TABLE nk.study_study_links(
            source_id                INT             NULL
          , sd_sid                   VARCHAR         NULL
          , preferred_sd_sid         VARCHAR         NULL
          , preferred_source_id      INT             NULL
          );";

        ExecuteSQL(sql_string);
    }


    public void create_table_temp_study_ids()
    {
        string sql_string = @"DROP TABLE IF EXISTS nk.temp_study_ids;
        CREATE TABLE nk.temp_study_ids(
            study_id                 INT             NULL
          , source_id                INT             NULL
          , sd_sid                   VARCHAR         NULL
          , datetime_of_data_fetch   TIMESTAMPTZ     NULL
          , is_preferred             BOOLEAN         NULL
          , status                   INT             NULL
          , date_of_study_data       TIMESTAMPTZ     NULL
         );";

        ExecuteSQL(sql_string);
    }
}