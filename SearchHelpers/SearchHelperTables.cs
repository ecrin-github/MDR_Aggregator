using System.Data.SqlTypes;

namespace MDR_Aggregator;

public class SearchHelperTables
{
    private readonly DBUtilities db;

    private readonly ILoggingHelper _loggingHelper;

    public SearchHelperTables(string connString, ILoggingHelper loggingHelper)
    {
        db = new DBUtilities(connString, loggingHelper);
        _loggingHelper = loggingHelper;
    }

    public void CreateObjectDataSearchTables()
    {
        string sql_string = @"DROP TABLE IF EXISTS search.new_objects_json;
        CREATE TABLE search.new_objects_json(
          id                       INT             NOT NULL PRIMARY KEY
        , full_object              JSON            NULL
        );
        CREATE INDEX search_objects_json_id_new ON search.new_objects_json(id);";
        db.ExecuteSQL(sql_string);

        sql_string = @"drop table if exists search.new_objects;
        create table search.new_objects
        (
            id                    int                 PRIMARY KEY GENERATED ALWAYS AS IDENTITY
          , oid                   int                 not null
          , ob_name               varchar             null
          , typeid                int                 null
          , typename              varchar             null
          , url                   varchar             null
          , res_type_id           int                 null
          , res_icon              varchar             null
          , year_pub              varchar             null
          , acc_icon              varchar             null
          , prov                  varchar             null
        );
        create index os_object_id_new on search.new_objects(oid);";
        db.ExecuteSQL(sql_string);
    }

    public void CreateStudyDataSearchTables()
    {
        string sql_string = @"DROP TABLE IF EXISTS search.new_studies_json;
        CREATE TABLE search.new_studies_json(
          id                       INT             NOT NULL PRIMARY KEY
        , search_res               JSON            NULL
        , full_study               JSON            NULL
        , open_aire                JSON            NULL
        , c19p                     JSON            NULL 
        );
        CREATE INDEX search_studies_json_id_new ON search.new_studies_json(id);";
        db.ExecuteSQL(sql_string);

        sql_string = @"drop table if exists search.new_studies;
        create table search.new_studies
        (
            study_id                int primary key   not null
            , study_name            varchar           null
            , start_year            int               null
            , start_month           int               null
            , type_id               int               null
            , status_id             int               null
            , phase_id              int               null
            , alloc_id              int               null
            , has_objects           varchar           default '000000000000000000'
            , study_sres_json       json              null
        );
        create index ss_start_year_new on search.new_studies(start_year);
        create index ss_type_new on search.new_studies(type_id);
        create index ss_status_new on search.new_studies(status_id);
        create index ss_phase_id_new on search.new_studies(phase_id);
        create index ss_alloc_id_new on search.new_studies(alloc_id);";
        db.ExecuteSQL(sql_string);
    }


    public void CreatePMIDSearchData()
    {
        string sql_string = @"drop table if exists search.new_pmids;
        create table search.new_pmids
        (
            pmid                 int                not null
          , study_id             int                not null
          , study_json           json               null
        );
        create index sp_pmid_new on search.new_pmids(pmid);
        create index sp_study_id_new on search.new_pmids(study_id);";

        db.ExecuteSQL(sql_string);
    }

    // idents table.

    public void CreateIdentifierSearchData()
    {
        string sql_string = @"drop table if exists search.new_idents;
         create table search.new_idents
         (
            ident_type  		int                not null
          , ident_value			varchar            not null		
          , study_id 			int                not null
          , study_json          json               null
         );
        create index si_type_value_new on search.new_idents(ident_type, ident_value);
        create index si_study_id_new on search.new_idents(study_id);";

        db.ExecuteSQL(sql_string);
    }

    public void CreateCountrySearchData()
    {
        string sql_string = @"drop table if exists search.new_countries;
         create table search.new_countries
         (
            country_id   		int                not null		
          , study_id 			int                not null
          , study_json          json               null
         );
        create index sc_country_id_new on search.new_countries(country_id);
        create index sc_study_id_new on search.new_countries(study_id);";

        db.ExecuteSQL(sql_string);
    }

    public void CreateSearchLexemesTable()
    {
        string sql_string = @"drop table if exists search.new_lexemes;
        create table search.new_lexemes
        (
            study_id            int               primary key not null
          , study_name          varchar           null 
          , tt                  varchar           null   
          , tt_lex  		    tsvector          null
          , conditions 	        varchar           null   
          , conditions_lex      tsvector          null
          , study_json          json              null
        ) ;
        CREATE INDEX tt_search_idx_new ON search.new_lexemes USING GIN (tt_lex);
        CREATE INDEX cond_search_idx_new ON search.new_lexemes USING GIN (conditions_lex);";

        db.ExecuteSQL(sql_string);
    }

    public void RenameTable(string table_name)
    {
        string new_table_name = "new_" + table_name;
        string sql_string = $"DROP TABLE IF EXISTS search.{table_name}";
        db.ExecuteSQL(sql_string);
        sql_string = $"ALTER TABLE search.{new_table_name} RENAME TO {table_name};";
        db.ExecuteSQL(sql_string);
    }
}
