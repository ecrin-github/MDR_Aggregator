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
        string sql_string = @"DROP TABLE IF EXISTS core.new_search_objects_json;
        CREATE TABLE core.new_search_objects_json(
          id                       INT             NOT NULL PRIMARY KEY
        , full_object              JSON            NULL
        );
        CREATE INDEX search_objects_json_id_new ON core.new_search_objects_json(id);";
        db.ExecuteSQL(sql_string);

        sql_string = @"drop table if exists core.new_search_objects;
        create table core.new_search_objects
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
        create index os_object_id_new on core.new_search_objects(oid);";
        db.ExecuteSQL(sql_string);
    }
    
    public void CreateStudyDataSearchTables()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.new_search_studies_json;
        CREATE TABLE core.new_search_studies_json(
          id                       INT             NOT NULL PRIMARY KEY
        , search_res               JSON            NULL
        , full_study               JSON            NULL
        , open_aire                JSON            NULL
        , c19p                     JSON            NULL 
        );
        CREATE INDEX search_studies_json_id_new ON core.new_search_studies_json(id);";
        db.ExecuteSQL(sql_string);
        
        sql_string = @"drop table if exists core.new_search_studies;
        create table core.new_search_studies
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
        create index ss_start_year_new on core.new_search_studies(start_year);
        create index ss_type_new on core.new_search_studies(type_id);
        create index ss_status_new on core.new_search_studies(status_id);
        create index ss_phase_id_new on core.new_search_studies(phase_id);
        create index ss_alloc_id_new on core.new_search_studies(alloc_id);";
        db.ExecuteSQL(sql_string);
    }
    
    
    public void CreatePMIDSearchData()
    {
        string sql_string = @"drop table if exists core.new_search_pmids;
        create table core.new_search_pmids
        (
            pmid                 int                not null
          , study_id             int                not null
          , study_json           json               null
        );
        create index sp_pmid_new on core.new_search_pmids(pmid);
        create index sp_study_id_new on core.new_search_pmids(study_id);";
        
        db.ExecuteSQL(sql_string);
    }    
    
    // idents table.
    
    public void CreateIdentifierSearchData()
    {
        string sql_string = @"drop table if exists core.new_search_idents;
         create table core.new_search_idents
         (
            ident_type  		int                not null
          , ident_value			varchar            not null		
          , study_id 			int                not null
          , study_json          json               null
         );
        create index si_type_value_new on core.new_search_idents(ident_type, ident_value);
        create index si_study_id_new on core.new_search_idents(study_id);";

        db.ExecuteSQL(sql_string);
    }
    
    public void CreateCountrySearchData()
    {
        string sql_string = @"drop table if exists core.new_search_countries;
         create table core.new_search_countries
         (
            country_id   		int                not null		
          , study_id 			int                not null
          , study_json          json               null
         );
        create index sc_country_id_new on core.new_search_countries(country_id);
        create index sc_study_id_new on core.new_search_countries(study_id);";

        db.ExecuteSQL(sql_string);
    }
    
    public void CreateSearchLexemesTable()
    {
        string sql_string = @"drop table if exists core.new_search_lexemes;
        create table core.new_search_lexemes
        (
            study_id            int               primary key not null
          , study_name          varchar           null 
          , tt                  varchar           null   
          , tt_lex  		    tsvector          null
          , conditions 	        varchar           null   
          , conditions_lex      tsvector          null
          , study_json          json              null
        ) ;
        CREATE INDEX tt_search_idx_new ON core.new_search_lexemes USING GIN (tt_lex);
        CREATE INDEX cond_search_idx_new ON core.new_search_lexemes USING GIN (conditions_lex);";

        db.ExecuteSQL(sql_string);
    }
    
}
