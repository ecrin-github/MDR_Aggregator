using Dapper;
using Npgsql;

namespace MDR_Aggregator;

public class SearchTableBuilder
{
    readonly string _db_conn;
    private readonly ILoggingHelper _loggingHelper;

    public SearchTableBuilder(string db_conn, ILoggingHelper loggingHelper)
    {
        _loggingHelper = loggingHelper;
        _db_conn = db_conn;
    }

    private void ExecuteSQL(string sql_string)
    {
        using var conn = new NpgsqlConnection(_db_conn);
        conn.Execute(sql_string);
    }

    public void create_table_search_studies()
    {
        string sql_string = @"drop table if exists core.search_studies;
        create table core.search_studies
        (
            study_id                int primary key   not null
            , study_name            varchar           null
            , description           varchar           null
            , dss                   varchar           null
            , start_year            int               null
            , start_month           int               null
            , type_id               int               null
            , type_name             varchar           null
            , status_id             int               null
            , status_name           varchar           null
            , gender_elig_id        int               null
            , gender_elig           varchar           null
            , min_age               varchar           null
            , min_age_units_id      int               null
            , max_age               varchar           null
            , max_age_units_id      int               null
            , phase_id              int               null
            , phase_name            varchar           null
            , alloc_id              int               null
            , alloc_name            varchar           null
            , purpose_name          varchar           null
            , interv_name           varchar           null
            , masking_name          varchar           null
            , obsmodel_name         varchar           null
            , timepersp_name        varchar           null
            , biospec_name          varchar           null
            , has_objects           bit(16)           default '0000000000000000'
            , country_list          varchar           null
            , condition_list        varchar           null
            , provenance            varchar           null
        );
        create index ss_start_year on core.search_studies(start_year);
        create index ss_type on core.search_studies(type_id);
        create index ss_status on core.search_studies(status_id);
        create index ss_phase_id on core.search_studies(phase_id);
        create index ss_alloc_id on core.search_studies(alloc_id);";

        ExecuteSQL(sql_string);
    }

    public void create_table_search_lexemes()
    {
        string sql_string = @"drop table if exists core.search_lexemes;
        create table core.search_lexemes
        (
           study_id               int   primary key  not null
         , study_name    		  varchar            null     		
         , titles_lex   		  varchar            null
         , topics_lex   		  varchar            null
        );";

        ExecuteSQL(sql_string);
     }
    
    
     public void create_table_search_pmids()
     {
        string sql_string = @"drop table if exists core.search_pmids;
        create table core.search_pmids
        (
            pmid                 int                not null
          , study_id             int                not null
        );
        create index sp_pmid on core.search_pmids(pmid);";
        
        ExecuteSQL(sql_string);
     }
     
     public void create_table_search_idents()
     {
         string sql_string = @"drop table if exists core.search_idents;
         create table core.search_idents
         (
            ident_type  		int                not null
          , ident_value			varchar            not null		
          , study_id 			int                not null
         );
        create index si_type_value on core.search_idents(ident_type, ident_value);";

        ExecuteSQL(sql_string);
     }

     public void create_table_search_objects()
     {
        string sql_string = @"drop table if exists core.search_objects;
        create table core.search_objects
        (
            study_id          int                 not null
          , object_id         int                 not null
          , object_name       varchar             not null
          , type_id           int                 null
          , type_name         varchar             null
          , url               varchar             null
          , resource_type_id  int                 null
          , resource_icon     varchar             null
          , year_published    int                 null
          , access_type_id    int                 null
          , access_icon       varchar             null
          , provenance        varchar             null
        );
        create index so_study_id on core.search_objects(study_id);
        create index so_object_id on core.search_objects(object_id);";
         
        ExecuteSQL(sql_string);
     }
}
