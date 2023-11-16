using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Diagnostics.Metrics;
using System.Drawing;
using System.Net.NetworkInformation;
using static NpgsqlTypes.NpgsqlTsVector;

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
        , c19p                     Varchar         NULL 
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
         );
        create index sc_country_id_new on search.new_countries(country_id);
        create index sc_study_id_new on search.new_countries(study_id);";

        db.ExecuteSQL(sql_string);
    }

    public void CreateObjectTypeSearchData()
    {
        string sql_string = @"drop table if exists search.new_object_types;
         create table search.new_object_types
         (
            object_type_id   	int                not null		
          , study_id 			int                not null
         );
        create index sb_type_id_new on search.new_object_types(object_type_id, study_id);
        create index sb_study_id_new on search.new_object_types(study_id);";

        db.ExecuteSQL(sql_string);
    }

    public void CreateSearchLexemesTable()
    {
        string sql_string = @"drop table if exists search.new_lexemes;
        create table search.new_lexemes
        (
            study_id            int               primary key not null
          , bucket              int               null
          , study_name          varchar           null 
          , tt                  varchar           null   
          , tt_lex  		    tsvector          null
          , conditions 	        varchar           null   
          , conditions_lex      tsvector          null
          , study_json          json              null
        ) ;
        CREATE INDEX tt_search_idx_new ON search.new_lexemes USING GIN (tt_lex);
        CREATE INDEX cond_search_idx_new ON search.new_lexemes USING GIN (conditions_lex);
        CREATE INDEX bucket_study_new ON search.new_lexemes(bucket, study_id);";

        db.ExecuteSQL(sql_string);
    }


    public void TileLexemeTable()
    {
        // This splits the lexeme search table into 20 'buckets' which can then be 
        // searched in sequence in any Lexeme saearch request

        // Get count from table and calculate the 'batch size' to span the table in 20 buckets
        // Then use those parameters to add the bucket number to each record

        int lex_count = db.GetCount("search.new_lexemes");
        int batch_size = lex_count / 20;
        if (lex_count % 20 != 0)
        {
            batch_size++;  // add one to ensure all records covered
        }

        for (int i = 0; i < 20; i++)
        {
            string sql_string = $@"update search.new_lexemes sl
                  set bucket = {i + 1}
                  from
                  (  select study_id 
                     from search.new_lexemes 
                     order by study_id 
                     offset {i * batch_size} limit {batch_size} 
                  ) b
                  where sl.study_id = b.study_id; ";
            db.ExecuteSQL(sql_string);
        }
    }

    // Some of the cluster commands take over 5 minutes to run
    // Change connection parameters to try and allow this

    public void ClusterTable(string table_name, string index_name)
    {
        string new_table_name = "new_" + table_name;
        string new_index_name = index_name + "_new";
        string sql_string = $"CLUSTER search.{new_table_name} USING {new_index_name}";
        db.ExecuteSQL(sql_string);
        sql_string = $"ANALYZE search.{new_table_name};";
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

    public void RenameIndex(string index_name)
    {
        string old_index_name = index_name + "_new";
        string sql_string = $"ALTER INDEX search.{old_index_name} RENAME TO {index_name};";
        db.ExecuteSQL(sql_string);
    }

    public void RenamePK(string index_name)
    {
        string old_index_name = "new_" + index_name;
        string sql_string = $"ALTER INDEX search.{old_index_name} RENAME TO {index_name};";
        db.ExecuteSQL(sql_string);
    }
}
