using Dapper;
using Npgsql;

namespace MDR_Aggregator;

public class IECTransferrer
{
    private readonly string _db_conn;
    private readonly string field_list;

    public IECTransferrer(string connString, ILoggingHelper logginghelper)
    {
        _db_conn = connString;
        field_list = @"sd_sid, seq_num, iec_type_id, split_type, leader, indent_level,
        sequence_string, iec_text ";
    }
    

    public void Execute_SQL(string sql_string)
    {
        using var conn = new NpgsqlConnection(_db_conn);
        conn.Execute(sql_string);
    }
    
    internal void BuildNewIECTables()
    {
        BuildIECTable("study_iec_null");
        BuildIECTable("study_iec_pre06");
        BuildIECTable("study_iec_0608");
        BuildIECTable("study_iec_0910");
        BuildIECTable("study_iec_1112");
        BuildIECTable("study_iec_1314");
        for (int i = 15; i <= 30; i++)
        {
            BuildIECTable($"study_iec_{i}");
        }
    }
    
    private void BuildIECTable(string table_name)
    {
        string sql_string = $@"DROP TABLE IF EXISTS ad.{table_name};
        CREATE TABLE ad.{table_name}(
            id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
          , study_id               INT             NOT NULL                                              
          , source_id              INT             NOT NULL                
          , sd_sid                 VARCHAR         NOT NULL
          , seq_num                INT             NULL
          , iec_type_id            INT             NULL       
          , split_type             VARCHAR         NULL              
          , leader                 VARCHAR         NOT NULL
          , indent_level           INT             NULL
          , sequence_string        VARCHAR         NULL
          , iec_text               VARCHAR         NULL
          , added_on               TIMESTAMPTZ     NOT NULL default now()
        );
        CREATE INDEX {table_name}_study_id ON ad.{table_name}(study_id);
        CREATE INDEX {table_name}_srce_sid ON ad.{table_name}(source_id, sd_sid);";

        Execute_SQL(sql_string);
    }
    
    
    internal Int64 TransferIECData(Source source)
    {
        Int64 recs_trans = 0;
        if (source.study_iec_storage_type == "Single Table")
        {
            recs_trans += TransferData(source, "study_iec", "study_iec_null", false);
            recs_trans += TransferData(source, "study_iec", "study_iec_pre06", false);
            recs_trans += TransferData(source, "study_iec", "study_iec_0608", false);
            recs_trans += TransferData(source, "study_iec", "study_iec_0910", false);
            recs_trans += TransferData(source, "study_iec", "study_iec_1112", false);
            recs_trans += TransferData(source, "study_iec", "study_iec_1314", false);
            for (int i = 15; i <= 30; i++)
            {
                recs_trans += TransferData(source,$"study_iec", "study_iec_{i}", false);
            }
        }
    
        if (source.study_iec_storage_type == "By Year Groupings")
        {
            recs_trans += TransferData(source, "study_iec_upto12", "study_iec_null", false);
            recs_trans += TransferData(source, "study_iec_upto12", "study_iec_pre06", false);
            recs_trans += TransferData(source, "study_iec_upto12", "study_iec_0608", false);
            recs_trans += TransferData(source, "study_iec_upto12", "study_iec_0910", false);
            recs_trans += TransferData(source, "study_iec_upto12", "study_iec_1112", false);
            recs_trans += TransferData(source, "study_iec_13to19", "study_iec_1314", false);
            for (int i = 15; i <= 19; i++)
            {
                recs_trans += TransferData(source, $"study_iec_13to19", "study_iec_{i}", false);
            }
            for (int i = 20; i <= 30; i++)
            {
                recs_trans += TransferData(source, $"study_iec_20on", "study_iec_{i}", false);
            }
        } 
        
        if (source.study_iec_storage_type == "By Years")
        {
            recs_trans += TransferData(source, "study_iec_null", "study_iec_null", true);
            recs_trans += TransferData(source, "study_iec_pre06", "study_iec_pre06", true);
            recs_trans += TransferData(source, "study_iec_0608", "study_iec_0608", true);
            recs_trans += TransferData(source, "study_iec_0910", "study_iec_0910", true);
            recs_trans += TransferData(source, "study_iec_1112", "study_iec_1112", true);
            recs_trans += TransferData(source, "study_iec_1314", "study_iec_1314", true);
            for (int i = 15; i <= 30; i++)
            {
                recs_trans += TransferData(source, $"study_iec_{i}", $"study_iec_{i}", true);
            }
        }

        return recs_trans;
    }

    private int TransferData(Source source, string srce_table_name, 
                              string dest_table_name, bool uses_whole_table)
    {

        int source_id = source.id;
        string FTW_schema_name = source.database_name! + "_ad";
        string top_sql = $@" Insert into {dest_table_name} (source_id, {field_list})
                             select {source_id},{field_list})  
                             from {FTW_schema_name}.{srce_table_name} ";
        
        string where_sql = "";
        if (!uses_whole_table)
        {
            where_sql = " where study_start_year " + dest_table_name switch
            {
                "study_iec_null" => " where study_start_year ",
                "study_iec_pre06" => "foo",
                "study_iec_0608" => "foo",
                "study_iec_0910" => "foo",
                "study_iec_1112" => "foo",
                "study_iec_1314" => "foo",
                "study_iec_15" => "= 2015 ",
                "study_iec_16" => "= 2016",
                "study_iec_17" => "= 2017",
                "study_iec_18" => "= 2018",
                "study_iec_19" => "= 2019",
                "study_iec_20" => "= 2020",
                "study_iec_21" => "= 2021",
                "study_iec_22" => "= 2022",
                "study_iec_23" => "= 2023",
                "study_iec_24" => "= 2024",
                "study_iec_25" => "foo",
                "study_iec_26" => "foo",
                "study_iec_27" => "foo",
                "study_iec_28" => "foo",
                "study_iec_29" => "foo",
                "study_iec_30" => "foo",
                _ => ""
            };
        }

        top_sql += where_sql;
        
        // transfer in chunks...

    }


    public int LoadCoreStudyData(string schema_name)
    {
        //string sql_string = $@"INSERT INTO core.studies({field_string})
        //        SELECT {field_string}
        //        FROM {schema_name}.studies";
        //return db.ExecuteCoreTransferSQL(sql_string, " where ", "aggs_st.studies");
        return 0;
    }

    
}