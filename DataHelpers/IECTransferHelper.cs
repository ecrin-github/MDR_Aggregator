using Dapper;
using Npgsql;

namespace MDR_Aggregator;

public class IECTransferrer
{
    private readonly string _db_conn;
    private readonly string field_list;
    private readonly ILoggingHelper _loggingHelper;

    public IECTransferrer(string connString, ILoggingHelper loggingHelper)
    {
        _db_conn = connString;
        _loggingHelper = loggingHelper;
        field_list = @"sd_sid, seq_num, iec_type_id, split_type, leader, indent_level,
        sequence_string, iec_text ";
    }
    
    public int ExecuteSQL(string sql_string)
    {
        using var conn = new NpgsqlConnection(_db_conn);
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
    
    public int GetMinId(string full_table_name)
    {
        string sql_string = $"select min(id) from {full_table_name}";
        using var conn = new NpgsqlConnection(_db_conn);
        return conn.ExecuteScalar<int>(sql_string);
    }

    
    public int GetMaxId(string full_table_name)
    {
        string sql_string = $"select max(id) from {full_table_name}";
        using var conn = new NpgsqlConnection(_db_conn);
        return conn.ExecuteScalar<int>(sql_string);
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

        BuildStudiesTable();
    }
    
    private void BuildIECTable(string table_name)
    {
        string sql_string = $@"DROP TABLE IF EXISTS ad.{table_name};
        CREATE TABLE ad.{table_name}(
            id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
          , study_id               INT             NULL                                              
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

        ExecuteSQL(sql_string);
    }
    
    private void BuildStudiesTable()
    {
        string sql_string = $@"DROP TABLE IF EXISTS ad.studies;
        CREATE TABLE ad.studies(
            id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
          , study_id               INT             NOT NULL                                                
          , source_id              INT             NOT NULL                
          , sd_sid                 VARCHAR         NOT NULL
          , is_preferred           BOOLEAN         NOT NULL  
          , display_title          VARCHAR         NULL
          , brief_description      VARCHAR         NULL      
          , iec_level_id           INT             NULL                    
          , iec_level              VARCHAR         NULL                               
          , study_start_year       INT             NULL 
          , study_start_month      INT             NULL                    
          , study_type_id          INT             NULL                    
          , study_type             VARCHAR         NULL
          , study_enrolment        VARCHAR         NULL                               
          , study_gender_elig_id   INT             NULL                    
          , study_gender_elig      VARCHAR         NULL  
          , min_age                VARCHAR         NULL                       
          , min_age_units_id       INT             NULL                    
          , min_age_units          VARCHAR         NULL                                
          , max_age                VARCHAR         NULL                       
          , max_age_units_id       INT             NULL                    
          , max_age_units          VARCHAR         NULL          
          , datetime_of_data_fetch TIMESTAMPTZ     NULL
        );
        CREATE INDEX studies_study_id ON ad.studies(study_id);
        CREATE INDEX studies_srce_sid ON ad.studies(source_id, sd_sid);";

        ExecuteSQL(sql_string);
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
                recs_trans += TransferData(source,"study_iec", $"study_iec_{i}", false);
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
                recs_trans += TransferData(source, "study_iec_13to19", $"study_iec_{i}", false);
            }
            for (int i = 20; i <= 30; i++)
            {
                recs_trans += TransferData(source, "study_iec_20on", $"study_iec_{i}", false);
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
        string FTW_schema_name = source.db_conn!;   // being used for different purpose
        string top_sql = $@" Insert into ad.{dest_table_name} (source_id, {field_list})
                             select {source_id}, t.{field_list}  
                             from {FTW_schema_name}.{srce_table_name} t 
                             inner join {FTW_schema_name}.studies s
                             on t.sd_sid = s.sd_sid ";
        string where_sql = "";
        if (!uses_whole_table)
        {
            where_sql = dest_table_name switch
            {
                "study_iec_null" => " where (study_start_year is null or study_start_year > 2030) ",
                "study_iec_pre06" => " where study_start_year < 2006 ",
                "study_iec_0608" => " where study_start_year in (2007, 2008, 2009) ",
                "study_iec_0910" => " where study_start_year in (2009, 2010) ",
                "study_iec_1112" => " where study_start_year in (2011, 2012) ",
                "study_iec_1314" => " where study_start_year in (2013, 2014) ",
                "study_iec_15" =>  " where study_start_year = 2015 ",
                "study_iec_16" => " where study_start_year = 2016",
                "study_iec_17" => " where study_start_year = 2017",
                "study_iec_18" => " where study_start_year = 2018",
                "study_iec_19" => " where study_start_year = 2019",
                "study_iec_20" => " where study_start_year = 2020",
                "study_iec_21" => " where study_start_year = 2021",
                "study_iec_22" => " where study_start_year = 2022",
                "study_iec_23" => " where study_start_year = 2023",
                "study_iec_24" => " where study_start_year = 2024",
                "study_iec_25" => " where study_start_year = 2025",
                "study_iec_26" => " where study_start_year = 2026",
                "study_iec_27" => " where study_start_year = 2027",
                "study_iec_28" => " where study_start_year = 2028",
                "study_iec_29" => " where study_start_year = 2029",
                "study_iec_30" => " where study_start_year = 2030",
                _ => ""
            };
        }
        string sql_string = top_sql + where_sql;
        string full_table_name = $"{FTW_schema_name}.{srce_table_name}";
        
        // transfer in chunks...
        
        try
        {
            int transferred = 0;
            int min_id = GetMinId(full_table_name);
            int max_id = GetMaxId(full_table_name);
            int rec_batch = 100000;
            string qualifier = uses_whole_table ? " where " : " and ";
            string fbc = $"IEC records, from {full_table_name} to {dest_table_name},";
            if (max_id - min_id > rec_batch)
            {
                sql_string += qualifier;
                for (int r = min_id; r <= max_id; r += rec_batch)
                {
                    string batch_sql_string = sql_string + $" t.id >= {r} and t.id < {r + rec_batch} ";
                    int res = ExecuteSQL(batch_sql_string);
                    transferred += res;
                    int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                    _loggingHelper.LogLine($"Transferred {res} {fbc} ids {r} to {e}");
                }
                _loggingHelper.LogLine($"Transferred {transferred} {fbc} in total");
            }
            else
            {
                transferred = ExecuteSQL(sql_string);
                _loggingHelper.LogLine($"Transferred {transferred} {fbc} as a single batch");
            }
            return transferred;
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In IEC transfer of ({full_table_name} to aggregated table: {e.Message}");
            return 0;
        }
    }

    public void TransferKeyStudyIdData(string FTW_schema)
    {
        string sql_string = $@"insert into ad.studies(study_id, source_id, sd_sid, 
        is_preferred, datetime_of_data_fetch) 
        select study_id, source_id, sd_sid, is_preferred, datetime_of_data_fetch
        from {FTW_schema}.study_ids k ";

        int max_id = GetMaxId($"{FTW_schema}.study_ids");
        int transferred = 0;
        int rec_batch = 100000;
        string fbc = "study record data, from aggs DB to IEC DB,";
        try
        {
            for (int r = 1; r <= max_id; r += rec_batch)
            {
                string batch_sql_string = sql_string + $" where k.id >= {r} and k.id < {r + rec_batch} ";
                int res = ExecuteSQL(batch_sql_string);
                transferred += res;
                
                int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                _loggingHelper.LogLine($"Transferred {res} {fbc} ids {r} to {e}");
            }
            _loggingHelper.LogLine($"Transferred {transferred} {fbc} in total");
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In transfer of {fbc}: {e.Message}");
        }
    }
    
    
    public void TransferKeyStudyRecordData(string FTW_schema)
    {
        string sql_string = $@"update ad.studies k
               set display_title = s.display_title, 
               brief_description = s.brief_description, 
               iec_level_id = s.iec_level,               
               study_start_year = s.study_start_year, study_start_month = s.study_start_month, 
               study_type_id = s.study_type_id, study_enrolment = s.study_enrolment, 
               study_gender_elig_id = s.study_gender_elig_id, 
               min_age = s.min_age, min_age_units_id = s.min_age_units_id,
               max_age = s.max_age, max_age_units_id = s.max_age_units_id, 
               datetime_of_data_fetch = k.datetime_of_data_fetch
            from {FTW_schema}.studies s
            where k.study_id = s.id "; 
        
        int max_id = GetMaxId($"ad.studies");
        int updated = 0;
        int rec_batch = 50000;
        string fbc = "study records, with study details,";
        try
        {
            for (int r = 1; r <= max_id; r += rec_batch)
            {
                string batch_sql_string = sql_string + $" and k.id >= {r} and k.id < {r + rec_batch} ";
                int res = ExecuteSQL(batch_sql_string);
                updated += res;
                
                int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                _loggingHelper.LogLine($"Updated {res} {fbc} ids {r} to {e}");
            }
            _loggingHelper.LogLine($"Updated {updated} {fbc} in total");
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In transfer of {fbc}: {e.Message}");
        }
    }

    
    public void DecodeStudyData()
    {
        int min_id = GetMinId("ad.studies");
        int max_id = GetMaxId("ad.studies");
        
        string sql_string = @"update ad.studies s
                      set study_type = 
                              CASE study_type_id 
                                    WHEN 11 THEN 'Interventional'
                                    WHEN 12 THEN 'Observational'
                                    WHEN 13 THEN 'Observational patient registry'
                                    WHEN 14 THEN 'Expanded access'
                                    WHEN 15 THEN 'Funded programme'
                                    WHEN 16 THEN 'Other'
                                    WHEN 0 THEN 'Not yet known' 
                                    END ";
        ExecuteChunkedSQL(sql_string, min_id, max_id, 50000, "study records, decoding gender eligibility ids") ;
 
        sql_string = @"update ad.studies s
                      set iec_level = 
                              CASE iec_level_id 
                                    WHEN 0 THEN 'None'
                                    WHEN 1 THEN 'Single statement'
                                    WHEN 3 THEN 'Multiple general statements'
                                    WHEN 7 THEN 'Single inclusion + Single exclusion'
                                    WHEN 8 THEN 'Single inclusion + Multiple exclusion'
                                    WHEN 9 THEN 'Multiple inclusion + Single exclusion'
                                    WHEN 10 THEN 'Multiple inclusion + exclusion' 
                                    END ";
        ExecuteChunkedSQL(sql_string, min_id, max_id, 50000, "study records, decoding study type ids") ;

        sql_string = @"update ad.studies s
                      set study_gender_elig = 
                              CASE study_gender_elig_id 
                                    WHEN 900 THEN 'All'
                                    WHEN 905 THEN 'Female'
                                    WHEN 910 THEN 'Male'
                                    WHEN 915 THEN 'Not provided'
                                    WHEN 0 THEN 'Unknown status' 
                                    END ";
        ExecuteChunkedSQL(sql_string, min_id, max_id, 50000, "study records, decoding gender eligibility ids") ;
        
        sql_string = @"update ad.studies s
                       set min_age_units = 
                           CASE min_age_units_id
                                WHEN 17 THEN 'Years'
                                WHEN 16 THEN 'Months'
                                WHEN 15 THEN 'Weeks'
                                WHEN 14 THEN 'Days'                                    
                                WHEN 0 THEN 'Not provided' 
                                END ";
        ExecuteChunkedSQL(sql_string, min_id, max_id, 50000, "study records, decoding min age unit ids") ;
        
        sql_string = @"update ad.studies s
                       set max_age_units = 
                           CASE max_age_units_id
                                WHEN 17 THEN 'Years'
                                WHEN 16 THEN 'Months'
                                WHEN 15 THEN 'Weeks'
                                WHEN 14 THEN 'Days'                                    
                                WHEN 0 THEN 'Not provided' 
                                END ";
        ExecuteChunkedSQL(sql_string, min_id, max_id, 50000, "study records, decoding max age unit ids") ;
    }


    private void ExecuteChunkedSQL(string sql_string, int min_id, int max_id, int batch_size, string fbc)
    {
        int updated = 0;
        for (int r = 1; r <= max_id; r += batch_size)
        {
            string batch_sql_string = sql_string + $" where s.id >= {r} and s.id < {r + batch_size} ";
            int res = ExecuteSQL(batch_sql_string);
            updated += res;
            int e = r + batch_size < max_id ? r + batch_size - 1 : max_id;
            _loggingHelper.LogLine($"Updated {res} {fbc} ids {r} to {e}");
        }
        _loggingHelper.LogLine($"Updated {updated} {fbc} in total");
    }


    public void UpdateIECWithStudyIds()
    {
        UpdateIECTable("study_iec_null");
        UpdateIECTable("study_iec_pre06");
        UpdateIECTable("study_iec_0608");
        UpdateIECTable("study_iec_0910");
        UpdateIECTable("study_iec_1112");
        UpdateIECTable("study_iec_1314");
        for (int i = 15; i <= 30; i++)
        {
            UpdateIECTable($"study_iec_{i}");
        }
    }

    private void UpdateIECTable(string table_name)
    {
        string sql_string = $@"update ad.{table_name} t
                               set study_id = s.study_id
                               from ad.studies s
                               where t.source_id = s.source_id 
                               and t.sd_sid = s.sd_sid "; 
        int updated = 0;
        int max_id = GetMaxId($"ad.{table_name}");
        int rec_batch = max_id < 400000 ? 50000 : 25000;
        string fbc = $"IEC records, in table {table_name},";
        try
        {
            for (int r = 1; r <= max_id; r += rec_batch)
            {
                string batch_sql_string = sql_string + $" and t.id >= {r} and t.id < {r + rec_batch} ";
                int res = ExecuteSQL(batch_sql_string);
                updated += res;
                
                int e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
                _loggingHelper.LogLine($"Updated {res} {fbc} ids {r} to {e}");
            }
            _loggingHelper.LogLine($"Updated {updated} {fbc} in total");
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In updating {fbc}: {e.Message}");
        }
    }
 }


public class IECStudyDetails
{
    public int study_id { get; set; }                                       
    public int source_id { get; set; }                
    public string? sd_sid { get; set; }    
    public bool is_preferred { get; set; }    
    public string? display_title  { get; set; }    
    public string? brief_description { get; set; }        
    public int? iec_level_id  { get; set; }                    
    public int? study_start_year { get; set; }    
    public int? study_start_month { get; set; }                     
    public int? study_type_id { get; set; }               
    public string? study_enrolment { get; set; }                               
    public int?  study_gender_elig_id { get; set; }                     
    public int? min_age { get; set; }                    
    public int? min_age_units_id { get; set; }                
    public int? max_age { get; set; }                  
    public int? max_age_units_id { get; set; }            
    public DateTime? datetime_of_data_fetch { get; set; }

}