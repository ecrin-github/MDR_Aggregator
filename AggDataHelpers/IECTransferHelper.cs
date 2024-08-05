using Dapper;
using MDR_Aggregator.LoggingHelpers;
using MDR_Aggregator.LoggingHelpers.Interfaces;
using Npgsql;

namespace MDR_Aggregator.AggDataHelpers;

public class IecTransferrer
{
    private readonly string _dbConn;
    private readonly string _fieldList;
    private readonly ILoggingHelper _loggingHelper;

    public IecTransferrer(string connString, ILoggingHelper loggingHelper)
    {
        _dbConn = connString;
        _loggingHelper = loggingHelper;
        _fieldList = """
                     sd_sid, seq_num, iec_type_id, split_type, leader, indent_level,
                             sequence_string, iec_text 
                     """;
    }

    private int ExecuteSql(string sqlString)
    {
        using var conn = new NpgsqlConnection(_dbConn);
        try
        {
            return conn.Execute(sqlString);
        }
        catch (Exception e)
        {
            _loggingHelper.LogError("In ExecuteSQL; " + e.Message + ", \nSQL was: " + sqlString);
            return 0;
        }
    }

    private int GetMinId(string fullTableName)
    {
        var sql_string = $"select min(id) from {fullTableName}";
        using var conn = new NpgsqlConnection(_dbConn);
        return conn.ExecuteScalar<int>(sql_string);
    }


    private int GetMaxId(string fullTableName)
    {
        var sql_string = $"select max(id) from {fullTableName}";
        using var conn = new NpgsqlConnection(_dbConn);
        return conn.ExecuteScalar<int>(sql_string);
    }

   
    
    internal void BuildNewIecTables()
    {
        BuildIecTable("study_iec_null");
        BuildIecTable("study_iec_pre06");
        BuildIecTable("study_iec_0608");
        BuildIecTable("study_iec_0910");
        BuildIecTable("study_iec_1112");
        BuildIecTable("study_iec_1314");
        for (int i = 15; i <= 30; i++)
        {
            BuildIecTable($"study_iec_{i}");
        }

        BuildStudiesTable();
    }
    
    private void BuildIecTable(string tableName)
    {
        var sql_string = $"""
                          DROP TABLE IF EXISTS ad.{tableName};
                                  CREATE TABLE ad.{tableName}(
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
                                  CREATE INDEX {tableName}_study_id ON ad.{tableName}(study_id);
                                  CREATE INDEX {tableName}_srce_sid ON ad.{tableName}(source_id, sd_sid);
                          """;

        ExecuteSql(sql_string);
    }
    
    private void BuildStudiesTable()
    {
        const string sqlString = """
                                  DROP TABLE IF EXISTS ad.studies;
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
                                          CREATE INDEX studies_srce_sid ON ad.studies(source_id, sd_sid);
                                  """;

        ExecuteSql(sqlString);
    }
    
    
    internal long TransferIECData(Source source)
    {
        long recs_trans = 0;
        switch (source.study_iec_storage_type)
        {
            case "Single Table":
            {
                recs_trans += TransferData(source, "study_iec", "study_iec_null", false);
                recs_trans += TransferData(source, "study_iec", "study_iec_pre06", false);
                recs_trans += TransferData(source, "study_iec", "study_iec_0608", false);
                recs_trans += TransferData(source, "study_iec", "study_iec_0910", false);
                recs_trans += TransferData(source, "study_iec", "study_iec_1112", false);
                recs_trans += TransferData(source, "study_iec", "study_iec_1314", false);
                for (var i = 15; i <= 30; i++)
                {
                    recs_trans += TransferData(source,"study_iec", $"study_iec_{i}", false);
                }

                break;
            }
            case "By Year Groupings":
            {
                recs_trans += TransferData(source, "study_iec_upto12", "study_iec_null", false);
                recs_trans += TransferData(source, "study_iec_upto12", "study_iec_pre06", false);
                recs_trans += TransferData(source, "study_iec_upto12", "study_iec_0608", false);
                recs_trans += TransferData(source, "study_iec_upto12", "study_iec_0910", false);
                recs_trans += TransferData(source, "study_iec_upto12", "study_iec_1112", false);
                recs_trans += TransferData(source, "study_iec_13to19", "study_iec_1314", false);
                for (var i = 15; i <= 19; i++)
                {
                    recs_trans += TransferData(source, "study_iec_13to19", $"study_iec_{i}", false);
                }
                for (var i = 20; i <= 30; i++)
                {
                    recs_trans += TransferData(source, "study_iec_20on", $"study_iec_{i}", false);
                }

                break;
            }
            case "By Years":
            {
                recs_trans += TransferData(source, "study_iec_null", "study_iec_null", true);
                recs_trans += TransferData(source, "study_iec_pre06", "study_iec_pre06", true);
                recs_trans += TransferData(source, "study_iec_0608", "study_iec_0608", true);
                recs_trans += TransferData(source, "study_iec_0910", "study_iec_0910", true);
                recs_trans += TransferData(source, "study_iec_1112", "study_iec_1112", true);
                recs_trans += TransferData(source, "study_iec_1314", "study_iec_1314", true);
                for (var i = 15; i <= 30; i++)
                {
                    recs_trans += TransferData(source, $"study_iec_{i}", $"study_iec_{i}", true);
                }

                break;
            }
        }

        return recs_trans;
    }

    private int TransferData(Source source, string srceTableName, 
                              string destTableName, bool usesWholeTable)
    {

        var source_id = source.id;
        var ftwSchemaName = source.db_conn!;   // being used for different purpose
        var top_sql = $"""
                        Insert into ad.{destTableName} (source_id, {_fieldList})
                                                    select {source_id}, t.{_fieldList}  
                                                    from {ftwSchemaName}.{srceTableName} t 
                                                    inner join {ftwSchemaName}.studies s
                                                    on t.sd_sid = s.sd_sid 
                       """;
        var where_sql = "";
        if (!usesWholeTable)
        {
            where_sql = destTableName switch
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
        var sql_string = top_sql + where_sql;
        var full_table_name = $"{ftwSchemaName}.{srceTableName}";
        
        // transfer in chunks...
        
        try
        {
            var transferred = 0;
            var min_id = GetMinId(full_table_name);
            var max_id = GetMaxId(full_table_name);
            const int recBatch = 100000;
            var qualifier = usesWholeTable ? " where " : " and ";
            var fbc = $"IEC records, from {full_table_name} to {destTableName},";
            if (max_id - min_id > recBatch)
            {
                sql_string += qualifier;
                for (var r = min_id; r <= max_id; r += recBatch)
                {
                    var batch_sql_string = sql_string + $" t.id >= {r} and t.id < {r + recBatch} ";
                    var res = ExecuteSql(batch_sql_string);
                    transferred += res;
                    var e = r + recBatch < max_id ? r + recBatch - 1 : max_id;
                    _loggingHelper.LogLine($"Transferred {res} {fbc} ids {r} to {e}");
                }
                _loggingHelper.LogLine($"Transferred {transferred} {fbc} in total");
            }
            else
            {
                transferred = ExecuteSql(sql_string);
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

    public void TransferKeyStudyIdData(string ftwSchema)
    {
        var sql_string = $"""
                          insert into ad.studies(study_id, source_id, sd_sid, 
                                  is_preferred, datetime_of_data_fetch) 
                                  select study_id, source_id, sd_sid, is_preferred, datetime_of_data_fetch
                                  from {ftwSchema}.study_ids k 
                          """;

        var max_id = GetMaxId($"{ftwSchema}.study_ids");
        var transferred = 0;
        const int recBatch = 100000;
        var fbc = "study record data, from aggs DB to IEC DB,";
        try
        {
            for (var r = 1; r <= max_id; r += recBatch)
            {
                var batch_sql_string = sql_string + $" where k.id >= {r} and k.id < {r + recBatch} ";
                var res = ExecuteSql(batch_sql_string);
                transferred += res;
                
                var e = r + recBatch < max_id ? r + recBatch - 1 : max_id;
                _loggingHelper.LogLine($"Transferred {res} {fbc} ids {r} to {e}");
            }
            _loggingHelper.LogLine($"Transferred {transferred} {fbc} in total");
        }
        catch (Exception e)
        {
            _loggingHelper.LogError($"In transfer of {fbc}: {e.Message}");
        }
    }
    
    
    public void TransferKeyStudyRecordData(string ftwSchema)
    {
        var sql_string = $"""
                          update ad.studies k
                                         set display_title = s.display_title, 
                                         brief_description = s.brief_description, 
                                         iec_level_id = s.iec_level,               
                                         study_start_year = s.study_start_year, study_start_month = s.study_start_month, 
                                         study_type_id = s.study_type_id, study_enrolment = s.study_enrolment, 
                                         study_gender_elig_id = s.study_gender_elig_id, 
                                         min_age = s.min_age, min_age_units_id = s.min_age_units_id,
                                         max_age = s.max_age, max_age_units_id = s.max_age_units_id, 
                                         datetime_of_data_fetch = k.datetime_of_data_fetch
                                      from {ftwSchema}.studies s
                                      where k.study_id = s.id 
                          """; 
        
        var max_id = GetMaxId($"ad.studies");
        var updated = 0;
        const int recBatch = 25000;
        var fbc = "study records, with study details,";
        try
        {
            for (var r = 1; r <= max_id; r += recBatch)
            {
                var batch_sql_string = sql_string + $" and k.id >= {r} and k.id < {r + recBatch} ";
                var res = ExecuteSql(batch_sql_string);
                updated += res;
                
                var e = r + recBatch < max_id ? r + recBatch - 1 : max_id;
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
        var min_id = GetMinId("ad.studies");
        var max_id = GetMaxId("ad.studies");
        
        var sql_string = """
                         update ad.studies s
                                               set study_type = 
                                                       CASE study_type_id 
                                                             WHEN 11 THEN 'Interventional'
                                                             WHEN 12 THEN 'Observational'
                                                             WHEN 13 THEN 'Observational patient registry'
                                                             WHEN 14 THEN 'Expanded access'
                                                             WHEN 15 THEN 'Funded programme'
                                                             WHEN 16 THEN 'Other'
                                                             WHEN 0 THEN 'Not yet known' 
                                                             END 
                         """;
        ExecuteChunkedSql(sql_string, min_id, max_id, 50000, "study records, decoding gender eligibility ids") ;
 
        sql_string = """
                     update ad.studies s
                                           set iec_level = 
                                                   CASE iec_level_id 
                                                         WHEN 0 THEN 'None'
                                                         WHEN 1 THEN 'Single statement'
                                                         WHEN 3 THEN 'Multiple general statements'
                                                         WHEN 7 THEN 'Single inclusion + Single exclusion'
                                                         WHEN 8 THEN 'Single inclusion + Multiple exclusion'
                                                         WHEN 9 THEN 'Multiple inclusion + Single exclusion'
                                                         WHEN 10 THEN 'Multiple inclusion + exclusion' 
                                                         END 
                     """;
        ExecuteChunkedSql(sql_string, min_id, max_id, 50000, "study records, decoding study type ids") ;

        sql_string = """
                     update ad.studies s
                                           set study_gender_elig = 
                                                   CASE study_gender_elig_id 
                                                         WHEN 900 THEN 'All'
                                                         WHEN 905 THEN 'Female'
                                                         WHEN 910 THEN 'Male'
                                                         WHEN 915 THEN 'Not provided'
                                                         WHEN 0 THEN 'Unknown status' 
                                                         END 
                     """;
        ExecuteChunkedSql(sql_string, min_id, max_id, 50000, "study records, decoding gender eligibility ids") ;
        
        sql_string = """
                     update ad.studies s
                                            set min_age_units = 
                                                CASE min_age_units_id
                                                     WHEN 17 THEN 'Years'
                                                     WHEN 16 THEN 'Months'
                                                     WHEN 15 THEN 'Weeks'
                                                     WHEN 14 THEN 'Days'                                    
                                                     WHEN 0 THEN 'Not provided' 
                                                     END 
                     """;
        ExecuteChunkedSql(sql_string, min_id, max_id, 50000, "study records, decoding min age unit ids") ;
        
        sql_string = """
                     update ad.studies s
                                            set max_age_units = 
                                                CASE max_age_units_id
                                                     WHEN 17 THEN 'Years'
                                                     WHEN 16 THEN 'Months'
                                                     WHEN 15 THEN 'Weeks'
                                                     WHEN 14 THEN 'Days'                                    
                                                     WHEN 0 THEN 'Not provided' 
                                                     END 
                     """;
        ExecuteChunkedSql(sql_string, min_id, max_id, 50000, "study records, decoding max age unit ids") ;
    }


    private void ExecuteChunkedSql(string sqlString, int minId, int maxId, int batchSize, string fbc)
    {
        var updated = 0;
        for (var r = 1; r <= maxId; r += batchSize)
        {
            var batch_sql_string = sqlString + $" where s.id >= {r} and s.id < {r + batchSize} ";
            var res = ExecuteSql(batch_sql_string);
            updated += res;
            var e = r + batchSize < maxId ? r + batchSize - 1 : maxId;
            _loggingHelper.LogLine($"Updated {res} {fbc} ids {r} to {e}");
        }
        _loggingHelper.LogLine($"Updated {updated} {fbc} in total");
    }


    public void UpdateIecWithStudyIds()
    {
        UpdateIECTable("study_iec_null");
        UpdateIECTable("study_iec_pre06");
        UpdateIECTable("study_iec_0608");
        UpdateIECTable("study_iec_0910");
        UpdateIECTable("study_iec_1112");
        UpdateIECTable("study_iec_1314");
        for (var i = 15; i <= 30; i++)
        {
            UpdateIECTable($"study_iec_{i}");
        }
    }

    private void UpdateIECTable(string tableName)
    {
        var sql_string = $"""
                          update ad.{tableName} t
                                                         set study_id = s.study_id
                                                         from ad.studies s
                                                         where t.source_id = s.source_id 
                                                         and t.sd_sid = s.sd_sid 
                          """; 
        var updated = 0;
        var max_id = GetMaxId($"ad.{tableName}");
        var rec_batch = max_id < 400000 ? 50000 : 25000;
        var fbc = $"IEC records, in table {tableName},";
        try
        {
            for (var r = 1; r <= max_id; r += rec_batch)
            {
                var batch_sql_string = sql_string + $" and t.id >= {r} and t.id < {r + rec_batch} ";
                var res = ExecuteSql(batch_sql_string);
                updated += res;
                
                var e = r + rec_batch < max_id ? r + rec_batch - 1 : max_id;
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