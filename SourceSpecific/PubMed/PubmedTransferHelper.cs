using Dapper;
using Npgsql;
using PostgreSQLCopyHelper;

namespace MDR_Aggregator;

internal class PubmedTransferHelper
{
    private readonly string _connString;
    private readonly string _schema_name;
    private readonly DBUtilities db;
    private readonly ILoggingHelper _loggingHelper;

    public PubmedTransferHelper(string schema_name, string connString, ILoggingHelper loggingHelper)
    {
        _schema_name = schema_name;
        _connString = connString;
        _loggingHelper = loggingHelper;
        db = new DBUtilities(connString, _loggingHelper);
    }

    // Tables and functions used for the PMIDs collected from DB Sources

    public void SetupTempPMIDTables()
    {
        using var conn = new NpgsqlConnection(_connString);
        string sql_string = @"DROP TABLE IF EXISTS nk.temp_collected_pmids;
                  CREATE TABLE IF NOT EXISTS nk.temp_collected_pmids(
                    id                       INT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY
                  , source_id                INT
                  , sd_oid                   VARCHAR
                  , parent_study_source_id   INT 
                  , parent_study_sd_sid      VARCHAR
                  , type_id                  INT
                  , datetime_of_data_fetch   TIMESTAMPTZ
                  ); ";
        conn.Execute(sql_string);

        sql_string = @"DROP TABLE IF EXISTS nk.temp_all_pmids;
                  CREATE TABLE IF NOT EXISTS nk.temp_all_pmids(
                    id                       INT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY
                  , object_id                INT
                  , source_id                INT
                  , sd_oid                   VARCHAR
                  , object_type_id		   INT            
                  , title                    VARCHAR      
                  , is_preferred_object      BOOLEAN
                  , parent_study_source_id   INT 
                  , parent_study_sd_sid      VARCHAR
                  , parent_study_id          INT
                  , is_preferred_study       BOOLEAN
                  , datetime_of_data_fetch   TIMESTAMPTZ
                  , match_status             INT   default 0
                  );
             CREATE INDEX temp_all_pmids_objectid ON nk.temp_all_pmids(object_id);
             CREATE INDEX temp_all_pmids_sdidsource ON nk.temp_all_pmids(source_id, sd_oid);
             CREATE INDEX temp_all_pmids_parent_study_sdidsource 
                             ON nk.temp_all_pmids(parent_study_source_id, parent_study_sd_sid);";

        conn.Execute(sql_string);
 
        sql_string = @"DROP TABLE IF EXISTS nk.temp_pmids;
                  CREATE TABLE IF NOT EXISTS nk.temp_pmids(
                    id                       INT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY
                  , object_id                INT
                  , source_id                INT
                  , sd_oid                   VARCHAR
                  , object_type_id		   INT            
                  , title                    VARCHAR      
                  , is_preferred_object      BOOLEAN
                  , parent_study_source_id   INT 
                  , parent_study_sd_sid      VARCHAR
                  , parent_study_id          INT
                  , is_preferred_study       BOOLEAN
                  , datetime_of_data_fetch   TIMESTAMPTZ
                  , match_status             INT   default 0
                  );
             CREATE INDEX temp_pmids_objectid ON nk.temp_pmids(object_id);
             CREATE INDEX temp_pmids_sdidsource ON nk.temp_pmids(source_id, sd_oid);
             CREATE INDEX temp_pmids_parent_study_sdidsource 
                             ON nk.temp_pmids(parent_study_source_id, parent_study_sd_sid);";

        conn.Execute(sql_string);
    }

    public int ExecuteSQL(string sql_string)
    {
           using var conn = new NpgsqlConnection(_connString);
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
    

    public int FetchBankReferences(int source_id, string ftw_schema_name)
    {
           string sql_string = $"select max(id) FROM {ftw_schema_name}.object_db_links";
           using var conn = new NpgsqlConnection(_connString);
           int max_id = conn.ExecuteScalar<int>(sql_string);
           int batch_size = 25000;
           try
           {
                  int obtained = 0;
                  sql_string = $@"Insert into nk.temp_collected_pmids(source_id, sd_oid, parent_study_source_id, 
                        parent_study_sd_sid, type_id, datetime_of_data_fetch)
                        select {source_id}, k.sd_oid, d.id, k.id_in_db, 12, a.datetime_of_data_fetch
                        from {ftw_schema_name}.object_db_links k
                        inner join {ftw_schema_name}.data_objects a 
                        on k.sd_oid = a.sd_oid
                        inner join context_ctx.nlm_databanks d
                        on k.db_name = d.nlm_abbrev ";
                  if (max_id > batch_size)
                  {
                         for (int r = 1; r <= max_id; r += batch_size)
                         {
                                string batch_sql_string = sql_string + $" and k.id >= {r} and k.id < {r + batch_size} ";
                                int num_obtained = ExecuteSQL(batch_sql_string);
                                obtained += num_obtained;
                                int e = r + batch_size < max_id ? r + batch_size - 1 : max_id;
                                _loggingHelper.LogLine($"Obtained {num_obtained} pmid ids, from ids {r} to {e}");
                         }
                  }
                  else
                  {
                         obtained = ExecuteSQL(sql_string);
                         _loggingHelper.LogLine($"Obtained {obtained} pmid ids as a single batch");
                  }
                  return obtained;
           } 
           catch (Exception e)
           {
                  _loggingHelper.LogError($"In obtaining pmid ids: {e.Message}");
                  return 0;
           }
    }

    
    public ulong StorePMIDLinks(PostgreSQLCopyHelper<PMIDLink> copyHelper, 
                                IEnumerable<PMIDLink> entities)
    {
        using var conn = new NpgsqlConnection(_connString);
        conn.Open();
        return copyHelper.SaveAll(conn, entities);
    }

    
    public void CleanPMIDsdsidData()
    {
        CleanPMIDsdsidData1();
        CleanPMIDsdsidData2();
        CleanPMIDsdsidData3();
        CleanPMIDsdsidData4();
    }

    private void CleanPMIDsdsidData1() 
    {
        using var conn = new NpgsqlConnection(_connString);
        string sql_string = @"UPDATE nk.temp_collected_pmids
                    SET parent_study_sd_sid = 'ACTRN' || parent_study_sd_sid
                    WHERE parent_study_source_id = 100116
                    AND length(parent_study_sd_sid) = 14;";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids
                    SET parent_study_sd_sid = Replace(parent_study_sd_sid, ' ', '')
                    WHERE parent_study_source_id = 100116;";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids
                    SET parent_study_sd_sid = Replace(parent_study_sd_sid, 'O', '')
                    WHERE parent_study_source_id = 100116;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
                    SET parent_study_sd_sid = Replace(parent_study_sd_sid, '#', '')
                    WHERE parent_study_source_id = 100116;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
                    SET parent_study_sd_sid = Replace(parent_study_sd_sid, ':', '')
                    WHERE parent_study_source_id = 100116;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
                    SET parent_study_sd_sid = Replace(parent_study_sd_sid, '[', '')
                    WHERE parent_study_source_id = 100116;";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids
                    SET parent_study_sd_sid = substring(parent_study_sd_sid, 1, 19)
                    WHERE parent_study_source_id = 100116
                    AND parent_study_sd_sid like 'ACTRN%'
                    AND length(parent_study_sd_sid) > 19;";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids
                    SET parent_study_source_id = 100126,
                    parent_study_sd_sid = Replace(parent_study_sd_sid, 'ACTRN', '')
                    WHERE parent_study_source_id = 100116
                    AND parent_study_sd_sid like 'ACTRNISRCTN%';";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids
                    SET parent_study_sd_sid = Replace(parent_study_sd_sid, '--', '-')
                    WHERE parent_study_source_id = 100118;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
                    SET parent_study_sd_sid = Replace(parent_study_sd_sid, 'CHICTR', 'ChiCTR')
                    WHERE parent_study_source_id = 100118;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
                    SET parent_study_sd_sid = 'ChiCTR-' || parent_study_sd_sid
                    WHERE parent_study_source_id = 100118
                    and parent_study_sd_sid not ilike 'ChiCTR-%';";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
                    SET parent_study_sd_sid = Replace(parent_study_sd_sid, 'ChiCTR-ChiCTR', 'ChiCTR-')
                    WHERE parent_study_source_id = 100118;";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids
                    SET parent_study_sd_sid = Replace(parent_study_sd_sid, '-', '')
                    WHERE parent_study_source_id = 100118
                    and parent_study_sd_sid ~'^ChiCTR-\d{10}$' ;";
       conn.Execute(sql_string);
       
       sql_string = @"UPDATE nk.temp_collected_pmids
                    SET parent_study_source_id = 100120,
                    parent_study_sd_sid = regexp_match(parent_study_sd_sid, 'NCT\d{8}')
                    WHERE parent_study_source_id = 100118
                    and parent_study_sd_sid ~'-NCT\d{8}' ;";
       conn.Execute(sql_string);
    }

    private void CleanPMIDsdsidData2()
    {
        using var conn = new NpgsqlConnection(_connString);
        string sql_string = @"UPDATE nk.temp_collected_pmids p
                 SET parent_study_sd_sid = new_id
                 FROM nk.ctg_id_checker k
                 WHERE p.parent_study_sd_sid = k.old_id
                 and parent_study_source_id = 100120;";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids
                 SET parent_study_sd_sid = Replace(parent_study_sd_sid, '}', '')
                 WHERE parent_study_source_id = 100120;";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids
                 SET parent_study_sd_sid = Replace(parent_study_sd_sid, '{', '')
                 WHERE parent_study_source_id = 100120;";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids
                 SET parent_study_sd_sid = Replace(parent_study_sd_sid, ' ', '')
                 WHERE parent_study_source_id = 100120;";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids
                 SET parent_study_sd_sid = Replace(parent_study_sd_sid, '.', '')
                 WHERE parent_study_source_id = 100120;";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids
                 SET parent_study_sd_sid = Replace(parent_study_sd_sid, ')', '')
                 WHERE parent_study_source_id = 100120;";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids p
                 SET parent_study_sd_sid = regexp_match(parent_study_sd_sid, 'NCT\d{8}')
                 WHERE p.parent_study_sd_sid ilike '%clinicaltrials%'
                 AND  p.parent_study_sd_sid ilike '%nct%'
                 and parent_study_source_id = 100120;";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids
                 SET parent_study_sd_sid = Replace(parent_study_sd_sid, '/', '-')
                 WHERE parent_study_source_id = 100121;";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids
                 SET parent_study_source_id = 100121,
                 parent_study_sd_sid = Replace(parent_study_sd_sid, '/', '-')
                 WHERE parent_study_sd_sid like 'CTRI%'
                 and parent_study_sd_sid like '%/%';";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
                 SET parent_study_sd_sid = 'CTRI-' || parent_study_sd_sid
                 WHERE parent_study_source_id = 100121
                 and parent_study_sd_sid not ilike 'CTRI-%';";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
                 SET parent_study_sd_sid = Replace(parent_study_sd_sid, 'REF-', '')
                 WHERE parent_study_source_id = 100121;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
                 SET parent_study_sd_sid = Replace(parent_study_sd_sid, 'CTRI-CTRI', 'CTRI-')
                 WHERE parent_study_source_id = 100121;";
        conn.Execute(sql_string);


        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = 'RPCEC' || parent_study_sd_sid
               WHERE parent_study_source_id = 100122
               and parent_study_sd_sid not ilike 'RPCEC%';";
        conn.Execute(sql_string);


        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = UPPER(parent_study_sd_sid)
               WHERE parent_study_source_id = 100123;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, ' ', '')
               WHERE parent_study_source_id = 100123;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, '–', '-')
               WHERE parent_study_source_id = 100123;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, 'EUDRA-CT', 'EUDRACT')
               WHERE parent_study_source_id = 100123;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, 'EUDRACT', '')
                WHERE parent_study_source_id = 100123;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, 'EURODRACT', '')
               WHERE parent_study_source_id = 100123;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, 'EU', '')
               WHERE parent_study_source_id = 100123;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, 'CT', '')
               WHERE parent_study_source_id = 100123;";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = left(parent_study_sd_sid, 14)
               WHERE parent_study_source_id = 100123
               and length(parent_study_sd_sid) > 14;";
        conn.Execute(sql_string);
    }

    private void CleanPMIDsdsidData3()
    {
        using var conn = new NpgsqlConnection(_connString);
        string sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = Replace(parent_study_sd_sid, ' ', '')
               WHERE parent_study_source_id = 100124;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = Replace(parent_study_sd_sid, '-', '')
               WHERE parent_study_source_id = 100124;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = Replace(parent_study_sd_sid, 'DKRS', 'DRKS')
               WHERE parent_study_source_id = 100124;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = Replace(parent_study_sd_sid, 'DRK0', 'DRKS0')
               WHERE parent_study_source_id = 100124;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = 'DRKS' || parent_study_sd_sid
               WHERE parent_study_source_id = 100124
               and parent_study_sd_sid not ilike 'DRKS%';";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = 'IRCT' || parent_study_sd_sid
               WHERE parent_study_source_id = 100125
               and parent_study_sd_sid not ilike 'IRCT%';";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, 'SRCTN', 'ISRCTN')
               WHERE parent_study_source_id = 100126
               and parent_study_sd_sid ilike 'SRCTN%';";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, '.', '')
               WHERE parent_study_source_id = 100126;";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, ',', '')
               WHERE parent_study_source_id = 100126;";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, 'ISRTN', 'ISRCTN')
               WHERE parent_study_source_id = 100126;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, 'ISRNT', 'ISRCTN')
               WHERE parent_study_source_id = 100126;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = 'ISRCTN' || parent_study_sd_sid
               WHERE parent_study_source_id = 100126
               and parent_study_sd_sid not ilike 'ISRCTN%';";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, 'ISRCTNISCRTN', 'ISRCTN')
               WHERE parent_study_source_id = 100126;";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, 'ISRCTNISCTRN', 'ISRCTN')
               WHERE parent_study_source_id = 100126;";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, ' ', '')
               WHERE parent_study_source_id = 100126;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, '#', '')
               WHERE parent_study_source_id = 100126;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, '/', '')
               WHERE parent_study_source_id = 100126;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, ':', '')
               WHERE parent_study_source_id = 100126;";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids
               SET source_id = 100126,
               parent_study_sd_sid = replace(parent_study_sd_sid, 'ACTN', '')
               WHERE parent_study_sd_sid like 'ACTNISRCTN%'; ";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_source_id = 100125,
               parent_study_sd_sid = replace(parent_study_sd_sid, 'ISRCTN', '')
               WHERE parent_study_sd_sid like '%IRCTISRCTN%';";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_source_id = 100126,
               parent_study_sd_sid = replace(parent_study_sd_sid, 'IRCT', '')
               WHERE parent_study_sd_sid like '%ISRCTNIRCT%';";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_source_id = 100127,
               parent_study_sd_sid = 'JPRN-' || parent_study_sd_sid
               WHERE parent_study_sd_sid like 'UMIN%'; ";
        conn.Execute(sql_string);
    }
    
    private void CleanPMIDsdsidData4()
    {
        using var conn = new NpgsqlConnection(_connString);
        string sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, ' ', '')
               WHERE parent_study_source_id = 100128;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, 'PACTCR', 'PACTR')
               WHERE parent_study_source_id = 100128;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = 'PACTR' || parent_study_sd_sid
               WHERE parent_study_source_id = 100128
               and parent_study_sd_sid not ilike 'PACTR%';";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, '/', '-')
               WHERE parent_study_source_id = 100130;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = 'SLCTR-' || parent_study_sd_sid
               WHERE parent_study_source_id = 100130
               and parent_study_sd_sid not ilike 'SLCTR-%';";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, '-', '')
               WHERE parent_study_source_id = 100131;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, ' ', '')
               WHERE parent_study_source_id = 100131;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = 'TCTR' || parent_study_sd_sid
               WHERE parent_study_source_id = 100131
               and parent_study_sd_sid not ilike 'TCTR%';";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, 'NTRR', 'NTR')
               WHERE parent_study_source_id = 100132;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, ' ', '')
               WHERE parent_study_source_id = 100132;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = replace(parent_study_sd_sid, '-', '')
               WHERE parent_study_source_id = 100132;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_collected_pmids
               SET parent_study_sd_sid = 'NL' || parent_study_sd_sid
               WHERE parent_study_source_id = 100132
               and parent_study_sd_sid not ilike 'NTR%'
               and parent_study_sd_sid not ilike 'NL%';";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_collected_pmids p
                SET parent_study_sd_sid = new_id
                FROM nk.dutch_id_checker k
                WHERE p.parent_study_sd_sid = k.old_id
                and parent_study_source_id = 100132;";
       conn.Execute(sql_string);
        
    }

    
    public ulong FetchSourceReferences(int source_id, string source_conn_string)
    {
           string sql_string = $"select max(id) FROM mn.dbrefs_all";
           using var conn = new NpgsqlConnection(source_conn_string);
           int max_id = conn.ExecuteScalar<int>(sql_string);
           int batch_size = 50000;
           try
           {
               ulong stored = 0;
               sql_string = $@"select {source_id} as source_id, pmid as sd_oid, 
                   source_id as parent_study_source_id,
                   sd_sid as parent_study_sd_sid, 
                   type_id, datetime_of_data_fetch
                   from mn.dbrefs_all k
                   where pmid is not null "; 
               
               if (max_id > batch_size)
               {
                  for (int r = 1; r <= max_id; r += batch_size)
                  {
                      string batch_sql_string = sql_string + $" and k.id >= {r} and k.id < {r + batch_size} ";
                      IEnumerable<PMIDLink> pmid_ids = conn.Query<PMIDLink>(batch_sql_string);
                      ulong num_stored = StorePMIDLinks(CopyHelpers.pmid_links_helper, pmid_ids); 
                      stored += num_stored;
                      int e = r + batch_size < max_id ? r + batch_size - 1 : max_id;
                      _loggingHelper.LogLine($"Obtained {num_stored} pmid ids, from ids {r} to {e}");
                  }
               }
               else
               { 
                  IEnumerable<PMIDLink> pmid_ids = conn.Query<PMIDLink>(sql_string);
                  stored = StorePMIDLinks(CopyHelpers.pmid_links_helper, pmid_ids);
                  _loggingHelper.LogLine($"Obtained {stored} pmid ids as a single batch");
               }
               return stored;
           } 
           catch (Exception e)
           {
                  _loggingHelper.LogError($"In obtaining pmid ids: {e.Message}");
                  return 0;
           }
    }

    
    public void TransferPMIDLinksToTempObjectIds()
    {
         string sql_string = @"INSERT INTO nk.temp_all_pmids(
                source_id, sd_oid, object_type_id, 
                parent_study_source_id, 
                parent_study_sd_sid, datetime_of_data_fetch)
                SELECT  
                source_id, trim(sd_oid), type_id, 
                parent_study_source_id, 
                parent_study_sd_sid, datetime_of_data_fetch
                FROM nk.temp_collected_pmids t ";
        int res = db.Update_UsingTempTable("nk.temp_collected_pmids", "nk.temp_all_pmids", sql_string, " where "
                    , 50000, ", with temp_pmid records");
        _loggingHelper.LogLine($"{res} PMID-study references passed to temp all pmid table");
    }


    public void UpdateTempObjectIdsWithStudyDetails()
    {
        // First put in study id and whether is preferred, from the study_identifiers table.

        string sql_string = @"UPDATE nk.temp_all_pmids t
                SET 
                parent_study_id = si.study_id,
                is_preferred_study = si.is_preferred
                from nk.study_ids si
                where t.parent_study_source_id = si.source_id
                and t.parent_study_sd_sid = si.sd_sid ";

        int res = db.Update_UsingTempTable("nk.temp_all_pmids", "nk.temp_all_pmids", sql_string, " and "
                               , 50000, ", with parent study and is preferred status");
        _loggingHelper.LogLine($"{res} parent studies matched in temp all PMIDs table");

        // Some pubmed entries are not matched, as the ids in the pubmed bank
        // data are still non-standard, or refer to obsolete ids (about 4600 in total)
        // Possibly put this up to the banks id section as all the DB references should be matched

        sql_string = @"DELETE from nk.temp_all_pmids t
                   where t.parent_study_id is null;";
        res = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{res} PMID records with non-matched studies deleted from total");

        // Then convert sd_sid and source to the preferred version - all equivalent studies
        // therefore have the same study id / source data / is_study_preferred value
        // Increases the duplication if a paper has been cited in two or more study sources
        // but makes it easier to eliminate it using a later 'select distinct' call

        sql_string = @"UPDATE nk.temp_all_pmids t
                SET 
                parent_study_source_id = si.source_id,
                parent_study_sd_sid = si.sd_sid,
                is_preferred_study = true
                from 
                nk.study_ids si
                where t.parent_study_id = si.study_id
                and si.is_preferred = true ";

        res = db.Update_UsingTempTable("nk.temp_all_pmids", "nk.temp_all_pmids", sql_string, " and "
                               , 50000, ", with parent study details");
        _loggingHelper.LogLine($"{res} PMID records updated with preferred study data");
    }


    public void FillDistinctTempObjectsTable()
    {
        // Then transfer the distinct data - to get the set of all the study-PMID combinations

        string sql_string = @"INSERT INTO nk.temp_pmids(
                    source_id, sd_oid, object_type_id, parent_study_source_id, 
                    parent_study_sd_sid, parent_study_id, is_preferred_study)
                    SELECT DISTINCT
                    source_id, sd_oid, object_type_id, parent_study_source_id, 
                    parent_study_sd_sid, parent_study_id, is_preferred_study
                    FROM nk.temp_all_pmids ";
        int res = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{res} distinct study-PMID links found");

        // Update with latest datetime_of_data_fetch for each study-PMID combination
        // object_id, title, is_preferred_object, match_status all null at present

        sql_string = @"UPDATE nk.temp_pmids dp
                    set datetime_of_data_fetch = mx.max_fetch_date
                    FROM 
                        (select parent_study_id, sd_oid,
                         max(datetime_of_data_fetch) as max_fetch_date
                         FROM nk.temp_all_pmids
                         group by parent_study_id, sd_oid) mx
                    WHERE dp.parent_study_id = mx.parent_study_id
                    and dp.sd_oid = mx.sd_oid ";
        res = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{res} record updated with latest date of data fetch");
        
        // Update with maximum of the type id (non defaults are always bigger than the
        // default value of 12) for each study-PMID combination.
        
        // N.B. object_id, title, is_preferred_object, match_status all remain null 

        sql_string = @"UPDATE nk.temp_pmids dp
                    set object_type_id = mx.max_type_id
                    FROM 
                        (select parent_study_id, sd_oid,
                         max(object_type_id) as max_type_id
                         FROM nk.temp_all_pmids
                         group by parent_study_id, sd_oid) mx
                    WHERE dp.parent_study_id = mx.parent_study_id
                    and dp.sd_oid = mx.sd_oid ";
        res = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{res} record updated with most specific type of pubmed article");
    }


    public void MatchExistingPMIDLinks()
    {
        // identify the matched records in the temp table. Matching is against PMID and study.

        string sql_string = @"UPDATE nk.temp_pmids t
        set match_status = 1
        from nk.data_object_ids doi
        where t.parent_study_id = doi.parent_study_id
        and t.sd_oid = doi.sd_oid ";

        db.Update_UsingTempTable("nk.temp_pmids", "nk.temp_pmids", 
                                  sql_string, " and ", 50000, ", with match status = 1 for existing objects");
        _loggingHelper.LogLine("Existing objects matched in temp table");

        // Update the matched records in the data object identifier table
        // with the updated date time of data fetch

        sql_string = @"UPDATE nk.data_object_ids doi
        set match_status = 1,
        datetime_of_data_fetch = t.datetime_of_data_fetch
        from nk.temp_pmids t
        where doi.parent_study_id = t.parent_study_id
        and doi.sd_oid = t.sd_oid ";

        int res = db.Update_UsingTempTable("nk.temp_pmids", "data_object_identifiers", 
                                            sql_string, " and ", 50000
                                            , ", with match status 1 and time of most recent data fetch");
        _loggingHelper.LogLine($"{res} existing PMID objects matched in identifiers table");

        // delete the matched records from the temp table
        sql_string = @"DELETE from nk.temp_pmids
        where match_status = 1 ";
        db.ExecuteSQL(sql_string);
    }


    public void IdentifyNewPMIDLinks()
    {
        // First insert title from the pubmed DB data objects.

        string sql_string = @"Drop table if exists nk.pub_titles;
        Create table nk.pub_titles as select sd_oid, title
                 from " + _schema_name + @".data_objects ";
        db.ExecuteSQL(sql_string);

        sql_string = @"UPDATE nk.temp_pmids t
        SET title = pt.title
        FROM nk.pub_titles pt
        where t.sd_oid = pt.sd_oid ";

        int res = db.Update_UsingTempTable("nk.temp_pmids", "nk.temp_pmids", 
                                           sql_string, " and ", 50000, ", with article title for new objects");
        _loggingHelper.LogLine($"{res} new PMID-study combinations updated with article titles");

        // There are still currently some PMIDs that do not match any entry in the Pubmed table (reason not clear). 
        // Indicates that they need to be downloaded and imported, or that they no longer exist, or were errors

        sql_string = @"Delete from nk.temp_pmids t
        where title is null";

        res = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{res} records deleted from PMID-study combinations as PMID cannot be found in pubmed data");

        // Identify and label completely new PMIDs first. This identifies PMIDs that are completely
        // new to the system (may be one or more instances if cited by multiple studies).

        sql_string = @"Drop table if exists nk.new_pmids;
        Create table nk.new_pmids as 
                 select p.id
                 from nk.temp_pmids p
                 left join nk.data_object_ids doi
                 on p.sd_oid = doi.sd_oid
                 where doi.sd_oid is null; ";

        db.ExecuteSQL(sql_string);

        sql_string = @"UPDATE nk.temp_pmids t
        set match_status = 3
        FROM nk.new_pmids n
        where t.id = n.id
        and t.match_status = 0 ";

        res = db.Update_UsingTempTable("nk.temp_pmids", "nk.temp_pmids", 
                                        sql_string, " and ", 50000, ", with status = 3, for new pubmed records");
        _loggingHelper.LogLine($"{res} new PMID-study combinations found with completely new PMIDs");

        // Then identify records where the PMID exists but the link with that study is not yet in the
        // data_object_ids table. N.B. May duplicate an existing study-PMID link but use a 
        // different version of the study.

        sql_string = @"Drop table if exists nk.new_links;
        Create table nk.new_links as 
        select p.id
        from nk.temp_pmids p
        left join nk.data_object_ids doi
        on p.sd_oid = doi.sd_oid
        and p.parent_study_id = doi.parent_study_id
        and p.match_status = 0 
        where doi.sd_oid is null";

        db.ExecuteSQL(sql_string);

        sql_string = @"UPDATE nk.temp_pmids t
        set match_status = 2
        from nk.new_links n 
        where t.match_status = 0
        and t.id = n.id ";

        res = db.Update_UsingTempTable("nk.temp_pmids", "nk.temp_pmids", 
                                        sql_string, " and ", 50000
                                        , ", with status = 2, for new links for existing pubmed records");
        _loggingHelper.LogLine($"{res} new PMID-study combinations found for existing PMIDs");

    }

    public void AddNewPMIDStudyLinks()
    {
        // New links found for PMIDs already in the system. Get the preferred object id for this PMID link.
        // This means that the PubMed object is only in the system once, even if it has multiple links.

         string sql_string = @"UPDATE nk.temp_pmids t
                      SET object_id = doi.object_id,
                      is_preferred_object = false
                      FROM nk.data_object_ids doi
                      where t.sd_oid = doi.sd_oid
                      and doi.is_preferred_object = true
                      and t.match_status = 2 ";

         int res = db.Update_UsingTempTable("nk.temp_pmids", "nk.temp_pmids", 
                                            sql_string, " and ", 50000, ", with object id, for existing objects");
         _loggingHelper.LogLine($"{res} new PMID-study combinations updated");

         sql_string = @"Insert into nk.data_object_ids
         (object_id, source_id, sd_oid, object_type_id, title, is_preferred_object,
                        parent_study_source_id, parent_study_sd_sid,
                        parent_study_id, is_preferred_study, datetime_of_data_fetch, match_status)
         select object_id, source_id, sd_oid, object_type_id, title, is_preferred_object,
                        parent_study_source_id, parent_study_sd_sid,
                        parent_study_id, is_preferred_study, datetime_of_data_fetch, match_status
         FROM nk.temp_pmids t
         where t.match_status = 2 ";

         res = db.Update_UsingTempTable("nk.temp_pmids", "nk.data_object_ids",  
                                        sql_string, " and ", 50000, ", adding new data object id records");
        _loggingHelper.LogLine($"{res} new PMID-study combinations added");

    }

    public void AddCompletelyNewPMIDs()
    {
        // Use the PMID-Study combination with the minimum study_id as the 'preferred' object record
        // for this PMID. This means only one 'new' pubmed record will be identified as such (several
        // Study-PMID combinations may have been added at the same time.)

        string sql_string = @"UPDATE nk.temp_pmids s
                              SET is_preferred_object = true
                              FROM 
                                  (select sd_oid, min(parent_study_id) as min_study
                                   from nk.temp_pmids 
                                   where match_status = 3
                                   group by sd_oid) m
                              where s.sd_oid = m.sd_oid
                              and s.parent_study_id = m.min_study
                              and s.match_status = 3 ";
        int res = db.Update_UsingTempTable("nk.temp_pmids", "nk.temp_pmids", 
               sql_string, " and ", 50000, ", with preferred status, for min of parent study ids");
        _loggingHelper.LogLine($"{res} objects set as 'preferred' for new PMIDs");
        
        // Put the remaining study-PMID combinations as non-preferred.

        sql_string = @"UPDATE nk.temp_pmids t
                             SET is_preferred_object = false
                             where is_preferred_object is null
                             and t.match_status = 3 ";
        res = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{res} objects set as 'non-preferred' for new PMIDs");

        // Add in the preferred new PMID records. Note that the match status (3) is included.

        sql_string = @"Insert into nk.data_object_ids
        (source_id, sd_oid, object_type_id, title, is_preferred_object,
                 parent_study_source_id, parent_study_sd_sid,
                 parent_study_id, is_preferred_study, 
                 datetime_of_data_fetch, match_status)
         select source_id, sd_oid, object_type_id, title, is_preferred_object,
                 parent_study_source_id, parent_study_sd_sid,
                 parent_study_id, is_preferred_study, 
                 datetime_of_data_fetch, match_status
         FROM nk.temp_pmids t
         where t.match_status = 3 
         and is_preferred_object = true";

         res = db.ExecuteSQL(sql_string);
         _loggingHelper.LogLine($"{res} new 'preferred' PMID-study combinations added for new PMIDs");

         // Update newly added records with object ids, if the 'preferred' object record.

         sql_string = @"Update nk.data_object_ids doi
                        set object_id = id
                        where match_status = 3 
                        and source_id = 100135
                        and object_id is null
                        and is_preferred_object = true;";
        res = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{res} object ids created for new PMIDs and applied to 'preferred' objects");

        // Update remaining study-PMID combinations with new object id
        // At this stage only the 'preferred' study-PMID links are in the doi table

        sql_string = @"UPDATE nk.temp_pmids t
                       SET object_id = doi.object_id
                       FROM nk.data_object_ids doi
                       where t.sd_oid = doi.sd_oid
                       and t.match_status = 3 
                       and t.is_preferred_object = false ";
        res = db.Update_UsingTempTable("nk.temp_pmids", "nk.temp_pmids", 
                                        sql_string, " and "
                                        , 50000, ", with object ids, for new pubmed-study combinations");
        _loggingHelper.LogLine($"{res} object ids applied to new PMIDs and 'non-preferred' objects");

        // Add remaining matching study-PMIDs records. Note object_id is included in the add this time.

        sql_string = @"Insert into nk.data_object_ids
        (object_id, source_id, sd_oid, object_type_id, title, is_preferred_object,
                 parent_study_source_id, parent_study_sd_sid,
                 parent_study_id, is_preferred_study, 
                 datetime_of_data_fetch, match_status)
         select object_id, source_id, sd_oid, object_type_id, title, is_preferred_object,
                 parent_study_source_id, parent_study_sd_sid,
                 parent_study_id, is_preferred_study, 
                 datetime_of_data_fetch, match_status
         FROM nk.temp_pmids t
         where t.match_status = 3 
         and is_preferred_object = false ";

         res = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{res} new 'non-preferred' PMID-study combinations added");
    }


    public void IdentifyPMIDDataForImport(int source_id)
    {
           string sql_string = $@"DROP TABLE IF EXISTS nk.temp_objects_to_add;
             CREATE TABLE nk.temp_objects_to_add as 
             select distinct object_id, sd_oid
             from nk.data_object_ids doi
             WHERE is_preferred_object = true and 
             source_id = {source_id}";
        db.ExecuteSQL(sql_string);
    }


    public void DropTempPMIDTables()
    {
           using var conn = new NpgsqlConnection(_connString);
           string sql_string = @"DROP TABLE IF EXISTS nk.temp_collected_pmids;
                                 DROP TABLE IF EXISTS nk.temp_all_pmids;
                                 DROP TABLE IF EXISTS nk.temp_pmids;
                                 DROP TABLE IF EXISTS nk.pub_titles;
                                 DROP TABLE IF EXISTS nk.new_links;
                                 DROP TABLE IF EXISTS nk.new_pmids;";
           conn.Execute(sql_string);
    }
}



