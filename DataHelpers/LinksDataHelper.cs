using Dapper;
using Npgsql;
using PostgreSQLCopyHelper;
namespace MDR_Aggregator;

public class LinksDataHelper
{
    private readonly string _aggs_connString;
    private readonly ILoggingHelper _loggingHelper;

    public LinksDataHelper(string aggs_connString, ILoggingHelper loggingHelper)
    {
        _aggs_connString = aggs_connString;
        _loggingHelper = loggingHelper;
    }

    public void SetUpTempPreferencesTable(IEnumerable<Source> sources)
    {
        // Creates a table that holds the id, preference rating and name of each source.
        
        using var conn = new NpgsqlConnection(_aggs_connString);
        var sql_string = @"DROP TABLE IF EXISTS nk.temp_preferences;
                  CREATE TABLE nk.temp_preferences(
                    id int
                  , preference_rating int
                  , database_name varchar
            )";
        conn.Execute(sql_string);

        List<DataSource> ds = new List<DataSource>();
        foreach (Source s in sources)
        {
            ds.Add(new DataSource(s.id, s.preference_rating, s.database_name!));
        }
        conn.Open(); 
        CopyHelpers.prefs_helper.SaveAll(conn, ds);
        
        // Also set up the link checking tables for CTG and NTR
        
        sql_string = @"DROP TABLE IF EXISTS nk.dutch_id_checker;
                  CREATE TABLE nk.dutch_id_checker(
                    old_id varchar
                  , new_id varchar
            )";
        conn.Execute(sql_string);
        
        sql_string = @"DROP TABLE IF EXISTS nk.ctg_id_checker;
                  CREATE TABLE nk.ctg_id_checker(
                    old_id varchar
                  , new_id varchar
            )";
        conn.Execute(sql_string);
    }


    public void SetUpTempLinkCollectorTable()
    {
        // Creates a table that will hold the links between studies
        
        using var conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = @"DROP TABLE IF EXISTS nk.temp_study_links_collector;
                  CREATE TABLE nk.temp_study_links_collector(
                    source_1 int
                  , sd_sid_1 varchar
                  , sd_sid_2 varchar
                  , source_2 int) ";
        conn.Execute(sql_string);
    }


    public void SetUpTempLinkSortedTable()
    {
        // Creates a table that will hold the sorted links, with
        // the 'preferred' study version identified in each case.
        
        using var conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = @"DROP TABLE IF EXISTS nk.temp_study_links_sorted;
                  CREATE TABLE nk.temp_study_links_sorted(
                    source_id int
                  , sd_sid varchar
                  , preferred_sd_sid varchar
                  , preferred_source_id int) ";
        conn.Execute(sql_string);
    }


    public IEnumerable<StudyLink> FetchLinks(int this_source_id, string source_conn_string)
    {
        using var conn = new NpgsqlConnection(source_conn_string);
        string sql_string = $@"select {this_source_id} as source_1, 
                sd_sid as sd_sid_1, 
                identifier_value as sd_sid_2, source_id as source_2
                from ad.study_identifiers i
                where i.identifier_type_id = 11
                and ((i.source_id > 100115 and i.source_id < 100133)
                      or i.source_id = 101989 or i.source_id = 109108)
                and i.source_id <> " + this_source_id;
        return conn.Query<StudyLink>(sql_string);
        
    }

    public IEnumerable<OldNewLink> GetOldAndNewids(int this_source_id, string source_conn_string, int id_type)
    {
        using var conn = new NpgsqlConnection(source_conn_string);
        string sql_string =$@"select sd_sid as new_id, 
                identifier_value as old_id
                from ad.study_identifiers i
                where identifier_type_id = {id_type};";
        return conn.Query<OldNewLink>(sql_string);
    }

    public ulong StoreLinksInTempTable(PostgreSQLCopyHelper<StudyLink> copyHelper, IEnumerable<StudyLink> entities)
    {
        using var conn = new NpgsqlConnection(_aggs_connString);
        conn.Open();
        return copyHelper.SaveAll(conn, entities);
    }
    
    public ulong StoreLinksInCTGLInksTable(PostgreSQLCopyHelper<OldNewLink> copyHelper, 
                                           IEnumerable<OldNewLink> entities)
    {
        using var conn = new NpgsqlConnection(_aggs_connString);
        conn.Open();
        return copyHelper.SaveAll(conn, entities);
    }
    
    public ulong StoreLinksInDutchLinksTable(PostgreSQLCopyHelper<OldNewLink> copyHelper, 
                                             IEnumerable<OldNewLink> entities)
    {
        using var conn = new NpgsqlConnection(_aggs_connString);
        conn.Open();
        return copyHelper.SaveAll(conn, entities);
    }
    
    public void CleanDutchSecIds()
    {
        // Identify the Dutch ids in nk.temp_study_links_collector on the RHS
        // where secondary id details have been stored, as these are the oes that
        // need to be checked.
        using var conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = @"update nk.temp_study_links_collector 
                    set sd_sid_2 = c.new_id
                    from nk.dutch_id_checker c
                    where sd_sid_2 = c.old_id ";
        conn.Execute(sql_string);
    }
    
    public void CleanCGTSecIds()
    {
        // Identify the CTG ids in nk.temp_study_links_collector on the RHS
        // where secondary id details have been stored, as these are the oes that
        // need to be checked.
        using var conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = @"update nk.temp_study_links_collector 
                    set sd_sid_2 = c.new_id
                    from nk.ctg_id_checker c
                    where sd_sid_2 = c.old_id ";
        conn.Execute(sql_string);
    }
    

    public void TidyIds1()
    {
        using var conn = new NpgsqlConnection(_aggs_connString);
        
        string sql_string = @"DELETE from nk.temp_study_links_collector
            where sd_sid_2 ilike 'U1111%' or sd_sid_2 ilike 'UTRN%'";
        conn.Execute(sql_string);

        // replace n and other 'odd' dashes
        sql_string = @"UPDATE nk.temp_study_links_collector
                    set sd_sid_2 = replace(sd_sid_2, '–', '-');";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_study_links_collector
                set sd_sid_2 = replace(sd_sid_2, '−', '-');";
        conn.Execute(sql_string);

        
        sql_string = @"UPDATE nk.temp_study_links_collector
            SET sd_sid_2 = 'ACTRN' || sd_sid_2
            WHERE source_2 = 100116
            and length(sd_sid_2) = 14";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_study_links_collector
            SET sd_sid_2 = left(sd_sid_2, 19)
            WHERE source_2 = 100116
            and length(sd_sid_2) > 19";
        conn.Execute(sql_string);


        sql_string = @"UPDATE nk.temp_study_links_collector
            set sd_sid_2 = replace(sd_sid_2, 'Chinese Clinical Trial Register', '')
            where source_2 = 100118;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_study_links_collector
            set sd_sid_2 = 'ChiCTR-' || sd_sid_2
            where source_2 = 100118
            and sd_sid_2 not ilike 'ChiCTR-%';";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_study_links_collector
            set sd_sid_2 = replace(sd_sid_2, 'ChiCTR-ChiCTR', 'ChiCTR-')
            where source_2 = 100118;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_study_links_collector
            set sd_sid_2 = replace(sd_sid_2, 'ChiCTR-CTR', 'ChiCTR-')
            where source_2 = 100118;";
        conn.Execute(sql_string);
    }

    public void TidyIds2()
    {
        using var conn = new NpgsqlConnection(_aggs_connString);
        
        string sql_string = @"UPDATE nk.temp_study_links_collector
            SET sd_sid_2 = replace(sd_sid_2, ' ', '')
            WHERE source_2 = 100120";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_study_links_collector
            SET sd_sid_2 = replace(sd_sid_2, '-', '')
            WHERE source_2 = 100120";
        conn.Execute(sql_string);

        sql_string = @"DELETE FROM nk.temp_study_links_collector
            WHERE source_2 = 100120
            and (sd_sid_2 = 'NCT00000000' or sd_sid_2 = 'NCT99999999' or 
            sd_sid_2 = 'NCT12345678' or sd_sid_2 = 'NCT87654321' or 
            lower(sd_sid_2) = 'na' or lower(sd_sid_2) = 'n/a'
            or lower(sd_sid_2) = 'nilknown' or lower(sd_sid_2) = 'noavailable');";
        conn.Execute(sql_string);
        
        sql_string = @"insert into nk.temp_study_links_collector
                (source_1, sd_sid_1, source_2, sd_sid_2)
                SELECT source_1, sd_sid_1, source_2, 
                UNNEST(STRING_TO_ARRAY(sd_sid_2, ',')) AS sd_sid_2
                FROM nk.temp_study_links_collector
                WHERE length(trim(sd_sid_2)) = 23 AND sd_sid_2 like '%,%'";
        conn.Execute(sql_string);

        sql_string = @"DELETE FROM nk.temp_study_links_collector
                WHERE source_2 = 100120
                and length (trim(sd_sid_2)) <> 11;";
        conn.Execute(sql_string);
        

        sql_string = @"UPDATE nk.temp_study_links_collector
            SET sd_sid_2 = replace(sd_sid_2, '/', '-')
            WHERE source_2 = 100121";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_study_links_collector
            SET sd_sid_2 = replace(sd_sid_2, 'REF-', 'CTRI-')
            WHERE source_2 = 100121;";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_study_links_collector
            SET sd_sid_2 = replace(sd_sid_2, 'REFCTRI', 'CTRI')
            WHERE source_2 = 100121;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_study_links_collector
                           SET sd_sid_2 = 'CTRI-' || sd_sid_2
            WHERE source_2 = 100121
            and length(sd_sid_2) = 14;";
        conn.Execute(sql_string);
        

        sql_string = @"DELETE from nk.temp_study_links_collector
            WHERE source_2 = 100123
            and(sd_sid_2 ilike 'no%' or sd_sid_2 ilike 'na%'
            or sd_sid_2 ilike 'n/%' or sd_sid_2 ilike 'ni%');";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_study_links_collector
            set sd_sid_2 = upper(sd_sid_2)
            WHERE source_2 = 100123;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_study_links_collector
            set sd_sid_2 = replace(sd_sid_2, ' ', '')
            WHERE source_2 = 100123;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_study_links_collector
            set sd_sid_2 = replace(sd_sid_2, 'NO.', '')
            WHERE source_2 = 100123;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_study_links_collector
            set sd_sid_2 = replace(sd_sid_2, 'N°', '')
            WHERE source_2 = 100123;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_study_links_collector
            set sd_sid_2 = replace(sd_sid_2, 'NUMBER', '')
            WHERE source_2 = 100123;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_study_links_collector
            set sd_sid_2 = replace(sd_sid_2, ':', '')
            WHERE source_2 = 100123;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_study_links_collector
            set sd_sid_2 = replace(sd_sid_2, 'EUDRACT-', 'EUDRACT')
            WHERE source_2 = 100123;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_study_links_collector
            set sd_sid_2 = replace(sd_sid_2, 'EUDRACT', '')
            WHERE source_2 = 100123;";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_study_links_collector
            set sd_sid_2 = substring(sd_sid_2, 1, 14)
            WHERE source_2 = 100123 and length(sd_sid_2) > 14;";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_study_links_collector
            set sd_sid_2 = substring(sd_sid_2, 1, 4) ||'-'|| 
                 substring(sd_sid_2, 5, 6)||'-'|| substring(sd_sid_2, 11, 2)  
            WHERE source_2 = 100123 and length(sd_sid_2) = 12;";
        conn.Execute(sql_string);
        
        sql_string = @"DELETE FROM nk.temp_study_links_collector
            WHERE source_2 = 100123 and length(sd_sid_2) <> 14;";
        conn.Execute(sql_string);
    }

    public void TidyIds3()
    {
        using var conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = @"UPDATE nk.temp_study_links_collector
            set sd_sid_2 = replace(sd_sid_2, 'ID ', '')
            WHERE source_2 = 100124;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_study_links_collector
            set sd_sid_2 = replace(sd_sid_2, ' ', '')
            WHERE source_2 = 100124;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_study_links_collector
            set sd_sid_2 = 'DRKS' || sd_sid_2
            WHERE source_2 = 100124
            and length(sd_sid_2) = 8;";
        conn.Execute(sql_string);
        
        sql_string = @"DELETE FROM nk.temp_study_links_collector
            WHERE source_2 = 100124
            and length(sd_sid_2) < 12;";
        conn.Execute(sql_string);

        
        sql_string = @"DELETE FROM nk.temp_study_links_collector
                WHERE source_2 = 100126
                and sd_sid_2 ilike 'nil%'";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_study_links_collector
            SET sd_sid_2 = replace(sd_sid_2, ' ', '')
            WHERE source_2 = 100126;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_study_links_collector
            SET sd_sid_2 = replace(sd_sid_2, '-', '')
            WHERE source_2 = 100126;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_study_links_collector
            SET sd_sid_2 = 'ISRCTN' || sd_sid_2
            WHERE source_2 = 100126
            and length(sd_sid_2) = 8;";
        conn.Execute(sql_string);

        sql_string = @"DELETE FROM nk.temp_study_links_collector
            WHERE source_2 = 100126
            and sd_sid_2 = 'ISRCTN' or sd_sid_2 = 'ISRCTN00000000'
            or sd_sid_2 = 'ISRCTN99999999'";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_study_links_collector
            SET sd_sid_2 = replace(sd_sid_2, 'ISCRTN', 'ISRCTN')
            WHERE source_2 = 100126";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_study_links_collector
            SET sd_sid_2 = 'I' || sd_sid_2
            WHERE source_2 = 100126 and sd_sid_2 like 'SRCTN%'";
        conn.Execute(sql_string);
        
        sql_string = @"DELETE FROM nk.temp_study_links_collector
            WHERE source_2 = 100126
            and length(sd_sid_2) < 12 ;";
        conn.Execute(sql_string);
        

        sql_string = @"UPDATE nk.temp_study_links_collector
            set sd_sid_2 = 'TCTR' || sd_sid_2
            WHERE source_2 = 100131
            and length(sd_sid_2) = 11;";
        conn.Execute(sql_string);

        
        sql_string = @"UPDATE nk.temp_study_links_collector
            set sd_sid_2 = replace(sd_sid_2, '-', '')
            WHERE source_2 = 100132;";
        conn.Execute(sql_string);
        
        sql_string = @"UPDATE nk.temp_study_links_collector
            SET sd_sid_2 = left(sd_sid_2, 7)
            where source_2 = 100132
            and length(sd_sid_2) > 7";
        conn.Execute(sql_string);
        
        sql_string = @"DELETE FROM nk.temp_study_links_collector
            where sd_sid_2 = ''";
        conn.Execute(sql_string);
    }


    public void TransferLinksToSortedTable()
    {
        // Transfers the collected links so that the 'preferred side' is consistently located.
        // A lower preference rating means 'more preferred' - i.e. should be used first when aggregating data.
        // Lower rated source data should therefore be in the 'preferred' fields on the right of the table,
        // and higher rated data should be on the left hand side.

        // In the case below the original link data matches what is required.
        
        using var conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = @"INSERT INTO nk.temp_study_links_sorted(
                      source_id, sd_sid, preferred_sd_sid, preferred_source_id) 
                      SELECT t.source_1, t.sd_sid_1, t.sd_sid_2, t.source_2
                      FROM nk.temp_study_links_collector t
                      inner join nk.temp_preferences r1
                      on t.source_1 = r1.id
                      inner join nk.temp_preferences r2
                      on t.source_2 = r2.id
                      WHERE r1.preference_rating > r2.preference_rating";

        int res1 = conn.Execute(sql_string);

        // In the case below the relative positions need to be reversed.

        sql_string = @"INSERT INTO nk.temp_study_links_sorted(
                      source_id, sd_sid, preferred_sd_sid, preferred_source_id) 
                      SELECT t.source_2, t.sd_sid_2, t.sd_sid_1, t.source_1
                      FROM nk.temp_study_links_collector t
                      inner join nk.temp_preferences r1
                      on t.source_1 = r1.id
                      inner join nk.temp_preferences r2
                      on t.source_2 = r2.id
                      WHERE r1.preference_rating < r2.preference_rating";

        int res2 = conn.Execute(sql_string);
        _loggingHelper.LogLine((res1 + res2) + " total study-study links found in source data");
    }


    public void CreateDistinctSourceLinksTable()
    {
        // The nk.temp_study_links_sorted table will have many duplicates. Create a distinct version of the data.

        using var conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = @"DROP TABLE IF EXISTS nk.temp_distinct_links;
                       CREATE TABLE nk.temp_distinct_links 
                       as SELECT distinct source_id, sd_sid, 
                          preferred_sd_sid, preferred_source_id, true as valid 
                          FROM nk.temp_study_links_sorted";

        conn.Execute(sql_string);
        
        sql_string = @"SELECT COUNT(*) FROM nk.temp_distinct_links";
        int res =  conn.ExecuteScalar<int>(sql_string);
        _loggingHelper.LogLine(res + " distinct study-study links found");

        sql_string = @"DROP TABLE IF EXISTS nk.temp_study_links_collector;
                DROP TABLE IF EXISTS nk.temp_study_links_sorted;";
        conn.Execute(sql_string);
    }
    
    
    public void ObtainStudyIds(int source_id, string source_conn_string, PostgreSQLCopyHelper<IdChecker> copyHelper)
    {
        // Get a list of study ids from the source, so secondary ids can be checked against it.
        
        using var agg_conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = @"DROP TABLE IF EXISTS nk.temp_id_checker;
                    CREATE TABLE nk.temp_id_checker (sd_sid VARCHAR) ";
        agg_conn.Execute(sql_string);

        using var srce_conn = new NpgsqlConnection(source_conn_string);
        sql_string = @"select sd_sid from ad.studies";
        IEnumerable<IdChecker> Ids = srce_conn.Query<IdChecker>(sql_string);
        
        agg_conn.Open();
        copyHelper.SaveAll(agg_conn, Ids);

        sql_string = @"SELECT COUNT(*) FROM nk.temp_id_checker";
        int res = agg_conn.ExecuteScalar<int>(sql_string);
        _loggingHelper.LogLine($"Source {source_id}: {res} sd_sids found");
    }

    public void CheckIdsAgainstSourceStudyIds(int source_id)
    {
        // Carry out the check on secondary ids and mark then as valid or invalid.
        
        using var conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = $@"UPDATE nk.temp_distinct_links t
               SET valid = false 
                   FROM 
                      (SELECT k.sd_sid as sd_sid
                       FROM  nk.temp_distinct_links k
                       LEFT JOIN nk.temp_id_checker s
                       ON k.sd_sid = s.sd_sid
					   WHERE k.source_id = {source_id}
					   AND s.sd_sid is null) invalids
                   where t.sd_sid = invalids.sd_sid";

        int res1 = conn.Execute(sql_string);
        _loggingHelper.LogLine($"\t\t{res1} set to invalid on sd_sid");

        sql_string = $@"UPDATE nk.temp_distinct_links t
                 SET valid = false 
                     FROM
                        (SELECT k.preferred_sd_sid 
                         FROM  nk.temp_distinct_links k
                         LEFT JOIN nk.temp_id_checker s
                         ON k.preferred_sd_sid = s.sd_sid
					     WHERE k.preferred_source_id = {source_id}
					     AND s.sd_sid is null) invalids
                    where t.preferred_sd_sid = invalids.preferred_sd_sid";

        int res2 = conn.Execute(sql_string);
        _loggingHelper.LogLine($"\t\t{res2} set to invalid on preferred_sd_sid");
    }    

    public void DeleteInvalidLinks()
    {
        // Delete the invalid secondary ids from the table.
        
        using var conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = @"DELETE 
                FROM nk.temp_distinct_links
                WHERE valid = false";
        int res = conn.Execute(sql_string);
        _loggingHelper.LogLine($"{res} study-study links deleted because of an invalid secondary id");

        sql_string = @"DROP TABLE IF EXISTS nk.temp_id_checker;";
        conn.Execute(sql_string);

        sql_string = @"ALTER TABLE nk.temp_distinct_links DROP COLUMN valid;";
        conn.Execute(sql_string);

        sql_string = @"SELECT COUNT(*) FROM nk.temp_distinct_links";
        res = conn.ExecuteScalar<int>(sql_string);
        _loggingHelper.LogLine($"{res} distinct study-study links remaining");
    }
        
        
    /*
    // One set of relationships are not 'the same study in a different registry'
    // but one-to-many relationships, to multiple studies in a different registry.
    // Such links represent 'study relationship' rather than being straight
    // equivalents. There can be multiple studies in the 'preferred' registry
    // or the 'less preferred' registry. In either case each group is equivalent to
    // a registry entry elsewhere that represents a single study, or sometimes a 
    // single project / programme, or grant.

    public void IdentifyGroupedStudies()
    {
        // Set up a table to hold group definitions (i.e. the list of studies in each
        // group), using both the LHS and RHS of the distinct links table.

        using var conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = @"DROP TABLE IF EXISTS nk.temp_grouping_studies;
                CREATE TABLE nk.temp_grouping_studies 
                (  
                    source_id   INT,
                    sd_sid      VARCHAR,
                    matching_source_id  INT,
                    side        VARCHAR
                );";
        conn.Execute(sql_string);

        // Studies of interest have more than one matching study within the SAME matching
        // source registry. Therefore group on one side, against the source_id of the other.
        
        // Studies with a 'preferred' entry that matches multiple 'less preferred' entries

        sql_string = @"INSERT INTO nk.temp_grouping_studies
                         (source_id, sd_sid, matching_source_id, side)
                         SELECT source_id, sd_sid, preferred_source_id, 'L'
                         FROM nk.temp_distinct_links 
                         group by source_id, sd_sid, preferred_source_id
                         HAVING count(sd_sid) > 1;";
        conn.Execute(sql_string);

        // Studies with a 'less preferred' entry that matches multiple 'preferred' entries
        
        sql_string = @"INSERT INTO nk.temp_grouping_studies
                         (source_id, sd_sid, matching_source_id, side)
                         SELECT preferred_source_id, preferred_sd_sid, 
                         source_id, 'R'
                         FROM nk.temp_distinct_links 
                         group by preferred_source_id, preferred_sd_sid, source_id
                         HAVING count(preferred_sd_sid) > 1;";
        conn.Execute(sql_string);
    }


    public void ExtractGroupedStudies()
    {
        using var conn = new NpgsqlConnection(_aggs_connString);
        
        // Create a table that takes the rows from the linked studies table that match
        // the grouped studies, as identified above. Needs to be done in two passes.

        // The source_id side is the group and the preferred side includes the grouped studies.

        string sql_string = @"DROP TABLE IF EXISTS nk.temp_linked_studies;
            create table nk.temp_linked_studies as
            select k.* from
            nk.temp_distinct_links k 
            inner join nk.temp_grouping_studies g
            on k.source_id = g.source_id
            and k.sd_sid = g.sd_sid
            and k.preferred_source_id = g.matching_source_id
            where g.side = 'L';";

        conn.Execute(sql_string);

        // Below the non-preferred studies are the group, so the data has to be switched around.

        sql_string = @"INSERT into nk.temp_linked_studies
            (source_id, sd_sid, preferred_source_id, preferred_sd_sid)
            select k.preferred_source_id, k.preferred_sd_sid, k.source_id, k.sd_sid from
            nk.temp_distinct_links k
            inner join nk.temp_grouping_studies g
            on k.preferred_source_id = g.source_id
            and k.preferred_sd_sid = g.sd_sid
            and k.source_id = g.matching_source_id
            where g.side = 'R'; ";

        conn.Execute(sql_string);

        // Put the grouping data into a permanent linked_study_groups table.
        
        // The possible study relationships are 
        // 25	Includes target as one of a group of non-registered studies 
        // This study includes<the target study>.That study is not registered independently, 
        // but instead shares this registry entry with one or more other non-registered studies.
        // 26	Non registered but included within a registered study group 
        // This study is registered as <the target study>, along with one or more other studies
        // that share the same registry entry and id.
        // 28	Includes target as one of a group of registered studies 
        // This study includes <the target study>, which is registered elsewhere along with one 
        // or more other registered studies, forming a group that collectively equates to this study.
        // 29	Registered and is included elsewhere in group 
        // This study is also registered, along with one or more other studies that together form an
        // equivalent group, as <the target study>.
        // Non registered studies only occur at the moment in BioLINCC (101900) or Yoda (101901)
        
        // N.B. nk.linked_study_groups will have been re-created at the beginning of the Aggregation process.
        
        sql_string = @"INSERT INTO nk.linked_study_groups 
                         (source_id, sd_sid, relationship_id, 
                         target_sd_sid, target_source_id)
                         select distinct source_id, sd_sid, 
                         case when preferred_source_id = 101900 
                              or preferred_source_id = 101901 then 25 
                         else 28 end, 
                         preferred_sd_sid, preferred_source_id 
                         from nk.temp_linked_studies;";
        conn.Execute(sql_string);

        sql_string = @"INSERT INTO nk.linked_study_groups 
                         (source_id, sd_sid, relationship_id, 
                         target_sd_sid, target_source_id)
                         select distinct preferred_source_id, preferred_sd_sid, 
                         case when source_id = 101900 
                              or source_id = 101901 then 26 
                         else 29 end, 
                         sd_sid, source_id
                         from nk.temp_linked_studies;";
        conn.Execute(sql_string);
    }


    public void DeleteGroupedStudyLinkRecords()
    {
        // Now need to delete the grouped records from the links table.
        
        using var conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = @"DELETE FROM nk.temp_distinct_links k
                           USING nk.temp_grouping_studies g
                           WHERE k.source_id = g.source_id
                           and k.sd_sid = g.sd_sid
                           and k.preferred_source_id = g.matching_source_id
                           and g.side = 'L';";
        conn.Execute(sql_string);

        sql_string = @"DELETE FROM nk.temp_distinct_links k
                           USING nk.temp_grouping_studies g
                           WHERE k.preferred_source_id = g.source_id
                           and k.preferred_sd_sid = g.sd_sid
                           and k.source_id = g.matching_source_id
                           and g.side = 'R';";
        conn.Execute(sql_string);

        sql_string = @"DROP TABLE IF EXISTS nk.temp_grouping_studies;
            DROP TABLE IF EXISTS nk.temp_linked_studies";
        conn.Execute(sql_string);
    }
*/

    // A subset of inter-study linkages do not reflect 'the same study in a different registry',
    // link type, but are one-to-many linkages instead. These linkages need to be stored 
    // explicitly in the study relationships table, and must be removed from the current process,
    // which is only concerned with individual studies having multiple registry Ids.
    // The multiple studies in the 1 to n relationship often represent a linked project, programme,
    // or grant, that is registered elsewhere as a single study.

    public void ProcessGroupedStudies()
    {
        // The following steps are required to identify and remove the 1-to-many linked studies.
        
        Identify1ToNGroupedStudies();
        IdentifyNToNGroupedStudies();
        Extract1ToNGroupedStudies();
        ExtractNToNGroupedStudyData();
        DeleteGroupedStudyLinkRecords();
    }

    private void Identify1ToNGroupedStudies()
    {
        // First create a table to store the rows that involve studies with multiple matches in the
        // SAME (other) source - which is the defining characteristic of the 1-to-many studies
        
        using var conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = @"DROP TABLE IF EXISTS nk.temp_multilinked_studies;
            CREATE TABLE nk.temp_multilinked_studies(
                  group_source_id int
                , group_sd_sid varchar
                , member_sd_sid varchar
                , member_source_id int
                , source_side varchar
                , complex int DEFAULT 0
            )";

        conn.Execute(sql_string);

        // Groups can occur with either the preferred or the least preferred registry entry having multiple
        // matches. First identify and transfer when the non preferred side is the group and the preferred side
        // is comprised of the grouped studies.

        sql_string = @"INSERT into nk.temp_multilinked_studies
            (group_source_id, group_sd_sid, 
             member_source_id, member_sd_sid,
             source_side)
             SELECT k.source_id, k.sd_sid, 
             k.preferred_source_id, k.preferred_sd_sid, 'L'
             from nk.temp_distinct_links k 
             inner join 
                  (SELECT source_id, sd_sid
                   FROM nk.temp_distinct_links 
                   group by source_id, sd_sid, preferred_source_id
                   HAVING count(sd_sid) > 1) lhs_groups
             ON k.source_id = lhs_groups.source_id
             AND k.sd_sid = lhs_groups.sd_sid";

        conn.Execute(sql_string);

        // Then identify and transfer when the preferred_source_id side is the group and the
        // non-preferred side is comprised of grouped studies.
        
        sql_string = @"INSERT into nk.temp_multilinked_studies
            (group_source_id, group_sd_sid, 
             member_source_id, member_sd_sid,
             source_side)
             SELECT k.preferred_source_id, k.preferred_sd_sid, 
             k.source_id, k.sd_sid, 'R' from
             nk.temp_distinct_links k
             inner join 
                  (SELECT preferred_source_id, preferred_sd_sid
                   FROM nk.temp_distinct_links 
                   group by preferred_source_id, preferred_sd_sid, source_id
                   HAVING count(preferred_sd_sid) > 1) rhs_groups
            ON k.preferred_source_id = rhs_groups.preferred_source_id
            AND k.preferred_sd_sid = rhs_groups.preferred_sd_sid";

        conn.Execute(sql_string);
    }

    private void IdentifyNToNGroupedStudies()
    {
        // There are also some studies in more complex groupings, that have a 
        // One-To-Many relationship of registry Ids but with overlapping groups...
        //       A --- B
        //       A --- C
        //       D --- C
        // The actual relationship between the studies in these groupings is unclear, and they need to be 
        // identified and given a general 'is related' link (to all the other members fo the group).
        // These studies will occur in 'both sides' of the temp_multilinked_studies table, i.e. both
        // A and C will be grouping studies, and could look as though they are each grouping the other.
        //      A groups B
        //        groups C
        //      C groups A
        //        groups D 
       
        // A 'complexity' score is set for studies involved in these complex relationships. The two studies that
        // 'group each other' (the A-C and C-A links above) are initially given a score of 2. The first SQL
        // statement identifies them by selecting any study that appears as both a grouping and a member study,
        // but as a mix of 'L' and 'R' sourced entries. (In a more normal cascade of linked study groups,
        // studies can also appear as groupers and members, but will always have a consistent 'direction', 
        // i.e. will all either be 'L' or 'R'). The second SQL statement then gives the other 'members'
        // associated with those studies (with both considered as grouping studies) a score of 4, with the original
        // studies adding this to their original 2. Thus, using the example above, A and c end up with a score
        // of 6, B and D with a score of 4.
    
        using var conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = @"Update nk.temp_multilinked_studies ks1
            SET complex = ks1.complex + 2
            FROM nk.temp_multilinked_studies ks2
            WHERE ks1.member_source_id = ks2.group_source_id
            AND ks1.member_sd_sid = ks2.group_sd_sid
            AND ks1.source_side <> ks2.source_side";

        conn.Execute(sql_string);

        sql_string = @"Update nk.temp_multilinked_studies ks1
            SET complex = ks1.complex + 4
            FROM nk.temp_multilinked_studies ks2
            WHERE ks1.group_source_id = ks2.group_source_id
            AND ks1.group_sd_sid = ks2.group_sd_sid
            AND ks2.complex = 2";

        conn.Execute(sql_string);
    }


    private void Extract1ToNGroupedStudies()
    {
        // Entries that are not 'complex' can be placed into the permanent linked_study_groups table.
        // The 4 possible study relationships are:
        // 25	Includes target as one of a group of non-registered studies 
        // This study includes<the target study>.That study is not registered independently, 
        // but instead shares this registry entry with one or more other non-registered studies.
        // 26	Non registered but included within a registered study group 
        // This study is registered as <the target study>, along with one or more other studies
        // that share the same registry entry and id.
        // 28	Includes target as one of a group of registered studies 
        // This study includes <the target study>, which is registered elsewhere along with one 
        // or more other registered studies, forming a group that collectively equates to this study.
        // 29	Registered and is included elsewhere in group 
        // This study is also registered, along with one or more other studies that together form an
        // equivalent group, as <the target study>.
        
        // N.B. Non registered studies only occur at the moment in BioLINCC (101900) or Yoda (101901)
        
        using var conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = @"INSERT INTO nk.linked_study_groups
                         (source_id, sd_sid, relationship_id,
                         target_sd_sid, target_source_id)
                         select group_source_id, group_sd_sid,
                         case when group_source_id = 101900
                              or group_source_id = 101901 then 25
                         else 28 end, 
                         member_sd_sid, member_source_id
                         from nk.temp_multilinked_studies
                         where complex = 0;";

        int res1 = conn.Execute(sql_string);
        _loggingHelper.LogLine($"{res1} relationship records added as part of 1 to n study relationships");

        sql_string = @"INSERT INTO nk.linked_study_groups
                         (source_id, sd_sid, relationship_id,
                         target_sd_sid, target_source_id)
                         select member_source_id, member_sd_sid,
                         case when member_source_id = 101900
                              or member_source_id = 101901 then 26
                         else 29 end, 
                         group_sd_sid, group_source_id
                         from nk.temp_multilinked_studies
                         where complex = 0;";

        int res2 = conn.Execute(sql_string);
        _loggingHelper.LogLine($"{res2} relationship records added as part of n to 1 study relationships");
    }


        private void ExtractNToNGroupedStudyData()
        {
            // The 'complex' cases still need to be dealt with. If the individual studies
            // involved in each of these complex groups can be identified, they can all be linked 
            // to each of the others in the group.
            
            // The strategy is to examine each pair of the linked 'complex' studies in turn. 
            // Each pair is obviously in the same group (beginning with the first). If a succeeding 
            // pair has either member already in a group, both studies will be in the same group.
            // If neither is in a group, they begin as the nucleus of a new group. There will be many 
            // repeated allocations but these can be resolved by a 'select distinct' at the end.
            
            // Create a List of entities to hold the study's source id, sd sid and group number. Set the 
            // 'next_group_number' to start at 1 and increment it as necessary.

            List<ComplexStudy> complex_studies = new();
            using var conn = new NpgsqlConnection(_aggs_connString);
            string sql_string = @"select group_source_id, group_sd_sid, 
                                  member_source_id, member_sd_sid 
                                  from nk.temp_multilinked_studies 
                                  where complex > 0";
            List<ComplexStudyRow> complex_rows = conn.Query<ComplexStudyRow>(sql_string).ToList();
            if (complex_rows.Any())
            {
                int next_group_num = 1;
                foreach (var r in complex_rows)
                {
                    if (complex_studies.Count == 0)    // first record
                    {
                        complex_studies.Add( new ComplexStudy(1, r.group_source_id, r.group_sd_sid));
                        complex_studies.Add( new ComplexStudy(1, r.member_source_id, r.member_sd_sid));
                    }
                    else
                    {
                        // is the grouping study already listed in a group?
                        // Check the grouping study first, then the member study.
                        
                        int group_num = 0;
                        foreach (var cs in complex_studies)
                        {
                            if (r.group_source_id == cs.src_id && r.group_sd_sid == cs.sid_id)
                            {
                                group_num = cs.group_number;
                                break;
                            }
                        }
                        if (group_num == 0)
                        {
                            foreach (var cs in complex_studies)
                            {
                                if (r.member_source_id == cs.src_id && r.member_sd_sid == cs.sid_id)
                                {
                                    group_num = cs.group_number;
                                    break;
                                }
                            }
                        }
                        if (group_num == 0)
                        {
                            // not found - create a new group
                            next_group_num++;
                            group_num = next_group_num;
                        }
                        complex_studies.Add( new ComplexStudy(group_num, r.group_source_id, r.group_sd_sid));
                        complex_studies.Add( new ComplexStudy(group_num, r.member_source_id, r.member_sd_sid));
                    }
                }
                
                // Almost certainly lots of duplicates so use Linq to select distinct
                // and obtain maximum group number.
                
                List<ComplexStudy> complex_studies2 = complex_studies.Select(i => i)
                                                    .Distinct().OrderBy(i => i.group_number)
                                                    .ToList();
                
                // Each group can now be identified and treated separately 
                List<ComplexLink> links = new();
                for (int g = 1; g <= next_group_num; g++)
                {
                    List<ComplexStudy> study_group =
                        complex_studies2.Select(i => i).Where(i => i.group_number == g).ToList();
                    
                    if (study_group.Any())
                    {
                        // Get first study and add an object to List of links where target iterates over
                        // the rest of the studies in the list - add the reverse relationship also. Repeat
                        // until the penultimate study - which has just one pairing, with the last study.

                        int studynum = study_group.Count;
                        for (int i = 0; i < studynum - 1; i++)
                        {
                            ComplexStudy s = study_group[i];
                            for (int j = i + 1; j < studynum; j++)
                            {
                                ComplexStudy t = study_group[j];
                                links.Add(new ComplexLink(s.src_id, s.sid_id, 30, t.src_id, t.sid_id));
                                links.Add(new ComplexLink(t.src_id, t.sid_id, 30, s.src_id, s.sid_id));
                            }
                        }
                        
                    }
                }

                if (links.Count > 0)
                {
                    CopyHelpers.complex_links_helper.SaveAll(conn, links);
                    string feedback = $"{links.Count} relationship records added as part of n to n study relationships";
                    _loggingHelper.LogLine(feedback);
                }
            }
        }


        private void DeleteGroupedStudyLinkRecords()
        {
            // The grouped records then need to be deleted from the links table...
            
            using var conn = new NpgsqlConnection(_aggs_connString);
            string sql_string = @"DELETE FROM nk.temp_distinct_links k
                               USING nk.temp_multilinked_studies g
                               WHERE k.source_id = g.group_source_id
                               and k.sd_sid = g.group_sd_sid
                               and k.preferred_source_id = g.member_source_id
                               and g.source_side = 'L';";
            int res1 = conn.Execute(sql_string);

            sql_string = @"DELETE FROM nk.temp_distinct_links k
                               USING nk.temp_multilinked_studies g
                               WHERE k.preferred_source_id = g.group_source_id
                               and k.preferred_sd_sid = g.group_sd_sid
                               and k.source_id = g.member_source_id
                               and g.source_side = 'R';";
            int res2 = conn.Execute(sql_string);
            _loggingHelper.LogLine($"{res1 + res2} study-study links extracted as grouped (1 to n, n to n) records");

            sql_string = @"DROP TABLE IF EXISTS nk.temp_multilinked_studies;
                DROP TABLE IF EXISTS nk.temp_complex_multi_links;
                DROP TABLE IF EXISTS nk.temp_multi_links;
                DROP TABLE IF EXISTS nk.temp_group_list";
            conn.Execute(sql_string);

            sql_string = @"SELECT COUNT(*) FROM nk.temp_distinct_links";
            int res = conn.ExecuteScalar<int>(sql_string);
            _loggingHelper.LogLine(res.ToString() + " distinct study-study links remaining");
        }

        /*
    public void ManageIncompleteLinks()
    {
        // if there are only 2 studies linked to each other their management is straightforward. If there
        // are three or more, however (i.e. a study was registered 3 or more times) it is necessary to 
        // ensure that all the 'lass preferred' registry entries match to the same preferred entry.
        // This is done by the CascadeLinks function below, but before that can operate safely on the data
        // an additional problem needs to be solved.
        
        // This is that some links may be missing, in the sense that Study A is listed as being the
        // same as more preferred studies B and C, but no link exists between Study B to C, or Study C to B.
        // The 'link path' is therefore broken (so the cascade function will not work) and the B to C link
        // needs to be added. Otherwise a study will retain two, occasionally more, 'preferred studies',
        // which does not make sense in the system.
        
        // The requirement is to create the missing links and then add them to the distinct_links table.
        // To create those links, studies B and C need to be linked in the correct preference relationship.
        // That is done by linking both to A in the correct pattern, in a table that holds (firstly) A and B,
        // and then A and B and C, as described below. The B-C linked pairs are then extracted as new records.
        
        // First identify the studies that have more than one 'preferred' option cutting across DIFFERENT
        // sources. Groups have already been removed, so this should find only those with a 'missing link'.
        
        using var conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = @"DROP TABLE IF EXISTS nk.temp_studies_with_multiple_links;
                   CREATE TABLE nk.temp_studies_with_multiple_links
                   as SELECT source_id, sd_sid
                   from nk.temp_distinct_links
                   group by source_id, sd_sid
                   having count(distinct preferred_source_id) > 1;";
        conn.Execute(sql_string);

        // Then create a table with details of these 'missing link' records. It has all 4 fields
        // from the nk.temp_distinct_links table, i.e. it includes the 2 or more preferred entry data
        // rows for each identified 'problem' study, plus the preference ratings for each source.
        
        sql_string = @"DROP TABLE IF EXISTS nk.temp_missing_links;
                CREATE TABLE nk.temp_missing_links as
                select k.source_id, r1.preference_rating as source_rating, 
                k.sd_sid, k.preferred_source_id, r2.preference_rating, k.preferred_sd_sid 
                from nk.temp_distinct_links k
                inner join nk.temp_studies_with_multiple_links m
                on k.source_id = m.source_id
                and k.sd_sid = m.sd_sid
                inner join nk.temp_preferences r1
                on k.source_id = r1.id
                inner join nk.temp_preferences r2
                on k.preferred_source_id = r2.id
                order by k.source_id, k.sd_sid, preferred_source_id;";
        conn.Execute(sql_string);

        // Create a further temp table with 6 fields - two initially populated with the source id / sd_sid
        // that identifies the 'problem' record (A) and the next pair with have the source / sd_sid pair
        // from B and C that is NOT the most preferred. (If there are more than 3 matching entries all 
        // those that are not the most preferred are included, as separate rows. The table therefore has A-B
        // links but C is to be added.
        
        sql_string = @"DROP TABLE IF EXISTS nk.temp_new_links;
            CREATE TABLE nk.temp_new_links as
            select m.source_id, m.sd_sid, m.preferred_source_id as new_source_id, 
            m.preferred_sd_sid as new_sd_sid, 0 as new_preferred_source, '' as new_preferred_sd_sid from
            nk.temp_missing_links m
            inner join
                (select source_id, sd_sid, min(preference_rating) as min_rating
                 from nk.temp_missing_links
                 group by source_id, sd_sid) mins
            on m.source_id = mins.source_id
            and m.sd_sid = mins.sd_sid
            and m.preference_rating <> mins.min_rating
            order by source_id, sd_sid;";
        conn.Execute(sql_string);

        // Update the last pair of fields (i.e. C) in the temp_new_links table with the source / sd_sid 
        // that represents the study with the most 'preferred' rating, with the smallest preference value.
        // The table is now in the form A-B-C.

        sql_string = @"UPDATE nk.temp_new_links k
                  SET new_preferred_source = min_set.preferred_source_id
                  , new_preferred_sd_sid = min_set.preferred_sd_sid
                  FROM
                      (select m.* from
                      nk.temp_missing_links m
                      INNER JOIN 
                            (select source_id, sd_sid, min(preference_rating) as min_rating
                            from nk.temp_missing_links
                            group by source_id, sd_sid) mins
                      on m.source_id = mins.source_id
                      and m.sd_sid = mins.sd_sid
                      and m.preference_rating = mins.min_rating) min_set
                  WHERE k.source_id = min_set.source_id
                  AND k.sd_sid = min_set.sd_sid;";
        conn.Execute(sql_string);

        // Insert the new B-C links into the distinct_links table. These links will need re-processing
        // through the CascadeLinksTable() function. Then drop the temp tables.

        sql_string = @"INSERT INTO nk.temp_distinct_links
                 (source_id, sd_sid, preferred_sd_sid, preferred_source_id)
                 SELECT new_source_id, new_sd_sid, new_preferred_sd_sid, new_preferred_source from
                 nk.temp_new_links;";
        conn.Execute(sql_string);

        sql_string = @"DROP TABLE IF EXISTS nk.temp_missing_links;
            DROP TABLE IF EXISTS nk.temp_new_links;";
        conn.Execute(sql_string);
    }


    public void CascadeLinksInDistinctLinksTable()
    {
        using var conn = new NpgsqlConnection(_aggs_connString);
        
        // Telescope the preferred links to the most preferred
        // i.e. A -> B, B -> C becomes A -> C, B -> C.
        // Do this as long as there remains links to be telescoped
        // (a few have to be done twice)

        string sql_string;
        int match_number = 500;  // arbitrary start number
        while (match_number > 0)
        {
            // get match number as number of link records where the rhs sd_sid
            // appears elsewhere on the left...

            sql_string = @"SELECT count(*) 
                      FROM nk.temp_distinct_links t1
                      inner join nk.temp_distinct_links t2
                      on t1.preferred_source_id = t2.source_id
                      and t1.preferred_sd_sid = t2.sd_sid";

            match_number = conn.ExecuteScalar<int>(sql_string);

            if (match_number > 0)
            {
                // do the update

                sql_string = @"UPDATE nk.temp_distinct_links t1
                      SET preferred_source_id = t2.preferred_source_id,
                      preferred_sd_sid = t2.preferred_sd_sid
                      FROM nk.temp_distinct_links t2
                      WHERE t1.preferred_source_id = t2.source_id
                      AND t1.preferred_sd_sid = t2.sd_sid";

                conn.Execute(sql_string);
            }
        }

        // but in some cases the telescoped link may have already been 
        // present - i.e. a study has two other reg identifiers
        // one of which will be the most preferred
        // Process above will result in duplicates in these cases
        // and these duplicates therefore need to be removed.

        sql_string = @"DROP TABLE IF EXISTS nk.temp_distinct_links2;
                       CREATE TABLE nk.temp_distinct_links2 
                       as SELECT distinct * FROM nk.temp_distinct_links";

        conn.Execute(sql_string);

        sql_string = @"DROP TABLE IF EXISTS nk.temp_distinct_links;
            ALTER TABLE nk.temp_distinct_links2 RENAME TO temp_distinct_links;";

        conn.Execute(sql_string);
    }
*/
    
    
        public void AddMissingLinks()
        {
            // There are a set if links that may be missing, in the sense that
            // Study A is listed as being the same as Study B and Study C, but no
            // link exists beteween either Study B to C, or Study C to B.
            // The 'link path' is therefore broken and the B to C link needs to be added.
            // These studies have two, occasionally more, 'preferred studies', which
            // does not make sense in the system.

            // First create a table with these 'missing link' records

            // Working from the inside out, this query
            // a) gets the source id/sd_sids of the LHS of the study links table
            //    that has more than one 'preferred' study associated with it (dataset d)
            // b) takes those records and identifies the distinct preferred source ids
            //    that are linked to each RHS study (dataset a)
            // c) Further identifies the records that have more than one 
            //    source referenced on the RHS (so all the linked records have the 
            //    'impossible' property of having more than one preferred 
            //    source / sd_sid study record (dataset agg)
            // d) joins that dataset back to the linked records table, to 
            //    identify the source records that meet the criteria of 
            //    having a 'missing link'

            string sql_string;
            using (var conn = new NpgsqlConnection(_aggs_connString))
            {
                // First identify the studies that have more than one 'preferred' option
                // cutting across more than one source. 
                // Groups have already been removed, so this should find only those
                // with the 'missing link'.

                sql_string = @"DROP TABLE IF EXISTS nk.temp_studies_with_multiple_links;
                       CREATE TABLE nk.temp_studies_with_multiple_links
                       as SELECT source_id, sd_sid
                       from nk.temp_distinct_links
                       group by source_id, sd_sid
                       having count(distinct preferred_source_id) > 1;";
                conn.Execute(sql_string);

                sql_string = @"DROP TABLE IF EXISTS nk.temp_missing_links;
                    CREATE TABLE nk.temp_missing_links as
                    select k.source_id, r1.preference_rating as source_rating, 
                    k.sd_sid, k.preferred_source_id, r2.preference_rating, k.preferred_sd_sid 
                    from nk.temp_distinct_links k
                    inner join nk.temp_studies_with_multiple_links m
                    on k.source_id = m.source_id
                    and k.sd_sid = m.sd_sid
                    inner join nk.temp_preferences r1
                    on k.source_id = r1.id
                    inner join nk.temp_preferences r2
                    on k.preferred_source_id = r2.id
                    order by k.source_id, k.sd_sid, preferred_source_id;";
                conn.Execute(sql_string);

                // Create a further temp table that will hold the links between studies B and C,
                // which are currently both 'preferred' studies (both on the RHS of the table)
                // for any particular source id / sd_sid study.

                // This table has 6 fields - two initially populated with the source id / sd_sid, 
                // to identify the record, and the next pair that have the source / sd_sid pair
                // that does NOT have the minimum source rating, i.e. is the study that will need 
                // to be 'existing studies' in the new link

                sql_string = @"DROP TABLE IF EXISTS nk.temp_new_links;
                CREATE TABLE nk.temp_new_links as
                select m.source_id, m.sd_sid, m.preferred_source_id as new_source_id, 
                m.preferred_sd_sid as new_sd_sid, 0 as new_preferred_source, '' as new_preferred_sd_sid from
                nk.temp_missing_links m
                inner join
                    (select source_id, sd_sid, min(preference_rating) as min_rating
                     from nk.temp_missing_links
                     group by source_id, sd_sid) mins
                on m.source_id = mins.source_id
                and m.sd_sid = mins.sd_sid
                and m.preference_rating <> mins.min_rating
                order by source_id, sd_sid;";
                conn.Execute(sql_string);

                // Update the last pair of tables in ther temp_new_links table with the source / sd_sid 
                // that represents the study with the minimally rated source id, i.e. the 'correct' preferred option

                sql_string = @"UPDATE nk.temp_new_links k
                      SET new_preferred_source = min_set.preferred_source_id
                      , new_preferred_sd_sid = min_set.preferred_sd_sid
                      FROM
                          (select m.* from
                          nk.temp_missing_links m
                          INNER JOIN 
                                (select source_id, sd_sid, min(preference_rating) as min_rating
                                from nk.temp_missing_links
                                group by source_id, sd_sid) mins
                          on m.source_id = mins.source_id
                          and m.sd_sid = mins.sd_sid
                          and m.preference_rating = mins.min_rating) min_set
                      WHERE k.source_id = min_set.source_id
                      AND k.sd_sid = min_set.sd_sid;";
                conn.Execute(sql_string);

                // Insert the distinct versions of the new links into the distinct_links table.
                // may be some duplicates because of...
                // These links will need re-processing through the CascadeLinksTable() function.

                sql_string = @"INSERT INTO nk.temp_distinct_links
                     (source_id, sd_sid, preferred_sd_sid, preferred_source_id)
                     SELECT distinct new_source_id, new_sd_sid, new_preferred_sd_sid, new_preferred_source from
                     nk.temp_new_links;";
                int res = conn.Execute(sql_string);
                _loggingHelper.LogLine(res.ToString() + " new study-study links added to complete linkage chains");

                // drop the temp tables 
                sql_string = @"DROP TABLE IF EXISTS nk.temp_missing_links;
                DROP TABLE IF EXISTS nk.temp_studies_with_multiple_links;
                DROP TABLE IF EXISTS nk.temp_new_links;";
                conn.Execute(sql_string);
            }

            MakeLinksDistinct();
        }


        public void CascadeLinks()
        {
            using (var conn = new NpgsqlConnection(_aggs_connString))
            {
                // telescope the preferred links to the most preferred
                // i.e. A -> B, B -> C becomes A -> C, B -> C
                // do as long as there remains links to be telescoped
                // (a few have to be done twice)

                string sql_string;
                int match_number = 500;  // arbitrary start number
                while (match_number > 0)
                {
                    // get match number as number of link records where the rhs sd_sid
                    // appears elsewhere on the left...

                      sql_string = @"SELECT count(*) 
                          FROM nk.temp_distinct_links t1
                          inner join nk.temp_distinct_links t2
                          on t1.preferred_source_id = t2.source_id
                          and t1.preferred_sd_sid = t2.sd_sid";

                    match_number = conn.ExecuteScalar<int>(sql_string);
                    _loggingHelper.LogLine(match_number.ToString() + " cascading study-study links found, to 'telescope'");


                    if (match_number > 0)
                    {
                        // do the update

                        sql_string = @"UPDATE nk.temp_distinct_links t1
                          SET preferred_source_id = t2.preferred_source_id,
                          preferred_sd_sid = t2.preferred_sd_sid
                          FROM nk.temp_distinct_links t2
                          WHERE t1.preferred_source_id = t2.source_id
                          AND t1.preferred_sd_sid = t2.sd_sid";

                        conn.Execute(sql_string);
                    }
                }
            }

            MakeLinksDistinct();
        }


        public void MakeLinksDistinct()
        {
            using (var conn = new NpgsqlConnection(_aggs_connString))
            {
                string sql_string = @"SELECT COUNT(*) FROM nk.temp_distinct_links";
                int res1 = conn.ExecuteScalar<int>(sql_string);
                
                sql_string = @"DROP TABLE IF EXISTS nk.temp_distinct_links2;
                           CREATE TABLE nk.temp_distinct_links2 
                           as SELECT distinct * FROM nk.temp_distinct_links";

                conn.Execute(sql_string);

                sql_string = @"DROP TABLE IF EXISTS nk.temp_distinct_links;
                ALTER TABLE nk.temp_distinct_links2 RENAME TO temp_distinct_links;";

                conn.Execute(sql_string);

                sql_string = @"SELECT COUNT(*) FROM nk.temp_distinct_links";
                int res2 = conn.ExecuteScalar<int>(sql_string);
                int diff = res1 - res2;
                _loggingHelper.LogLine(res2.ToString() + " records now in temp distinct links table, having dropped " + diff.ToString());
            }
        }
        
    
    
    

    public void TransferNewLinksToDataTable()
    {
        // Select into a permanent table. A distinct selection is used as a double check against duplicates.
        // N.B. nk.study_study_links will have been re-created at the beginning of the Aggregation process.

        using var conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = @"Insert into nk.study_study_links
                  (source_id, sd_sid, preferred_sd_sid, preferred_source_id)
                  select distinct source_id, sd_sid, preferred_sd_sid, preferred_source_id
                  from nk.temp_distinct_links";

        int res = conn.Execute(sql_string);
        _loggingHelper.LogLine(res.ToString() + " study-study links transfered to final table");
    }

    public void UpdateLinksWithStudyIds()
    {
        // Study Ids are updated where they already exist in the nkstudy_ids table
        // Preferred study ids first - any existing lionks plus
        // links where preferred has just been joined by a noin-preferred version

        using (var conn = new NpgsqlConnection(_aggs_connString))
        {
            string sql_string = @"UPDATE nk.study_study_links ssk
                      SET study_id = s.study_id
                      from nk.study_ids s
                      where ssk.preferred_sd_sid = s.sd_sid
                      and ssk.preferred_source_id = s.source_id;";

            int res = conn.Execute(sql_string);
            _loggingHelper.LogLine(res.ToString() + " study Ids updated using preferred Id");

            // then any non-preferred studies that have been added 
            // previously, with a new preferred side

            sql_string = @"UPDATE nk.study_study_links ssk
                      SET study_id = s.study_id
                      from nk.study_ids s
                      where ssk.sd_sid = s.sd_sid
                      and ssk.source_id = s.source_id
                      and ssk.study_id is null;";

            res = conn.Execute(sql_string);
            _loggingHelper.LogLine(res.ToString() + " study Ids updated using non-preferred Id");
        }
    }

    
    public int ObtainTotalOfNewLinks()
    {
        using var conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = @"SELECT COUNT(*) FROM nk.temp_distinct_links";
        return conn.ExecuteScalar<int>(sql_string);
    }


    public void DropTempTables()
    {
        using var conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = @"DROP TABLE IF EXISTS nk.temp_preferences;
            DROP TABLE IF EXISTS nk.temp_distinct_links;";
        conn.Execute(sql_string);
    }


    public void AddStudyStudyRelationshipRecords()
    {
        // Use the study_all_ids to insert the study Ids for the linked 
        // sources / sd_sids, using  nk.linked_study_groups as the source

        using var conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = @"Insert into st.study_relationships
                      (study_id, relationship_type_id, target_study_id)
                      select s1.study_id, g.relationship_id, s2.study_id
                      from nk.linked_study_groups g
                      inner join nk.study_ids s1
                      on g.source_id = s1.source_id
                      and g.sd_sid = s1.sd_sid
                      inner join nk.study_ids s2
                      on g.target_source_id = s2.source_id
                      and g.target_sd_sid = s2.sd_sid";

        conn.Execute(sql_string);
    }


    public void TransferTestIdentifiers(int source_id)
    {
        // This called only during testing
        // Transfers the study identifier records so that they can be used to
        // obtain links, in imitation of the normal process

        using var conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = @"truncate table ad.study_identifiers; 
                        INSERT into ad.study_identifiers (sd_sid, identifier_type_id, 
                        identifier_value, source, source_id, source_ror_id,
                        identifier_date, identifier_link)
                        SELECT sd_sid, identifier_type_id,
                        identifier_value, source, source_id, source_ror_id,
                        identifier_date, identifier_link 
                        from adcomp.study_identifiers
                        where source_id = " + source_id.ToString();
        conn.Execute(sql_string);
    }
}