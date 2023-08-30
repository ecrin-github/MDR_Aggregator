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

    public IEnumerable<OldNewLink> GetOldAndNewIds(string source_conn_string, int id_type)
    {
        using var conn = new NpgsqlConnection(source_conn_string);
        string sql_string = $@"select sd_sid as new_id, 
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
        // where secondary id details have been stored, as these are the ones that
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
        // where secondary id details have been stored, as these are the ones that
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
            set sd_sid_2 = replace(sd_sid_2, '/', '-')
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
        
        // any source
        sql_string = @"UPDATE nk.temp_study_links_collector
            SET sd_sid_2 = replace(sd_sid_2, '-', '')
            where sd_sid_2 like 'ChiCTR-%'    
            and length(sd_sid_2) = 17";
        conn.Execute(sql_string);

        
        sql_string = @"DELETE FROM nk.temp_study_links_collector
            where sd_sid_2 = ''";
        conn.Execute(sql_string);
    }


    public void AddAdditionalLinksUsingIdenticalSponsorIds()
    {
        // Using the data from the previous aggregation, this set of sql statements identifies and
        // adds studies that have the same identifier from the ame source (if at least 4 characters long)
        // but which are in the previous aggregation data as separate studies.
        
        using var conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = @"drop table if exists nk.temp_ids_of_interest;
        create table nk.temp_ids_of_interest as 
            select sids.source_id as reg_id, sids.sd_sid, si.study_id, 
        lower(si.identifier_value) as identifier, si.identifier_type_id, si.source_id, lower(si.source) as source
        from st.study_identifiers si 
        inner join nk.study_ids sids
        on si.study_id = sids.study_id
        where identifier_type_id not in (0, 11, 44, 45)
        and identifier_value is not null 
            and length(identifier_value)>= 4 ;";
        
        conn.Execute(sql_string);
        _loggingHelper.LogLine($"Table of sponsor and similar identifiers created");
        
        // The statement above creates a table with (about) 645,000 records
        // It is necessary to remove some identifiers (chiefly NIH budget IDs),
        // that are applied to groups of studies

        sql_string = @"drop table if exists nk.temp_multi_idents;
            create table nk.temp_multi_idents as 
            select source, identifier, count (study_id)
            from nk.temp_ids_of_interest
            group by source, identifier
            having count(identifier) > 2";
        conn.Execute(sql_string);
        _loggingHelper.LogLine($"Table of identifiers with count > 2 created");
        
        // This drops 13,000 or so records

        sql_string = @"delete from nk.temp_ids_of_interest t
            using nk.temp_multi_idents mi
            where t.source = mi.source
            and t.identifier = mi.identifier";
        conn.Execute(sql_string);
        _loggingHelper.LogLine($"Identifiers with count > 2 removed from identifier table");
        
        // This initially gave 7,700+ records, so about 3750 pairs
        // that were not picked up previously (or they would have had the same study id)
        // this number will go down to a very small increment once the initial
        // identification has occured, though may increase if sources (i.e. organisation names)
        // are further standardised. This is because the recognition of 'identity' requires the 
        // same source name to be present as well as the same identifier.

        sql_string = @"drop table if exists nk.temp_pairs_from_last_agg;
            create table nk.temp_pairs_from_last_agg as
            select s1.reg_id as s1, s1.sd_sid as sid1, s1.study_id as st1,
            s1.source, s1.identifier,
            s2.reg_id as s2, s2.sd_sid as sid2, s2.study_id as st2
            from nk.temp_ids_of_interest s1 
            inner join nk.temp_ids_of_interest s2
            on s1.identifier = s2.identifier
            and s1.source = s2.source
            and s1.reg_id <> s2.reg_id";
        conn.Execute(sql_string);
        _loggingHelper.LogLine($"Table of links identified by sponsor ids created");
        
        // Add the pairings found to links already found using secondary ids.

        sql_string = @"insert into nk.temp_study_links_collector(source_1, sd_sid_1, sd_sid_2, source_2)
            select s1, sid1, sid2, s2
            from nk.temp_pairs_from_last_agg;";
        int res = conn.Execute(sql_string);
        _loggingHelper.LogLine($"{res} additional link records, derived from sponsor ids, added to temporary table");

        // Tidy up.

        sql_string = @"drop table if exists nk.temp_pairs_from_last_agg;
        drop table if exists nk.temp_multi_idents;
        drop table if exists nk.temp_ids_of_interest; ";
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
        // The nk.temp_study_links_sorted table will have many duplicates.
        // Create a distinct version of the data.

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
        _loggingHelper.LogLine($"Source {source_id}: {res} sd_sids in total");
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
        _loggingHelper.LogLine($"\t\t{res1} identified as invalid on sd_sid");

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
        _loggingHelper.LogLine($"\t\t{res2} identified as invalid on preferred_sd_sid");
    }    

    public void DeleteInvalidLinks()
    {
        // Delete the invalid secondary ids from the table.
        /*
        select * from  nk.temp_distinct_links
        WHERE valid = false order by preferred_source_id, preferred_sd_sid
        */
        
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
                , complex bool DEFAULT false
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
       
        // The complex indicator is set to true for studies involved in these complex relationships. The two
        // studies that 'group each other' (the A-C and C-A links above) are found with a SQL statement
        // that identifies any study that appears as both a grouping and a member study, but as a mix of
        // 'L' and 'R' sourced entries. (In a more normal cascade of linked study groups, studies can also 
        // appear as groupers and members, but will always have a consistent 'direction', i.e. will all either
        // be 'L' or 'R'). The second SQL statement then sets 'complex' to true for the other studies associated
        // with those studies (i.e. with both considered as grouping studies).
    
        using var conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = @"Update nk.temp_multilinked_studies ks1
            SET complex = true
            FROM nk.temp_multilinked_studies ks2
            WHERE ks1.member_source_id = ks2.group_source_id
            AND ks1.member_sd_sid = ks2.group_sd_sid
            AND ks1.source_side <> ks2.source_side";

        conn.Execute(sql_string);

        sql_string = @"Update nk.temp_multilinked_studies ks1
            SET complex = true
            FROM nk.temp_multilinked_studies ks2
            WHERE ks1.group_source_id = ks2.group_source_id
            AND ks1.group_sd_sid = ks2.group_sd_sid
            AND ks2.complex = true";

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
                         where complex = false;";

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
                         where complex = false;";

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
                              where complex = true";
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

                    int study_num = study_group.Count;
                    for (int i = 0; i < study_num - 1; i++)
                    {
                        ComplexStudy s = study_group[i];
                        for (int j = i + 1; j < study_num; j++)
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
                conn.Open();
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

        sql_string = @"DROP TABLE IF EXISTS nk.temp_multilinked_studies;";
        conn.Execute(sql_string);

        sql_string = @"SELECT COUNT(*) FROM nk.temp_distinct_links";
        int res = conn.ExecuteScalar<int>(sql_string);
        _loggingHelper.LogLine($"{res} distinct study-study links remaining");
    }

    public void AddMissingLinks()
    {
        // if there are only 2 studies linked to each other their management is straightforward. If there
        // are three or more, however (i.e. a study was registered 3 or more times) it is necessary to 
        // ensure that all the 'lass preferred' registry entries match to the same 'most preferred' entry.
        // This is done by the CascadeLinks function below, but before that can operate safely on the data
        // an additional problem needs to be solved.
        
        // This is that some links may be missing, in the sense that Study A is listed as being the
        // same as more preferred studies B and C, but no link exists between Study B to C, or Study C to B.
        // The 'link path' is therefore broken (so the cascade function will not work) and the B to C link
        // needs to be added. Otherwise a study will retain two, occasionally more, 'preferred studies',
        // which does not make sense in the system.
        
        // The requirement is to create the missing links and then add them to the distinct_links table.
        // To create those links, studies B and C need to be joined with the correct preference relationship.
        // That is done by linking both to A in succession, in a table that holds (firstly) A and B,
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
        // that identifies the 'problem' record (A) and the next pair with the source / sd_sid pair
        // from B and C that is NOT the most preferred. (If there are more than 3 matching entries all those
        //  that are not the most preferred are included, as separate rows. The table therefore has A-B links.

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
       
        
        // Update the last pair of tables in the temp_new_links table with the source / sd_sid 
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

        // Insert the new B-C links into the distinct_links table. These links will need re-processing
        // through the CascadeLinksTable() function. Then drop the temp tables.

        sql_string = @"INSERT INTO nk.temp_distinct_links
                 (source_id, sd_sid, preferred_sd_sid, preferred_source_id)
                 SELECT distinct new_source_id, new_sd_sid, new_preferred_sd_sid, new_preferred_source from
                 nk.temp_new_links;";
        int res = conn.Execute(sql_string);
        _loggingHelper.LogLine(res.ToString() + " new study-study links added to complete linkage chains");

        // drop the temp tables and ensure records in the table are distinct
        
        sql_string = @"DROP TABLE IF EXISTS nk.temp_missing_links;
            DROP TABLE IF EXISTS nk.temp_studies_with_multiple_links;
            DROP TABLE IF EXISTS nk.temp_new_links;";
        conn.Execute(sql_string);

        MakeLinksDistinct();
    }


    public void CascadeLinks()
    {
        // telescope the preferred links to the most preferred, i.e. A -> B, B -> C becomes A -> C, B -> C.
        // Do as long as there remains links to be telescoped (a few have to be done twice).

        using var conn = new NpgsqlConnection(_aggs_connString);
        int match_number = 500;  // arbitrary start number
        while (match_number > 0)
        {
            // get match number as number of link records where the rhs sd_sid
            // appears elsewhere on the left...

            string sql_string = @"SELECT count(*) 
                      FROM nk.temp_distinct_links t1
                      inner join nk.temp_distinct_links t2
                      on t1.preferred_source_id = t2.source_id
                      and t1.preferred_sd_sid = t2.sd_sid";

            match_number = conn.ExecuteScalar<int>(sql_string);
            _loggingHelper.LogLine($"{match_number} cascading study-study links found, to 'telescope'");

            if (match_number > 0)      // do the update
            {
                sql_string = @"UPDATE nk.temp_distinct_links t1
                      SET preferred_source_id = t2.preferred_source_id,
                      preferred_sd_sid = t2.preferred_sd_sid
                      FROM nk.temp_distinct_links t2
                      WHERE t1.preferred_source_id = t2.source_id
                      AND t1.preferred_sd_sid = t2.sd_sid";
                conn.Execute(sql_string);
            }
        }
        
        // In some cases the telescoped link may have already been present. The process above
        // will result in duplicates in these cases and these therefore need to be removed.
        
        MakeLinksDistinct();
    }

    public void MakeLinksDistinct()
    {
        using var conn = new NpgsqlConnection(_aggs_connString);
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
        _loggingHelper.LogLine($"{res2} records now in temp distinct links table, having dropped {diff}");
    }

    
    public void TransferLinksToPermanentTable()
    {
        // Select the processed study-study into the permanent table (re-created at the beginning of the
        // Aggregation process). A distinct selection is used as a final check against duplicates. 
        
        using var conn = new NpgsqlConnection(_aggs_connString);
        
        string sql_string = @"DROP TABLE IF EXISTS nk.study_study_links;
        CREATE TABLE nk.study_study_links(
            source_id                INT             NULL
          , sd_sid                   VARCHAR         NULL
          , preferred_sd_sid         VARCHAR         NULL
          , preferred_source_id      INT             NULL
          , study_id                 INT             NULL
          );";
        conn.Execute(sql_string);

        sql_string = @"Insert into nk.study_study_links
                  (source_id, sd_sid, preferred_sd_sid, preferred_source_id)
                  select distinct source_id, sd_sid, preferred_sd_sid, preferred_source_id
                  from nk.temp_distinct_links";
        int res = conn.Execute(sql_string);
        _loggingHelper.LogLine($"{res} study-study links created");
    }

    
    public void UpdateLinksWithStudyIds()
    {
        // New links may have been added which means that the study_ids table need to be updated.
        // First ensure that all the studies identifies as 'preferred' in the links table are listed as such
        // in the Study_ids table, (where they exist in that table) with a study_id that equals the Id and
        // an is_preferred status of true. Then ensure that study Id is stored in the links table.
        
        using var conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = @"update nk.study_ids si 
                        set study_id = si.id,
                        is_preferred = true
                        from nk.study_study_links ssk
                        where si.source_id = ssk.preferred_source_id 
                        and si.sd_sid = ssk.preferred_sd_sid ";
        int res = conn.Execute(sql_string);
        string message = "preferred studies in study_ids checked / modified to ensure correct data";
        _loggingHelper.LogLine($"{res} {message}");

        sql_string = @"update nk.study_study_links ssk
                  set study_id = si.study_id
                  from nk.study_ids si 
                  where ssk.preferred_source_id  = si.source_id
                  and ssk.preferred_sd_sid = si.sd_sid ";
        res = conn.Execute(sql_string);
        message = "study_ids added to the link table using the preferred study's id";
        _loggingHelper.LogLine($"{res} {message}");
        
        // This function repairs any cases of any non preferred studies in the links table that 
        // are not linked to a correct study_id entry. These study_id entries should have
        // is_preferred = false and the study_id should be equal to that in the links table.
        
         sql_string = @"Update nk.study_ids sids
                  set is_preferred = false,
                  study_id = ssk.study_id
                  from nk.study_study_links ssk
                  where sids.source_id = ssk.source_id
                  and sids.sd_sid = ssk.sd_sid
                  and ssk.study_id is not null ";
         res = conn.Execute(sql_string);   
         message = "non-preferred studies in study_ids checked / modified to ensure correct data";
         _loggingHelper.LogLine($"{res} {message}");

         // identify any non-preferred in the study_ids table that do not match a non-preferred entry
         // in the link table - (May have been part of a simple link arrangement in the past but now 
         // probably part of a different type of relationship)

         sql_string = @"update nk.study_ids si 
                  set is_preferred = true,
                  study_id = si.id
                  where id in
                      (select si.id from nk.study_ids si 
                    left join nk.study_study_links ssk
                    on si.source_id = ssk.source_id 
                    and si.sd_sid = ssk.sd_sid 
                    where is_preferred is false
                    and ssk.sd_sid is null ) ";

         res = conn.Execute(sql_string);
         message = "non-preferred study_ids records not in the non-preferred side modified to correct data";
         _loggingHelper.LogLine($"{res} {message}");
 
         // study_ids and link_link table should now be in sync.
         // The only rows in the links table that will not have a linked study will be the ones where
         // the preferred study is not yet in the study_ids table. Both the preferred and non-preferred
         // studies in these rows can only be properly sorted out in the study_ids table during (at the
         // start of) each source based addition.
    }

    public void DropTempTables()
    {
        using var conn = new NpgsqlConnection(_aggs_connString);
        string sql_string = @"DROP TABLE IF EXISTS nk.temp_preferences;
            DROP TABLE IF EXISTS nk.temp_distinct_links;
            DROP TABLE IF EXISTS nk.temp_distinct_links";
        conn.Execute(sql_string);
    }


    public void AddStudyStudyRelationshipRecords()
    {
        // Use the study_ids table to insert the study Ids for the linked 
        // sources / sd_sids, using nk.linked_study_groups as the source

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

}