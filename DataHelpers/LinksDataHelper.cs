using Dapper;
using Npgsql;
using PostgreSQLCopyHelper;
namespace MDR_Aggregator;

public class LinksDataHelper
{
    readonly string _mdr_connString;

    public LinksDataHelper(string mdr_connString)
    {
        _mdr_connString = mdr_connString;
    }

    public void SetUpTempPreferencesTable(IEnumerable<Source> sources)
    {
        using var conn = new NpgsqlConnection(_mdr_connString);
        var sql_string = @"DROP TABLE IF EXISTS nk.temp_preferences;
                  CREATE TABLE IF NOT EXISTS nk.temp_preferences(
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
    }


    public void SetUpTempLinkCollectorTable()
    {
        using var conn = new NpgsqlConnection(_mdr_connString);
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
        using var conn = new NpgsqlConnection(_mdr_connString);
        string sql_string = @"DROP TABLE IF EXISTS nk.temp_study_links_sorted;
                    CREATE TABLE nk.temp_study_links_sorted(
                    source_id int
                  , sd_sid varchar
                  , preferred_sd_sid varchar
                  , preferred_source_id int) ";
        conn.Execute(sql_string);
    }


    public IEnumerable<StudyLink> FetchLinks(int source_id, string source_conn_string)
    {
        using var conn = new NpgsqlConnection(source_conn_string);
        string sql_string = @"select " + source_id.ToString() + @" as source_1, 
                sd_sid as sd_sid_1, 
                identifier_value as sd_sid_2, identifier_org_id as source_2
                from ad.study_identifiers
                where identifier_type_id = 11
                and identifier_org_id > 100115
                and (identifier_org_id < 100133 or identifier_org_id = 101989)
                and identifier_org_id <> " + source_id.ToString();
        return conn.Query<StudyLink>(sql_string);
    }


    public ulong StoreLinksInTempTable(PostgreSQLCopyHelper<StudyLink> copyHelper, IEnumerable<StudyLink> entities)
    {
        using var conn = new NpgsqlConnection(_mdr_connString);
        conn.Open();
        return copyHelper.SaveAll(conn, entities);
    }


    public void TidyIds1()
    {
        using var conn = new NpgsqlConnection(_mdr_connString);
        string sql_string = @"DELETE from nk.temp_study_links_collector
            where sd_sid_2 ilike 'U1111%' or sd_sid_2 ilike 'UTRN%'";
        conn.Execute(sql_string);

        // replace n dashes
        sql_string = @"UPDATE nk.temp_study_links_collector
                    set sd_sid_2 = replace(sd_sid_2, '–', '-');";
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
        using var conn = new NpgsqlConnection(_mdr_connString);
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

        sql_string = @"UPDATE nk.temp_study_links_collector
            SET sd_sid_2 = replace(sd_sid_2, '/', '-')
            WHERE source_2 = 100121";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_study_links_collector
            SET sd_sid_2 = replace(sd_sid_2, '/', '-')
            WHERE source_2 = 100121;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_study_links_collector
            SET sd_sid_2 = replace(sd_sid_2, 'REF-', 'CTRI-')
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
    }

    public void TidyIds3()
    {
        using var conn = new NpgsqlConnection(_mdr_connString);
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
            set sd_sid_2 = 'TCTR' || sd_sid_2
            WHERE source_2 = 100131
            and length(sd_sid_2) = 11;";
        conn.Execute(sql_string);


        sql_string = @"UPDATE nk.temp_study_links_collector
            set sd_sid_2 = replace(sd_sid_2, '-', '')
            WHERE source_2 = 100132;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_study_links_collector
            set sd_sid_2 = 'NTR' || sd_sid_2
            WHERE source_2 = 100132
            and length(sd_sid_2) = 4;";
        conn.Execute(sql_string);

        sql_string = @"UPDATE nk.temp_study_links_collector
            SET sd_sid_2 = left(sd_sid_2, 7)
            where source_2 = 100132
            and length(sd_sid_2) > 7";
        conn.Execute(sql_string);
    }


    public void TransferLinksToSortedTable()
    {
        using var conn = new NpgsqlConnection(_mdr_connString);
        // needs to be done twice to keep the ordering of sources correct
        // A lower rating means 'more preferred' - i.e. should be used in preference
        // Therefore lower rated source data should be in the 'preferred' fields
        // and higher rated data should be on the left hand side

        // Original data matches what is required

        string sql_string = @"INSERT INTO nk.temp_study_links_sorted(
                      source_id, sd_sid, preferred_sd_sid, preferred_source_id) 
                      SELECT t.source_1, t.sd_sid_1, t.sd_sid_2, t.source_2
                      FROM nk.temp_study_links_collector t
                      inner join nk.temp_preferences r1
                      on t.source_1 = r1.id
                      inner join nk.temp_preferences r2
                      on t.source_2 = r2.id
                      WHERE r1.preference_rating > r2.preference_rating";

        conn.Execute(sql_string);

        // Original data is the opposite of what is required - therefore switch

        sql_string = @"INSERT INTO nk.temp_study_links_sorted(
                      source_id, sd_sid, preferred_sd_sid, preferred_source_id) 
                      SELECT t.source_2, t.sd_sid_2, t.sd_sid_1, t.source_1
                      FROM nk.temp_study_links_collector t
                      inner join nk.temp_preferences r1
                      on t.source_1 = r1.id
                      inner join nk.temp_preferences r2
                      on t.source_2 = r2.id
                      WHERE r1.preference_rating < r2.preference_rating";

        conn.Execute(sql_string);
    }


    public void CreateDistinctSourceLinksTable()
    {
        // The nk.temp_study_links_sorted table will have 
        // many duplicates... create a distinct version of the data

        using var conn = new NpgsqlConnection(_mdr_connString);
        string sql_string = @"DROP TABLE IF EXISTS nk.temp_distinct_links;
                       CREATE TABLE nk.temp_distinct_links 
                       as SELECT distinct source_id, sd_sid, preferred_sd_sid, preferred_source_id
                       FROM nk.temp_study_links_sorted";

        conn.Execute(sql_string);
    }


    // One set of relationships are not 'same study in a different registry'
    // but multiple studies in a different registry.
    // Such studies have a study relationship rather than being straight equivalents.
    // There can be multiple studies in the 'preferred' registry
    // or in the existing studies registry - each group being equivalent to
    // a registry entry that represents a single study, or sometimes a 
    // single project / programme, or grant

    public void IdentifyGroupedStudies()
    {
        // Set up a table to hold group definitions (i.e. the list 
        // of studies in each group, can be from the LHS or the RHS 
        // of the distinct links table

        using var conn = new NpgsqlConnection(_mdr_connString);
        string sql_string = @"DROP TABLE IF EXISTS nk.temp_grouping_studies;
                CREATE TABLE nk.temp_grouping_studies 
                (  
                    source_id   INT,
                    sd_sid      VARCHAR,
                    matching_source_id  INT,
                    side        VARCHAR
                );";
        conn.Execute(sql_string);

        // Studies of interest have more than one matching study
        // within the SAME matching source registry.
        // Therefore group on one side, plus the source_id of the other

        sql_string = @"INSERT INTO nk.temp_grouping_studies
                         (source_id, sd_sid, matching_source_id, side)
                         SELECT source_id, sd_sid, 
                         preferred_source_id, 'L'
                         FROM nk.temp_distinct_links 
                         group by source_id, sd_sid, preferred_source_id
                         HAVING count(sd_sid) > 1;";
        conn.Execute(sql_string);

        sql_string = @"INSERT INTO nk.temp_grouping_studies
                         (source_id, sd_sid, matching_source_id, side)
                         SELECT preferred_source_id, preferred_sd_sid, 
                         source_id, 'R'
                         FROM nk.temp_distinct_links 
                         group by preferred_source_id, preferred_sd_sid, source_id
                         HAVING count(preferred_sd_sid) > 1;";
        conn.Execute(sql_string);
    }


    public void ExtractGroupedStudiess()
    {
        using var conn = new NpgsqlConnection(_mdr_connString);
        // create a table that takes the rows from the linked 
        // studies table that match the 'L' grouping studies

        // The source_id side is the group and the preferred side 
        // is comprised of the grouped studies.

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

        // To retain the same arrangement of grouping study on 
        // the LHS the input data from the RHS has to be switched around

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

        // Put this data into the permanent linked_study_groups table
        // The study relationships are 
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
        using var conn = new NpgsqlConnection(_mdr_connString);
        // Now need to delete these grouped records from the links table...

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


    public void ManageIncompleteLinks()
    {

        // There are a set if links that may be missing, in the sense that
        // Study A is listed as being the same as Study B and Study C, but no
        // link exists between either Study B to C, or Study C to B.
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
        
        // First identify the studies that have more than one 'preferred' option
        // cutting across more than one source. 
        // Groups have already been removed, so this should find only those
        // with the 'missing link'.
        
        using var conn = new NpgsqlConnection(_mdr_connString);
        string sql_string = @"DROP TABLE IF EXISTS nk.temp_studies_with_multiple_links;
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

        // Insert the new links into the distinct_links table.
        // These links will need re-processing through the CascadeLinksTable() function.

        sql_string = @"INSERT INTO nk.temp_distinct_links
                 (source_id, sd_sid, preferred_sd_sid, preferred_source_id)
                 SELECT new_source_id, new_sd_sid, new_preferred_sd_sid, new_preferred_source from
                 nk.temp_new_links;";
        conn.Execute(sql_string);

        // drop the temp tables 
        sql_string = @"DROP TABLE IF EXISTS nk.temp_missing_links;
            DROP TABLE IF EXISTS nk.temp_new_links;";
        conn.Execute(sql_string);
    }


    public void CascadeLinksInDistinctLinksTable()
    {
        using var conn = new NpgsqlConnection(_mdr_connString);
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


    public void TransferNewLinksToDataTable()
    {
        // A distinct selection is required because the most recent
        // link cascade may have generated duplicates

        using var conn = new NpgsqlConnection(_mdr_connString);
        string sql_string = @"Insert into nk.study_study_links
                  (source_id, sd_sid, preferred_sd_sid, preferred_source_id)
                  select distinct source_id, sd_sid, preferred_sd_sid, preferred_source_id
                  from nk.temp_distinct_links";

        conn.Execute(sql_string);
    }

    public int ObtainTotalOfNewLinks()
    {
        using var conn = new NpgsqlConnection(_mdr_connString);
        string sql_string = @"SELECT COUNT(*) FROM nk.temp_distinct_links";
        return conn.ExecuteScalar<int>(sql_string);
    }


    public void DropTempTables()
    {
        using var conn = new NpgsqlConnection(_mdr_connString);
        string sql_string = @"DROP TABLE IF EXISTS nk.temp_preferences;
            DROP TABLE IF EXISTS nk.temp_study_links_collector;
            DROP TABLE IF EXISTS nk.temp_distinct_links;";
        conn.Execute(sql_string);
    }


    public void AddStudyStudyRelationshipRecords()
    {
        // Use the study_all_ids to insert the study Ids
        // for the linked sources / sd_sids, using 
        // nk.linked_study_groups as the source

        using var conn = new NpgsqlConnection(_mdr_connString);
        string sql_string = @"Insert into st.study_relationships
                  (study_id, relationship_type_id, target_study_id)
                  select s1.study_id, g.relationship_id, s2.study_id
                  from nk.linked_study_groups g
                  inner join nk.all_ids_studies s1
                  on g.source_id = s1.source_id
                  and g.sd_sid = s1.sd_sid
                  inner join nk.all_ids_studies s2
                  on g.target_source_id = s2.source_id
                  and g.target_sd_sid = s2.sd_sid";

        conn.Execute(sql_string);
    }


    public void TransferTestIdentifiers(int source_id)
    {
        // This called only during testing
        // Transfers the study identifier records so that they can be used to
        // obtain links, in imitation of the normal process

        using var conn = new NpgsqlConnection(_mdr_connString);
        string sql_string = @"truncate table ad.study_identifiers; 
                        INSERT into ad.study_identifiers (sd_sid, identifier_type_id, 
                        identifier_value, identifier_org, identifier_org_id, identifier_org_ror_id,
                        identifier_date, identifier_link)
                        SELECT sd_sid, identifier_type_id,
                        identifier_value, identifier_org, identifier_org_id, identifier_org_ror_id,
                        identifier_date, identifier_link 
                        from adcomp.study_identifiers
                        where source_id = " + source_id.ToString();
        conn.Execute(sql_string);
    }
}