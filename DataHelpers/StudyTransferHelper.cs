using Dapper;
using Npgsql;
using PostgreSQLCopyHelper;
namespace MDR_Aggregator;

public class StudyDataTransferrer
{
    readonly string _connString;
    readonly DBUtilities db;
    readonly ILoggingHelper _loggingHelper;
    int nonpref_number;
    int status1number, status2number, status3number;
    
    public StudyDataTransferrer(string connString, ILoggingHelper logginghelper)
    {
        _connString = connString;
        _loggingHelper = logginghelper;
        db = new DBUtilities(connString, _loggingHelper);
    }
   
    public void SetUpTempStudyIdsTable()
    {
        using var conn = new NpgsqlConnection(_connString);
        string sql_string = @"DROP TABLE IF EXISTS nk.temp_study_ids;
                   CREATE TABLE nk.temp_study_ids(
                    id                       INT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY
                  , study_id                 INT
                  , source_id                INT
                  , sd_sid                   VARCHAR
                  , is_preferred             BOOLEAN
                  , datetime_of_data_fetch   TIMESTAMPTZ
                  , match_status             INT  default 0
                  ); ";
        conn.Execute(sql_string);
    }

    public ulong FetchStudyIds(int source_id, string source_conn_string)
    {
        string sql_string = $"select max(id) FROM ad.studies";
        using var conn = new NpgsqlConnection(source_conn_string);
        int max_id = conn.ExecuteScalar<int>(sql_string);
        int batch_size = 100000;
        try
        {
            ulong stored = 0;
            sql_string = $@"select {source_id} as source_id, sd_sid, datetime_of_data_fetch
                            from ad.studies t ";
            if (max_id > batch_size)
            {
                for (int r = 1; r <= max_id; r += batch_size)
                {
                    string batch_sql_string = sql_string + $" where t.id >= {r} and t.id < {r + batch_size} ";
                    IEnumerable<StudyId> study_ids = conn.Query<StudyId>(batch_sql_string);
                    ulong num_stored = StoreStudyIds(CopyHelpers.study_ids_helper, study_ids);
                    stored += num_stored;
                    int e = r + batch_size < max_id ? r + batch_size - 1 : max_id;
                    _loggingHelper.LogLine($"Obtained {num_stored} study ids, from ids {r} to {e}");
                }
            }
            else
            {
                IEnumerable<StudyId> study_ids = conn.Query<StudyId>(sql_string);
                stored = StoreStudyIds(CopyHelpers.study_ids_helper, study_ids);
                _loggingHelper.LogLine($"Obtained {stored} study ids as a single batch");
            }
            return stored;
        } 
        catch (Exception e)
        {
            _loggingHelper.LogError($"In obtaining study ids: {e.Message}");
            return 0;
        }
    }
    
    public ulong StoreStudyIds(PostgreSQLCopyHelper<StudyId> copyHelper, IEnumerable<StudyId> entities)
    {
        // Stores the study id data in a temporary table.

        using var conn = new NpgsqlConnection(_connString);
        conn.Open();
        return copyHelper.SaveAll(conn, entities);
    }

    
    public void MatchExistingStudyIds()
    {
        // Do these source id-study id combinations already exist in the system, i.e. have a known id?
        // If they do they can be matched, to leave only the new study ids to process.
        
        using var conn = new NpgsqlConnection(_connString);
        string sql_string = @"UPDATE nk.temp_study_ids t
                    SET study_id = si.study_id, is_preferred = si.is_preferred,
                    match_status = 1
                    from nk.study_ids si
                    where t.source_id = si.source_id
                    and t.sd_sid = si.sd_sid ";

        int res = db.Update_UsingTempTable("nk.temp_study_ids", "nk.temp_study_ids", sql_string, " and ", 
                                          25000, ", with study id, status = 1, for matched studies");
        _loggingHelper.LogLine($"{res} Existing studies matched in temp table");
        _loggingHelper.LogLine("");
        
        // Also update the study_ids table: indicate has been matched and update the data fetch date.

        sql_string = @"UPDATE nk.study_ids si
            set match_status = 1,
            datetime_of_data_fetch = t.datetime_of_data_fetch
            from nk.temp_study_ids t
            where t.source_id = si.source_id
            and t.sd_sid = si.sd_sid ";

        status1number = db.Update_UsingTempTable("nk.temp_study_ids", "nk.study_ids", sql_string, " and ", 
                                            25000, ", with last data fetch time, status 1, for matched studies");
        _loggingHelper.LogLine($"{status1number} existing studies matched in study_ids table");
        _loggingHelper.LogLine("");
    }


    public void IdentifyNewLinkedStudyIds()
    {
        // For the new studies...where match_status still 0...
        // Does any study id correspond to a study already in study_identifiers table, that is
        // linked to it via study-study link table. Such a study will match the left hand side
        // of the study-study link table (the 'least preferred', the one to be replaced), and
        // take on the study_id used for the 'preferred' right hand side. This should already 
        // exist because addition of studies is done in the order 'more preferred first'.

        string sql_string = @"UPDATE nk.temp_study_ids t
                       SET study_id = k.study_id, is_preferred = false,
                       match_status = 2
                       FROM nk.study_study_links k
                       WHERE t.sd_sid = k.sd_sid
                       AND t.source_id =  k.source_id
                       AND t.match_status = 0;";

        status2number = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{status2number} existing studies found under other study source ids");
        _loggingHelper.LogLine("");
    }


    public void AddNewStudyIds(int source_id)
    {
        // Add all the new study id records to the all Ids table. This includes those identified
        // above (match_status = 2) and those yet to be added to the system (match_status = 0)
        
        using var conn = new NpgsqlConnection(_connString);
        string sql_string = @"INSERT INTO nk.study_ids
                        (study_id, source_id, sd_sid, 
                         datetime_of_data_fetch, is_preferred,
                         match_status)
                         select study_id, source_id, sd_sid, 
                         datetime_of_data_fetch, is_preferred, 
                         match_status
                         from nk.temp_study_ids t
                         where (match_status = 0 or match_status = 2) ";
        db.Update_UsingTempTable("nk.temp_study_ids", "nk.study_ids", sql_string, " and "
                                 , 25000, ", inserting new studies");
        _loggingHelper.LogLine("");

        // Where the study_ids are null they can take on the value of the 
        // record id. The 3 indicates they are new on this addition.

        sql_string = $@"UPDATE nk.study_ids
                        SET study_id = id, is_preferred = true,
                        match_status = 3
                        WHERE study_id is null
                        AND source_id = {source_id}";
        conn.Execute(sql_string);

        // 'Back-update' the study temp table using the newly created study_ids
        // now all records in this table should have a match and preferred status

        sql_string = $@"UPDATE nk.temp_study_ids t
                       SET study_id = si.study_id, is_preferred = true,
                       match_status = 3
                       FROM nk.study_ids si
                       WHERE si.sd_sid = t.sd_sid
                       AND si.source_id = t.source_id
                       AND t.study_id is null ";
        status3number = db.Update_UsingTempTable("nk.temp_study_ids", "nk.temp_study_ids", sql_string, " and "
                                      , 25000, ", with study id and status = 3");
        _loggingHelper.LogLine($"{status3number} new study ids found");
        _loggingHelper.LogLine("");

        // Also update any new entries in study links table that still have 
        // no study id attached - update from the updated study_ids table

        sql_string = @"UPDATE nk.study_study_links ssk
                  SET study_id = s.study_id
                  FROM nk.study_ids s
                  WHERE ssk.preferred_sd_sid = s.sd_sid 
                  AND ssk.preferred_source_id = s.source_id
                  AND ssk.study_id is null ";
        conn.Execute(sql_string);
    }

    public void CreateTempStudyIdTables(int source_id)
    {
        // Create two tables that have just the study_ids and sd_sids for the 'preferred' (new)
        // studies (used to import all the linked data for these studies), and the non-preferred
        // (existing) studies (used for the import any of additional data from these studies)
        
        using var conn = new NpgsqlConnection(_connString);   
        int total_studies = status1number + status2number + status3number;
        _loggingHelper.LogLine($"{total_studies} total studies found");

        string sql_string = $@"DROP TABLE IF EXISTS nk.new_studies;
                           CREATE TABLE nk.new_studies as 
                                   SELECT sd_sid, study_id
                                   FROM nk.study_ids
                                   WHERE source_id = {source_id}
                                   and is_preferred = true";
        db.ExecuteSQL(sql_string);
        int res = db.GetCount("nk.new_studies");
        _loggingHelper.LogLine($"{res} new studies to be added");

        sql_string = $@"DROP TABLE IF EXISTS nk.existing_studies;
                           CREATE TABLE nk.existing_studies as 
                                   SELECT sd_sid, study_id
                                   FROM nk.study_ids
                                   WHERE source_id = {source_id}
                                   and is_preferred = false";
        db.ExecuteSQL(sql_string);
        res = db.GetCount("nk.existing_studies");
        _loggingHelper.LogLine($"{res} existing studies (additional data added)");
        nonpref_number = res;
    }
   
    private readonly Dictionary<string, string> studyDestFields = new() 
    {
        { "studies", @"id, display_title, title_lang_code, brief_description, data_sharing_statement,
                study_start_year, study_start_month, study_type_id, study_status_id,
                study_enrolment, study_gender_elig_id, min_age, min_age_units_id,
                max_age, max_age_units_id, iec_level" },
        { "study_identifiers", @"study_id, identifier_value, identifier_type_id, 
                source_id, source, source_ror_id,
                identifier_date, identifier_link"},
        { "study_titles", @"study_id, 
                title_type_id, title_text, lang_code, 
                lang_usage_id, is_default, comments" },
        { "study_people", @"study_id, contrib_type_id, person_given_name, person_family_name, 
                person_full_name, orcid_id, person_affiliation, 
                organisation_id, organisation_name, organisation_ror_id" },
        { "study_organisations", @"study_id, contrib_type_id,  
                organisation_id, organisation_name, organisation_ror_id" },
        { "study_topics", @"study_id, 
                topic_type_id, original_value, original_ct_type_id, 
                original_ct_code, mesh_code, mesh_value" },
        { "study_relationships", @"study_id, 
                relationship_type_id, target_study_id" },
        { "study_features", @"study_id, 
                feature_type_id, feature_value_id" },
        { "study_countries", @"study_id, country_id, country_name, status_id" },
        { "study_locations", @"study_id, facility_org_id, facility, facility_ror_id,
        city_id, city_name, country_id, country_name, status_id" },
        { "study_conditions", @"study_id, original_value, original_ct_type_id, original_ct_code, 
        icd_code, icd_name" },
        { "study_iec", @"study_id, seq_num, iec_type_id, split_type, leader, indent_level,
          sequence_string, iec_text" }
    };
    
    private readonly Dictionary<string, string> studySourceFields = new() 
    {
        { "studies", @"s.display_title, s.title_lang_code, s.brief_description, s.data_sharing_statement,
                s.study_start_year, s.study_start_month, s.study_type_id, s.study_status_id,
                s.study_enrolment, s.study_gender_elig_id, s.min_age, s.min_age_units_id,
                s.max_age, s.max_age_units_id, s.iec_level" },
        { "study_identifiers", @"s.identifier_value, s.identifier_type_id, 
                s.source_id, s.source, s.source_ror_id,
                s.identifier_date, s.identifier_link" },
        { "study_titles", @"s.title_type_id, s.title_text, s.lang_code, 
                s.lang_usage_id, s.is_default, s.comments" },
        { "study_people", @"s.contrib_type_id, s.person_given_name, s.person_family_name, 
                s.person_full_name, s.orcid_id, s.person_affiliation, 
                s.organisation_id, s.organisation_name, s.organisation_ror_id" },
        { "study_organisations", @"s.contrib_type_id,  
                s.organisation_id, s.organisation_name, s.organisation_ror_id" },
        { "study_topics", @"s.topic_type_id, s.original_value, s.original_ct_type_id, 
                s.original_ct_code, s.mesh_code, s.mesh_value" },
        { "study_relationships", @"s.relationship_type_id, s.target_study_id" },
        { "study_features", @"s.feature_type_id, s.feature_value_id" },
        { "study_countries", @"s.country_id, s.country_name, s.status_id" },
        { "study_locations", @"s.facility_org_id, s.facility, s.facility_ror_id,
        s.city_id, s.city_name, s.country_id, s.country_name, s.status_id" },
        { "study_conditions", @"s.original_value, s.original_ct_type_id, s.original_ct_code, 
        s.icd_code, s.icd_name" },
        { "study_iec", @"s.seq_num, s.iec_type_id, s.split_type, s.leader, s.indent_level,
          s.sequence_string, s.iec_text" }
    };
    
    private void CreateSourceDataTable(string ftw_schema_name, string table_name)
    {
        // This sets up a table of all the subset of data in the source DB table,
        // that matches existing studies in the aggregated db schema. It is the data that 
        // * could * be added, but only if it does not match what is already there.
        
        string sql_string = $@"DROP TABLE IF EXISTS nk.source_data;
                       CREATE TABLE nk.source_data as 
                       SELECT es.study_id, d.* 
                       FROM {ftw_schema_name}.{table_name} d
                       INNER JOIN nk.existing_studies es
                       ON d.sd_sid = es.sd_sid";
        db.ExecuteSQL(sql_string);
    }
    
    private void CreateExistingDataTable(string dest_schema_name, string table_name, string comparison_fields)
    {
        // This sets up a table of the subset of data in the aggregated (st) table, with identifiers and
        // a few key comparison fields, that match the 'existing studies' group. This data is used to test
        // the source data for these studies to see which represents new data.
        
        string sql_string = $@"DROP TABLE IF EXISTS nk.existing_data;
                       CREATE TABLE nk.existing_data as 
                       SELECT es.sd_sid, es.study_id, {comparison_fields}
                       FROM {dest_schema_name}.{table_name} c
                       INNER JOIN nk.existing_studies es
                       ON c.study_id = es.study_id;";
        db.ExecuteSQL(sql_string);
    }

    
    public int LoadStudies(string ftw_schema_name)
    {
        // Insert the study data unless it is already in under another id (i.e. t.is_preferred = false).
        
        string destFields = studyDestFields["studies"];
        string sourceFields = studySourceFields["studies"];
        string sql_string = $@"INSERT INTO st.studies({destFields})
                SELECT t.study_id, {sourceFields}
                FROM nk.new_studies t
                INNER JOIN {ftw_schema_name}.studies s
                on t.sd_sid = s.sd_sid ";
        
        int res = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "studies", " where ", "new studies");
        _loggingHelper.LogLine($"Loaded {res} new studies");
        _loggingHelper.LogLine("");
        return res;
    }

    
    public int LoadStudyIdentifiers(string ftw_schema_name)
    {
        string destFields = studyDestFields["study_identifiers"];
        string sourceFields = studySourceFields["study_identifiers"];
        
        // For 'preferred' study Ids add all identifiers.

        string sql_string = $@"INSERT INTO st.study_identifiers({destFields})
                SELECT k.study_id, {sourceFields}
                FROM nk.new_studies k
                INNER JOIN {ftw_schema_name}.study_identifiers s
                on k.sd_sid = s.sd_sid ";

        int res1 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "study_identifiers", " where ", "new studies");
        _loggingHelper.LogLine($"Loaded {res1} study identifiers, for new studies");
        
        // For 'existing studies' study Ids, if there are any, add only new identifiers.

        if (nonpref_number == 0)
        {
            _loggingHelper.LogLine("");
            return res1;
        }

        CreateSourceDataTable(ftw_schema_name, "study_identifiers");
        CreateExistingDataTable("st", "study_identifiers", " c.identifier_type_id, c.identifier_value ");

        sql_string = $@"INSERT INTO st.study_identifiers({destFields})
                   SELECT s.study_id, {sourceFields}
                   FROM nk.source_data s
                   LEFT JOIN nk.existing_data e
                   ON s.sd_sid = e.sd_sid
                   AND s.identifier_type_id = e.identifier_type_id
                   AND s.identifier_value = e.identifier_value
                   WHERE e.study_id is null ";

        int res2 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "study_identifiers", " and ", "existing studies");
        _loggingHelper.LogLine($"Loaded {res2} study identifiers, for existing studies");
        _loggingHelper.LogLine("");
        db.ExecuteSQL("DROP TABLE IF EXISTS nk.source_data; DROP TABLE IF EXISTS nk.existing_data;");
        return res1 + res2;
    }


    public int LoadStudyTitles(string ftw_schema_name)
    {
        string destFields = studyDestFields["study_titles"];
        string sourceFields = studySourceFields["study_titles"];

        // For 'preferred' study Ids add all titles.

        string sql_string = $@"INSERT INTO st.study_titles({destFields})
                SELECT k.study_id, {sourceFields}
                FROM nk.new_studies k
                INNER JOIN {ftw_schema_name}.study_titles s
                on k.sd_sid = s.sd_sid ";

        int res1 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "study_titles", " where ", "new studies");
        _loggingHelper.LogLine($"Loaded {res1} study titles, for new studies");

        // For 'existing studies' study Ids, if there are any, add only new titles.

        if (nonpref_number == 0)
        {
            _loggingHelper.LogLine("");
            return res1;
        }

        CreateSourceDataTable(ftw_schema_name, "study_titles");
        CreateExistingDataTable("st", "study_titles", " c.title_text ");

        // Non preferred titles must all be non-default (default will be from the preferred source)

        sql_string = $@"UPDATE nk.source_data SET is_default = false;";
        db.ExecuteSQL(sql_string);

        // For titles which are the same as one that already exist the comments field should be updated.
        // For titles which are new to the study simply add them.

        sql_string = $@"UPDATE st.study_titles t
                   set comments = t.comments || '; ' || s.comments 
                   FROM nk.source_data s
                   WHERE t.study_id = s.study_id
                   AND lower(t.title_text) = lower(s.title_text);";
        db.ExecuteSQL(sql_string);

        sql_string = $@"INSERT INTO st.study_titles({destFields})
                   SELECT s.study_id, {sourceFields}
                   FROM nk.source_data s
                   LEFT JOIN nk.existing_data e
                   ON s.sd_sid = e.sd_sid
                   AND lower(s.title_text) = lower(e.title_text)
                   WHERE e.study_id is null ";

        int res2 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "study_titles", " and ", "existing studies");
        _loggingHelper.LogLine($"Loaded {res2} study titles, for existing studies");
        _loggingHelper.LogLine("");
        db.ExecuteSQL("DROP TABLE IF EXISTS nk.source_data; DROP TABLE IF EXISTS nk.existing_data;");
        return res1 + res2;
    }


    public int LoadStudyPeople(string ftw_schema_name)
    {
        string destFields = studyDestFields["study_people"];
        string sourceFields = studySourceFields["study_people"];

        // For 'preferred' study Ids add all people.

        string sql_string = $@"INSERT INTO st.study_people({destFields})
                SELECT k.study_id, {sourceFields}
                FROM nk.new_studies k
                INNER JOIN {ftw_schema_name}.study_people s
                on k.sd_sid = s.sd_sid ";

        int res1 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "study_people", " where ", "new studies");
        _loggingHelper.LogLine($"Loaded {res1} study people, for new studies");
        
        // For 'existing studies' study Ids, if there are any, add only new people.
        
        if (nonpref_number == 0)
        {
            _loggingHelper.LogLine("");
            return res1;
        }

        CreateSourceDataTable(ftw_schema_name, "study_people");
        CreateExistingDataTable("st", "study_people", " c.contrib_type_id, c.person_full_name ");
        sql_string = $@"INSERT INTO st.study_people({destFields})
                   SELECT s.study_id, {sourceFields}
                   FROM nk.source_data s
                   LEFT JOIN nk.existing_data e
                   ON s.sd_sid = e.sd_sid
                   AND s.contrib_type_id = e.contrib_type_id
                   AND s.person_full_name = e.person_full_name
                   WHERE e.study_id is null ";

        int res2 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "study_people", " and ", "existing studies");
        _loggingHelper.LogLine($"Loaded {res2} study people, for existing studies");
        _loggingHelper.LogLine("");
        db.ExecuteSQL("DROP TABLE IF EXISTS nk.source_data; DROP TABLE IF EXISTS nk.existing_data;");
        return res1 + res2;
    }


    public int LoadStudyOrganisations(string ftw_schema_name)
    {
        string destFields = studyDestFields["study_organisations"];
        string sourceFields = studySourceFields["study_organisations"];

        // For 'preferred' study Ids add all contributors.

        string sql_string = $@"INSERT INTO st.study_organisations({destFields})
                SELECT k.study_id, {sourceFields}
                FROM nk.new_studies k
                INNER JOIN {ftw_schema_name}.study_organisations s
                on k.sd_sid = s.sd_sid";

        int res1 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "study_organisations", " where ", "new studies");
        _loggingHelper.LogLine($"Loaded {res1} study organisations, for new studies");
        
        // For 'existing studies' study Ids, if there are any, add only new organisations.

        if (nonpref_number == 0)
        {
            _loggingHelper.LogLine("");
            return res1;
        }
        
        CreateSourceDataTable(ftw_schema_name, "study_organisations");
        CreateExistingDataTable("st", "study_organisations", " c.contrib_type_id, c.organisation_name ");
        sql_string = $@"INSERT INTO st.study_organisations({destFields})
                   SELECT s.study_id, {sourceFields}
                   FROM nk.source_data s
                   LEFT JOIN nk.existing_data e
                   ON s.sd_sid = e.sd_sid
                   AND s.contrib_type_id = e.contrib_type_id
                   AND s.organisation_name = e.organisation_name
                   WHERE e.study_id is null ";

        int res2 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "study_organisations", " and ", "existing studies");
        _loggingHelper.LogLine($"Loaded {res2} study organisations, for existing studies");
        _loggingHelper.LogLine("");
        db.ExecuteSQL("DROP TABLE IF EXISTS nk.source_data; DROP TABLE IF EXISTS nk.existing_data;");
        return res1 + res2;
    }
    
    
    public int LoadStudyTopics(string ftw_schema_name)
    {
        string destFields = studyDestFields["study_topics"];
        string sourceFields = studySourceFields["study_topics"];
  
        // For 'preferred' study Ids add all topics.

        string sql_string = $@"INSERT INTO st.study_topics({destFields})
                SELECT k.study_id, {sourceFields}
                FROM nk.new_studies k
                INNER JOIN {ftw_schema_name}.study_topics s
                on k.sd_sid = s.sd_sid ";

        int res1 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "study_topics", " where ", "new studies");
        _loggingHelper.LogLine($"Loaded {res1} study topics, for new studies");
  
        // For 'existing studies' study Ids, if there are any, add only new topics.

        if (nonpref_number == 0)
        {
            _loggingHelper.LogLine("");
            return res1;
        }
    
        CreateSourceDataTable(ftw_schema_name, "study_topics");
        CreateExistingDataTable("st", "study_topics", " c.mesh_code, c.original_value ");
        
        // Need to do this first to speed things up, otherwise system can time out.
        
        sql_string = @"DROP TABLE IF EXISTS nk.existing_coded_data;
                   CREATE TABLE nk.existing_coded_data as 
                   select * from nk.existing_data e
                   where e.mesh_code is not null;";
        db.ExecuteSQL(sql_string);
        
        sql_string = @"DROP TABLE IF EXISTS nk.existing_non_coded_data;
                   CREATE TABLE nk.existing_non_coded_data as 
                   select sd_sid, lower(original_value) as original_value 
                   from nk.existing_data e
                   where e.mesh_code is null;";
        db.ExecuteSQL(sql_string);
        
        sql_string = $@"INSERT INTO st.study_topics({destFields})
                   SELECT s.study_id, {sourceFields}
                   FROM nk.source_data s
                   LEFT JOIN nk.existing_coded_data e1
                   ON s.sd_sid = e1.sd_sid
                   AND s.mesh_code = e1.mesh_code
                   WHERE s.mesh_code is not null and e1.sd_sid is null "; // for MESH coded data

        int res2 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "study_topics", " and ", "existing studies");
        _loggingHelper.LogLine($"Loaded {res2} mesh coded study topics, for existing studies");

        sql_string = $@"INSERT INTO st.study_topics({destFields})
                   SELECT s.study_id, {sourceFields}
                   FROM nk.source_data s
                   LEFT JOIN nk.existing_non_coded_data e2
                   ON s.sd_sid = e2.sd_sid
                   AND lower(s.original_value) = e2.original_value
                   WHERE s.mesh_code is null and e2.sd_sid is null "; // for non mesh coded data

        int res3 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "study_topics", " and ", "existing studies");
        _loggingHelper.LogLine($"Loaded {res3} non mesh coded study topics, for existing studies");
        _loggingHelper.LogLine("");
        db.ExecuteSQL("DROP TABLE IF EXISTS nk.existing_coded_data; " +
                      "DROP TABLE IF EXISTS nk.existing_non_coded_data;");
        db.ExecuteSQL("DROP TABLE IF EXISTS nk.source_data; DROP TABLE IF EXISTS nk.existing_data;");
        return res1 + res2 + res3;
    }

    
    public int LoadStudyConditions(string ftw_schema_name)
    {
        string destFields = studyDestFields["study_conditions"];
        string sourceFields = studySourceFields["study_conditions"];
        
        // For 'preferred' study Ids add all conditions.

        string sql_string = $@"INSERT INTO st.study_conditions({destFields})
                SELECT k.study_id, {sourceFields}
                FROM nk.new_studies k
                INNER JOIN {ftw_schema_name}.study_conditions s
                on k.sd_sid = s.sd_sid ";

        int res1 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "study_conditions", " where ", "new studies");
        _loggingHelper.LogLine($"Loaded {res1} study_conditions, for new studies");
        
        // For 'existing studies' study Ids, if there are any, add only new conditions.
       
        if (nonpref_number == 0)
        {
            _loggingHelper.LogLine("");
            return res1;
        }
        
        CreateSourceDataTable(ftw_schema_name, "study_conditions");
        CreateExistingDataTable("st", "study_conditions", " c.icd_code, c.original_value ");
    
        // Need to do this first to speed things up, otherwise system can time out.
        
        sql_string = @"DROP TABLE IF EXISTS nk.existing_coded_data;
                   CREATE TABLE nk.existing_coded_data as 
                   select * from nk.existing_data e
                   where e.icd_code is not null;";
        db.ExecuteSQL(sql_string);
        
        sql_string = @"DROP TABLE IF EXISTS nk.existing_non_coded_data;
                   CREATE TABLE nk.existing_non_coded_data as 
                   select sd_sid, lower(original_value) as original_value 
                   from nk.existing_data e
                   where e.icd_code is null;";
        db.ExecuteSQL(sql_string);
        
        sql_string = $@"INSERT INTO st.study_conditions({destFields})
                   SELECT s.study_id, {sourceFields}
                   FROM nk.source_data s
                   LEFT JOIN nk.existing_coded_data e1
                   ON s.sd_sid = e1.sd_sid
                   AND s.icd_code = e1.icd_code
                   WHERE s.icd_code is not null and e1.sd_sid is null "; // for icd coded data

        int res2 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "study_conditions", " and ", "existing studies");
        _loggingHelper.LogLine($"Loaded {res2} icd coded study conditions, for existing studies");

        sql_string = $@"INSERT INTO st.study_conditions({destFields})
                   SELECT s.study_id, {sourceFields}
                   FROM nk.source_data s
                   LEFT JOIN nk.existing_non_coded_data e2
                   ON s.sd_sid = e2.sd_sid
                   AND lower(s.original_value) = e2.original_value
                   WHERE s.icd_code is null and e2.sd_sid is null "; // for non icd coded data

        int res3 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "study_conditions", " and ", "existing studies");
        _loggingHelper.LogLine($"Loaded {res3} non icd coded study conditions, for existing studies");
        _loggingHelper.LogLine("");
        db.ExecuteSQL("DROP TABLE IF EXISTS nk.existing_coded_data; " +
                      "DROP TABLE IF EXISTS nk.existing_non_coded_data;");
        db.ExecuteSQL("DROP TABLE IF EXISTS nk.source_data; DROP TABLE IF EXISTS nk.existing_data;");
        return res1 + res2 + res3;
    }


    public int LoadStudyFeatures(string ftw_schema_name)
    {
        string destFields = studyDestFields["study_features"];
        string sourceFields = studySourceFields["study_features"];
        
        // For 'preferred' study Ids add all features.

        string sql_string = $@"INSERT INTO st.study_features({destFields})
                SELECT k.study_id, {sourceFields}
                FROM nk.new_studies k
                INNER JOIN {ftw_schema_name}.study_features s
                on k.sd_sid = s.sd_sid ";

        int res1 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "study_features", " where ", "new studies");
        _loggingHelper.LogLine($"Loaded {res1} mesh coded study features, new studies");
        
        // For 'existing studies' study Ids, if there are any, add only new features.
        
        if (nonpref_number == 0)
        {
            _loggingHelper.LogLine("");
            return res1;
        }
        
        CreateSourceDataTable(ftw_schema_name, "study_features");
        CreateExistingDataTable("st", "study_features", " c.feature_value_id ");
        sql_string = $@"INSERT INTO st.study_features({destFields})
                   SELECT s.study_id, {sourceFields}
                   FROM nk.source_data s
                   LEFT JOIN nk.existing_data e
                   ON s.sd_sid = e.sd_sid
                   AND s.feature_value_id = e.feature_value_id
                   WHERE e.study_id is null ";

        int res2 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "study_features", " and ", "existing studies");
        _loggingHelper.LogLine($"Loaded {res2} study features, for existing studies");
        _loggingHelper.LogLine("");
        db.ExecuteSQL("DROP TABLE IF EXISTS nk.source_data; DROP TABLE IF EXISTS nk.existing_data;");
        return res1 + res2;
    }


    public int LoadStudyRelationShips(string ftw_schema_name)
    {
        string destFields = studyDestFields["study_relationships"];
        string sourceFields = studySourceFields["study_relationships"];
        
        // Loading relationships is more complex because the target study (an sd_sid in the source db)
        // also needs to be converted to an integer study id. Because all DB study relationships are within
        // the same DB, and all main study records in the source db (and therefore all target sd_sids)
        // have already been allocated a study id, it is easiest to update the target sd_sids once,
        // before any transfer takes place. Rather than doing it in the source DB, a temporary table with the
        // relationship data is created and then modified.

        string sql_string = @"DROP TABLE IF EXISTS st.temp_relationships;
        CREATE TABLE st.temp_relationships(
              sd_sid                 VARCHAR         NOT NULL
            , relationship_type_id   INT             NULL
            , target_sd_sid          VARCHAR         NULL            
            , target_study_id        INT             NULL
            , aggregated_on          TIMESTAMPTZ     NOT NULL DEFAULT Now()
        ) ";
        db.ExecuteSQL(sql_string);
        
        sql_string = $@"Insert into st.temp_relationships (sd_sid, relationship_type_id, target_sd_sid)
                        select sd_sid, relationship_type_id, target_sd_sid from {ftw_schema_name}.study_relationships ";
        db.ExecuteSQL(sql_string);

        sql_string = @"UPDATE st.temp_relationships tr
                       SET target_study_id = tsi.study_id
                       from nk.temp_study_ids tsi
                       where tr.target_sd_sid = tsi.sd_sid ";
        db.ExecuteSQL(sql_string);
                   
        // For 'preferred' study Ids add all relationships.

        sql_string = $@"INSERT INTO st.study_relationships({destFields})
                SELECT k.study_id, {sourceFields}
                FROM nk.new_studies k
                INNER JOIN st.temp_relationships s
                on k.sd_sid = s.sd_sid ";

        int res1 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "study_relationships", " where ", "new studies");
        _loggingHelper.LogLine($"Loaded {res1} study relationships, new studies");
        
        // For 'existing studies' study Ids, if there are any, add only new relationships.
        
        if (nonpref_number == 0)
        {
            _loggingHelper.LogLine("");
            return res1;
        }
        
        CreateSourceDataTable("st", "temp_relationships");
        CreateExistingDataTable("st", "study_relationships", " c.relationship_type_id, c.target_study_id ");
        sql_string = $@"INSERT INTO st.study_relationships({destFields})
                   SELECT s.study_id, {sourceFields}
                   FROM nk.source_data s
                   LEFT JOIN nk.existing_data e
                   ON s.sd_sid = e.sd_sid
                   AND s.relationship_type_id = e.relationship_type_id
                   and s.target_study_id = e.target_study_id
                   WHERE e.study_id is null ";

        int res2 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "study_relationships", " and ", "existing studies");
        _loggingHelper.LogLine($"Loaded {res2} study relationships, for existing studies");
        _loggingHelper.LogLine("");
        db.ExecuteSQL(@"DROP TABLE IF EXISTS nk.source_data; 
                    DROP TABLE IF EXISTS nk.existing_data;
                    DROP TABLE IF EXISTS st.temp_relationships; ");
        return res1 + res2;
    }

    
    public int LoadStudyCountries(string ftw_schema_name)
    {
        string destFields = studyDestFields["study_countries"];
        string sourceFields = studySourceFields["study_countries"];
        
        // For 'preferred' study Ids add all countries.

        string sql_string = $@"INSERT INTO st.study_countries({destFields})
                SELECT k.study_id, {sourceFields}
                FROM nk.new_studies k
                INNER JOIN {ftw_schema_name}.study_countries s
                on k.sd_sid = s.sd_sid ";

        int res1 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "study_countries", " where ", "new studies");
        _loggingHelper.LogLine($"Loaded {res1} study countries, new studies");
       
        // For 'existing studies' study Ids, if there are any, add only new countries.
        
        if (nonpref_number == 0)
        {
            _loggingHelper.LogLine("");
            return res1;
        }
        
        CreateSourceDataTable(ftw_schema_name, "study_countries");
        CreateExistingDataTable("st", "study_countries", " c.country_name ");
        sql_string = $@"INSERT INTO st.study_countries({destFields})
                   SELECT s.study_id, {sourceFields}
                   FROM nk.source_data s
                   LEFT JOIN nk.existing_data e
                   ON s.sd_sid = e.sd_sid
                   AND s.country_name = e.country_name
                   WHERE e.study_id is null ";

        int res2 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "study_countries"," and ", "existing studies");
        _loggingHelper.LogLine($"Loaded {res2} study_countries , for existing studies");
        _loggingHelper.LogLine("");
        db.ExecuteSQL("DROP TABLE IF EXISTS nk.source_data; DROP TABLE IF EXISTS nk.existing_data;");
        return res1 + res2;
    }

    public int LoadStudyLocations(string ftw_schema_name)
    {
        string destFields = studyDestFields["study_locations"];
        string sourceFields = studySourceFields["study_locations"];
        
        // For 'preferred' study Ids add all locations.

        string sql_string = $@"INSERT INTO st.study_locations({destFields})
                SELECT k.study_id, {sourceFields}
                FROM nk.new_studies k
                INNER JOIN {ftw_schema_name}.study_locations s
                on k.sd_sid = s.sd_sid ";

        int res1 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "study_locations", " where ", "new studies");
        _loggingHelper.LogLine($"Loaded {res1} study locations, new studies");
        
        // For 'existing studies' study Ids, if there are any, add only new locations.

        if (nonpref_number == 0)
        {
            _loggingHelper.LogLine("");
            return res1;
        }
        
        CreateSourceDataTable(ftw_schema_name, "study_locations");
        CreateExistingDataTable("st", "study_locations", " c.city_name, c.country_name ");
        sql_string = $@"INSERT INTO st.study_locations({destFields})
                   SELECT s.study_id, {sourceFields}
                   FROM nk.source_data s
                   LEFT JOIN nk.existing_data e
                   ON s.sd_sid = e.sd_sid
                   AND s.city_name = e.city_name
                   AND s.country_name = e.country_name
                   WHERE e.study_id is null ";

        int res2 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "study_locations", " and ", "existing studies");
        _loggingHelper.LogLine($"Loaded {res2} study locations, for existing studies");
        _loggingHelper.LogLine("");
        return res1 + res2;
    }

    public void DropTempStudyIdsTable()
    {
        string sql_string = @"DROP TABLE IF EXISTS nk.temp_study_ids;
                                  DROP TABLE IF EXISTS nk.new_studies;
                                  DROP TABLE IF EXISTS nk.existing_studies;
                                  DROP TABLE IF EXISTS nk.source_data;
                                  DROP TABLE IF EXISTS nk.existing_data;";
        db.ExecuteSQL(sql_string);
    }
}