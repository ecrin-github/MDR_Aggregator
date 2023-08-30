using Dapper;
using Npgsql;
using PostgreSQLCopyHelper;
namespace MDR_Aggregator;

public class ObjectDataTransferrer
{
    readonly string _connString;
    readonly DBUtilities db;
    readonly ILoggingHelper _loggingHelper;
    int num_to_check;
    int status1number, status2number, status3number;
    
    public ObjectDataTransferrer(string connString, ILoggingHelper logginghelper)
    {
        _connString = connString;
        _loggingHelper = logginghelper;
        db = new DBUtilities(connString, _loggingHelper);
    }

    public void SetUpTempObjectIdsTables()
    {
        using var conn = new NpgsqlConnection(_connString);
        string sql_string = @"DROP TABLE IF EXISTS nk.temp_object_ids;
                      CREATE TABLE IF NOT EXISTS nk.temp_object_ids(
                        id                       INT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY
                      , object_id                INT
                      , source_id                INT
                      , sd_oid                   VARCHAR
                      , object_type_id		  	 INT            
                      , title                    VARCHAR      
                      , is_preferred_object      BOOLEAN
                      , parent_study_source_id   INT 
                      , parent_study_sd_sid      VARCHAR
                      , parent_study_id          INT
                      , is_preferred_study       BOOLEAN
                      , datetime_of_data_fetch   TIMESTAMPTZ
                      , match_status             INT   default 0
                      );
                 CREATE INDEX temp_object_ids_objectid ON nk.temp_object_ids(object_id);
                 CREATE INDEX temp_object_ids_sdidsource ON nk.temp_object_ids(source_id, sd_oid);
                 CREATE INDEX temp_object_ids_parent_study_sdidsource 
                             ON nk.temp_object_ids(parent_study_source_id, parent_study_sd_sid);";

        conn.Execute(sql_string);
                 
        sql_string = @"DROP TABLE IF EXISTS nk.temp_objects_to_add;
                      CREATE TABLE IF NOT EXISTS nk.temp_objects_to_add(
                        id                       INT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY
                      , object_id                INT
                      , sd_oid                   VARCHAR
                      , object_type_id		  	 INT            
                      , title                    VARCHAR      
                      , is_preferred_object      BOOLEAN
                      ); 
                      CREATE INDEX temp_objects_to_add_sd_oid on nk.temp_objects_to_add(sd_oid);";

        conn.Execute(sql_string);

        sql_string = @"DROP TABLE IF EXISTS nk.temp_objects_to_check;
                      CREATE TABLE IF NOT EXISTS nk.temp_objects_to_check(
                        id                       INT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY
                      , object_id                INT
                      , sd_oid                   VARCHAR
                      , object_type_id		  	 INT            
                      , title                    VARCHAR      
                      , is_preferred_object      BOOLEAN
                      ); 
                      CREATE INDEX temp_objects_to_check_sd_oid on nk.temp_objects_to_check(sd_oid);";

        conn.Execute(sql_string);
    }

    public ulong FetchObjectIds(int source_id, string source_conn_string)
    {
        string sql_string = $"select max(id) FROM ad.data_objects";
        using var conn = new NpgsqlConnection(source_conn_string);
        int max_id = conn.ExecuteScalar<int>(sql_string);
        int batch_size = 100000;
        try
        {
            ulong stored = 0;
            sql_string = $@"select {source_id} as source_id, {source_id} as parent_study_source_id, 
                          sd_oid, object_type_id, title, sd_sid as parent_study_sd_sid, 
                          datetime_of_data_fetch
                          from ad.data_objects t ";
            if (max_id > batch_size)
            {
                for (int r = 1; r <= max_id; r += batch_size)
                {
                    string batch_sql_string = sql_string + $" where t.id >= {r} and t.id < {r + batch_size} ";
                    IEnumerable<ObjectId> object_ids = conn.Query<ObjectId>(batch_sql_string);
                    ulong num_stored = StoreObjectIds(CopyHelpers.object_ids_helper, object_ids);
                    stored += num_stored;
                    int e = r + batch_size < max_id ? r + batch_size - 1 : max_id;
                    _loggingHelper.LogLine($"Obtained {num_stored} object ids, from ids {r} to {e}");
                }
            }
            else
            {
                IEnumerable<ObjectId> object_ids = conn.Query<ObjectId>(sql_string);
                stored = StoreObjectIds(CopyHelpers.object_ids_helper, object_ids);
                _loggingHelper.LogLine($"Obtained {stored} object ids as a single batch");
            }
            return stored;
        } 
        catch (Exception e)
        {
            _loggingHelper.LogError($"In obtaining object ids: {e.Message}");
            return 0;
        }
    }
    
    
    public ulong StoreObjectIds(PostgreSQLCopyHelper<ObjectId> copyHelper, IEnumerable<ObjectId> entities)
    {
        using var conn = new NpgsqlConnection(_connString);
        conn.Open();
        return copyHelper.SaveAll(conn, entities);
    }
    
    public void MatchExistingObjectIds(int source_id)
    {
        // Do these source-object id combinations already exist in the system, i.e. have a known id?
        // If they do they can be matched, to leave only the new object ids to process

        string sql_string = $@"UPDATE nk.temp_object_ids t
                SET object_id = doi.object_id, 
                is_preferred_object = doi.is_preferred_object,
                parent_study_id = doi.parent_study_id,
                is_preferred_study = doi.is_preferred_study,
                match_status = 1
                from nk.data_object_ids doi
                where doi.source_id = {source_id} 
                and t.sd_oid = doi.sd_oid ";

        db.Update_UsingTempTable("nk.temp_object_ids", "nk.temp_object_ids", sql_string, " and "
                                 , 25000, ", with object details, status = 1, for matched objects");
        _loggingHelper.LogLine("Existing objects matched in temp table");
        _loggingHelper.LogLine("");
        
        // Also update the data_object_identifiers table. Indicates has been matched
        // and updates the data fetch date

        sql_string = $@"UPDATE nk.data_object_ids doi
        set match_status = 1,
        datetime_of_data_fetch = t.datetime_of_data_fetch
        from nk.temp_object_ids t
        where doi.source_id = {source_id} 
        and t.sd_oid = doi.sd_oid ";

        status1number = db.Update_UsingTempTable("nk.temp_object_ids", "data_object_identifiers", 
                  sql_string, " and ", 25000, ", with status = 1, latest data fetch time");
        _loggingHelper.LogLine($"{status1number} existing objects matched in identifiers table");
        _loggingHelper.LogLine("");
    }


    public void UpdateNewObjectsWithStudyIds(int source_id)
    {
        // For the new objects...where match_status in temp table still 0...
        // Update the object parent study_id using the 'correct' value found in the study_ids table

        string sql_string = $@"UPDATE nk.temp_object_ids t
                    SET parent_study_id = si.study_id, 
                    is_preferred_study = si.is_preferred
                    FROM nk.study_ids si
                    WHERE t.parent_study_sd_sid = si.sd_sid
                    and t.parent_study_source_id = {source_id}";

        int res = db.Update_UsingTempTable("nk.temp_object_ids", "temp_object_ids", 
                 sql_string, " and ", 25000, ", with study id and 'is preferred' status");
        _loggingHelper.LogLine($"{res} objects updated with parent study details");

        // Drop those object records that cannot be matched
        // N.B. study linked object records only - Pubmed objects do not travel down this path

        sql_string = @"DELETE FROM nk.temp_object_ids
                        WHERE parent_study_id is null;";
        res = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{res} objects dropped because of a missing matching study");
        _loggingHelper.LogLine("");
    }


    public void AddNewObjectsToIdentifiersTable(int source_id)
    {
        // Add all the new object id records to the all Ids table

        string sql_string = @"INSERT INTO nk.data_object_ids
                        (source_id, sd_oid, object_type_id, title, is_preferred_object,
                        parent_study_source_id, parent_study_sd_sid,
                        parent_study_id, is_preferred_study, datetime_of_data_fetch)
                        select 
                        source_id, sd_oid, object_type_id, title, is_preferred_object,
                        parent_study_source_id, parent_study_sd_sid,
                        parent_study_id, is_preferred_study, datetime_of_data_fetch
                        from nk.temp_object_ids t
                        where match_status = 0 ";

        int res = db.Update_UsingTempTable("nk.temp_object_ids", "data_object_identifiers", sql_string, " and "
                                 , 25000, ", adding new data object ids");
        _loggingHelper.LogLine($"{res} Non-matched objects inserted into object identifiers table");
        _loggingHelper.LogLine("");
        
        // For study based data, if the study is 'preferred' it is the first time that it
        // and related data objects can be added to the database, so set the object id to
        // the table id and set the match_status to 2

        sql_string = $@"UPDATE nk.data_object_ids
                    SET object_id = id, is_preferred_object = true,
                    match_status = 2
                    WHERE object_id is null
                    AND source_id = {source_id}
                    AND is_preferred_study = true;";

        status2number = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{status2number} objects identified as new additions from 'preferred' studies");
        _loggingHelper.LogLine("");
        
        // For data objects from 'non-preferred' studies, there may be duplicate data objects
        // already in the system, though that will not apply to registry linked objects such as
        // registry entries (13), results entries (28), web landing pages (38)

        sql_string = $@"UPDATE nk.data_object_ids
                    SET object_id = id, is_preferred_object = true,
                    match_status = 3
                    WHERE object_id is null
                    AND source_id = {source_id}
                    AND object_type_id in (13, 28);";
        int res1 = db.ExecuteSQL(sql_string);

        if (source_id is 101900 or 101901)  // BioLINCC or Yoda 
        {
            sql_string = $@"UPDATE nk.data_object_ids
                    SET object_id = id, is_preferred_object = true,
                    match_status = 3
                    WHERE object_id is null
                    AND source_id = {source_id}
                    AND object_type_id in (38);";
            res1 += db.ExecuteSQL(sql_string);
        }

        _loggingHelper.LogLine($"{res1} 'always added' objects identified as new additions from 'non-preferred' studies");
        status3number = res1;
    }


    public void CheckNewObjectsForDuplicateTitles(int source_id)
    {
        // Any new data object records that need to be checked as potential duplicates?
        // Duplicates may be picked up from considering type and title within the same study
        // First need to identify the (distinct) existing studies that currently have unmatched objects 

        string sql_string = $@"drop table if exists nk.studies_with_poss_dup_objects;
              create table nk.studies_with_poss_dup_objects as 
              select distinct parent_study_id 
              from nk.data_object_ids
              WHERE object_id is null
              AND source_id = {source_id}";
        db.ExecuteSQL(sql_string);

        // Identify data object records that have the same type and title as existing records
        // for those studies. Use that table to update the data_object_ids table to insert the
        // existing object id and tho set the is_valid_link field to false.

        sql_string = $@"drop table if exists nk.dup_objects_by_type_and_title;
              create table nk.dup_objects_by_type_and_title
              as 
            select existing.object_id as old_object_id, new.id 
            from
               (select doi.* from nk.studies_with_poss_dup_objects p
               inner join nk.data_object_ids doi
               on p.parent_study_id = doi.parent_study_id
               where doi.parent_study_source_id <> {source_id}) existing
            inner join
              (select doi2.* from nk.data_object_ids doi2
              WHERE object_id is null
              AND source_id = {source_id}) new
            on existing.parent_study_id = new.parent_study_id
            and existing.object_type_id = new.object_type_id
            and existing.title = new.title";

        db.ExecuteSQL(sql_string);

        sql_string = @"Update nk.data_object_ids doi
        set object_id = old_object_id, is_preferred_object = false, 
        is_valid_link = false, match_status = 4
        from nk.dup_objects_by_type_and_title dup
        where doi.match_status is null
        and doi.id = dup.id;";

        int res = db.ExecuteSQL(sql_string);
        status2number += res;
        string feedback_text = "objects from 'non-preferred' studies identified as duplicates using type and title";
        _loggingHelper.LogLine($"{res} {feedback_text}");
    }


    public void CheckNewObjectsForDuplicateURLs(int source_id, string ftw_schema_name)
    {
        // Duplicates may also be picked up by finding the same instance URL within the same study
        // Use the (distinct) studies that currently have unmatched objects, as above. If a 
        // duplicate found update the object id to that of the existing object and indicate a non-valid link.

        string sql_string = $@"drop table if exists nk.dup_objects_by_url;
              create table nk.dup_objects_by_url
              as 
              select existing.object_id as old_object_id, new.id 
                 from
               (select doi.*, i.url from nk.studies_with_poss_dup_objects p
               inner join nk.data_object_ids doi
               on p.parent_study_id = doi.parent_study_id
               inner join ob.object_instances i
               on doi.object_id = i.object_id
               where i.url is not null) existing
             inner join
             (select doi2.*, i.url from nk.data_object_ids doi2
              inner join {ftw_schema_name}.object_instances i
              on doi2.sd_oid = i.sd_oid
              WHERE doi2.object_id is null
              AND doi2.source_id = {source_id}) new
             on existing.parent_study_id = new.parent_study_id
             and existing.url = new.url";

        db.ExecuteSQL(sql_string);

        sql_string = @"Update nk.data_object_ids doi
        set object_id = old_object_id, is_preferred_object = false, 
        is_valid_link = false, match_status = 4
        from nk.dup_objects_by_url dup
        where doi.match_status is null
        and doi.id = dup.id;";

        int res = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{res} objects from 'non-preferred' studies identified as duplicates using url");
        status2number += res;

         // tidy up temp tables
         sql_string = @"drop table if exists nk.studies_with_poss_dup_objects;
                     drop table if exists nk.dup_objects_by_type_and_title;
                     drop table if exists nk.dup_objects_by_url";
        db.ExecuteSQL(sql_string);
    }


    public void CompleteNewObjectsStatuses(int source_id)
    {
        // Complete setting status of new objects. Any still null must be genuinely new

        string sql_string = $@"Update nk.data_object_ids doi
        set object_id = id, is_preferred_object = true, 
        match_status = 3
        where doi.match_status is null
        AND source_id = {source_id}";

        int res = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{res} remaining objects from 'non-preferred' studies identified as new objects");
        status3number += res;
    }


    public void FillObjectsToAddTables(int source_id)
    {
        int total_objects = status1number + status2number + status3number;
        _loggingHelper.LogLine($"{total_objects} total objects found");
            
        string sql_string = $@"INSERT INTO nk.temp_objects_to_add
                            (object_id, sd_oid)
                            SELECT distinct object_id, sd_oid 
                            FROM nk.data_object_ids
                            WHERE is_preferred_object = true and 
                            source_id = {source_id}";
        int res = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{res} objects to be added");

        sql_string = $@"INSERT INTO nk.temp_objects_to_check
                            (object_id, sd_oid)
                            SELECT distinct object_id, sd_oid 
                            FROM nk.data_object_ids
                            WHERE is_preferred_object = false
                            and source_id = {source_id}";
        res = db.ExecuteSQL(sql_string);
        num_to_check = res;
        if (num_to_check > 0)
        {
            _loggingHelper.LogLine($"{res} objects identifies as likely duplicates");
            _loggingHelper.LogLine("and will not be added (but attributes may be checked)");
        }
    }

    
    private readonly Dictionary<string, string> objectDestFields = new() 
    {
        {"data_objects", @"id, title, version, display_title, doi, doi_status_id, publication_year,
        object_class_id, object_type_id, managing_org_id, managing_org, managing_org_ror_id, 
        lang_code, access_type_id, access_details, access_details_url, url_last_checked, 
        eosc_category, add_study_contribs, add_study_topics"},
        {"object_datasets", @"record_keys_type_id, record_keys_details, deident_type_id, 
        deident_direct, deident_hipaa, deident_dates, deident_nonarr, deident_kanon, deident_details,
        consent_type_id, consent_noncommercial, consent_geog_restrict,
        consent_research_type, consent_genetic_only, consent_no_methods, consent_details" },
        { "object_instances", @"system_id, system, url, url_accessible, url_last_checked, 
        resource_type_id, resource_size, resource_size_units, resource_comments" },
        { "object_titles", @"title_type_id, title_text, lang_code, lang_usage_id, is_default, comments" },
        { "object_dates", @"date_type_id, date_is_range, date_as_string, start_year, 
        start_month, start_day, end_year, end_month, end_day, details" },
        { "object_people", @" contrib_type_id, person_given_name, 
        person_family_name, person_full_name, orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id" },
        { "object_organisations", @"contrib_type_id, organisation_id, 
        organisation_name, organisation_ror_id" },
        { "object_topics", @"topic_type_id, original_value, original_ct_type_id,
          original_ct_code, mesh_code, mesh_value" },
        { "object_descriptions", @"description_type_id, label, description_text, lang_code" },
        { "object_identifiers", @"identifier_value, identifier_type_id, 
        source_id, source, source_ror_id, identifier_date" },
        { "object_rights", @"rights_name, rights_uri, comments" },
        { "object_relationships", @"relationship_type_id, target_sd_oid" }
    };
    
    private readonly Dictionary<string, string> objectSourceFields = new() 
    {
        {"data_objects",@"s.title, s.version, s.display_title, s.doi, s.doi_status_id, s.publication_year,
        s.object_class_id, s.object_type_id, s.managing_org_id, s.managing_org, s.managing_org_ror_id, 
        s.lang_code, s.access_type_id, s.access_details, s.access_details_url, s.url_last_checked, 
        s.eosc_category, s.add_study_contribs, s.add_study_topics"},
        {"object_datasets", @"s.record_keys_type_id, s.record_keys_details, s.deident_type_id, 
        s.deident_direct, s.deident_hipaa, s.deident_dates, s.deident_nonarr, s.deident_kanon, s.deident_details,
        s.consent_type_id, s.consent_noncommercial, s.consent_geog_restrict,
        s.consent_research_type, s.consent_genetic_only, s.consent_no_methods, s.consent_details" },
        { "object_instances", @"s.system_id, s.system, s.url, s.url_accessible, s.url_last_checked, 
        s.resource_type_id, s.resource_size, s.resource_size_units, s.resource_comments" },
        { "object_titles", @"s.title_type_id, s.title_text, s.lang_code, s.lang_usage_id, s.is_default, s.comments" },
        { "object_dates", @"s.date_type_id, s.date_is_range, s.date_as_string, s.start_year, 
        s.start_month, s.start_day, s.end_year, s.end_month, s.end_day, s.details" },
        { "object_people", @" s.contrib_type_id, s.person_given_name, 
        s.person_family_name, s.person_full_name, s.orcid_id, s.person_affiliation, s.organisation_id, 
        s.organisation_name, s.organisation_ror_id" },
        { "object_organisations", @"s.contrib_type_id, s.organisation_id, 
        s.organisation_name, s.organisation_ror_id" },
        { "object_topics", @"s.topic_type_id, s.original_value, s.original_ct_type_id,
          s.original_ct_code, s.mesh_code, s.mesh_value" },
        { "object_descriptions", @"s.description_type_id, s.label, s.description_text, s.lang_code" },
        { "object_identifiers", @"s.identifier_value, s.identifier_type_id, 
        s.source_id, s.source, s.source_ror_id, s.identifier_date" },
        { "object_rights", @"s.rights_name, s.rights_uri, s.comments" },
        { "object_relationships", @"s.relationship_type_id, s.target_sd_oid" }
    };
        
    public int LoadDataObjects(string ftw_schema_name)
    {
        string destFields = objectDestFields["data_objects"];
        string srceFields = objectSourceFields["data_objects"];
        
         string sql_string = $@"INSERT INTO ob.data_objects({destFields})
                SELECT t.object_id, {srceFields}
                FROM {ftw_schema_name}.data_objects s
                INNER JOIN nk.temp_objects_to_add t
                on s.sd_oid = t.sd_oid ";

        int res = db.ExecuteTransferSQL(sql_string, ftw_schema_name,  "data_objects", " where ", "new objects");
        _loggingHelper.LogLine($"Loaded {res} data_objects");
        _loggingHelper.LogLine("");
        return res;
    }


    public int LoadObjectDatasets(string ftw_schema_name)
    {
        string destFields = objectDestFields["object_datasets"];
        string srceFields = objectSourceFields["object_datasets"];
        
        string sql_string = $@"INSERT INTO ob.object_datasets(object_id, {destFields})
        SELECT t.object_id, {srceFields}
        FROM {ftw_schema_name}.object_datasets s
        INNER JOIN nk.temp_objects_to_add t
        on s.sd_oid = t.sd_oid ";

        int res = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "object_datasets", " where ", "new objects");
        _loggingHelper.LogLine($"Loaded {res} object datasets");
        _loggingHelper.LogLine("");
        return res;
    }


    public int LoadObjectInstances(string ftw_schema_name)
    {
        string destFields = objectDestFields["object_instances"];
        string srceFields = objectSourceFields["object_instances"];
        
        // add the instances known to require adding
        
        string sql_string = $@"INSERT INTO ob.object_instances(object_id, {destFields})
        SELECT t.object_id, {srceFields}
        FROM {ftw_schema_name}.object_instances s
        INNER JOIN nk.temp_objects_to_add t
        on s.sd_oid = t.sd_oid ";

        int res1 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "object_instances", " where ", "new objects");
        _loggingHelper.LogLine($"Loaded {res1} object_instances");
        
        if (num_to_check == 0)
        {
            _loggingHelper.LogLine("");
            return res1;
        }
        
        // Add any additional instances (i.e. that have a URL not already present)
        // from objects linked to non-preferred versions of studies...
        // Source data is the instance data associated with these particular objects

        sql_string = $@"DROP TABLE IF EXISTS nk.source_data;
                       CREATE TABLE nk.source_data as 
                       SELECT es.object_id, d.* 
                       FROM {ftw_schema_name}.object_instances d
                       INNER JOIN nk.temp_objects_to_check es
                       ON d.sd_oid = es.sd_oid";
        db.ExecuteSQL(sql_string);

        // Destination data (to check against) is the current set of URLs
        // in the existing instance data for these objects

        sql_string = @"DROP TABLE IF EXISTS nk.existing_data;
                       CREATE TABLE nk.existing_data as 
                       SELECT k.sd_oid, 
                       c.object_id, c.url
                       FROM ob.object_instances c
                       INNER JOIN nk.temp_objects_to_check k
                       ON c.object_id = k.object_id;";
        db.ExecuteSQL(sql_string);

        // for urls which are new to the study - i.e. do not appear io the RHS
        // of a LEFT JOIN on oid and url, add the instance data as a new record

        sql_string = $@"INSERT INTO ob.object_instances(object_id, {destFields})
                       SELECT s.object_id, {srceFields}
                       FROM nk.source_data s
                       LEFT JOIN nk.existing_data e
                       ON s.sd_oid = e.sd_oid
                       and lower(s.url) = lower(e.url)
                       WHERE e.object_id is null ";

        int res2 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "object_instances", " and ", "existing objects");
        _loggingHelper.LogLine($"Transferred {res2} object instances, from 'non-preferred' objects");
        _loggingHelper.LogLine("");
        return res1 + res2;
    }


    public int LoadObjectTitles(string ftw_schema_name)
    {
        string destFields = objectDestFields["object_titles"];
        string srceFields = objectSourceFields["object_titles"];
        
        string sql_string = $@"INSERT INTO ob.object_titles(object_id, {destFields})
        SELECT t.object_id, {srceFields}
        FROM {ftw_schema_name}.object_titles s
        INNER JOIN nk.temp_objects_to_add t
        on s.sd_oid = t.sd_oid ";

        int res1 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "object_titles", " where ", "new objects");
        _loggingHelper.LogLine($"Loaded {res1} object titles");
        
        if (num_to_check == 0)
        {
            _loggingHelper.LogLine("");
            return res1;
        }

            // add any additional titles 

            sql_string = $@"DROP TABLE IF EXISTS nk.source_data;
                           CREATE TABLE nk.source_data as 
                           SELECT es.object_id, d.* 
                           FROM {ftw_schema_name}.object_titles d
                           INNER JOIN nk.temp_objects_to_check es
                           ON d.sd_oid = es.sd_oid";
            db.ExecuteSQL(sql_string);

            // Also, all non preferred titles must be non-default (default 
            // will be from the preferred source)

            sql_string = @"UPDATE nk.source_data 
                           SET is_default = false;";
            db.ExecuteSQL(sql_string);

            sql_string = @"DROP TABLE IF EXISTS nk.existing_data;
                           CREATE TABLE nk.existing_data as 
                           SELECT k.sd_oid, 
                           c.object_id, c.title_text 
                           FROM ob.object_titles c
                           INNER JOIN nk.temp_objects_to_check k
                           ON c.object_id = k.object_id;";
            db.ExecuteSQL(sql_string);

            // for titles which are the same as some that already exist
            // the comments field should be updated to reflect this...

            sql_string = @"UPDATE ob.object_titles t
                           set comments = t.comments || '; ' || s.comments 
                           FROM nk.source_data s
                           WHERE t.object_id = s.object_id
                           AND lower(t.title_text) = lower(s.title_text);";
            db.ExecuteSQL(sql_string);

            // for titles which are new to the study (have null on the RHS of a
            // LEFT JOIN on oid and title text, simply add them

            sql_string = $@"INSERT INTO ob.object_titles(object_id, {destFields})
                           SELECT s.object_id, {srceFields}
                           FROM nk.source_data s
                           LEFT JOIN nk.existing_data e
                           ON s.sd_oid = e.sd_oid
                           AND lower(s.title_text) = lower(e.title_text)
                           WHERE e.object_id is null ";

            int res2 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "object_titles", " and ", "existing objects");
            _loggingHelper.LogLine($"Transferred {res2} object titles, from 'non-preferred' objects");
            _loggingHelper.LogLine("");
            return res1 + res2;
    }


    public int LoadObjectDates(string ftw_schema_name)
    {
        string destFields = objectDestFields["object_dates"];
        string srceFields = objectSourceFields["object_dates"];
        
        string sql_string = $@"INSERT INTO ob.object_dates(object_id, {destFields})
        SELECT t.object_id, {srceFields}
        FROM {ftw_schema_name}.object_dates s
        INNER JOIN nk.temp_objects_to_add t
        on s.sd_oid = t.sd_oid ";

        int res1 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "object_dates", " where ", "new objects");
        _loggingHelper.LogLine($"Loaded {res1} object dates");
        
        if (num_to_check == 0)
        {
            _loggingHelper.LogLine("");
            return res1;
        }

        // add any additional dates 

        sql_string = $@"DROP TABLE IF EXISTS nk.source_data;
                           CREATE TABLE nk.source_data as 
                           SELECT es.object_id, d.* 
                           FROM {ftw_schema_name}.object_dates d
                           INNER JOIN nk.temp_objects_to_check es
                           ON d.sd_oid = es.sd_oid";
        db.ExecuteSQL(sql_string);

        sql_string = @"DROP TABLE IF EXISTS nk.existing_data;
                           CREATE TABLE nk.existing_data as 
                           SELECT k.sd_oid, 
                           c.object_id, c.start_year, c.start_month, c.start_day
                           FROM ob.object_dates c
                           INNER JOIN nk.temp_objects_to_check k
                           ON c.object_id = k.object_id;";
        db.ExecuteSQL(sql_string);

        // for dates which are new to the study (have null on the RHS of a
        // LEFT JOIN on oid and year, month and day, add them

        sql_string = $@"INSERT INTO ob.object_dates(object_id, {destFields})
                           SELECT s.object_id, {srceFields}
                           FROM nk.source_data s
                           LEFT JOIN nk.existing_data e
                           ON s.sd_oid = e.sd_oid
                           AND s.start_year = e.start_year
                           AND s.start_month = e.start_month
                           AND s.start_day = e.start_day
                           WHERE e.object_id is null ";

        int res2 = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "object_dates", " and ", "existing objects");
        _loggingHelper.LogLine($"Transferred {res2} object dates, from 'non-preferred' objects");
        _loggingHelper.LogLine("");
        return res1 + res2;
    }


    public int LoadObjectPeople(string ftw_schema_name)
    {
        string destFields = objectDestFields["object_people"];
        string srceFields = objectSourceFields["object_people"];
        
        string sql_string = $@"INSERT INTO ob.object_people(object_id, {destFields})
        SELECT t.object_id, {srceFields}
        FROM {ftw_schema_name}.object_people s
        INNER JOIN nk.temp_objects_to_add t
        on s.sd_oid = t.sd_oid ";

        int res = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "object_people", " where ", "new objects");
        _loggingHelper.LogLine($"Loaded {res} object people");
        _loggingHelper.LogLine("");
        return res;
    }

    
    public int LoadObjectOrganisations(string ftw_schema_name)
    {
        string destFields = objectDestFields["object_organisations"];
        string srceFields = objectSourceFields["object_organisations"];
        
        string sql_string = $@"INSERT INTO ob.object_organisations(object_id, {destFields})
        SELECT t.object_id, {srceFields}
        FROM {ftw_schema_name}.object_organisations s
        INNER JOIN nk.temp_objects_to_add t
        on s.sd_oid = t.sd_oid ";

        int res = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "object_organisations", " where ", "new objects");
        _loggingHelper.LogLine($"Loaded {res} object organisations");
        _loggingHelper.LogLine("");
        return res;
    }


    public int LoadObjectTopics(string ftw_schema_name)
    {
        string destFields = objectDestFields["object_topics"];
        string srceFields = objectSourceFields["object_topics"];
        
        string sql_string = $@"INSERT INTO ob.object_topics(object_id, {destFields})
        SELECT t.object_id, {srceFields}
        FROM {ftw_schema_name}.object_topics s
        INNER JOIN nk.temp_objects_to_add t
        on s.sd_oid = t.sd_oid ";

        int res = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "object_topics", " where ", "new objects");
        _loggingHelper.LogLine($"Loaded {res} object topics");
        _loggingHelper.LogLine("");
        return res;
    }


    public int LoadObjectDescriptions(string ftw_schema_name)
    {
        string destFields = objectDestFields["object_descriptions"];
        string srceFields = objectSourceFields["object_descriptions"];
        
        string sql_string = $@"INSERT INTO ob.object_descriptions(object_id, {destFields})
        SELECT t.object_id, {srceFields}
        FROM {ftw_schema_name}.object_descriptions s
        INNER JOIN nk.temp_objects_to_add t
        on s.sd_oid = t.sd_oid ";

        int res = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "object_descriptions", " where ", "new objects");
        _loggingHelper.LogLine($"Loaded {res} object descriptions");
        _loggingHelper.LogLine("");
        return res;
    }


    public int LoadObjectIdentifiers(string ftw_schema_name)
    {
        string destFields = objectDestFields["object_identifiers"];
        string srceFields = objectSourceFields["object_identifiers"];
        
        string sql_string = $@"INSERT INTO ob.object_identifiers(object_id, {destFields})
        SELECT t.object_id, {srceFields}
        FROM {ftw_schema_name}.object_identifiers s
        INNER JOIN nk.temp_objects_to_add t
        on s.sd_oid = t.sd_oid ";

        int res = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "object_identifiers"," where ", "new objects");
        _loggingHelper.LogLine($"Loaded {res} object identifiers");
        _loggingHelper.LogLine("");
        return res;
    }


    public int LoadObjectRelationships(string ftw_schema_name)
    {
        string destFields = objectDestFields["object_relationships"];
        string srceFields = objectSourceFields["object_relationships"];
        
        string sql_string = $@"INSERT INTO ob.object_relationships(object_id, {destFields})
        SELECT t.object_id, {srceFields}
        FROM {ftw_schema_name}.object_relationships s
        INNER JOIN nk.temp_objects_to_add t
        on s.sd_oid = t.sd_oid ";

        // NEED TO DO UPDATE OF TARGET SEPARATELY

        int res = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "object_relationships", " where ", "new objects");
        _loggingHelper.LogLine($"Loaded {res} object relationships");
        _loggingHelper.LogLine("");
        return res;
    }


    public int LoadObjectRights(string ftw_schema_name)
    {
        string destFields = objectDestFields["object_rights"];
        string srceFields = objectSourceFields["object_rights"];
        
        string sql_string = $@"INSERT INTO ob.object_rights(object_id, {destFields})
        SELECT t.object_id, {srceFields}
        FROM {ftw_schema_name}.object_rights s
        INNER JOIN nk.temp_objects_to_add t
        on s.sd_oid = t.sd_oid ";

        int res = db.ExecuteTransferSQL(sql_string, ftw_schema_name, "object_rights", " where ", "new objects");
        _loggingHelper.LogLine($"Loaded {res} object rights");
        _loggingHelper.LogLine("");
        return res;
    }
    
    public void DropTempObjectIdsTable()
    {
        using var conn = new NpgsqlConnection(_connString);
        string sql_string = @"DROP TABLE IF EXISTS nk.temp_object_ids;
                                  DROP TABLE IF EXISTS nk.temp_objects_to_add;
                                  DROP TABLE IF EXISTS nk.source_data;
                                  DROP TABLE IF EXISTS nk.existing_data;
                                  DROP TABLE IF EXISTS nk.temp_objects_to_check;";
        conn.Execute(sql_string);
    }
}