using Dapper;
using Npgsql;
using PostgreSQLCopyHelper;
namespace MDR_Aggregator;

public class ObjectDataTransferrer
{
    readonly string _connString;
    readonly DBUtilities db;
    readonly ILoggingHelper _loggingHelper;

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
                    object_id                INT
                  , source_id                INT
                  , sd_oid                   VARCHAR
                  , parent_study_source_id   INT 
                  , parent_study_sd_sid      VARCHAR
                  , parent_study_id          INT
                  , is_preferred_study       BOOLEAN
                  , datetime_of_data_fetch   TIMESTAMPTZ
                  ); ";
        conn.Execute(sql_string);

        sql_string = @"DROP TABLE IF EXISTS nk.temp_objects_to_add;
                  CREATE TABLE IF NOT EXISTS nk.temp_objects_to_add(
                    object_id                INT
                  , sd_oid                   VARCHAR
                  ); 
                  CREATE INDEX temp_objects_to_add_sd_oid on nk.temp_objects_to_add(sd_oid);";
        conn.Execute(sql_string);
    }


    public IEnumerable<ObjectId> FetchObjectIds(int source_id, string source_conn_string)
    {
        using var conn = new NpgsqlConnection(source_conn_string);
        string sql_string = @"select " + source_id.ToString() + @" as source_id, " 
                            + source_id.ToString() + @" as parent_study_source_id, 
                      sd_oid, sd_sid as parent_study_sd_sid, datetime_of_data_fetch
                      from ad.data_objects";

        return conn.Query<ObjectId>(sql_string);
    }


    public ulong StoreObjectIds(PostgreSQLCopyHelper<ObjectId> copyHelper, IEnumerable<ObjectId> entities)
    {
        using var conn = new NpgsqlConnection(_connString);
        conn.Open();
        return copyHelper.SaveAll(conn, entities);
    }


    public void UpdateObjectsWithStudyIds(int source_id)
    {
        // Update the object parent study_id using the 'correct'
        // value found in the all_ids_studies table

        using var conn = new NpgsqlConnection(_connString);
        string sql_string = @"UPDATE nk.temp_object_ids t
                       SET parent_study_id = s.study_id, 
                       is_preferred_study = s.is_preferred
                       FROM nk.all_ids_studies s
                       WHERE t.parent_study_sd_sid = s.sd_sid
                       and t.parent_study_source_id = s.source_id;";
        conn.Execute(sql_string);

        // Drop those link records that cannot be matched

        sql_string = @"DELETE FROM nk.temp_object_ids
                         WHERE parent_study_id is null;";
        conn.Execute(sql_string);
    }


    public void CheckStudyObjectsForDuplicates(int source_id)
    {
        // TO DO - very rare at the moment
    }


    public void UpdateAllObjectIdsTable(int source_id)
    {
        // Add the new object id records to the all Ids table
        // For study based data, the assumption here is that within each source 
        // the data object sd_oid is unique, (because they are each linked to different studies) 
        // which means that the link is also unique.
        // BUT FOR PUBMED and other data object based data this is not true
        // therefore need to do the ResetIdsOfDuplicatedPMIDs later

        using var conn = new NpgsqlConnection(_connString);
        string sql_string = @"INSERT INTO nk.all_ids_data_objects
                         (source_id, sd_oid, parent_study_source_id, parent_study_sd_sid,
                         parent_study_id, is_preferred_study, datetime_of_data_fetch)
                         select source_id, sd_oid, parent_study_source_id, parent_study_sd_sid,
                         parent_study_id, is_preferred_study, datetime_of_data_fetch
                         from nk.temp_object_ids";
        conn.Execute(sql_string);

        // update the table with the object id (will always be the same as the 
        // identity at the moment as there is no object-object checking
        // If objects are amalgamated from different sources in the future
        // the object-object check will need to be added at this stage

        sql_string = @"UPDATE nk.all_ids_data_objects
                        SET object_id = id
                        WHERE source_id = " + source_id + @"
                        and object_id is null;";
        conn.Execute(sql_string);
    }


    public void FillObjectsToAddTable(int source_id)
    {
        using var conn = new NpgsqlConnection(_connString);
        string sql_string = @"INSERT INTO nk.temp_objects_to_add
                         (object_id, sd_oid)
                         SELECT distinct object_id, sd_oid 
                         FROM nk.all_ids_data_objects
                         WHERE source_id = " + source_id;
        conn.Execute(sql_string);
    }

    private readonly Dictionary<string, string> objectFields = new() 
    {
        {"data_objects", @"id, title, version, display_title, doi, doi_status_id, publication_year,
        object_class_id, object_type_id, managing_org_id, managing_org, managing_org_ror_id, 
        lang_code, access_type_id, access_details, access_details_url, url_last_checked, 
        eosc_category, add_study_contribs, add_study_topics"},
        {"data_objects_source",@"s.title, s.version, s.display_title, s.doi, s.doi_status_id, s.publication_year,
        s.object_class_id, s.object_type_id, s.managing_org_id, s.managing_org, s.managing_org_ror_id, 
        s.lang_code, s.access_type_id, s.access_details, s.access_details_url, s.url_last_checked, 
        s.eosc_category, s.add_study_contribs, s.add_study_topics"},
        {"object_datasets", @"record_keys_type_id, record_keys_details, deident_type_id, 
        deident_direct, deident_hipaa, deident_dates, deident_nonarr, deident_kanon, deident_details,
        consent_type_id, consent_noncommercial, consent_geog_restrict,
        consent_research_type, consent_genetic_only, consent_no_methods, consent_details" },
        { "object_instances", @"system_id, system, url, url_accessible, url_last_checked, 
        resource_type_id, resource_size, resource_size_units, resource_comments" },
        { "object_titles", @"title_type_id, title_text, lang_code, lang_usage_id, is_default, comments" },
        { "object_dates", @"date_type_id, date_is_range, date_as_string, start_year, 
        start_month, start_day, end_year, end_month, end_day, details" },
        { "object_people", @" contrib_type_id, person_id, person_given_name, 
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
        
        
    public int LoadDataObjects(string schema_name)
    {
        string destFields = objectFields["data_objects"];
        string srceFields = objectFields["data_objects_source"];
        
         string sql_string = $@"INSERT INTO ob.data_objects({destFields})
                SELECT t.object_id, {srceFields}
                FROM " + schema_name + @".data_objects s
                INNER JOIN nk.temp_objects_to_add t
                on s.sd_oid = t.sd_oid ";

        int res = db.ExecuteTransferSQL(sql_string, schema_name, "data_objects", "");
        _loggingHelper.LogLine($"Loaded records - {res} data_objects");
        db.Update_SourceTable_ExportDate(schema_name, "data_objects");
        return res;
    }


    public void LoadObjectDatasets(string schema_name)
    {
        string fieldList = objectFields["object_datasets"];
        
        string sql_string = $@"INSERT INTO ob.object_datasets(object_id, ({fieldList})
        SELECT t.object_id, {fieldList}
        FROM {schema_name}.object_datasets s
        INNER JOIN nk.temp_objects_to_add t
        on s.sd_oid = t.sd_oid ";

        int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_datasets", "");
        _loggingHelper.LogLine($"Loaded records - {res} object datasets");
        db.Update_SourceTable_ExportDate(schema_name, "object_datasets");
    }


    public void LoadObjectInstances(string schema_name)
    {
        string fieldList = objectFields["object_instances"];
        
        string sql_string = $@"INSERT INTO ob.object_instances(object_id, ({fieldList})
        SELECT t.object_id, {fieldList}
        FROM {schema_name}.object_instances s
        INNER JOIN nk.temp_objects_to_add t
        on s.sd_oid = t.sd_oid ";

        int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_instances", "");
        _loggingHelper.LogLine($"Loaded records - {res} object_instances");
        db.Update_SourceTable_ExportDate(schema_name, "object instances");
    }


    public void LoadObjectTitles(string schema_name)
    {
        string fieldList = objectFields["object_titles"];
        
        string sql_string = $@"INSERT INTO ob.object_titles(object_id, ({fieldList})
        SELECT t.object_id, {fieldList}
        FROM {schema_name}.object_titles s
        INNER JOIN nk.temp_objects_to_add t
        on s.sd_oid = t.sd_oid ";

        int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_titles", "");
        _loggingHelper.LogLine($"Loaded records - {res} object titles");
        db.Update_SourceTable_ExportDate(schema_name, "object_titles");
    }


    public void LoadObjectDates(string schema_name)
    {
        string fieldList = objectFields["object_dates"];
        
        string sql_string = $@"INSERT INTO ob.object_dates(object_id, ({fieldList})
        SELECT t.object_id, {fieldList}
        FROM {schema_name}.object_dates s
        INNER JOIN nk.temp_objects_to_add t
        on s.sd_oid = t.sd_oid ";

        int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_dates", "");
        _loggingHelper.LogLine($"Loaded records - {res} object dates");
        db.Update_SourceTable_ExportDate(schema_name, "object_dates");
    }


    public void LoadObjectPeople(string schema_name)
    {
        string fieldList = objectFields["object_people"];
        
        string sql_string = $@"INSERT INTO ob.object_people(object_id, ({fieldList})
        SELECT t.object_id, {fieldList}
        FROM {schema_name}.object_people s
        INNER JOIN nk.temp_objects_to_add t
        on s.sd_oid = t.sd_oid ";

        int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_people", "");
        _loggingHelper.LogLine($"Loaded records - {res} object people");
        db.Update_SourceTable_ExportDate(schema_name, "object_people");
    }

    
    public void LoadObjectOrganisations(string schema_name)
    {
        string fieldList = objectFields["object_organisations"];
        
        string sql_string = $@"INSERT INTO ob.object_organisations(object_id, ({fieldList})
        SELECT t.object_id, {fieldList}
        FROM {schema_name}.object_organisations s
        INNER JOIN nk.temp_objects_to_add t
        on s.sd_oid = t.sd_oid ";

        int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_organisations", "");
        _loggingHelper.LogLine($"Loaded records - {res} object organisations");

        db.Update_SourceTable_ExportDate(schema_name, "object_organisations");
    }


    public void LoadObjectTopics(string schema_name)
    {
        string fieldList = objectFields["object_topics"];
        
        string sql_string = $@"INSERT INTO ob.object_topics(object_id, ({fieldList})
        SELECT t.object_id, {fieldList}
        FROM {schema_name}.object_topics s
        INNER JOIN nk.temp_objects_to_add t
        on s.sd_oid = t.sd_oid ";

        int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_topics", "");
        _loggingHelper.LogLine($"Loaded records - {res} object topics");

        db.Update_SourceTable_ExportDate(schema_name, "object_topics");
    }


    public void LoadObjectDescriptions(string schema_name)
    {
        string fieldList = objectFields["object_descriptions"];
        
        string sql_string = $@"INSERT INTO ob.object_descriptions(object_id, ({fieldList})
        SELECT t.object_id, {fieldList}
        FROM {schema_name}.object_descriptions s
        INNER JOIN nk.temp_objects_to_add t
        on s.sd_oid = t.sd_oid ";

        int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_descriptions", "");
        _loggingHelper.LogLine($"Loaded records - {res} object descriptions");
        db.Update_SourceTable_ExportDate(schema_name, "object_descriptions");
    }


    public void LoadObjectIdentifiers(string schema_name)
    {
        string fieldList = objectFields["object_identifiers"];
        
        string sql_string = $@"INSERT INTO ob.object_identifiers(object_id, ({fieldList})
        SELECT t.object_id, {fieldList}
        FROM {schema_name}.object_identifiers s
        INNER JOIN nk.temp_objects_to_add t
        on s.sd_oid = t.sd_oid ";

        int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_identifiers", "");
        _loggingHelper.LogLine($"Loaded records - {res} object identifiers");
        db.Update_SourceTable_ExportDate(schema_name, "object_identifiers");
    }



    public void LoadObjectRelationships(string schema_name)
    {
        string fieldList = objectFields["object_relationships"];
        
        string sql_string = $@"INSERT INTO ob.object_relationships(object_id, ({fieldList})
        SELECT t.object_id, {fieldList}
        FROM {schema_name}.object_relationships s
        INNER JOIN nk.temp_objects_to_add t
        on s.sd_oid = t.sd_oid ";

        // NEED TO DO UPDATE OF TARGET SEPARATELY

        int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_relationships", "");
        _loggingHelper.LogLine($"Loaded records - {res} object relationships");
        db.Update_SourceTable_ExportDate(schema_name, "object_relationships");
    }



    public void LoadObjectRights(string schema_name)
    {
        string fieldList = objectFields["object_rights"];
        
        string sql_string = $@"INSERT INTO ob.object_rights(object_id, ({fieldList})
        SELECT t.object_id, {fieldList}
        FROM {schema_name}.object_rights s
        INNER JOIN nk.temp_objects_to_add t
        on s.sd_oid = t.sd_oid ";

        int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_rights", "");
        _loggingHelper.LogLine($"Loaded records - {res} object rights");
        db.Update_SourceTable_ExportDate(schema_name, "object_rights");
    }


    public void DropTempObjectIdsTable()
    {
        using var conn = new NpgsqlConnection(_connString);
        string sql_string = @"DROP TABLE IF EXISTS nk.temp_object_ids;
                      DROP TABLE IF EXISTS nk.temp_objects_to_add;";
        conn.Execute(sql_string);
    }

}