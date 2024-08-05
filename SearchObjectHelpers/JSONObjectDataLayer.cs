using Dapper;
using Dapper.Contrib.Extensions;
using Npgsql;
using NpgsqlTypes;

namespace MDR_Aggregator.SearchObjectHelpers;

public class JSONObjectDataLayer
{
    private readonly string _connString;

    private string? _dataObjectQueryString, _dataSetQueryString;
    private string? _objectLinkQueryString, _objectIdentifierQueryString;
    private string? _objectDateQueryString, _objectTitleQueryString;
    private string? _objectPersonQueryString, _objectOrganisationQueryString;
    private string? _objectTopicQueryString, _objectInstanceQueryString;
    private string? _objectDescriptionQueryString, _objectRelationshipsQueryString;
    private string? _objectRightsQueryString;

    public JSONObjectDataLayer(string connString)
    {
        _connString = connString;
        ConstructObjectQueryStrings();
    }

    public int FetchMinId()
    {
        string sql_string = @"select min(id) from core.data_objects";
        using var conn = new NpgsqlConnection(_connString);
        return conn.ExecuteScalar<int>(sql_string);
    }

    public int FetchMaxId()
    {
        string sql_string = @"select max(id) from core.data_objects";
        using var conn = new NpgsqlConnection(_connString);
        return conn.ExecuteScalar<int>(sql_string);
    }
   
    public IEnumerable<int> FetchIds(int n, int batch)
    {
        string sql_string = @"select id from core.data_objects
                 where id between " + n + @" 
                 and " + (n + batch - 1);
        using var conn = new NpgsqlConnection(_connString);
        return conn.Query<int>(sql_string);
    }
    
    private void ConstructObjectQueryStrings()
    {
        // data object query string
        
        _dataObjectQueryString = @"Select dob.id, dob.doi, 
            dob.display_title, dob.version, 
            dob.object_class_id, oc.name as object_class,
            dob.object_type_id, ot.name as object_type,
            dob.publication_year, dob.lang_code, 
            dob.managing_org_id, dob.managing_org, dob.managing_org_ror_id,
            dob.access_type_id, oat.name as access_type,
            dob.access_details, dob.access_details_url, dob.url_last_checked,
            dob.eosc_category, dob.add_study_contribs, dob.add_study_topics,
            dob.provenance_string
            from core.data_objects dob
            left join context_lup.object_classes oc on dob.object_class_id = oc.id
            left join context_lup.object_types ot on dob.object_type_id = ot.id
            left join context_lup.object_access_types oat on dob.access_type_id = oat.id
            where dob.id = ";

        
        // dataset query string
        
        _dataSetQueryString = @"select ds.id, 
            ds.record_keys_type_id, rt.name as record_keys_type, 
            ds.record_keys_details,
            ds.deident_type_id, it.name as deident_type, 
            ds.deident_direct, ds.deident_hipaa, ds.deident_dates,
            ds.deident_nonarr, ds.deident_kanon, ds.deident_details,
            ds.consent_type_id, ct.name as consent_type, 
            ds.consent_noncommercial, ds.consent_geog_restrict, ds.consent_research_type,
            ds.consent_genetic_only, ds.consent_no_methods, ds.consent_details
            from core.object_datasets ds
            left join context_lup.dataset_recordkey_types rt on ds.record_keys_type_id = rt.id
            left join context_lup.dataset_deidentification_levels it on ds.deident_type_id = it.id
            left join context_lup.dataset_consent_types ct on ds.consent_type_id = ct.id
            where object_id = ";


        // object instances
        
        _objectInstanceQueryString = @"select
            oi.id, system_id, system, url,
            url_accessible, url_last_checked,
            resource_type_id, rt.name as resource_type,
            resource_size, resource_size_units, resource_comments as comments
            from core.object_instances oi
            left join context_lup.resource_types rt on oi.resource_type_id = rt.id
            where object_id = ";

        // object title query string
        
        _objectTitleQueryString = @"select
            ot.id, ot.title_type_id, tt.name as title_type, 
            ot.title_text, ot.lang_code, ot.comments
            from core.object_titles ot
            left join context_lup.title_types tt on ot.title_type_id = tt.id
            where object_id = ";


        // object date query string
        
        _objectDateQueryString = @"select
            od.id, date_type_id, dt.name as date_type, date_is_range,
            date_as_string, start_year, start_month, start_day,
            end_year, end_month, end_day, details as comments
            from core.object_dates od
            left join context_lup.date_types dt on od.date_type_id = dt.id
            where object_id = ";
        
        // object contributors
        
        _objectPersonQueryString = @"select 
            op.id, op.contrib_type_id, ct.name as contrib_type, op.person_full_name,
            op.orcid_id, op.person_affiliation, op.organisation_id, 
            op.organisation_name, op.organisation_ror_id
            from core.object_people op
            left join context_lup.contribution_types ct on op.contrib_type_id = ct.id
            where object_id = ";

        _objectOrganisationQueryString = @"select
            og.id, og.contrib_type_id, ct.name as contrib_type, og.organisation_id, 
            og.organisation_name, og.organisation_ror_id
            from core.object_organisations og
            left join context_lup.contribution_types ct on og.contrib_type_id = ct.id
            where object_id = ";
        
        // object topics 
        
        _objectTopicQueryString = @"select
            ot.id, topic_type_id, tt.name as topic_type, original_value, 
            original_ct_type_id, tv.name as original_ct_type, original_ct_code,
            mesh_code, mesh_value 
            from core.object_topics ot
            left join context_lup.topic_types tt on ot.topic_type_id = tt.id
            left join context_lup.topic_vocabularies tv on ot.original_ct_type_id = tv.id
            where ot.object_id = ";

        
        // object identifiers query string 
        
        _objectIdentifierQueryString = @"select
            oi.id, identifier_value, 
            identifier_type_id, it.name as identifier_type,
            source_id, oi.source, 
            source_ror_id, identifier_date
            from core.object_identifiers oi
            left join context_lup.identifier_types it on oi.identifier_type_id = it.id
            where object_id = ";


        // object description query string 
        _objectDescriptionQueryString = @"select
            od.id, description_type_id, dt.name as description_type,
            label, description_text, lang_code 
            from core.object_descriptions od
            left join context_lup.description_types dt
            on od.description_type_id = dt.id
            where object_id = ";


        // object relationships query string 
        _objectRelationshipsQueryString = @"select 
            r.id, relationship_type_id, rt.name as relationship_type,
            target_object_id 
            from core.object_relationships r
            left join context_lup.object_relationship_types rt 
            on r.relationship_type_id = rt.id
            where object_id = ";


        // object rights query string 
        _objectRightsQueryString = @"select
            id, rights_name, rights_uri, comments
            from core.object_rights
            where object_id = ";


        // data study object link query string
        _objectLinkQueryString = @"select study_id
            from core.study_object_links
            where object_id = ";
    }


    // Fetches the main singleton data object attributes, used during the initial 
    // construction of a data object by the Processor's CreateObject routine.

    public DBDataObject? FetchDbDataObject(int id)
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_connString);
        string sql_string = _dataObjectQueryString + id;
        return conn.QueryFirstOrDefault<DBDataObject>(sql_string);
    }


    // Fetches the data related to dataset properties, for 
    // data objects that are datasets.

    public DBDatasetProperties? FetchDbDatasetProperties(int id)
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_connString);
        string sql_string = _dataSetQueryString + id;
        return conn.QueryFirstOrDefault<DBDatasetProperties>(sql_string);
    }


    // Fetches all linked instance records for the specified data object

    public IEnumerable<DBObjectInstance> FetchObjectInstances(int id)
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_connString);
        string sql_string = _objectInstanceQueryString + id;
        return conn.Query<DBObjectInstance>(sql_string);
    }


    // Fetches all linked study records for the specified data object

    public IEnumerable<int> FetchLinkedStudies(int Id)
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_connString);
        string sql_string = _objectLinkQueryString + Id;
        return conn.Query<int>(sql_string);
    }

    
    // Fetches all linked title records for the specified data object

    public IEnumerable<DBObjectTitle> FetchObjectTitles(int id)
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_connString);
        string sql_string = _objectTitleQueryString + id;
        return conn.Query<DBObjectTitle>(sql_string);
    }


    // Fetches all linked dates for the specified data object

    public IEnumerable<DBObjectDate> FetchObjectDates(int Id)
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_connString);
        string sql_string = _objectDateQueryString + Id;
        return conn.Query<DBObjectDate>(sql_string);
    }
    
    
    // Fetches all linked people for the specified data object

    public IEnumerable<DBObjectPerson> FetchObjectPeople(int Id)
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_connString);
        string sql_string = _objectPersonQueryString + Id;
        return conn.Query<DBObjectPerson>(sql_string);
    }
    
    
    // Fetches all linked organisations for the specified data object

    public IEnumerable<DBObjectOrganisation> FetchObjectOrganisations(int Id)
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_connString);
        string sql_string = _objectOrganisationQueryString + Id;
        return conn.Query<DBObjectOrganisation>(sql_string);
    }

    
    // Fetches all linked topics for the specified data object

    public IEnumerable<DBObjectTopic> FetchObjectTopics(int Id)
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_connString);
        string sql_string = _objectTopicQueryString + Id;
        return conn.Query<DBObjectTopic>(sql_string);
    }
    
   // Fetches all linked identifier records for the specified data object

    public IEnumerable<DBObjectIdentifier> FetchObjectIdentifiers(int id)
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_connString);
        string sql_string = _objectIdentifierQueryString + id;
        return conn.Query<DBObjectIdentifier>(sql_string);
    }

    public IEnumerable<DBObjectDescription> FetchObjectDescriptions(int id)
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_connString);
        string sql_string = _objectDescriptionQueryString + id;
        return conn.Query<DBObjectDescription>(sql_string);
    }

    public IEnumerable<DBObjectRelationship> FetchObjectRelationships(int id)
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_connString);
        string sql_string = _objectRelationshipsQueryString + id;
        return conn.Query<DBObjectRelationship>(sql_string);
    }

    
    public IEnumerable<DBObjectRight> FetchObjectRights(int id)
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_connString);
        string sql_string = _objectRightsQueryString + id;
        return conn.Query<DBObjectRight>(sql_string);
    }
   
    public void StoreSearchRecord(JSONSearchResObject sres)
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_connString);
        conn.Insert(sres);
    }

    public void StoreJSONObjectInDB(int id, string objectJson)
    {
        using NpgsqlConnection conn = new NpgsqlConnection(_connString);
        conn.Open();

        // To insert the string into a json field the parameters for the 
        // command have to be explicitly declared and typed

        using (var cmd = new NpgsqlCommand())
        {
            cmd.CommandText = "INSERT INTO core.new_search_objects_json (id, full_object) VALUES (@id, @p)";
            cmd.Parameters.Add(new NpgsqlParameter("@id", NpgsqlDbType.Integer) { Value = id });
            cmd.Parameters.Add(new NpgsqlParameter("@p", NpgsqlDbType.Json) { Value = objectJson });
            cmd.Connection = conn;
            cmd.ExecuteNonQuery();
        }
        conn.Close();
    }
}



