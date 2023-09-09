using Dapper;
using Npgsql;
using NpgsqlTypes;
namespace MDR_Aggregator;

public class JSONObjectDataLayer
{
    private readonly string _connString;

    private string? data_object_query_string, data_set_query_string;
    private string? object_link_query_string, object_identifier_query_string;
    private string? object_date_query_string, object_title_query_string;
    private string? object_person_query_string, object_organisation_query_string;
    private string? object_topic_query_string, object_instance_query_string;
    private string? object_description_query_string, object_relationships_query_string;
    private string? object_rights_query_string;

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
        
        data_object_query_string = @"Select dob.id, dob.doi, 
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
        
        data_set_query_string = @"select ds.id, 
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
        
        object_instance_query_string = @"select
            oi.id, instance_type_id, it.name as instance_type,
            system_id, system, url,
            url_accessible, url_last_checked,
            resource_type_id, rt.name as resource_type,
            resource_size, resource_size_units, resource_comments as comments
            from core.object_instances oi
            left join context_lup.resource_types rt on oi.resource_type_id = rt.id
            left join context_lup.object_instance_types it on oi.instance_type_id = it.id
            where object_id = ";


        // object title query string
        
        object_title_query_string = @"select
            ot.id, ot.title_type_id, tt.name as title_type, 
            ot.title_text, ot.lang_code, ot.comments
            from core.object_titles ot
            left join context_lup.title_types tt on ot.title_type_id = tt.id
            where object_id = ";


        // object date query string
        
        object_date_query_string = @"select
            od.id, date_type_id, dt.name as date_type, date_is_range,
            date_as_string, start_year, start_month, start_day,
            end_year, end_month, end_day, details as comments
            from core.object_dates od
            left join context_lup.date_types dt on od.date_type_id = dt.id
            where object_id = ";

        
        // object contributor
        
        object_person_query_string = @"select 
            op.id, op.contrib_type_id, ct.name as contrib_type, op.person_full_name,
            op.orcid_id, op.person_affiliation, op.organisation_id, 
            op.organisation_name, op.organisation_ror_id
            from core.object_people op
            left join context_lup.contribution_types ct on op.contrib_type_id = ct.id
            where object_id = ";

        object_organisation_query_string = @"select
            og.id, og.contrib_type_id, ct.name as contrib_type, og.organisation_id, 
            og.organisation_name, og.organisation_ror_id
            from core.object_organisations og
            left join context_lup.contribution_types ct on og.contrib_type_id = ct.id
            where object_id = ";
        
        /*
        /*  three strings below no longer used - may need to be resurrected 

        // object people (using object contributors AND study people) - part 1
        // would need to be 'unionised' to any from the object itself
             
        object_study_people_query_string = @"select
            sc.id, contrib_type_id, ct.name as contrib_type, 
            person_full_name, orcid_id, person_affiliation,
            organisation_id, organisation_name, organisation_ror_id
            from core.study_object_links k
            inner join core.study_people sc on k.study_id = sc.study_id
            left join context_lup.contribution_types ct on oc.contrib_type_id = ct.id
            where object_id = ";

        // object organisations (using object contributors AND study organisations) - part 2
        
        object_study_orgs_query_string = @"select
            sc.id, contrib_type_id, ct.name as contrib_type, 
            organisation_id, organisation_name, organisation_ror_id
            from core.study_object_links k
            inner join core.study_organisations sc on k.study_id = sc.study_id
            left join context_lup.contribution_types ct on sc.contrib_type_id = ct.id
            where object_id = ";

        // object topics (using study objects)
        
        object_study_topics_query_string = @"select
            st.id, topic_type_id, tt.name as topic_type, 
            mesh_coded, mesh_code, mesh_value, 
            original_ct_id, original_ct_code, original_value
            from core.study_object_links k
            inner join core.study_topics st on k.study_id = st.study_id
            left join context_lup.topic_types tt on st.topic_type_id = tt.id
            where k.object_id = ";

         */


        // object topics 
        
        object_topic_query_string = @"select
            ot.id, topic_type_id, tt.name as topic_type, original_value, 
            original_ct_type_id, tv.name as original_ct_type, original_ct_code,
            mesh_code, mesh_value 
            from core.object_topics ot
            left join context_lup.topic_types tt on ot.topic_type_id = tt.id
            left join context_lup.topic_vocabularies tv on ot.original_ct_type_id = tv.id
            where ot.object_id = ";

        
        // object identifiers query string 
        
        object_identifier_query_string = @"select
            oi.id, identifier_value, 
            identifier_type_id, it.name as identifier_type,
            source_id, oi.source, 
            source_ror_id, identifier_date
            from core.object_identifiers oi
            left join context_lup.identifier_types it on oi.identifier_type_id = it.id
            where object_id = ";


        // object description query string 
        object_description_query_string = @"select
            od.id, description_type_id, dt.name as description_type,
            label, description_text, lang_code 
            from core.object_descriptions od
            left join context_lup.description_types dt
            on od.description_type_id = dt.id
            where object_id = ";


        // object relationships query string 
        object_relationships_query_string = @"select 
            r.id, relationship_type_id, rt.name as relationship_type,
            target_object_id 
            from core.object_relationships r
            left join context_lup.object_relationship_types rt 
            on r.relationship_type_id = rt.id
            where object_id = ";


        // object rights query string 
        object_rights_query_string = @"select
            id, rights_name, rights_uri, comments
            from core.object_rights
            where object_id = ";


        // data study object link query string
        object_link_query_string = @"select study_id
            from core.study_object_links
            where object_id = ";
    }


    // Fetches the main singleton data object attributes, used during the initial 
    // construction of a data object by the Processor's CreateObject routine.

    public DBDataObject? FetchDbDataObject(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = data_object_query_string + id;
        return Conn.QueryFirstOrDefault<DBDataObject>(sql_string);
    }


    // Fetches the data related to dataset properties, for 
    // data objects that are datasets.

    public DBDatasetProperties? FetchDbDatasetProperties(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = data_set_query_string + id;
        return Conn.QueryFirstOrDefault<DBDatasetProperties>(sql_string);
    }


    // Fetches all linked instance records for the specified data object

    public IEnumerable<DBObjectInstance> FetchObjectInstances(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = object_instance_query_string + id;
        return Conn.Query<DBObjectInstance>(sql_string);
    }


    // Fetches all linked study records for the specified data object

    public IEnumerable<int> FetchLinkedStudies(int Id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = object_link_query_string + Id;
        return Conn.Query<int>(sql_string);
    }

    
    // Fetches all linked title records for the specified data object

    public IEnumerable<DBObjectTitle> FetchObjectTitles(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = object_title_query_string + id;
        return Conn.Query<DBObjectTitle>(sql_string);
    }


    // Fetches all linked dates for the specified data object

    public IEnumerable<DBObjectDate> FetchObjectDates(int Id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = object_date_query_string + Id;
        return Conn.Query<DBObjectDate>(sql_string);
    }
    
    
    // Fetches all linked people for the specified data object

    public IEnumerable<DBObjectPerson> FetchObjectPeople(int Id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = object_person_query_string + Id;
        return Conn.Query<DBObjectPerson>(sql_string);
    }
    
    
    // Fetches all linked organisations for the specified data object

    public IEnumerable<DBObjectOrganisation> FetchObjectOrganisations(int Id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = object_organisation_query_string + Id;
        return Conn.Query<DBObjectOrganisation>(sql_string);
    }

    
    // Fetches all linked topics for the specified data object

    public IEnumerable<DBObjectTopic> FetchObjectTopics(int Id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = object_topic_query_string + Id;
        return Conn.Query<DBObjectTopic>(sql_string);
    }
    
    
    /*/* May need o re-used in the future 
    // Fetches all linked contributor records for the specified data object.
    // The boolean add_study_contribs, if true, indicates that the system should draw
    // the contributors from the corresponding 'parent' study's contributors
    // In these circumstances the object is assumed to have no linked contributors itself.
    // If false, the system draws the topics from the object's own contributor records, but 
    // it also unions these from any organisational contributors attached to the parent study 
    // (e.g. the sponsor).

    public IEnumerable<DBObjectContributor>? FetchObjectContributors(int id, bool? add_study_contribs)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string;
        if (add_study_contribs is true)
        {
            sql_string = object_study_contrib_query_string + id;
        }
        else
        {
            sql_string = object_contrib_query_string1 + id + " union ";
            sql_string += object_contrib_query_string2 + id + " and is_individual = false";
        }
        return Conn.Query<DBObjectContributor>(sql_string);
    }


    // Fetches all linked topic records for the specified data object.
    // The boolean use_study_topics, if true, indicates that the system should draw
    // the topics from the corresponding 'parent' study's topics.
    // In these circumstances the object is assumed to have no linked topics itself.
    // If false, the system draws the topics from the object's own topic records.

    public IEnumerable<DBObjectTopic>? FetchObjectTopics(int id, bool? use_study_topics)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string;
        if (use_study_topics is true)
        {
            sql_string = object_study_topics_query_string + id;
        }
        else
        {
            sql_string = object_topics_query_string + id;
        }
        return Conn.Query<DBObjectTopic>(sql_string);
    }
    */
    
    
    // Fetches all linked identifier records for the specified data object

    public IEnumerable<DBObjectIdentifier> FetchObjectIdentifiers(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = object_identifier_query_string + id;
        return Conn.Query<DBObjectIdentifier>(sql_string);
    }


    public IEnumerable<DBObjectDescription> FetchObjectDescriptions(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = object_description_query_string + id;
        return Conn.Query<DBObjectDescription>(sql_string);
    }

    public IEnumerable<DBObjectRelationship> FetchObjectRelationships(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = object_relationships_query_string + id;
        return Conn.Query<DBObjectRelationship>(sql_string);
    }


    public IEnumerable<DBObjectRight> FetchObjectRights(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = object_rights_query_string + id;
        return Conn.Query<DBObjectRight>(sql_string);
    }


    public void StoreJSONObjectInDB(int id, string object_json)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        Conn.Open();

        // To insert the string into a json field the parameters for the 
        // command have to be explicitly declared and typed

        using (var cmd = new NpgsqlCommand())
        {
            cmd.CommandText = "INSERT INTO core.objects_json (id, json) VALUES (@id, @p)";
            cmd.Parameters.Add(new NpgsqlParameter("@id", NpgsqlDbType.Integer) { Value = id });
            cmd.Parameters.Add(new NpgsqlParameter("@p", NpgsqlDbType.Json) { Value = object_json });
            cmd.Connection = Conn;
            cmd.ExecuteNonQuery();
        }
        Conn.Close();
    }
}



