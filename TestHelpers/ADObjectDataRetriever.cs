using Dapper;
using Npgsql;
namespace MDR_Aggregator;

public class ADObjectDataRetriever
{

    private string _source_id;
    private string _db_conn;

    public ADObjectDataRetriever(int source_id, string db_conn)
    {
        _source_id = source_id.ToString();
        _db_conn = db_conn;
    }


    private void Execute_SQL(string sql_string)
    {
        using (var conn = new NpgsqlConnection(_db_conn))
        {
            conn.Execute(sql_string);
        }
    }


    public void TransferDataObjects()
    {
        string sql_string = @"INSERT INTO ad.data_objects(sd_oid, sd_sid, 
        display_title, version, doi, doi_status_id, publication_year,
        object_class_id, object_type_id,  
        managing_org_id, managing_org, managing_org_ror_id, lang_code, access_type_id,
        access_details, access_details_url, url_last_checked, eosc_category, add_study_contribs,
        add_study_topics, datetime_of_data_fetch, record_hash, object_full_hash)
        SELECT sd_oid, sd_sid, 
        display_title, version, doi, doi_status_id, publication_year,
        object_class_id, object_type_id, 
        managing_org_id, managing_org, managing_org_ror_id, lang_code, access_type_id,
        access_details, access_details_url, url_last_checked, eosc_category, add_study_contribs,
        add_study_topics, datetime_of_data_fetch, record_hash, object_full_hash
        FROM adcomp.data_objects
        where source_id = " + _source_id;

        Execute_SQL(sql_string);
    }


    public void TransferObjectDatasets()
    {
        string sql_string = @"INSERT INTO ad.object_datasets(sd_oid,
        record_keys_type_id, record_keys_details, 
        deident_type_id, deident_direct, deident_hipaa,
        deident_dates, deident_nonarr, deident_kanon, deident_details,
        consent_type_id, consent_noncommercial, consent_geog_restrict,
        consent_research_type, consent_genetic_only, consent_no_methods, consent_details,
        record_hash)
        SELECT sd_oid,
        record_keys_type_id, record_keys_details, 
        deident_type_id, deident_direct, deident_hipaa,
        deident_dates, deident_nonarr, deident_kanon, deident_details,
        consent_type_id, consent_noncommercial, consent_geog_restrict,
        consent_research_type, consent_genetic_only, consent_no_methods, consent_details,
        record_hash
        FROM adcomp.object_datasets
        where source_id = " + _source_id;

        Execute_SQL(sql_string);
    }


    public void TransferObjectInstances()
    {
        string sql_string = @"INSERT INTO ad.object_instances(sd_oid,
        instance_type_id, repository_org_id, repository_org,
        url, url_accessible, url_last_checked, resource_type_id, 
        resource_size, resource_size_units, resource_comments, record_hash)
        SELECT sd_oid,
        instance_type_id, repository_org_id, repository_org,
        url, url_accessible, url_last_checked, resource_type_id, 
        resource_size, resource_size_units, resource_comments, record_hash
        FROM adcomp.object_instances
        where source_id = " + _source_id;

        Execute_SQL(sql_string);
    }


    public void TransferObjectTitles()
    {
        string sql_string = @"INSERT INTO ad.object_titles(sd_oid,
        title_type_id, title_text, lang_code,
        lang_usage_id, is_default, comments, record_hash)
        SELECT sd_oid,
        title_type_id, title_text, lang_code,
        lang_usage_id, is_default, comments, record_hash
        FROM adcomp.object_titles
        where source_id = " + _source_id;

        Execute_SQL(sql_string);
    }


    public void TransferObjectDates()
    {
        string sql_string = @"INSERT INTO ad.object_dates(sd_oid, 
        date_type_id, date_is_range, date_as_string, start_year, 
        start_month, start_day, end_year, end_month, end_day, details, record_hash)
        SELECT sd_oid,
        date_type_id, date_is_range, date_as_string, start_year, 
        start_month, start_day, end_year, end_month, end_day, details, record_hash
        FROM adcomp.object_dates
        where source_id = " + _source_id;

        Execute_SQL(sql_string);
    }


    public void TransferObjectContributors()
    {
        string sql_string = @"INSERT INTO ad.object_contributors(sd_oid,
        contrib_type_id, is_individual, 
        person_id, person_given_name, person_family_name, person_full_name,
        orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id, record_hash)
        SELECT sd_oid,
        contrib_type_id, is_individual, 
        person_id, person_given_name, person_family_name, person_full_name,
        orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id, record_hash
        FROM adcomp.object_contributors
        where source_id = " + _source_id;

        Execute_SQL(sql_string);
    }


    public void TransferObjectTopics()
    {
        string sql_string = @"INSERT INTO ad.object_topics(sd_oid, 
        topic_type_id, mesh_coded, mesh_code, mesh_value, 
        original_ct_id, original_ct_code, original_value, record_hash)
        SELECT sd_oid,
        topic_type_id, mesh_coded, mesh_code, mesh_value, 
        original_ct_id, original_ct_code, original_value, record_hash
        FROM adcomp.object_topics
        where source_id = " + _source_id;

        Execute_SQL(sql_string);
    }


    public void TransferObjectComments()
    {
        string sql_string = @"INSERT INTO ad.object_comments(sd_oid, 
        ref_type, ref_source, pmid, pmid_version, notes, record_hash)
        SELECT sd_oid,
        ref_type, ref_source, pmid, pmid_version, notes, record_hash
        FROM adcomp.object_comments
        where source_id = " + _source_id;

        Execute_SQL(sql_string);
    }


    public void TransferObjectDescriptions()
    {
        string sql_string = @"INSERT INTO ad.object_descriptions(sd_oid,
        description_type_id, label, description_text,
        lang_code, record_hash)
        SELECT sd_oid,
        description_type_id, label, description_text, 
        lang_code, record_hash
        FROM adcomp.object_descriptions
        where source_id = " + _source_id;

        Execute_SQL(sql_string);
    }


    public void TransferObjectidentifiers()
    {
        string sql_string = @"INSERT INTO ad.object_identifiers(sd_oid, 
        identifier_value, identifier_type_id, 
        identifier_org_id, identifier_org, identifier_org_ror_id,
        identifier_date, record_hash)
        SELECT sd_oid, 
        identifier_value, identifier_type_id, 
        identifier_org_id, identifier_org, identifier_org_ror_id,
        identifier_date, record_hash
        FROM adcomp.object_identifiers
        where source_id = " + _source_id;

        Execute_SQL(sql_string);
    }


    public void TransferObjectDBLinks()
    {
        string sql_string = @"INSERT INTO ad.object_db_links(sd_oid,
        db_sequence, db_name, id_in_db, record_hash)
        SELECT sd_oid,
        db_sequence, db_name, id_in_db, record_hash
        FROM adcomp.object_db_links
        where source_id = " + _source_id;

        Execute_SQL(sql_string);
    }

    public void TransferObjectPublicationTypes()
    {
        string sql_string = @"INSERT INTO ad.object_publication_types(sd_oid, 
        type_name, record_hash)
        SELECT sd_oid,
        type_name, record_hash
        FROM adcomp.object_publication_types
        where source_id = " + _source_id;

        Execute_SQL(sql_string);
    }


    public void TransferObjectRights()
    {
        string sql_string = @"INSERT INTO ad.object_rights(sd_oid,
        rights_name, rights_uri, comments, record_hash)
        SELECT sd_oid,
        rights_name, rights_uri, comments, record_hash
        FROM adcomp.object_rights
        where source_id = " + _source_id;

        Execute_SQL(sql_string);
    }


    public void TransferObjectRelationships()
    {
        string sql_string = @"INSERT INTO ad.object_relationships(sd_oid, 
        relationship_type_id, target_sd_oid, record_hash)
        SELECT sd_oid, 
        relationship_type_id, target_sd_oid, record_hash
        FROM adcomp.object_relationships
        where source_id = " + _source_id;

        Execute_SQL(sql_string);
    }


    public void TransferObjectHashes()
    {
        string sql_string = @"INSERT INTO ad.object_hashes(sd_oid,
        hash_type_id, composite_hash)
        SELECT sd_oid,
        hash_type_id, composite_hash
        FROM adcomp.object_hashes
        where source_id = " + _source_id;

        Execute_SQL(sql_string);
    }
}