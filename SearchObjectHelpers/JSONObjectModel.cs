﻿using Dapper.Contrib.Extensions;
namespace MDR_Aggregator;

public class JSONFullObject
{
    public string? file_type { get; set; }
    public int id { get; set; }
    public string? doi { get; set; }
    public string? display_title { get; set; }
    public string? version { get; set; }
    public Lookup? object_class { get; set; }
    public Lookup? object_type { get; set; }
    public int? publication_year { get; set; }
    public Organisation? managing_organisation { get; set; }
    public string? lang_code { get; set; }
    public Lookup? access_type { get; set; }
    public object_access? access_details { get; set; }
    public int? eosc_category { get; set; }
    public string? provenance_string { get; set; }

    public record_keys? dataset_record_keys { get; set; }
    public Deidentification? dataset_deident_level { get; set; }
    public Consent? dataset_consent { get; set; }

    public List<object_instance>? object_instances { get; set; }
    public List<object_title>? object_titles { get; set; }
    public List<object_person>? object_people { get; set; }
    public List<object_organisation>? object_organisations { get; set; }
    public List<object_date>? object_dates { get; set; }
    public List<object_topic>? object_topics { get; set; }
    public List<object_description>? object_descriptions { get; set; }
    public List<object_identifier>? object_identifiers { get; set; }
    public List<object_right>? object_rights { get; set; }
    public List<object_relationship>? object_relationships { get; set; }
    public List<int>? linked_studies { get; set; }

    
    public JSONFullObject(int _id, string? _doi, string? _display_title, string? _version,
        Lookup? _object_class, Lookup? _object_type, int? _publication_year,
        Organisation? _managing_organisation, string? _lang_code,
        Lookup? _access_type, object_access? _access_details,
        int? _eosc_category, string? _provenance_string)
    {
        file_type = "data_object";
        id = _id;
        doi = _doi;
        display_title = _display_title;
        version = _version;
        object_class = _object_class;
        object_type = _object_type;
        publication_year = _publication_year;
        managing_organisation = _managing_organisation;
        lang_code = _lang_code;
        access_type = _access_type;
        access_details = _access_details;
        eosc_category = _eosc_category;
        provenance_string = _provenance_string;
    }
}

[Table("search.new_objects")]
public class JSONSearchResObject
{
    public int oid { get; set; }
    public string? ob_name { get; set; }
    public int? typeid { get; set; }
    public string? typename { get; set; }
    public string? url { get; set; }
    public int? res_type_id { get; set; }
    public string? res_icon { get; set; }
    public string? year_pub { get; set; }
    public string? acc_icon { get; set; }
    public string? prov { get; set; }

    public JSONSearchResObject(int _oid, string? _ob_name,
        int? _typeid, string? _typename, string? _url,
        int? _res_type_id, string? _res_icon, string? _year_pub,
        string? _acc_icon, string? _prov)
    {
        oid = _oid;
        ob_name = _ob_name;
        typeid = _typeid;
        typename = _typename;
        url = _url;
        res_type_id = _res_type_id;
        res_icon = _res_icon;
        year_pub = _year_pub;
        acc_icon = _acc_icon;
        prov = _prov;
    }
    
    public JSONSearchResObject() {}
}


// Many of the properties of the data object are id / name pairs,
// here known as a lookup as they are derived from lookup tables.

public class Lookup
{
    public int? id { get; set; }
    public string? name { get; set; }

    public Lookup(int? _id, string? _name)
    {
        id = _id;
        name = _name;
    }
}

// these three small composite classes also used within the data model, for topic and condition data

public class MeshData
{
    public string? mesh_code { get; set; }
    public string? mesh_value { get; set; }

    public MeshData(string? _mesh_code, string? _mesh_value)
    {
        mesh_code = _mesh_code;
        mesh_value = _mesh_value;
    }
}

public class ICDData
{
    public string? icd_code { get; set; }
    public string? icd_name { get; set; }

    public ICDData(string? _icd_code, string? _icd_name)
    {
        icd_code = _icd_code;
        icd_name = _icd_name;
    }
}


public class CTData
{
    public int? ct_type_id { get; set; }
    public string? ct_type { get; set; }
    public string? ct_code { get; set; }

    public CTData(int? _ct_type_id, string? _ct_type, string? _ct_code)
    {
        ct_type_id = _ct_type_id;
        ct_type = _ct_type;
        ct_code = _ct_code;
    }
}


public class Organisation
{
    public int? id { get; set; }
    public string? name { get; set; }
    public string? ror_id { get; set; }

    public Organisation(int? _id, string? _name, string? _ror_id)
    {
        id = _id;
        name = _name;
        ror_id = _ror_id;
    }
}

public class object_access
{
    public string? description { get; set; }
    public string? url { get; set; }
    public string? url_last_checked { get; set; }

    public object_access(string? _description, string? _url, string? _url_last_checked)
    {
        description = _description;
        url = _url;
        url_last_checked = _url_last_checked;
    }
}

public class record_keys
{
    public int? record_keys_type_id { get; set; }
    public string? record_keys_type { get; set; }
    public string? record_keys_details { get; set; }

    public record_keys(int? _record_keys_type_id, 
                       string? _record_keys_type, string? _record_keys_details)
    {
        record_keys_type_id = _record_keys_type_id;
        record_keys_type = _record_keys_type;
        record_keys_details = _record_keys_details;
    }
}

public class Deidentification
{
    public int? deident_type_id { get; set; }
    public string? deident_type { get; set; }
    public bool? deident_direct { get; set; }
    public bool? deident_hipaa { get; set; }
    public bool? deident_dates { get; set; }
    public bool? deident_nonarr { get; set; }
    public bool? deident_kanon { get; set; }
    public string? deident_details { get; set; }

    public Deidentification(int? _deident_type_id, string? _deident_type,
                            bool? _deident_direct, bool? _deident_hipaa,
                            bool? _deident_dates, bool? _deident_nonarr,
                            bool? _deident_kanon, string? _deident_details)
    {
        deident_type_id = _deident_type_id;
        deident_type = _deident_type;
        deident_direct = _deident_direct;
        deident_hipaa = _deident_hipaa;
        deident_dates = _deident_dates;
        deident_nonarr = _deident_nonarr;
        deident_kanon = _deident_kanon;
        deident_details = _deident_details;
    }
}

public class Consent
{
    public int? consent_type_id { get; set; }
    public string? consent_type { get; set; }
    public bool? consent_noncommercial { get; set; }
    public bool? consent_geog_restrict { get; set; }
    public bool? consent_research_type { get; set; }
    public bool? consent_genetic_only { get; set; }
    public bool? consent_no_methods { get; set; }
    public string? consent_details { get; set; }

    public Consent(int? _consent_type_id, string? _consent_type,
                   bool? _consent_noncommercial, bool? _consent_geog_restrict,
                   bool? _consent_research_type, bool? _consent_genetic_only,
                   bool? _consent_no_methods, string? _consent_details)
    {
        consent_type_id = _consent_type_id;
        consent_type = _consent_type;
        consent_noncommercial = _consent_noncommercial;
        consent_geog_restrict = _consent_geog_restrict;
        consent_research_type = _consent_research_type;
        consent_genetic_only = _consent_genetic_only;
        consent_no_methods = _consent_no_methods;
        consent_details = _consent_details;
    }
}


// Corresponds to the repeating composite object_instance json element
// and is therefore part of the data object class as a List<> 

public class object_instance
{
    public int id { get; set; }
    public Lookup? system { get; set; }
    public access_details? access_details { get; set; }
    public resource_details? resource_details { get; set; }

    public object_instance(int _id, Lookup? _system,
                    access_details? _access_details, resource_details? _resource_details)
    {
        id = _id;
        system = _system;
        access_details = _access_details;
        resource_details = _resource_details;
    }
}


public class access_details
{
    public string? url { get; set; }
    public bool? direct_access { get; set; }
    public string? url_last_checked { get; set; }

    public access_details( string? _url, bool? _direct_access, string? _url_last_checked)
    {
        url = _url;
        direct_access = _direct_access;
        url_last_checked = _url_last_checked;
    }
}


public class resource_details
{
     public int? type_id { get; set; }
     public string? type_name { get; set; }
     public float? size { get; set; }
     public string? size_unit { get; set; }
     public string? comments { get; set; }

     public resource_details(int? _type_id, string? _type_name,
            float? _size, string? _size_unit, string? _comments)
    {
        type_id = _type_id;
        type_name = _type_name;
        size = _size;
        size_unit = _size_unit;
        comments = _comments;
    }
}


// Corresponds to the repeating composite object_title json element
// and is therefore part of the data object class as a List<> 

public class object_title
{
    public int id { get; set; }
    public Lookup? title_type { get; set; }
    public string? title_text { get; set; }
    public string? lang_code { get; set; }
    public string? comments { get; set; }

    public object_title(int _id, Lookup? _title_type, string? _title_text,
                    string? _lang_code, string? _comments)
    {
        id = _id;
        title_type = _title_type;
        title_text = _title_text;
        lang_code = _lang_code;
        comments = _comments;
    }
}


// Corresponds to the repeating composite object_date json element
// and is therefore part of the data object class as a List<> 

public class object_date
{
    public int id { get; set; }
    public Lookup? date_type { get; set; }
    public bool? date_is_range { get; set; }
    public string? date_as_string { get; set; }
    public sdate_as_ints? start_date { get; set; }
    public edate_as_ints? end_date { get; set; }
    public string? comments { get; set; }

    public object_date(int _id, Lookup? _date_type, bool? _date_is_range, string? _date_as_string,
                    sdate_as_ints? _start_date, edate_as_ints? _end_date, string? _comments)
    {
        id = _id;
        date_type = _date_type;
        date_is_range = _date_is_range;
        date_as_string = _date_as_string;
        start_date = _start_date;
        end_date = _end_date;
        comments = _comments;
    }
}


// A composite date element (the start date) within object_date

public class sdate_as_ints
{
    public int? start_year { get; set; }
    public int? start_month { get; set; }
    public int? start_day { get; set; }

    public sdate_as_ints(int? _start_year, int? _start_month, int? _start_day)
    {
        start_year = _start_year;
        start_month = _start_month;
        start_day = _start_day;
    }
}


// A composite date element (the end date) within object_date

public class edate_as_ints
{
    public int? end_year { get; set; }
    public int? end_month { get; set; }
    public int? end_day { get; set; }

    public edate_as_ints(int? _end_year, int? _end_month, int? _end_day)
    {
        end_year = _end_year;
        end_month = _end_month;
        end_day = _end_day;
    }
}


// Corresponds to the repeating composite object_topic json element
// and is therefore part of the data object class as a List<> 

public class object_topic
{
    public int id { get; set; }
    public Lookup? topic_type { get; set; }
    public string? original_value { get; set; }   
    public CTData? ct_data { get; set; }
    public MeshData? mesh_data { get; set; }

    public object_topic(int _id, Lookup _topic_type, string? _original_value,
        CTData? _ct_data, MeshData? _mesh_data)
    {
        id = _id;
        topic_type = _topic_type;
        original_value = _original_value;
        ct_data = _ct_data;
        mesh_data = _mesh_data;
    }
}


public class object_person
{
    public int id { get; set; }
    public Lookup? contrib_type { get; set; }
    public string? person_full_name { get; set; }
    public string? orcid_id { get; set; }
    public string? person_affiliation { get; set; }
    public Organisation? affiliation_org { get; set; }

    public object_person(int _id, Lookup? _contrib_type,
        string? _person_full_name, string? _orcid_id, 
        string? _person_affiliation, Organisation? _affiliation_org)
    {
        id = _id;
        contrib_type = _contrib_type;
        person_full_name = _person_full_name;
        orcid_id = _orcid_id;
        person_affiliation = _person_affiliation;
        affiliation_org = _affiliation_org;
    }
}

public class object_organisation
{
    public int id { get; set; }
    public Lookup? contrib_type { get; set; }
    public Organisation? org_details { get; set; }

    public object_organisation(int _id, Lookup? _contrib_type,
        Organisation? _org_details)
    {
        id = _id;
        contrib_type = _contrib_type;
        org_details = _org_details;
    }
}


// Corresponds to the repeating composite object_description json element
// and is therefore part of the data object class as a List<> 

public class object_description
{
    public int id { get; set; }
    public Lookup? description_type { get; set; }
    public string?description_label { get; set; }
    public string? description_text { get; set; }
    public string? lang_code { get; set; }

    public object_description(int _id, Lookup? _description_type,
                              string? _description_label, string? _description_text,
                              string? _lang_code)
    {
        id = _id;
        description_type = _description_type;
        description_label = _description_label;
        description_text = _description_text;
        lang_code = _lang_code;
    }
}


// Corresponds to the repeating composite object_identifier json element
// and is therefore part of the data object class as a List<> 

public class object_identifier
{
    public int id { get; set; }
    public string? identifier_value { get; set; }
    public Lookup? identifier_type { get; set; }
    public Organisation? source { get; set; }
    public string? identifier_date { get; set; }

    public object_identifier(int _id, string? _identifier_value, 
        Lookup? _identifier_type, Organisation? _source,
                             string? _identifier_date)
    {
        id = _id;
        identifier_type = _identifier_type;
        source = _source;
        identifier_value = _identifier_value;
        identifier_date = _identifier_date;
    }
}

// Corresponds to the repeating composite object_right json element
// and is therefore part of the data object class as a List<> 

public class object_right
{
    public int id { get; set; }
    public string? rights_name { get; set; }
    public string? rights_uri { get; set; }
    public string? comments { get; set; }

    public object_right(int _id, string? _rights_name, 
                        string? _rights_uri, string? _comments)
    {
        id = _id;
        rights_name = _rights_name;
        rights_uri = _rights_uri;
        comments = _comments;
    }
}


// Corresponds to the repeating composite related_object json element
// and is therefore part of the data object class as a List<> 

public class object_relationship
{
    public int id { get; set; }
    public Lookup? relationship_type { get; set; }
    public int? target_object_id { get; set; }

    public object_relationship(int _id, Lookup? _relationship_type,
                               int? _target_object_id)
    {
        id = _id;
        relationship_type = _relationship_type;
        target_object_id = _target_object_id;
    }
}


// These classes correspond to the data returned by the various 'Fetch' routines in the DataLayer
// class - each DB class matches the structure of the corresponding table.
// They represent the form in which data is presented to the Processor's CreateObject routine.
// That routine modifies the data as and when necessary, and then aggregates it, to make it conform
// to the structure required in the final data object that is delivered back to the main program.

[Table("core.data_objects")]
public class DBDataObject
{
    public int id { get; set; }
    public string? doi { get; set; }
    public string? display_title { get; set; }
    public string? version { get; set; }
    public int? object_class_id { get; set; }
    public string? object_class { get; set; }
    public int? object_type_id { get; set; }
    public string? object_type { get; set; }
    public int? publication_year { get; set; }
    public string? lang_code { get; set; }
    public int? managing_org_id { get; set; }
    public string? managing_org { get; set; }
    public string? managing_org_ror_id { get; set; }
    public int? access_type_id { get; set; }
    public string? access_type { get; set; }
    public string? access_details { get; set; }
    public string? access_details_url { get; set; }
    public string? url_last_checked { get; set; }
    public int? eosc_category { get; set; }
    public bool? add_study_contribs { get; set; }
    public bool? add_study_topics { get; set; }
    public string? provenance_string { get; set; }
}


[Table("core.dataset_properties")]
public class DBDatasetProperties
{
    public int id { get; set; }
    public int? record_keys_type_id { get; set; }
    public string? record_keys_type { get; set; }
    public string? record_keys_details { get; set; }
    public int? deident_type_id { get; set; }
    public string? deident_type { get; set; }
    public bool? deident_direct { get; set; }
    public bool? deident_hipaa { get; set; }
    public bool? deident_dates { get; set; }
    public bool? deident_nonarr { get; set; }
    public bool? deident_kanon { get; set; }
    public string? deident_details { get; set; }
    public int? consent_type_id { get; set; }
    public string? consent_type { get; set; }
    public bool? consent_noncommercial { get; set; }
    public bool? consent_geog_restrict { get; set; }
    public bool? consent_research_type { get; set; }
    public bool? consent_genetic_only { get; set; }
    public bool? consent_no_methods { get; set; }
    public string? consent_details { get; set; }
}


[Table("core.object_instances")]
public class DBObjectInstance
{
    public int id { get; set; }
    public int? instance_type_id { get; set; }
    public string? instance_type { get; set; }
    public int? system_id { get; set; }
    public string? system { get; set; }
    public string? url { get; set; }
    public bool? url_accessible { get; set; }
    public string? url_last_checked { get; set; }
    public int? resource_type_id { get; set; }
    public string? resource_type { get; set; }
    public float? resource_size { get; set; }
    public string? resource_size_units { get; set; }
    public string? comments { get; set; }
}


[Table("core.object_titles")]
public class DBObjectTitle
{
    public int id { get; set; }
    public int? title_type_id { get; set; }
    public string? title_type { get; set; }
    public string? title_text { get; set; }
    public string? lang_code { get; set; }
    public string? comments { get; set; }
}


[Table("core.object_dates")]
public class DBObjectDate
{
    public int id { get; set; }
    public int? date_type_id { get; set; }
    public string? date_type { get; set; }
    public bool? date_is_range { get; set; }
    public string? date_as_string { get; set; }
    public int? start_year { get; set; }
    public int? start_month { get; set; }
    public int? start_day { get; set; }
    public int? end_year { get; set; }
    public int? end_month { get; set; }
    public int? end_day { get; set; }
    public string? comments { get; set; }
}

[Table("core.object_identifiers")]
public class DBObjectIdentifier
{
    public int id { get; set; }
    public string? identifier_value { get; set; }
    public int? identifier_type_id { get; set; }
    public string? identifier_type { get; set; }
    public int? source_id { get; set; }
    public string? source { get; set; }
    public string? source_ror_id { get; set; }
    public string? identifier_date { get; set; }
}


[Table("core.object_people")]
public class DBObjectPerson
{
    public int id { get; set; }
    public int? contrib_type_id { get; set; }
    public string? contrib_type { get; set; }
    public string? person_full_name { get; set; }
    public string? orcid_id { get; set; }
    public string? person_affiliation { get; set; }
    public int? organisation_id { get; set; }
    public string? organisation_name { get; set; }
    public string? organisation_ror_id { get; set; }
}


[Table("core.object_organisations")]
public class DBObjectOrganisation
{
    public int id { get; set; }
    public int? contrib_type_id { get; set; }
    public string? contrib_type { get; set; }
    public int? organisation_id { get; set; }
    public string? organisation_name { get; set; }
    public string? organisation_ror_id { get; set; }
}


[Table("core.object_topics")]
public class DBObjectTopic
{
    public int id { get; set; }
    public int? topic_type_id { get; set; }
    public string? topic_type { get; set; }
    public string? original_value { get; set; }    
    public int? original_ct_type_id { get; set; }
    public string? original_ct_type { get; set; }
    public string? original_ct_code { get; set; }
    public string? mesh_code { get; set; }
    public string? mesh_value { get; set; }
}


[Table("core.object_descriptions")]
public class DBObjectDescription
{
    public int id { get; set; }
    public int? description_type_id { get; set; }
    public string? description_type { get; set; }
    public string? label { get; set; }
    public string? description_text { get; set; }
    public string? lang_code { get; set; }
}


[Table("core.object_relationships")]
public class DBObjectRelationship
{
    public int id { get; set; }
    public int? relationship_type_id { get; set; }
    public string? relationship_type { get; set; }
    public int? target_object_id { get; set; }
}


[Table("core.object_rights")]
public class DBObjectRight
{
    public int id { get; set; }
    public string? rights_name { get; set; }
    public string? rights_uri { get; set; }
    public string? comments { get; set; }
}


