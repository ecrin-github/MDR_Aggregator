using Dapper.Contrib.Extensions;
namespace MDR_Aggregator;

public class JSONFullStudy
{
    public string? file_type { get; set; }
    public int id { get; set; }
    public string? display_title { get; set; }
    public string? brief_description { get; set; }
    public string? data_sharing_statement { get; set; }
    public int? study_start_year { get; set; }
    public int? study_start_month { get; set; }  
    public Lookup? study_type { get; set; }
    public Lookup? study_status { get; set; }
    public string? study_enrolment { get; set; }
    public Lookup? study_gender_elig { get; set; }
    public age_param? min_age { get; set; }
    public age_param? max_age { get; set; }
    public string? provenance_string { get; set; }

    public List<study_identifier>? study_identifiers { get; set; }
    public List<study_title>? study_titles { get; set; }
    public List<study_person>? study_people { get; set; }
    public List<study_organisation>? study_organisations { get; set; }
    public List<study_topic>? study_topics { get; set; }
    public List<study_feature>? study_features { get; set; }
    public List<study_condition>? study_conditions { get; set; }
    public List<study_icd>? study_icds { get; set; }
    public List<study_country>? study_countries { get; set; }
    public List<study_location>? study_locations { get; set; }
    public List<study_relationship>? study_relationships { get; set; }
    public List<int>? linked_data_objects { get; set; }

    public JSONFullStudy(int _id, string? _display_title,
                     string? _brief_description, string? _data_sharing_statement,
                     int? _study_start_year, int? _study_start_month,
                     Lookup? _study_type, Lookup? _study_status, string? _study_enrolment,
                     Lookup? _study_gender_elig, age_param? _min_age, age_param? _max_age,
                     string? _provenance_string)
    {
        file_type = "study";
        id = _id;
        display_title = _display_title;
        brief_description = _brief_description;
        data_sharing_statement = _data_sharing_statement;
        study_start_year = _study_start_year;
        study_start_month = _study_start_month;
        study_type = _study_type;
        study_status = _study_status;
        study_enrolment = _study_enrolment;
        study_gender_elig = _study_gender_elig;
        min_age = _min_age;
        max_age = _max_age;
        provenance_string = _provenance_string;
    }
}

[Table("search.new_studies")]
public class StudyToSearchRecord
{
    public int study_id { get; set; }
    public string? study_name { get; set; }
    public int? start_year { get; set; }
    public int? start_month { get; set; }
    public int? type_id { get; set; }
    public int? status_id { get; set; }
    public int? phase_id { get; set; }
    public int? alloc_id { get; set; }   
    public string? has_objects { get; set; }

    public StudyToSearchRecord()
    { }
    
    public StudyToSearchRecord(JSONSSearchResStudy srs)
    {
        study_id = srs.study_id;
        study_name = srs.study_name;
        start_year = srs.start_year;
        start_month = srs.start_month;
        type_id = srs.type_id;
        status_id = srs.status_id;
        phase_id = srs.phase_id;
        alloc_id = srs.alloc_id;
        has_objects = srs.has_objects;
    }
}


public class JSONSSearchResStudy
{
    public int study_id { get; set; }
    public string? study_name { get; set; }
    public string? description { get; set; }
    public string? dss { get; set; }
    public int? start_year { get; set; }
    public int? start_month { get; set; }
    public int? type_id { get; set; }
    public string? type_name { get; set; }    
    public int? status_id { get; set; }
    public string? status_name { get; set; }
    public string? gender_elig { get; set; }
    public string? min_age { get; set; }
    public string? max_age { get; set; }
    public int? phase_id { get; set; }
    public int? alloc_id { get; set; }    
    public string? feature_list { get; set; }
    public string? country_list { get; set; }
    public string? condition_list { get; set; }
    public string? has_objects { get; set; }
    public List<JSONSearchResObject>? objects { get; set; }
    public string? provenance { get; set; }

}


public class JSONOAStudy
{
    public string? file_type { get; set; }
    public int id { get; set; }
    public string? display_title { get; set; }
    public string? brief_description { get; set; }
    public string? data_sharing_statement { get; set; }
    public Lookup? study_type { get; set; }
    public Lookup? study_status { get; set; }
    public string? study_enrolment { get; set; }
    public Lookup? study_gender_elig { get; set; }
    public age_param? min_age { get; set; }
    public age_param? max_age { get; set; }
    public string? provenance_string { get; set; }

    public List<study_identifier>? study_identifiers { get; set; }
    public List<study_title>? study_titles { get; set; }
    public List<study_person>? study_people { get; set; }
    public List<study_organisation>? study_organisations { get; set; }
    public List<study_topic>? study_topics { get; set; }
    public List<study_feature>? study_features { get; set; }
    public List<study_condition>? study_conditions { get; set; }
    public List<study_icd>? study_icds { get; set; }
    public List<study_country>? study_countries { get; set; }
    public List<study_location>? study_locations { get; set; }
    public List<study_relationship>? study_relationships { get; set; }
    public List<int>? linked_data_objects { get; set; }

    public JSONOAStudy(int _id, string? _display_title,
                     string? _brief_description, string? _data_sharing_statement,
                     Lookup? _study_type, Lookup? _study_status, string? _study_enrolment,
                     Lookup? _study_gender_elig, age_param? _min_age, age_param? _max_age,
                     string? _provenance_string)
    {
        file_type = "study";
        id = _id;
        display_title = _display_title;
        brief_description = _brief_description;
        data_sharing_statement = _data_sharing_statement;
        study_type = _study_type;
        study_status = _study_status;
        study_enrolment = _study_enrolment;
        study_gender_elig = _study_gender_elig;
        min_age = _min_age;
        max_age = _max_age;
        provenance_string = _provenance_string;
    }
}


public class JSONC19PStudy
{
    public string? file_type { get; set; }
    public int id { get; set; }
    public string? display_title { get; set; }
    public string? brief_description { get; set; }
    public string? data_sharing_statement { get; set; }
    public Lookup? study_type { get; set; }
    public Lookup? study_status { get; set; }
    public string? study_enrolment { get; set; }
    public Lookup? study_gender_elig { get; set; }
    public age_param? min_age { get; set; }
    public age_param? max_age { get; set; }
    public string? provenance_string { get; set; }

    public List<study_identifier>? study_identifiers { get; set; }
    public List<study_title>? study_titles { get; set; }
    public List<study_person>? study_people { get; set; }
    public List<study_organisation>? study_organisations { get; set; }
    public List<study_topic>? study_topics { get; set; }
    public List<study_feature>? study_features { get; set; }
    public List<study_condition>? study_conditions { get; set; }
    public List<study_icd>? study_icds { get; set; }
    public List<study_country>? study_countries { get; set; }
    public List<study_location>? study_locations { get; set; }
    public List<study_relationship>? study_relationships { get; set; }
    public List<int>? linked_data_objects { get; set; }

    public JSONC19PStudy(int _id, string? _display_title,
                     string? _brief_description, string? _data_sharing_statement,
                     Lookup? _study_type, Lookup? _study_status, string? _study_enrolment,
                     Lookup? _study_gender_elig, age_param? _min_age, age_param? _max_age,
                     string? _provenance_string)
    {
        file_type = "study";
        id = _id;
        display_title = _display_title;
        brief_description = _brief_description;
        data_sharing_statement = _data_sharing_statement;
        study_type = _study_type;
        study_status = _study_status;
        study_enrolment = _study_enrolment;
        study_gender_elig = _study_gender_elig;
        min_age = _min_age;
        max_age = _max_age;
        provenance_string = _provenance_string;
    }
}



public class age_param
{
    public int? value { get; set; }
    public int? unit_id { get; set; }
    public string? unit_name { get; set; }

    public age_param(int? _value, int? _unit_id, string? _unit_name)
    {
        value = _value;
        unit_id = _unit_id;
        unit_name = _unit_name;
    }
}

public class study_identifier
{
    public int id { get; set; }
    public string? identifier_value { get; set; }
    public Lookup? identifier_type { get; set; }
    public Organisation? source { get; set; }
    public string? identifier_date { get; set; }
    public string? identifier_link { get; set; }

    public study_identifier(int _id, string? _identifier_value,
        Lookup? _identifier_type, Organisation? _source,
                       string? _identifier_date, string? _identifier_link)
    {
        id = _id;
        identifier_value = _identifier_value;
        identifier_type = _identifier_type;
        source = _source;
        identifier_date = _identifier_date;
        identifier_link = _identifier_link;
    }
}

public class study_title
{
    public int id { get; set; }
    public Lookup? title_type { get; set; }
    public string? title_text { get; set; }
    public string? lang_code { get; set; }
    public string? comments { get; set; }

    public study_title(int _id, Lookup? _title_type,
                       string? _title_text, string? _lang_code,
                       string? _comments)
    {
        id = _id;
        title_type = _title_type;
        title_text = _title_text;
        lang_code = _lang_code;
        comments = _comments;
    }
}


public class study_person
{
    public int id { get; set; }
    public Lookup? contrib_type { get; set; }
    public string? person_full_name { get; set; }
    public string? orcid_id { get; set; }
    public string? person_affiliation { get; set; }
    public Organisation? affiliation_org { get; set; }

    public study_person(int _id, Lookup? _contrib_type,
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


public class study_organisation
{
    public int id { get; set; }
    public Lookup? contrib_type { get; set; }
    public Organisation? org_details { get; set; }

    public study_organisation(int _id, Lookup? _contrib_type,
        Organisation? _org_details)
    {
        id = _id;
        contrib_type = _contrib_type;
        org_details = _org_details;
    }
}


public class study_topic
{
    public int id { get; set; }
    public Lookup? topic_type { get; set; }
    public string? original_value { get; set; }    
    public CTData? ct_data { get; set; }
    public MeshData? mesh_data { get; set; }

    public study_topic(int _id, Lookup? _topic_type, string? _original_value,
        CTData? _ct_data, MeshData? _mesh_data)
    {
        id = _id;
        topic_type = _topic_type; 
        original_value = _original_value;
        ct_data = _ct_data;   
        mesh_data = _mesh_data;
    }
} 


public class study_condition
{
    public int id { get; set; }
    public string? original_value { get; set; }    
    public CTData? ct_data { get; set; }

    public study_condition(int _id, string? _original_value, CTData? _ct_data)
    {
        id = _id;
        original_value = _original_value;        
        ct_data = _ct_data;       
    }
} 

public class study_icd
{
    public int id { get; set; }
    public ICDData? icd_data { get; set; }

    public study_icd(int _id, ICDData? _icd_data)
    {
        id = _id;
        icd_data = _icd_data;
    }
} 

public class study_feature
{
    public int id { get; set; }
    public Lookup? feature_type { get; set; }
    public Lookup? feature_value { get; set; }

    public study_feature(int _id, Lookup? _feature_type,
        Lookup? _feature_value)
    {
        id = _id;
        feature_type = _feature_type;
        feature_value = _feature_value;
    }
}


public class study_country
{
    public int id { get; set; }
    public int? country_id { get; set; }
    public string? country_name { get; set; }
    public Lookup? status { get; set; }

    public study_country(int _id, int? _country_id,
        string? _country_name, Lookup? _status)
    {
        id = _id;
        country_id = _country_id;
        country_name = _country_name;
        status = _status;
    }
}


public class study_location
{
    public int id { get; set; }
    public Organisation? facility { get; set; }
    public int? city_id { get; set; }
    public string? city_name { get; set; }
    public int? country_id { get; set; }
    public string? country_name { get; set; }
    public Lookup? status { get; set; }

    public study_location(int _id, Organisation? _facility, int? _city_id,
        string? _city_name, int? _country_id, string? _country_name, Lookup? _status)
    {
        id = _id;
        facility = _facility;
        city_id = _city_id;
        city_name = _city_name;
        country_id = _country_id;
        country_name = _country_name;
        status = _status;
    }
}


public class study_relationship
{
    public int id { get; set; }
    public Lookup? relationship_type { get; set; }
    public int? target_study_id { get; set; }

    public study_relationship(int _id, Lookup? _relationship_type,
                              int? _target_study_id)
    {
        id = _id;
        relationship_type = _relationship_type;
        target_study_id = _target_study_id;
    }
}

public class study_object_link
{
    public int id { get; set; }
    public int study_id { get; set; }
    public int object_id { get; set; }
}


[Table("core.studies")]
public class DBStudy
{
    public int id { get; set; }
    public string? display_title { get; set; }
    public string? title_lang_code { get; set; }
    public string? brief_description { get; set; }
    public string? data_sharing_statement { get; set; }
    public int? study_start_year { get; set; }
    public int? study_start_month { get; set; }
    public int? study_type_id { get; set; }
    public string? study_type { get; set; }
    public int? study_status_id { get; set; }
    public string? study_status { get; set; }
    public string? study_enrolment { get; set; }
    public int? study_gender_elig_id { get; set; }
    public string? study_gender_elig { get; set; }
    public int? min_age { get; set; }
    public int? min_age_units_id { get; set; }
    public string? min_age_units { get; set; }
    public int? max_age { get; set; }
    public int? max_age_units_id { get; set; }
    public string? max_age_units { get; set; }
    public int? iec_level { get; set; }
    public string? provenance_string { get; set; }
}

[Table("core.study_identifiers")]
public class DBStudyIdentifier
{
    public int id { get; set; }
    public string? identifier_value { get; set; }
    public int? identifier_type_id { get; set; }
    public string? identifier_type { get; set; }
    public int? source_id { get; set; }
    public string? source { get; set; }
    public string? source_ror_id { get; set; }
    public string? identifier_date { get; set; }
    public string? identifier_link { get; set; }
}


[Table("core.study_titles")]
public class DBStudyTitle
{
    public int id { get; set; }
    public int? title_type_id { get; set; }
    public string? title_type { get; set; }
    public string? title_text { get; set; }
    public string? lang_code { get; set; }
    public string? comments { get; set; }
}

[Table("core.study_people")]
public class DBStudyPerson
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

[Table("core.study_organisations")]
public class DBStudyOrganisation
{
    public int id { get; set; }
    public int? contrib_type_id { get; set; }
    public string? contrib_type { get; set; }
    public int? organisation_id { get; set; }
    public string? organisation_name { get; set; }
    public string? organisation_ror_id { get; set; }
}


[Table("core.study_topics")]
public class DBStudyTopic
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


[Table("core.study_conditions")]
public class DBStudyCondition
{
    public int id { get; set; }
    public string? original_value { get; set; }    
    public int? original_ct_type_id { get; set; }
    public string? original_ct_type { get; set; }
    public string? original_ct_code { get; set; }
    public string? icd_code { get; set; }
    public string? icd_name { get; set; }
}


[Table("core.study_icd")]
public class DBStudyICD
{
    public int id { get; set; }
    public string? icd_code { get; set; }
    public string? icd_name { get; set; }
}


[Table("core.study_features")]
public class DBStudyFeature
{
    public int id { get; set; }
    public int? feature_type_id { get; set; }
    public string? feature_type { get; set; }
    public int? feature_value_id { get; set; }
    public string? feature_value { get; set; }
}


[Table("core.study_countries")]
public class DBStudyCountry
{
    public int id { get; set; }
    public int? country_id { get; set; }
    public string? country_name { get; set; }
    public int? status_id { get; set; }
    public string? status { get; set; }
}


[Table("core.study_locations")]
public class DBStudyLocation
{
    public int id { get; set; }
    public int? facility_org_id { get; set; }
    public string? facility { get; set; }
    public string? facility_ror_id { get; set; }
    public int? city_id { get; set; }
    public string? city_name { get; set; }
    public int? country_id { get; set; }
    public string? country_name { get; set; }
    public int? status_id { get; set; }
    public string? status { get; set; }
}


[Table("core.study_relationships")]
public class DBStudyRelationship
{
    public int id { get; set; }
    public int? relationship_type_id { get; set; }
    public string? relationship_type { get; set; }
    public int? target_study_id { get; set; }
}


[Table("core.study_object_links")]
public class DBStudyObjectLink
{
    public int id { get; set; }
    public int study_id { get; set; }
    public int object_id { get; set; }
}


