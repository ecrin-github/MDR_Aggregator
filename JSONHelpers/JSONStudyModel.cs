using Dapper.Contrib.Extensions;
namespace MDR_Aggregator;

public class JSONStudy
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
    public List<study_topic>? study_topics { get; set; }
    public List<study_feature>? study_features { get; set; }
    public List<study_relationship>? study_relationships { get; set; }
    public List<int>? linked_data_objects { get; set; }

    public JSONStudy(int _id, string? _display_title,
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


public class study_topic
{
    public int id { get; set; }
    public Lookup? topic_type { get; set; }
    public string? mesh_code { get; set; }
    public string? mesh_value { get; set; }
    public int? original_ct_id { get; set; }
    public string? original_ct_code { get; set; }
    public string? original_value { get; set; }

    public study_topic(int _id, Lookup? _topic_type, string? _mesh_code,
                         string? _mesh_value, int? _original_ct_id,
                         string? _original_ct_code, string? _original_value)
    {
        id = _id;
        topic_type = _topic_type;
        mesh_code = _mesh_code;
        mesh_value = _mesh_value;
        original_ct_id = _original_ct_id;
        original_ct_code = _original_ct_code;
        original_value = _original_value;
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



[Table("core.study_topics")]
public class DBStudyTopic
{
    public int id { get; set; }
    public int? topic_type_id { get; set; }
    public string? topic_type { get; set; }
    public string? mesh_code { get; set; }
    public string? mesh_value { get; set; }
    public int? original_ct_id { get; set; }
    public string? original_ct_code { get; set; }
    public string? original_value { get; set; }
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


[Table("core.studies_json")]
public class DBStudyJSON
{
    public int id { get; set; }
    public string json { get; set; }

    public DBStudyJSON(int _id, string _json)
    {
        id = _id;
        json = _json;
    }
}
