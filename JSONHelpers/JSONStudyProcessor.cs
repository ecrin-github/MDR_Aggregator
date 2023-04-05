namespace MDR_Aggregator;

public class JSONStudyProcessor
{
    private readonly JSONStudyDataLayer _repo;

    private Lookup? study_type;
    private Lookup? study_status;
    private Lookup? study_gender_elig;
    private age_param? min_age;
    private age_param? max_age;

    private List<study_identifier>? study_identifiers;
    private List<study_title>? study_titles;
    private List<study_topic>? study_topics;
    private List<study_feature>? study_features;
    private List<study_relationship>? study_relationships;
    private List<int>? linked_data_objects;

    public JSONStudyProcessor(JSONStudyDataLayer repo)
    {
        _repo = repo;
    }

    public JSONStudy CreateStudyObject(int id)
    {
        // Re-initialise these compound properties.
        study_type = null;
        study_status = null;
        study_gender_elig = null;
        min_age = null;
        max_age = null;

        study_identifiers = null;
        study_titles = null;
        study_topics = null;
        study_features = null;
        study_relationships = null;
        linked_data_objects = null;

        // Get the singleton study properties from DB

        var s = _repo.FetchDbStudy(id);

        // Instantiate the top level lookup types
        if (s.study_type_id != null)
        {
            study_type = new Lookup(s.study_type_id, s.study_type);
        }
        if (s.study_status_id != null)
        {
            study_status = new Lookup(s.study_status_id, s.study_status);
        }
        if (s.study_gender_elig_id != null)
        {
            study_gender_elig = new Lookup(s.study_gender_elig_id, s.study_gender_elig);
        }
        if (s.min_age != null)
        {
            min_age = new age_param(s.min_age, s.min_age_units_id, s.min_age_units);
        }
        if (s.max_age != null)
        {
            max_age = new age_param(s.max_age, s.max_age_units_id, s.max_age_units);
        }

        // instantiate a (json) study object and
        // fill it with study level details

        JSONStudy jst = new JSONStudy(s.id, s.display_title, s.brief_description,
                     s.data_sharing_statement, study_type, study_status, s.study_enrolment,
                     study_gender_elig, min_age, max_age, s.provenance_string);


        // add the study identifier details

        var db_study_identifiers = new List<DBStudyIdentifier>(_repo.FetchDbStudyIdentifiers(id));
        if (db_study_identifiers.Count > 0)
        {
            study_identifiers = new List<study_identifier>();
            foreach (DBStudyIdentifier t in db_study_identifiers)
            {
                study_identifiers.Add(new study_identifier(t.id, t.identifier_value,
                                      new Lookup(t.identifier_type_id, t.identifier_type),
                                      new Organisation(t.identifier_org_id, t.identifier_org, t.identifier_org_ror_id),
                                      t.identifier_date, t.identifier_link));
            }
        }


        // add the study title details

        var db_study_titles = new List<DBStudyTitle>(_repo.FetchDbStudyTitles(id));
        if (db_study_titles.Count > 0)
        {
            study_titles = new List<study_title>();
            foreach (DBStudyTitle t in db_study_titles)
            {
                study_titles.Add(new study_title(t.id, new Lookup(t.title_type_id, t.title_type), t.title_text,
                                    t.lang_code, t.comments));
            }
        }


        // add the study feature details
        var db_study_features = new List<DBStudyFeature>(_repo.FetchDbStudyFeatures(id));
        if (db_study_features.Count > 0)
        {
            study_features = new List<study_feature>();
            foreach (DBStudyFeature t in db_study_features)
            {
                study_features.Add(new study_feature(t.id, new Lookup(t.feature_type_id, t.feature_type),
                                                    new Lookup(t.feature_value_id, t.feature_value)));
            }
        }


        // add the study topic details
        var db_study_topics = new List<DBStudyTopic>(_repo.FetchDbStudyTopics(id));
        if (db_study_topics.Count > 0)
        {
            study_topics = new List<study_topic>();
            foreach (DBStudyTopic t in db_study_topics)
            {
                study_topics.Add(new study_topic(t.id, new Lookup(t.topic_type_id, t.topic_type),
                                     t.mesh_code, t.mesh_value,
                                     t.original_ct_id, t.original_ct_code, t.original_value));
            }
        }


        // add the study relationships, if any
        var db_study_relationships = new List<DBStudyRelationship>(_repo.FetchDbStudyRelationships(id));
        if (db_study_relationships.Count > 0)
        {
            study_relationships = new List<study_relationship>();
            foreach (DBStudyRelationship t in db_study_relationships)
            {
                study_relationships.Add(new study_relationship(t.id,
                                        new Lookup(t.relationship_type_id, t.relationship_type),
                                        t.target_study_id));
            }
        }


        // add the related objects data
        var db_study_object_links = new List<DBStudyObjectLink>(_repo.FetchDbStudyObjectLinks(id));
        if (db_study_object_links.Count > 0)
        {
            linked_data_objects = new List<int>();
            foreach (DBStudyObjectLink t in db_study_object_links)
            {
                linked_data_objects.Add(t.object_id);
            }
        }


        // return the resulting 'json ready' study
        jst.study_identifiers = study_identifiers;
        jst.study_titles = study_titles;
        jst.study_features = study_features;
        jst.study_topics = study_topics;
        jst.study_relationships = study_relationships;
        jst.linked_data_objects = linked_data_objects;

        return jst;
    }


    public void StoreJSONStudyInDB(int id, string study_json)
    {
        _repo.StoreJSONStudyInDB(id, study_json); ;
    }
}

