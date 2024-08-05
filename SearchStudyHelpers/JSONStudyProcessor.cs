using System.Collections;
using System.Text;
using MDR_Aggregator.SearchObjectHelpers;

namespace MDR_Aggregator.SearchStudyHelpers;

public class JSONStudyProcessor
{
    private readonly JsonStudyDataLayer _repo;

    private Lookup? _studyType;
    private Lookup? _studyStatus;
    private Lookup? _studyGenderElig;
    private age_param? _minAge;
    private age_param? _maxAge;

    private List<study_identifier>? _studyIdentifiers;
    private List<study_title>? _studyTitles;
    private List<study_person>? _studyPeople;
    private List<study_organisation>? _studyOrganisations;
    private List<study_topic>? _studyTopics;
    private List<study_condition>? _studyConditions;
    private List<study_icd>? _studyIcds;
    private List<study_feature>? _studyFeatures;
    private List<study_country>? _studyCountries;
    private List<study_location>? _studyLocations;
    private List<study_relationship>? _studyRelationships;
    private List<int>? _linkedDataObjects;

    public JSONStudyProcessor(JsonStudyDataLayer repo)
    {
        _repo = repo;
    }

    public JSONFullStudy? CreateFullStudyObject(int id)
    {
        // Re-initialise these compound properties.
        
        _studyType = null;
        _studyStatus = null;
        _studyGenderElig = null;
        _minAge = null;
        _maxAge = null;

        _studyIdentifiers = new List<study_identifier>();
        _studyTitles = new List<study_title>();
        _studyPeople = new List<study_person>();
        _studyOrganisations = new List<study_organisation>();
        _studyTopics = new List<study_topic>();
        _studyConditions = new List<study_condition>();
        _studyIcds = new List<study_icd>();
        _studyFeatures = new List<study_feature>();
        _studyCountries = new List<study_country>();
        _studyLocations = new List<study_location>();
        _studyRelationships = new List<study_relationship>();
        _linkedDataObjects = new List<int>();

        // Get the singleton study properties from DB
        // and instantiate the top level lookup types
        
        DBStudy? s = _repo.FetchDbStudy(id);
        if (s is null)
        {
            return null;
        }
        if (s.study_type_id != null)
        {
            _studyType = new Lookup(s.study_type_id, s.study_type);
        }
        if (s.study_status_id != null)
        {
            _studyStatus = new Lookup(s.study_status_id, s.study_status);
        }
        if (s.study_gender_elig_id != null)
        {
            _studyGenderElig = new Lookup(s.study_gender_elig_id, s.study_gender_elig);
        }
        if (s.min_age != null)
        {
            _minAge = new age_param(s.min_age, s.min_age_units_id, s.min_age_units);
        }
        if (s.max_age != null)
        {
            _maxAge = new age_param(s.max_age, s.max_age_units_id, s.max_age_units);
        }
        JSONFullStudy jst = new JSONFullStudy(s.id, s.display_title, s.brief_description,
                     s.data_sharing_statement, s.study_start_year, s.study_start_month, 
                     _studyType, _studyStatus, s.study_enrolment,
                     _studyGenderElig, _minAge, _maxAge, s.provenance_string);

        // fetch the study identifier details

        IEnumerable<DBStudyIdentifier> db_study_identifiers = _repo.FetchDbStudyIdentifiers(id);
        foreach (DBStudyIdentifier t in db_study_identifiers)
        {
            _studyIdentifiers.Add(new study_identifier(t.id, t.identifier_value,
                                  new Lookup(t.identifier_type_id, t.identifier_type),
                                  new Organisation(t.source_id, t.source, t.source_ror_id),
                                  t.identifier_date, t.identifier_link));
        }

        // fetch the study title details
       
        IEnumerable<DBStudyTitle> db_study_titles = _repo.FetchDbStudyTitles(id);
        foreach (DBStudyTitle t in db_study_titles)
        {
            _studyTitles.Add(new study_title(t.id, new Lookup(t.title_type_id, t.title_type), 
                t.title_text, t.lang_code, t.comments));
        }


        // fetch the study people details       
        
        IEnumerable<DBStudyPerson> db_study_people = _repo.FetchDbStudyPeople(id);
        foreach (DBStudyPerson t in db_study_people)
        {
            _studyPeople.Add(new study_person(t.id, new Lookup(t.contrib_type_id, t.contrib_type), 
                t.person_full_name, t.orcid_id, t.person_affiliation, 
                new Organisation(t.organisation_id, t.organisation_name, t.organisation_ror_id)));
        }
        
        
        // fetch the study organisations details
        
        IEnumerable<DBStudyOrganisation> db_study_orgs = _repo.FetchDbStudyOrganisations(id);
        foreach (DBStudyOrganisation t in db_study_orgs)
        {
            _studyOrganisations.Add(new study_organisation(t.id, new Lookup(t.contrib_type_id, t.contrib_type),  
                new Organisation(t.organisation_id, t.organisation_name, t.organisation_ror_id)));
        }
        

        // fetch the study topic details
        
        IEnumerable<DBStudyTopic> db_study_topics = _repo.FetchDbStudyTopics(id);
        foreach (DBStudyTopic t in db_study_topics)
        {
            MeshData? md = null;
            CTData? ct = null;
            if (t.mesh_code is not null && t.mesh_value is not null)
            {
                md = new MeshData(t.mesh_code, t.mesh_value);
            }
            if (t.original_ct_type_id is not null && t.original_ct_code is not null)
            {
                ct = new CTData(t.original_ct_type_id, t.original_ct_type, t.original_ct_code);
            }
            _studyTopics.Add(new study_topic(t.id, new Lookup(t.topic_type_id, t.topic_type),
                t.original_value, ct, md));
        }

        
        // fetch the study condition details
        
        IEnumerable<DBStudyCondition> db_study_conditions = _repo.FetchDbStudyConditions(id);
        foreach (DBStudyCondition t in db_study_conditions)
        {
            CTData? ct = null;
            if (t.original_ct_type_id is not null && t.original_ct_code is not null)
            {
                ct = new CTData(t.original_ct_type_id, t.original_ct_type, t.original_ct_code);
            }
            _studyConditions.Add(new study_condition(t.id, t.original_value, ct));
        }
        
        
        // fetch the study icd details
        
        IEnumerable<DBStudyICD> db_study_icds = _repo.FetchDbStudyICDs(id);
        foreach (DBStudyICD t in db_study_icds)
        {
            _studyIcds.Add(new study_icd(t.id, new ICDData(t.icd_code, t.icd_name)));
        }
        
        
        // fetch the study feature details
                
        IEnumerable<DBStudyFeature> db_study_features =_repo.FetchDbStudyFeatures(id);
        foreach (DBStudyFeature t in db_study_features)
        {
            _studyFeatures.Add(new study_feature(t.id, new Lookup(t.feature_type_id, t.feature_type),
                                                new Lookup(t.feature_value_id, t.feature_value)));
        }
        
        
        // fetch the study country details
        
        IEnumerable<DBStudyCountry> db_study_countries = _repo.FetchDbStudyCountries(id);
        foreach (DBStudyCountry t in db_study_countries)
        {
            Lookup? status = null;
            if (t.status_id is not null && t.status is not null)
            {
                status = new Lookup(t.status_id, t.status);
            }
            _studyCountries.Add(new study_country(t.id, t.country_id, t.country_name, status));
        }
        
        
        // fetch any study location details, if any
        
        IEnumerable<DBStudyLocation> db_study_locations = _repo.FetchDbStudyLocations(id);
        foreach (DBStudyLocation t in db_study_locations)
        {
            Lookup? status = null;
            if (t.status_id is not null && t.status is not null)
            {
                status = new Lookup(t.status_id, t.status); 
            }
            _studyLocations.Add(new study_location(t.id, 
                new Organisation(t.facility_org_id, t.facility, t.facility_ror_id),
                t.city_id, t.city_name, t.country_id, t.country_name, status));
        }


        // fetch the study relationships, if any

        IEnumerable<DBStudyRelationship> db_study_relationships = _repo.FetchDbStudyRelationships(id);
        foreach (DBStudyRelationship t in db_study_relationships)
        {
            _studyRelationships.Add(new study_relationship(t.id,
                                    new Lookup(t.relationship_type_id, t.relationship_type),
                                    t.target_study_id));
        }


        // fetch the related objects data
        
        IEnumerable<DBStudyObjectLink>  db_study_object_links = _repo.FetchDbStudyObjectLinks(id);
        foreach (DBStudyObjectLink t in db_study_object_links)
        {
            _linkedDataObjects.Add(t.object_id);
        }

        // return the resulting 'json ready' full study
        
        jst.study_identifiers = _studyIdentifiers.Any() ? _studyIdentifiers : null;
        jst.study_titles = _studyTitles.Any() ? _studyTitles : null;
        jst.study_people = _studyPeople.Any() ? _studyPeople : null;
        jst.study_organisations = _studyOrganisations.Any() ? _studyOrganisations : null;
        jst.study_features = _studyFeatures.Any() ? _studyFeatures : null;
        jst.study_topics = _studyTopics.Any() ? _studyTopics : null;
        jst.study_conditions = _studyConditions.Any() ? _studyConditions : null;
        jst.study_icds = _studyIcds.Any() ? _studyIcds : null;
        jst.study_countries = _studyCountries.Any() ? _studyCountries : null;
        jst.study_locations = _studyLocations.Any() ? _studyLocations : null;
        jst.study_relationships = _studyRelationships.Any() ? _studyRelationships : null;
        jst.linked_data_objects = _linkedDataObjects.Any() ? _linkedDataObjects : null;

        return jst;
    }

    public int AddNewStudySearchRecord(JSONSSearchResStudy srs)
    {
        StudyToSearchRecord tsr = new(srs);
        return _repo.StoreSearchRecord(tsr);
    }

    public JSONSSearchResStudy CreateStudySearchResult(JSONFullStudy st)
    {
        JSONSSearchResStudy srs = new JSONSSearchResStudy();
        srs.study_id = st.id;
        srs.study_name = st.display_title;
        srs.description = st.brief_description;
        srs.dss = st.data_sharing_statement;
        srs.start_year = st.study_start_year;
        srs.start_month = st.study_start_month;
        srs.type_id = st.study_type?.id ?? 00;
        srs.type_name = st.study_type?.name;
        srs.status_id = st.study_status?.id ?? 0;
        srs.status_name = st.study_status?.name;
        srs.gender_elig = st.study_gender_elig?.name;
        string min_age_units = st.min_age?.unit_id == 17 ? "" : " " + st.min_age?.unit_name;
        srs.min_age = st.min_age is not null ? st.min_age.value + min_age_units : null;
        string max_age_units = st.max_age?.unit_id == 17 ? "" : " " + st.max_age?.unit_name;
        srs.max_age = st.max_age is not null ? st.max_age.value + max_age_units : null;

        List<study_feature>? fs = st.study_features;
        
        if (fs?.Any() == true)
        {
            switch (srs.type_id)
            {
                case 11:
                {
                    string phase = "", alloc = "", focus = "", interv = "", masking = "";
                    foreach (study_feature f in fs)
                    {
                        switch (f.feature_type!.id)
                        {
                            case 20:
                            {
                                srs.phase_id = f.feature_value!.id;
                                string? ph = f.feature_value?.name;
                                if (!string.IsNullOrEmpty(ph) && ph.ToLower() != "not applicable")
                                {
                                    phase = "Phase: " + ph;
                                }

                                break;
                            }
                            case 21:
                            {
                                string? fc = f.feature_value?.name;
                                if (!string.IsNullOrEmpty(fc) && fc.ToLower() != "other")
                                {
                                    focus = "Focus: " + fc;
                                }

                                break;
                            }
                            case 22:
                            {
                                srs.alloc_id = f.feature_value!.id;
                                string? ac = f.feature_value?.name;
                                if (string.IsNullOrEmpty(ac))
                                {
                                    alloc = "Randomised: Not provided";
                                }
                                else
                                {
                                    alloc = "Randomised: " + ac.ToLower() switch
                                    {
                                        "randomised" => "Yes",
                                        "nonrandomised" => "No",
                                        "not applicable" => "No",
                                        _ => "Unclear"
                                    };
                                }

                                break;
                            }
                            case 23:
                            {
                                string? iv = f.feature_value?.name;
                                if (!string.IsNullOrEmpty(iv) && iv.ToLower() != "other")
                                {
                                    interv = "Intervention design: " + iv;
                                }

                                break;
                            }
                            case 24:
                            {
                                string? mk = f.feature_value?.name;
                                if (!string.IsNullOrEmpty(mk) && mk.ToLower() != "not applicable")
                                {
                                    masking = "Masking: " + mk;
                                }

                                break;
                            }
                        }
                    }
                    string int_feature_list = alloc;
                    if (!string.IsNullOrEmpty(phase))
                    { 
                        int_feature_list += !string.IsNullOrEmpty(int_feature_list) ? "; " + phase : phase;
                    }
                    if (!string.IsNullOrEmpty(focus))
                    { 
                        int_feature_list += !string.IsNullOrEmpty(int_feature_list) ? "; " + focus : focus;
                    }
                    if (!string.IsNullOrEmpty(interv))
                    { 
                        int_feature_list += !string.IsNullOrEmpty(int_feature_list) ? "; " + interv : interv;
                    }
                    if (!string.IsNullOrEmpty(masking))
                    { 
                        int_feature_list += !string.IsNullOrEmpty(int_feature_list) ? "; " + masking : masking;
                    }
                    srs.feature_list = int_feature_list;
                    break;
                }
                case 12:
                {
                    string time_persp = "", obs_model = "", bio_spec = "";
                    foreach (study_feature f in fs)
                    {
                        switch (f.feature_type!.id)
                        {
                            case 30:
                            {
                                string? tp = f.feature_value?.name;
                                if (!string.IsNullOrEmpty(tp))
                                {
                                    time_persp = "Time Perspective: ";
                                    time_persp += tp == "Other" ? "Not specified" : tp;
                                }

                                break;
                            }
                            case 31:
                            {
                                string? om = f.feature_value?.name;
                                if (!string.IsNullOrEmpty(om) && om.ToLower() != "other")
                                {
                                    obs_model = "Observation model: " + om;
                                }

                                break;
                            }
                            case 32:
                            {
                                string? bs = f.feature_value?.name;
                                if (!string.IsNullOrEmpty(bs) && bs.ToLower() != "other")
                                {
                                    bio_spec = bs.Trim() + " reported as available";
                                }

                                break;
                            }
                        }
                    }
                    string obs_feature_list = time_persp;
                    if (!string.IsNullOrEmpty(obs_model))
                    { 
                        obs_feature_list += !string.IsNullOrEmpty(obs_feature_list) ? "; " + obs_model : obs_model;
                    }
                    if (!string.IsNullOrEmpty(bio_spec))
                    { 
                        obs_feature_list += !string.IsNullOrEmpty(obs_feature_list) ? "; " + bio_spec : bio_spec;
                    }

                    srs.feature_list = obs_feature_list;
                    break;
                }
            }
        }
        
        List<study_condition>? sds = st.study_conditions;
        if (sds?.Any() == true)
        {
            string condition_list = sds.Aggregate("", (current, sd) => current + (", " + sd.original_value));
            srs.condition_list = condition_list[2..];
        }

        List<study_country>? scs = st.study_countries;
        if (scs?.Any() == true)
        {
            string country_list = scs.Aggregate("", (current, sc) => current + (", " + sc.country_name));
            srs.country_list = country_list[2..];
        }
        srs.provenance = st.provenance_string;

        List<JSONSearchResObject>? obs  = _repo.FetchObjectDetails(st.id)?.ToList();
        if (obs?.Any() != true) return srs;
        if (obs is [{ typeid: 13 }])
        {
            srs.has_objects = "100000000000000000";  // almost always the case for a single object
        }
        else
        {
            int?[] typeids = obs.Select(b => b.typeid).ToArray();
            srs.has_objects = GetHasObjectsString(typeids);
        }
            
        srs.objects = obs;

        return srs;
    }


    private string GetHasObjectsString(int?[] type_ids)
    {
        BitArray has_objects = new BitArray(18);   //18 positions
        foreach (int? type_id in type_ids)
        {
            switch (type_id)
            {
                case 13:
                    has_objects[0] = true; // registry entry
                    break;
                case 28:
                    has_objects[1] = true; // registry results
                    break;
                case 12 or 100 or 117 or 135 or 152 or 202 or 203 or 204 or 210:
                    has_objects[2] = true; // Article
                    break;
                case 201:
                    has_objects[2] = true; // Article
                    has_objects[3] = true; // Protocol
                    break;
                case 11 or 42:
                    has_objects[3] = true; // Protocol
                    break;
                case 74:
                    has_objects[3] = true; // Protocol
                    has_objects[8] = true; // SAP
                    break;
                case 75:
                    has_objects[3] = true; // Protocol
                    has_objects[5] = true; // PIC/IS
                    break;
                case 76:
                    has_objects[3] = true; // Protocol
                    has_objects[5] = true; // PIC/IS
                    has_objects[8] = true; // SAP
                    break;
                case 38:
                    has_objects[4] = true; // study overview
                    break;
                case 18 or 19:
                    has_objects[5] = true; // PIC/IS
                    break;
                case 21 or 30 or 40:
                    has_objects[6] = true; // CRFs
                    break;
                case 35 or 36:
                    has_objects[7] = true; // Procedures
                    break;
                case 22 or 29 or 43:
                    has_objects[8] = true; // SAP
                    break;
                case 26 or 27 or 29 or 85:
                    has_objects[9] = true; // CSR
                    break;
                case 20 or 31 or 32 or 81 or 82 or 73:
                    has_objects[10] = true; // Data Description
                    break;
                case 80 or 153 or 154 or (>= 51 and <= 72):
                    has_objects[11] = true; // IPD
                    break;
                case 14 or 15 or 16 or 17 or 23 or 24
                    or 25 or 33 or 34 or 39 or 77 or 78 or 83 or 84
                    or 86 or 115 or 171:
                    has_objects[12] = true; // Other resource
                    break;
                case 88 or 106 or 107 or 108 or 109
                    or 119 or 121 or 122 or 101 or 102 or 103 or 104
                    or 105 or 113 or 112 or 120 or 123 or 126 or 127 or 128:
                    has_objects[13] = true; // Other info
                    break;
                case 134:
                    has_objects[14] = true; // Website
                    break;
                case >= 166 and <= 170:
                    has_objects[15] = true; // Software
                    break;
                case 37 or 151 or 110 or 111 or 114 or
                    118 or 124 or 125 or 129 or 130 or 131 or 132 or 133
                    or (>= 155 and <= 165):
                    has_objects[16] = true; // Other
                    break;
                case 301:
                    has_objects[17] = true; // Samples
                    break;
            }
        }
        var sb = new StringBuilder();
        for (int i = 0; i < 18; i++)
        {
            char c = has_objects[i] ? '1' : '0';
            sb.Append(c);
        }
        return sb.ToString();
    }
    
    
    public JSONOAStudy? CreateStudyOAObject(JSONFullStudy st)
    {
        return null;
    }
    
    public JSONC19PStudy? CreateStudyC19PStudyObject(JSONFullStudy st)
    {
        return null;
    }
    

    public void StoreJSONStudyInDB(int id, string fullJson, string? searchResJson, 
                                   string? openAireJson, string? c19PJson)
    {
        _repo.StoreJSONStudyInDB(id, fullJson, searchResJson, openAireJson, c19PJson);
    }
    
    
}

