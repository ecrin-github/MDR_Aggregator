using MDR_Aggregator.LoggingHelpers.Interfaces;

namespace MDR_Aggregator.SearchObjectHelpers;

class JSONObjectProcessor
{
    private readonly JSONObjectDataLayer _repo;
    private readonly ILoggingHelper _loggingHelper;

    private DBDataObject? _ob;
    private Lookup? _objectClass;
    private Lookup? _objectType;
    private Organisation? _managingOrganisation;
    private Lookup? _accessType;
    private object_access? _accessDetails;
    private record_keys? _dsRecordKeys;
    private Deidentification? _dsDeidentLevel;
    private Consent? _dsConsent;
   
    private List<object_instance>? _objectInstances;
    private List<object_title>? _objectTitles;
    private List<object_person>? _objectPeople;
    private List<object_organisation>? _objectOrganisations;
    private List<object_date>? _objectDates;
    private List<object_topic>? _objectTopics;
    private List<object_description>? _objectDescriptions;
    private List<object_identifier>? _objectIdentifiers;
    private List<object_right>? _objectRights;
    private List<object_relationship>? _objectRelationships;
    private List<int>? _linkedStudies;

    public JSONObjectProcessor(JSONObjectDataLayer repo, ILoggingHelper loggingHelper)
    {
        _repo = repo;
        _loggingHelper = loggingHelper;
    }

    public JSONFullObject? CreateFullObject(int id)
    {
        // Re-initialise these compound properties.

        _objectClass = null;
        _objectType = null;
        _accessType = null;
        _managingOrganisation = null;
        _accessDetails = null;
        
        _dsRecordKeys = null;
        _dsDeidentLevel = null;
        _dsConsent = null;

        _objectTitles = new List<object_title>(); 
        _objectPeople = new List<object_person>();
        _objectOrganisations = new List<object_organisation>(); 
        _objectDates = new List<object_date>(); 
        _objectInstances = new List<object_instance>(); 
        _objectTopics = new List<object_topic>(); 
        _objectIdentifiers = new List<object_identifier>(); 
        _objectDescriptions = new List<object_description>(); 
        _objectRights = new List<object_right>(); 
        _objectRelationships = new List<object_relationship>(); 
        _linkedStudies = new List<int>(); 

        // Get the singleton data object properties from DB

        _ob = _repo.FetchDbDataObject(id);
        if (_ob is null)
        {
            return null;
        }

        // First check there is at least one linked study
       
        _linkedStudies = new List<int>(_repo.FetchLinkedStudies(id));
        if (_linkedStudies.Count == 0)
        {
            // May occur in a few cases, if it does need to investigate further !!!!!!!
            // Seems to be due to a (minor) error in data object linkage with journal articles.
            
            _loggingHelper.LogError("object " + (_ob?.id ?? 0).ToString() + " does not appear to be linked to studies");
            return null;
        }

        // Instantiate the top level lookup types

        _objectClass = new Lookup(_ob.object_class_id, _ob.object_class);
        _objectType = new Lookup(_ob.object_type_id, _ob.object_type);
        if (_ob.managing_org != null)
        {
            _managingOrganisation = new Organisation(_ob.managing_org_id, _ob.managing_org, _ob.managing_org_ror_id);
        }
        if (_ob.access_type_id != null)
        {
            _accessType = new Lookup(_ob.access_type_id, _ob.access_type);
        }
        if (_ob.access_details != null || _ob.access_details_url != null)
        {
            _accessDetails = new object_access(_ob.access_details, _ob.access_details_url, _ob.url_last_checked);
        }

        // Instantiate data object with those details

        JSONFullObject dobj = new JSONFullObject(_ob.id, _ob.doi, _ob.display_title, _ob.version, _objectClass,
                              _objectType, _ob.publication_year, _managingOrganisation, _ob.lang_code,
                              _accessType, _accessDetails, _ob.eosc_category, _ob.provenance_string);

        // Get dataset properties, if there are any...

        var db_ds = _repo.FetchDbDatasetProperties(id);
        if (db_ds is not null)
        {
            _dsRecordKeys = new record_keys(db_ds.record_keys_type_id, db_ds.record_keys_type, db_ds.record_keys_details);
            _dsDeidentLevel = new Deidentification(db_ds.deident_type_id, db_ds.deident_type, db_ds.deident_direct,
                                         db_ds.deident_hipaa, db_ds.deident_dates, db_ds.deident_nonarr, 
                                         db_ds.deident_kanon, db_ds.deident_details);
            _dsConsent = new Consent(db_ds.consent_type_id, db_ds.consent_type, db_ds.consent_noncommercial,
                                         db_ds.consent_geog_restrict, db_ds.consent_research_type, db_ds.consent_genetic_only,
                                         db_ds.consent_no_methods, db_ds.consent_details);

            dobj.dataset_record_keys = _dsRecordKeys;
            dobj.dataset_deident_level = _dsDeidentLevel;
            dobj.dataset_consent = _dsConsent;
        }


        // Get object instances.

        IEnumerable<DBObjectInstance> db_object_instances = _repo.FetchObjectInstances(id);
        foreach (DBObjectInstance i in db_object_instances)
        {
            Lookup? repo_org = null;
            access_details? access = null;
            resource_details? resource = null;
            if (i.system != null)
            {
                repo_org = new Lookup(i.system_id, i.system);
            }
            if (i.url != null || i.url_accessible != null)
            {
                access = new access_details(i.url, i.url_accessible, i.url_last_checked);
            }
            if (i.resource_type_id != null || i.comments != null)
            {
                resource = new resource_details(i.resource_type_id, i.resource_type,
                                        i.resource_size, i.resource_size_units, i.comments);
            }
            _objectInstances.Add(new object_instance(i.id, repo_org, access, resource));
        }


        // Get object titles.

        var db_object_titles = _repo.FetchObjectTitles(id);
        foreach (var t in db_object_titles)
        {
            _objectTitles.Add(new object_title(t.id, new Lookup(t.title_type_id, t.title_type), t.title_text,
                                t.lang_code, t.comments));
        }


        // Get object dates.

        IEnumerable<DBObjectDate> db_object_dates = _repo.FetchObjectDates(id);
        foreach (DBObjectDate d in db_object_dates)
        {        
            sdate_as_ints? start_date = null;
            edate_as_ints? end_date = null;
            if (d.start_year != null || d.start_month != null || d.start_day != null)
            {
                start_date = new sdate_as_ints(d.start_year, d.start_month, d.start_day);
            }
            if (d.end_year != null || d.end_month != null || d.end_day != null)
            {
                end_date = new edate_as_ints(d.end_year, d.end_month, d.end_day);
            }
            _objectDates.Add(new object_date(d.id, new Lookup(d.date_type_id, d.date_type), d.date_is_range,
                                        d.date_as_string, start_date, end_date, d.comments));
        }

        
        // Get object descriptions.

        IEnumerable<DBObjectDescription> db_object_descriptions = _repo.FetchObjectDescriptions(id);
        foreach (DBObjectDescription i in db_object_descriptions)
        {
            _objectDescriptions.Add(new object_description(i.id, new Lookup(i.description_type_id, i.description_type),
                                 i.label, i.description_text, i.lang_code));
        }
        
        // The 4 functions below are currently only required for Pubmed objects. To save time
        // it is easier to therefore only apply them to these objects. In the core tables an object's source
        // is no longer apparent, but - AT THE MOMENT AT LEAST - the add_study_contribs and add_study_topics
        // can be used as a proxy for a PubMed object, as these are false only for these objects.

        if (_ob.add_study_contribs == false && _ob.add_study_topics == false)
        {
            // Get object people 

            IEnumerable<DBObjectPerson> db_object_people = _repo.FetchObjectPeople(id);
            foreach (DBObjectPerson t in db_object_people)
            {
                _objectPeople.Add(new object_person(t.id, new Lookup(t.contrib_type_id, t.contrib_type),
                    t.person_full_name, t.orcid_id, t.person_affiliation,
                    new Organisation(t.organisation_id, t.organisation_name, t.organisation_ror_id)));
            }

            // Get object organisations

            IEnumerable<DBObjectOrganisation> db_object_organisations = _repo.FetchObjectOrganisations(id);
            foreach (DBObjectOrganisation t in db_object_organisations)
            {
                _objectOrganisations.Add(new object_organisation(t.id, new Lookup(t.contrib_type_id, t.contrib_type),
                    new Organisation(t.organisation_id, t.organisation_name, t.organisation_ror_id)));
            }

            // Get object topics 

            IEnumerable<DBObjectTopic> db_object_topics = _repo.FetchObjectTopics(id);
            foreach (DBObjectTopic t in db_object_topics)
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

                _objectTopics.Add(new object_topic(t.id, new Lookup(t.topic_type_id, t.topic_type),
                    t.original_value, ct, md));
            }


            // Get object identifiers.

            IEnumerable<DBObjectIdentifier> db_object_identifiers = _repo.FetchObjectIdentifiers(id);
            foreach (DBObjectIdentifier i in db_object_identifiers)
            {
                _objectIdentifiers.Add(new object_identifier(i.id, i.identifier_value,
                    new Lookup(i.identifier_type_id, i.identifier_type),
                    new Organisation(i.source_id, i.source, i.source_ror_id),
                    i.identifier_date));
            }
        }


        /*
         * The two routines below not currently used as there are no object rights or
         * relationships in the system. Uncomment the lines below to support these if and
         * when required in the future.

        // Get object rights.

        IEnumerable<DBObjectRight> db_object_rights = _repo.FetchObjectRights(id);
        foreach (DBObjectRight i in db_object_rights)
        {
            object_rights.Add(new object_right(i.id, i.rights_name, i.rights_uri, i.comments));
        }

        // Get object relationships.

        IEnumerable<DBObjectRelationship> db_object_relationships =_repo.FetchObjectRelationships(id);
        foreach (DBObjectRelationship i in db_object_relationships)
        {
            object_relationships.Add(new object_relationship(i.id, new Lookup(i.relationship_type_id, i.relationship_type),
                                                             i.target_object_id));
        }
        */


        // Construct the final data object by setting the composite 
        // and recreated properties to the classes and List<>s created above.

        dobj.dataset_consent = _dsConsent;
        dobj.dataset_record_keys = _dsRecordKeys;
        dobj.dataset_deident_level = _dsDeidentLevel;
        
        dobj.object_instances = _objectInstances.Any() ? _objectInstances : null;
        dobj.object_titles = _objectTitles.Any() ? _objectTitles : null;
        dobj.object_dates = _objectDates.Any() ? _objectDates : null;
        dobj.object_descriptions = _objectDescriptions.Any() ? _objectDescriptions : null;
        
        dobj.object_identifiers = _objectIdentifiers.Any() ? _objectIdentifiers : null;
        dobj.object_people = _objectPeople.Any() ? _objectPeople : null;
        dobj.object_organisations = _objectOrganisations.Any() ? _objectOrganisations : null;
        dobj.object_topics = _objectTopics.Any() ? _objectTopics : null;
        
        dobj.object_relationships = _objectRelationships.Any() ? _objectRelationships : null;
        dobj.object_rights = _objectRights.Any() ? _objectRights : null;     
        
        dobj.linked_studies = _linkedStudies.Any() ? _linkedStudies : null;
        return dobj;
    }

    public List<JSONSearchResObject> CreateSearchResObjects(JSONFullObject fob)
    {
        List<JSONSearchResObject> ores = new();
        string pub_year = fob.publication_year.ToString() ?? "No public. date";
        List<object_instance>? fi = fob.object_instances;

        if (fi?.Any() == true)
        {
            // for most objects there will be one instance.
            // For some journal articles there will be 2 (abstract and article)
            // For some 'virtual' objects with restricted access there will be none (see below)
            
            foreach (object_instance oi in fi)
            {
                int? access_type_id = fob.access_type?.id;
                int? resource_type_id = oi.resource_details?.type_id;
                string acc_icon = "X";  // default
                if (resource_type_id == 40)
                {
                    // S and T currently the same as R and G respectively
                    
                    acc_icon = access_type_id == 15 ? "S" : "T";
                }
                else
                {
                    if (access_type_id is 11 or 12 or 13 or 14 or 20)
                    {
                        acc_icon = "G";  // green
                    }
                    if (access_type_id is 15 or 16 or 17 or 18 or 19)
                    {
                        acc_icon = "R";  // orange
                    }
                }

                string res_icon = resource_type_id  switch
                {
                    37 or 38 or 39 or 40 => "WA",   // Web + API
                    35 => "WO",             // Web text
                    36 => "WD",             // Web + Download
                    11 => "P",              // PDF
                    12 or 13 => "D",        // Data
                    14 or 15 or 16 => "T",  // Other document
                    17 or 18 or 19 => "S",  // Spreadsheet
                    >=20 and <= 34 => "O",  // Other file
                    _ => "X"
                };                
  
                ores.Add(new JSONSearchResObject(fob.id, fob.display_title, 
                    fob.object_type?.id, fob.object_type?.name, oi.access_details?.url, 
                    oi.resource_details?.type_id, res_icon, pub_year, acc_icon, fob.provenance_string)
                );
            }
        }
        else
        {
            // No instance data in the database for the object
            // Indicates it is a 'virtual' instance under restricted access

            string object_display_text = fob.display_title + " ( " +
                                          fob.access_details + ")";
            int virtual_rectype_id = fob.object_class?.id switch
            {
                14 => 80,   // virtual dataset
                23 => 81,   // virtual document
                19 => 82,   // virtual samples
                _ => 0
            };
            string virtual_resicon = fob.object_class?.id  switch
            {
                14 => "D",   // virtual dataset
                23 => "T",   // virtual document
                19 => "M",   // virtual samples
                _ => "X"
            };

            ores.Add(new JSONSearchResObject(fob.id, object_display_text, 
                fob.object_type?.id, fob.object_type?.name, fob.access_details?.url, 
                virtual_rectype_id, virtual_resicon, pub_year, "R", fob.provenance_string));
        }
        return ores;
    }

    public void StoreSearchRecord(JSONSearchResObject sres)
    {
        _repo.StoreSearchRecord(sres);
    }
    
    public void StoreJSONObjectInDB(int id, string objectJson)
    {
        _repo.StoreJSONObjectInDB(id, objectJson);
    }
}