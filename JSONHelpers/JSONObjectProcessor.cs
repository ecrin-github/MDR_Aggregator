namespace MDR_Aggregator;

class JSONObjectProcessor
{
    private readonly JSONObjectDataLayer _repo;
    private readonly ILoggingHelper _loggingHelper;

    private DBDataObject? ob;
    private Lookup? object_class;
    private Lookup? object_type;
    private Organisation? managing_organisation;
    private Lookup? access_type;
    private object_access? access_details;
    private record_keys? ds_record_keys;
    private Deidentification? ds_deident_level;
    private Consent? ds_consent;
   
    private List<object_instance>? object_instances;
    private List<object_title>? object_titles;
    private List<object_person>? object_people;
    private List<object_organisation>? object_organisations;
    private List<object_date>? object_dates;
    private List<object_topic>? object_topics;
    private List<object_description>? object_descriptions;
    private List<object_identifier>? object_identifiers;
    private List<object_right>? object_rights;
    private List<object_relationship>? object_relationships;
    private List<int>? linked_studies;

    public JSONObjectProcessor(JSONObjectDataLayer repo, ILoggingHelper loggingHelper)
    {
        _repo = repo;
        _loggingHelper = loggingHelper;
    }

    public JSONDataObject? CreateObject(int id)
    {
        // Re-initialise these compound properties.

        object_class = null;
        object_type = null;
        access_type = null;
        managing_organisation = null;
        access_details = null;
        
        ds_record_keys = null;
        ds_deident_level = null;
        ds_consent = null;

        object_titles = new List<object_title>(); 
        object_people = new List<object_person>();
        object_organisations = new List<object_organisation>(); 
        object_dates = new List<object_date>(); 
        object_instances = new List<object_instance>(); 
        object_topics = new List<object_topic>(); 
        object_identifiers = new List<object_identifier>(); 
        object_descriptions = new List<object_description>(); 
        object_rights = new List<object_right>(); 
        object_relationships = new List<object_relationship>(); 
        linked_studies = new List<int>(); 

        // Get the singleton data object properties from DB

        ob = _repo.FetchDbDataObject(id);
        if (ob is null)
        {
            return null;
        }

        // First check there is at least one linked study
       
        linked_studies = new List<int>(_repo.FetchLinkedStudies(id));
        if (linked_studies.Count == 0)
        {
            // May occur in a few cases, if it does need to investigate further !!!!!!!
            // Seems to be due to a (minor) error in data object linkage with journal articles.
            
            _loggingHelper.LogError("object " + (ob?.id ?? 0).ToString() + " does not appear to be linked to studies");
            return null;
        }

        // Instantiate the top level lookup types

        object_class = new Lookup(ob.object_class_id, ob.object_class);
        object_type = new Lookup(ob.object_type_id, ob.object_type);
        if (ob.managing_org != null)
        {
            managing_organisation = new Organisation(ob.managing_org_id, ob.managing_org, ob.managing_org_ror_id);
        }
        if (ob.access_type_id != null)
        {
            access_type = new Lookup(ob.access_type_id, ob.access_type);
        }
        if (ob.access_details != null || ob.access_details_url != null)
        {
            access_details = new object_access(ob.access_details, ob.access_details_url, ob.url_last_checked);
        }

        // Instantiate data object with those details

        JSONDataObject dobj = new JSONDataObject(ob.id, ob.doi, ob.display_title, ob.version, object_class,
                              object_type, ob.publication_year, managing_organisation, ob.lang_code,
                              access_type, access_details, ob.eosc_category, ob.provenance_string);


        // Get dataset properties, if there are any...

        var db_ds = _repo.FetchDbDatasetProperties(id);
        if (db_ds is not null)
        {
            ds_record_keys = new record_keys(db_ds.record_keys_type_id, db_ds.record_keys_type, db_ds.record_keys_details);
            ds_deident_level = new Deidentification(db_ds.deident_type_id, db_ds.deident_type, db_ds.deident_direct,
                                         db_ds.deident_hipaa, db_ds.deident_dates, db_ds.deident_nonarr, 
                                         db_ds.deident_kanon, db_ds.deident_details);
            ds_consent = new Consent(db_ds.consent_type_id, db_ds.consent_type, db_ds.consent_noncommercial,
                                         db_ds.consent_geog_restrict, db_ds.consent_research_type, db_ds.consent_genetic_only,
                                         db_ds.consent_no_methods, db_ds.consent_details);

            dobj.dataset_record_keys = ds_record_keys;
            dobj.dataset_deident_level = ds_deident_level;
            dobj.dataset_consent = ds_consent;
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
            object_instances.Add(new object_instance(i.id, repo_org, access, resource));
        }


        // Get object titles.

        IEnumerable<DBObjectTitle> db_object_titles = _repo.FetchObjectTitles(id);
        foreach (DBObjectTitle t in db_object_titles)
        {
            object_titles.Add(new object_title(t.id, new Lookup(t.title_type_id, t.title_type), t.title_text,
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
            object_dates.Add(new object_date(d.id, new Lookup(d.date_type_id, d.date_type), d.date_is_range,
                                        d.date_as_string, start_date, end_date, d.comments));
        }

        
        // Get object descriptions.

        IEnumerable<DBObjectDescription> db_object_descriptions = _repo.FetchObjectDescriptions(id);
        foreach (DBObjectDescription i in db_object_descriptions)
        {
            object_descriptions.Add(new object_description(i.id, new Lookup(i.description_type_id, i.description_type),
                                 i.label, i.description_text, i.lang_code));
        }
        
        // The 4 functions below are currently only required for Pubmed objects. To save time
        // it is easier to therefore only apply them to these objects. In the core tables an object's source
        // is no longer apparent, but - AT THE MOMENT AT LEAST - the add_study_contribs and add_study_topics
        // can be used as a proxy for a PubMed object, as these are false only for these objects.

        if (ob.add_study_contribs == false && ob.add_study_topics == false)
        {
            // Get object people 

            IEnumerable<DBObjectPerson> db_object_people = _repo.FetchObjectPeople(id);
            foreach (DBObjectPerson t in db_object_people)
            {
                object_people.Add(new object_person(t.id, new Lookup(t.contrib_type_id, t.contrib_type),
                    t.person_full_name, t.orcid_id, t.person_affiliation,
                    new Organisation(t.organisation_id, t.organisation_name, t.organisation_ror_id)));
            }

            // Get object organisations

            IEnumerable<DBObjectOrganisation> db_object_organisations = _repo.FetchObjectOrganisations(id);
            foreach (DBObjectOrganisation t in db_object_organisations)
            {
                object_organisations.Add(new object_organisation(t.id, new Lookup(t.contrib_type_id, t.contrib_type),
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

                object_topics.Add(new object_topic(t.id, new Lookup(t.topic_type_id, t.topic_type),
                    t.original_value, ct, md));
            }


            // Get object identifiers.

            IEnumerable<DBObjectIdentifier> db_object_identifiers = _repo.FetchObjectIdentifiers(id);
            foreach (DBObjectIdentifier i in db_object_identifiers)
            {
                object_identifiers.Add(new object_identifier(i.id, i.identifier_value,
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

        dobj.dataset_consent = ds_consent;
        dobj.dataset_record_keys = ds_record_keys;
        dobj.dataset_deident_level = ds_deident_level;
        
        dobj.object_instances = object_instances.Any() ? object_instances : null;
        dobj.object_titles = object_titles.Any() ? object_titles : null;
        dobj.object_dates = object_dates.Any() ? object_dates : null;
        dobj.object_descriptions = object_descriptions.Any() ? object_descriptions : null;
        
        dobj.object_identifiers = object_identifiers.Any() ? object_identifiers : null;
        dobj.object_people = object_people.Any() ? object_people : null;
        dobj.object_organisations = object_organisations.Any() ? object_organisations : null;
        dobj.object_topics = object_topics.Any() ? object_topics : null;
        
        dobj.object_relationships = object_relationships.Any() ? object_relationships : null;
        dobj.object_rights = object_rights.Any() ? object_rights : null;     
        
        dobj.linked_studies = linked_studies.Any() ? linked_studies : null;
        return dobj;
    }

    public void StoreJSONObjectInDB(int id, string object_json)
    {
        _repo.StoreJSONObjectInDB(id, object_json);
    }

}