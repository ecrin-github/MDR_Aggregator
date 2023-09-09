namespace MDR_Aggregator;

public class StatisticsBuilder
{
    private readonly IMonDataLayer _monDatalayer;
    private readonly ILoggingHelper _loggingHelper;
    
    public StatisticsBuilder(IMonDataLayer monDatalayer, ILoggingHelper logginghelper)
    {
        _monDatalayer = monDatalayer;
        _loggingHelper = logginghelper;
    }

    public void WriteOutCoreDataSummary()
    {
        _loggingHelper.LogHeader("TABLE RECORD NUMBERS");
        
         // Obtains figures for aggregate tables.
                
        CoreSummary? csm = _monDatalayer.GetLatestCoreSummary();
        if (csm is not null)
        {
            _loggingHelper.LogBlank();
            _loggingHelper.LogLine("STATISTICS FOR CORE TABLES");
            _loggingHelper.LogBlank();
            
            // Logs the record count for each table in the core schema
        
            _loggingHelper.LogHeader("study tables");
            _loggingHelper.LogLine($"Studies: {csm.study_recs:n0}");
            _loggingHelper.LogLine($"Study Identifiers: {csm.study_identifiers_recs:n0}");
            _loggingHelper.LogLine($"Study Titles: {csm.study_titles_recs:n0}");
            
            _loggingHelper.LogLine($"Study Topics: {csm.study_topics_recs:n0}");
            _loggingHelper.LogLine($"Study Features: {csm.study_features_recs:n0}");
            _loggingHelper.LogLine($"Study Conditions: {csm.study_conditions_recs:n0}");
            _loggingHelper.LogLine($"Study ICD Records: {csm.study_icd_recs:n0}");
        
            _loggingHelper.LogLine($"Study People: {csm.study_people_recs:n0}");
            _loggingHelper.LogLine($"Study Organisations: {csm.study_organisations_recs:n0}");
            _loggingHelper.LogLine($"Study Relationships: {csm.study_relationships_recs:n0}");
            
            _loggingHelper.LogLine($"Study Countries: {csm.study_countries_recs:n0}");
            _loggingHelper.LogLine($"Study Locations: {csm.study_locations_recs:n0}");

            _loggingHelper.LogHeader("object tables");

            _loggingHelper.LogLine($"Data Objects: {csm.data_object_recs:n0}");
            _loggingHelper.LogLine($"Object Instances: {csm.object_instances_recs:n0}");
            _loggingHelper.LogLine($"Object titles: {csm.object_titles_recs:n0}");

            _loggingHelper.LogLine($"Object Datasets: {csm.object_datasets_recs:n0}");
            _loggingHelper.LogLine($"Object Dates: {csm.object_dates_recs:n0}");
            _loggingHelper.LogLine($"Object Relationships: {csm.object_relationships_recs:n0}");
            _loggingHelper.LogLine($"Object Rights: {csm.object_rights_recs:n0}");
        
            _loggingHelper.LogLine($"Object People: {csm.object_people_recs:n0}");
            _loggingHelper.LogLine($"Object Organisations: {csm.object_organisations_recs:n0}");
        
            _loggingHelper.LogLine($"Object Topics: {csm.object_topics_recs:n0}");
            _loggingHelper.LogLine($"Object Descriptions: {csm.object_descriptions_recs:n0}");
            _loggingHelper.LogLine($"Object Identifiers: {csm.object_identifiers_recs:n0}");
        
            _loggingHelper.LogHeader("Study-Object Linkage");
            _loggingHelper.LogLine($"Study-Object Links: {csm.study_object_link_recs:n0}");
        }
    }
    
    public void WriteOutStudyLinkData()
    {
        _loggingHelper.LogHeader("STUDY 1 TO 1 LINKS");
        string sub_heading = "Each link is in twice, with the ordering in each way, to make it easier\n";
        sub_heading += "                       to inspect the data or find a particular source pairing.";
        _loggingHelper.LogLine(sub_heading);
        _loggingHelper.LogBlank();
        
        List<Study1To1LinkData>? links_1to1 = _monDatalayer.GetLatestStudy1to1LinkData();
        if (links_1to1?.Any() == true)
        {
            foreach (Study1To1LinkData kd in links_1to1)
            {
                string link_line = $"{kd.source_id}: {kd.source_name} <==> ";
                link_line += $"{kd.other_source_id}: {kd.other_source_name} :: {kd.number_in_other_source:n0}";
                _loggingHelper.LogLine(link_line);
            }
        }
        
        _loggingHelper.LogHeader("STUDY 1 TO N LINKS");
        sub_heading = "These links are generally across registries rather then within them. They are added to \n";
        sub_heading += "                       study relationships found within source registries to form the full set of study relationships.\n";
        sub_heading += "                       Numbers, which are of the target study numbers in each cross-registry source-pair, give an\n";
        sub_heading += "                       approximate indication only, as relationships can be complex";
        _loggingHelper.LogLine(sub_heading);
        _loggingHelper.LogBlank();
        
        List<Study1ToNLinkData>? links_1ton = _monDatalayer.GetLatestStudy1toNLinkData();
        if (links_1ton?.Any() == true)
        {
            int old_rel_id = 0;
            foreach (Study1ToNLinkData kd in links_1ton)
            {
                if (kd.relationship_id != old_rel_id)
                {
                    // write out a heading
                    _loggingHelper.LogBlank();
                    _loggingHelper.LogLine($"Type {kd.relationship_id}: {kd.relationship}");
                    _loggingHelper.LogBlank();
                    old_rel_id = kd.relationship_id;
                }
                string link_line = $"{kd.source_id}: {kd.source_name} <--> ";
                link_line += $"{kd.target_source_id}: {kd.target_source_name} :: {kd.number_in_other_source:n0}";
                _loggingHelper.LogLine(link_line);
            }
        }
    }
    
    public void WriteOutCoreObjectTypes()
    {
        List<AggregationObjectNum>? objectNumbers = _monDatalayer.GetLatestObjectNumbers();
        if (objectNumbers?.Any() is true)
        {
            int small_cats = 0;
            _loggingHelper.LogBlank();
            _loggingHelper.LogHeader("OBJECT TYPE NUMBERS");
            foreach (AggregationObjectNum a in objectNumbers)
            {
                if (a.number_of_type >= 30)
                {
                    _loggingHelper.LogLine(
                        $"\t{a.object_type_id}\t{a.object_type_name}: {a.number_of_type:n0}");
                }
                else
                {
                    small_cats += a.number_of_type;
                }
            }

            _loggingHelper.LogLine($"\tXX\tTotal of smaller categories (n<30): {small_cats:n0}");
        }
    }
    
    public void WriteOutSourceDataTableSummaries()
    {
        // Get the list of sources, then loop through and obtain the dataset of record numbers for each.
        
        _loggingHelper.LogBlank();
        _loggingHelper.LogLine("SOURCE DATABASE DATA");
        string sub_heading = "The first of these two numbers is the number of records in the data base (AD) table.\n";
        sub_heading += "                       The second is the number of records transferred to the core MDR data from each source.\n";
        sub_heading += "                       They are often less than the total number of records in the DB because of study duplication.\n";
        sub_heading += "                       For some DBs, e.g. Biolincc, EUCTR, which have many multiple registrations, this effect is marked.\n";
        sub_heading += "                       Conversely, for ClinicalTrials.gov, where all adata is used, the numbers are equal";
        _loggingHelper.LogLine(sub_heading);
        int last_agg_event_id = _monDatalayer.GetLastAggEventId();
        int nextAuditId = _monDatalayer.GetNextAuditId();
        IEnumerable<Source> sources = _monDatalayer.RetrieveDataSources();
        foreach (Source s in sources)
        {
            string dbName = s.database_name!;
            SourceSummary? sm = _monDatalayer.RetrieveSourceSummary(last_agg_event_id, dbName);
            if (sm is null)
            {
                continue;
            }
            SourceADSummary sad = new(nextAuditId, dbName);
            string db_conn = _monDatalayer.GetConnectionString(dbName);
            _loggingHelper.LogLine(s.repo_name!);
            _loggingHelper.LogBlank();
            string spacer;
            if (s.has_study_tables is true)
            {
                sad.study_recs = _monDatalayer.GetRecNum("studies", db_conn);
                spacer = sad.study_recs < 1000000 ? "\t\t\t" : "\t\t";
                _loggingHelper.LogLine($"Studies: \t\t\t{sad.study_recs:N0}{spacer}{sm.study_recs:N0}");
                
                sad.study_identifiers_recs = _monDatalayer.GetRecNum("study_identifiers", db_conn);
                spacer = sad.study_identifiers_recs  < 1000000 ? "\t\t\t" : "\t\t";
                _loggingHelper.LogLine($"Study Identifiers: \t{sad.study_identifiers_recs:N0}{spacer}{sm.study_identifiers_recs:N0}");
                
                sad.study_titles_recs = _monDatalayer.GetRecNum("study_titles", db_conn);
                spacer = sad.study_titles_recs < 1000000 ? "\t\t\t" : "\t\t";
                _loggingHelper.LogLine($"Study Titles: \t\t{sad.study_titles_recs:N0}{spacer}{sm.study_titles_recs:N0}");
                
                if (s.has_study_people is true)
                {
                    sad.study_people_recs = _monDatalayer.GetRecNum("study_people",db_conn);
                    spacer = sad.study_people_recs < 1000000 ? "\t\t\t" : "\t\t";
                    _loggingHelper.LogLine($"Study People: \t\t{sad.study_people_recs:N0}{spacer}{sm.study_people_recs:N0}");
                }
                if (s.has_study_organisations is true)
                {
                    sad.study_organisations_recs = _monDatalayer.GetRecNum("study_organisations",db_conn);
                    spacer = sad.study_organisations_recs < 1000000 ? "\t\t\t" : "\t\t";
                    _loggingHelper.LogLine($"Study Organisations: {sad.study_organisations_recs:N0}{spacer}{sm.study_organisations_recs:N0}");
                }
                
                if (s.has_study_topics is true)
                {
                    sad.study_topics_recs = _monDatalayer.GetRecNum("study_topics",db_conn);
                    spacer = sad.study_topics_recs < 1000000 ? "\t\t\t" : "\t\t";
                    _loggingHelper.LogLine($"Study Topics: \t\t{sad.study_topics_recs:N0}{spacer}{sm.study_topics_recs:N0}");
                }
                
                if (s.has_study_conditions is true)
                {
                    sad.study_conditions_recs = _monDatalayer.GetRecNum("study_conditions",db_conn);
                    spacer = sad.study_conditions_recs < 1000000 ? "\t\t\t" : "\t\t";
                    _loggingHelper.LogLine($"Study Conditions: \t{sad.study_conditions_recs:N0}{spacer}{sm.study_conditions_recs:N0}");
                }
                
                if (s.has_study_features is true)
                {
                    sad.study_features_recs = _monDatalayer.GetRecNum("study_features",db_conn);
                    spacer = sad.study_features_recs < 1000000 ? "\t\t\t" : "\t\t";
                    _loggingHelper.LogLine($"Study Features: \t\t{sad.study_features_recs:N0}{spacer}{sm.study_features_recs:N0}");
                }
                
                if (s.has_study_relationships is true)
                {
                    sad.study_relationships_recs = _monDatalayer.GetRecNum("study_relationships",db_conn);
                    spacer = sad.study_relationships_recs < 1000000 ? "\t\t" : "\t";
                    _loggingHelper.LogLine($"Study Relationships: {sad.study_relationships_recs:N0}{spacer}{sm.study_relationships_recs:N0}");
                }
                
                if (s.has_study_countries is true)
                {
                    sad.study_countries_recs = _monDatalayer.GetRecNum("study_countries",db_conn);
                    spacer = sad.study_countries_recs < 1000000 ? "\t\t\t" : "\t\t";
                    _loggingHelper.LogLine($"Study Countries: \t{sad.study_countries_recs:N0}{spacer}{sm.study_countries_recs:N0}");
                }
                
                if (s.has_study_locations is true)
                {
                    sad.study_locations_recs = _monDatalayer.GetRecNum("study_locations",db_conn);
                    spacer = sad.study_locations_recs < 1000000 ? "\t\t\t" : "\t\t";
                    _loggingHelper.LogLine($"Study Locations: \t{sad.study_locations_recs:N0}{spacer}{sm.study_locations_recs:N0}");
                }
    
                if (s.has_study_references is true)
                {
                    sad.study_references_recs = _monDatalayer.GetRecNum("study_references",db_conn);
                    spacer = sad.study_references_recs < 1000000 ? "\t\t\t" : "\t\t";
                    _loggingHelper.LogLine($"Study References: \t{sad.study_references_recs:N0}{spacer}---");
                }
                
                if (s.has_study_links is true)
                {
                    sad.study_links_recs = _monDatalayer.GetRecNum("study_links",db_conn);
                    spacer = sad.study_links_recs < 1000000 ? "\t\t\t" : "\t\t";
                    _loggingHelper.LogLine($"Study Links: \t\t{sad.study_links_recs:N0}{spacer}---");
                }
                
                if (s.has_study_ipd_available is true)
                {
                    sad.study_ipd_available_recs = _monDatalayer.GetRecNum("study_ipd_available",db_conn);
                    spacer = sad.study_ipd_available_recs < 1000000 ? "\t\t\t" : "\t\t";
                    _loggingHelper.LogLine($"Study IPD Available: {sad.study_ipd_available_recs:N0}{spacer}---");
                }
            }
            
            sad.data_object_recs = _monDatalayer.GetRecNum("data_objects", db_conn);
            spacer = sad.data_object_recs < 1000000 ? "\t\t\t" : "\t\t";
            _loggingHelper.LogLine($"Data Objects: \t\t{sad.data_object_recs:N0}{spacer}{sm.data_object_recs:N0}");
            
            sad.object_titles_recs = _monDatalayer.GetRecNum("object_titles", db_conn);
            spacer = sad.object_titles_recs < 1000000 ? "\t\t\t" : "\t\t";
            _loggingHelper.LogLine($"Object Titles: \t\t{sad.object_titles_recs:N0}{spacer}{sm.object_titles_recs:N0}");
            
            if (s.has_object_datasets is true)
            {
                sad.object_datasets_recs = _monDatalayer.GetRecNum("object_datasets", db_conn);
                spacer = sad.object_datasets_recs < 1000000 ? "\t\t\t" : "\t\t";
                _loggingHelper.LogLine($"Object Datasets: \t{sad.object_datasets_recs:N0}{spacer}{sm.object_datasets_recs:N0}");
            }
            
            if (s.has_object_instances is true)
            {
                sad.object_instances_recs = _monDatalayer.GetRecNum("object_instances", db_conn);
                spacer = sad.study_recs < 1000000 ? "\t\t\t" : "\t\t";
                _loggingHelper.LogLine($"Object Instances: \t{sad.object_instances_recs:N0}{spacer}{sm.object_instances_recs:N0}");
            }
            
            if (s.has_object_dates is true)
            {
                sad.object_dates_recs = _monDatalayer.GetRecNum("object_dates", db_conn);
                spacer = sad.object_dates_recs < 1000000 ? "\t\t\t" : "\t\t";
                _loggingHelper.LogLine($"Object Dates: \t\t{sad.object_dates_recs:N0}{spacer}{sm.object_dates_recs:N0}");
            }
            
            if (s.has_object_people is true)
            {
                sad.object_people_recs = _monDatalayer.GetRecNum("object_people", db_conn);
                spacer = sad.object_people_recs < 1000000 ? "\t\t\t" : "\t\t";
                _loggingHelper.LogLine($"Object People: \t\t{sad.object_people_recs:N0}{spacer}{sm.object_people_recs:N0}");
            }
            
            if (s.has_object_organisations is true)
            {
                sad.object_organisations_recs = _monDatalayer.GetRecNum("object_organisations", db_conn);
                spacer = sad.object_organisations_recs < 1000000 ? "\t\t\t" : "\t\t";
                _loggingHelper.LogLine($"Object Organisations: {sad.object_organisations_recs:N0}{spacer}{sm.object_organisations_recs:N0}");
            }
            
            if (s.has_object_topics is true)
            {
                sad.object_topics_recs = _monDatalayer.GetRecNum("object_topics", db_conn);
                spacer = sad.object_topics_recs < 1000000 ? "\t\t\t" : "\t\t";
                _loggingHelper.LogLine($"Object Topics: \t\t{sad.object_topics_recs:N0}{spacer}{sm.object_topics_recs:N0}");
            }
            
            if (s.has_object_identifiers is true)
            {
                sad.object_identifiers_recs = _monDatalayer.GetRecNum("object_identifiers", db_conn);
                spacer = sad.object_identifiers_recs < 1000000 ? "\t\t\t" : "\t\t";
                _loggingHelper.LogLine($"Object Identifiers: \t{sad.object_identifiers_recs:N0}{spacer}{sm.object_identifiers_recs:N0}");
            }
            
            if (s.has_object_descriptions is true)
            {
                sad.object_descriptions_recs = _monDatalayer.GetRecNum("object_descriptions", db_conn);
                spacer = sad.object_descriptions_recs < 1000000 ? "\t\t\t" : "\t\t";
                _loggingHelper.LogLine($"Object Descriptions: {sad.object_descriptions_recs:N0}{spacer}{sm.object_descriptions_recs:N0}");
            }
            
            if (s.has_object_rights is true)
            {
                sad.object_rights_recs = _monDatalayer.GetRecNum("object_rights", db_conn);
                spacer = sad.object_rights_recs < 1000000 ? "\t\t\t" : "\t\t";
                _loggingHelper.LogLine($"Object Rights: \t\t{sad.object_rights_recs:N0}{spacer}{sm.object_rights_recs:N0}");
            }
            
            if (s.has_object_relationships is true)
            {
                sad.object_relationships_recs = _monDatalayer.GetRecNum("object_relationships", db_conn);
                spacer = sad.object_relationships_recs < 1000000 ? "\t\t\t" : "\t\t";
                _loggingHelper.LogLine($"Object Relationships: \t{sad.object_relationships_recs:N0}{spacer}{sm.object_relationships_recs:N0}");
            }
            
            if (s.has_object_comments is true)
            {
                sad.object_comments_recs = _monDatalayer.GetRecNum("object_comments", db_conn);
                spacer = sad.object_comments_recs < 1000000 ? "\t\t\t" : "\t\t";
                _loggingHelper.LogLine($"Object Comments: \t{sad.object_comments_recs:N0}{spacer}---");
            }
            
            if (s.has_object_db_links is true)
            {
                sad.object_db_link_recs = _monDatalayer.GetRecNum("object_db_links", db_conn);
                spacer = sad.object_db_link_recs < 1000000 ? "\t\t\t" : "\t\t";
                _loggingHelper.LogLine($"Object DB Links: \t{sad.object_db_link_recs:N0}{spacer}---");
            }
            
            if (s.has_journal_details is true)
            {
                sad.object_journal_detail_recs = _monDatalayer.GetRecNum("journal_details", db_conn);
                spacer = sad.object_journal_detail_recs < 1000000 ? "\t\t\t" : "\t\t";
                _loggingHelper.LogLine($"Journal Details: \t{sad.object_journal_detail_recs:N0}{spacer}---");
            }
            
            if (s.has_object_publication_types is true)
            {
                sad.object_publication_types_recs = _monDatalayer.GetRecNum("object_publication_types", db_conn);
                spacer = sad.object_publication_types_recs < 1000000 ? "\t\t\t" : "\t\t";
                _loggingHelper.LogLine($"Publication Types: \t{sad.object_publication_types_recs:N0}{spacer}---");
            }
            _monDatalayer.StoreAdSummary(sad);
            _loggingHelper.LogBlank();
        }
    }
}

