namespace MDR_Aggregator;

public class StatisticsBuilder
{
    private readonly IMonDataLayer _monDatalayer;
    private readonly ILoggingHelper _loggingHelper;
    private int _agg_event_id;
    
    public StatisticsBuilder(IMonDataLayer monDatalayer, ILoggingHelper logginghelper)
    {
        _monDatalayer = monDatalayer;
        _loggingHelper = logginghelper;
    }

    public void WriteOutCoreDataSummary()
    {
        _loggingHelper.LogHeader("TABLE RECORD NUMBERS");
        
         // Obtains figures for aggregate tables.
                
        _agg_event_id = _monDatalayer.GetLastAggEventId();
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
        sub_heading += "                    to inspect the data or find a particular source pairing.";
        _loggingHelper.LogLine(sub_heading);
        
        List<Study1To1LinkData>? links_1to1 = _monDatalayer.GetLatestStudy1to1LinkData();
        if (links_1to1?.Any() == true)
        {
            foreach (Study1To1LinkData kd in links_1to1)
            {
                string link_line = $"{kd.source_id}: {kd.source_name} <<>> ";
                link_line += $"{kd.other_source_id}: {kd.other_source_name} :: {kd.number_in_other_source:n0}";
                _loggingHelper.LogLine(link_line);
            }
        }
        
        _loggingHelper.LogHeader("STUDY 1 TO N LINKS");
        sub_heading = "Numbers, which are of the target study numbers in each source-pair, give an\n";
        sub_heading += "                    approximate indication only, as relationships are complex";
        _loggingHelper.LogLine(sub_heading);
        
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
                string link_line = $"{kd.source_id}\t{kd.source_name} -- ";
                link_line += $"{kd.target_source_id}\t{kd.target_source_name}: {kd.number_in_other_source:n0}";
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
        _loggingHelper.LogLine("STATISTICS FOR EACH SOURCE DATABASE");
        _loggingHelper.LogBlank();
        int last_agg_event_id = _monDatalayer.GetLastAggEventId();
        IEnumerable<Source> sources = _monDatalayer.RetrieveDataSources();
        foreach (Source s in sources)
        {
            SourceSummary? sm = _monDatalayer.RetrieveSourceSummary(last_agg_event_id, s.database_name!);
            if (sm is null)
            {
                continue;
            }
            _loggingHelper.LogBlank();
            _loggingHelper.LogLine(s.repo_name!);
            _loggingHelper.LogBlank();
            if (s.has_study_tables is true)
            {
                _loggingHelper.LogLine($"Studies: \t\t{sm.study_recs:N0}");
                _loggingHelper.LogLine($"Study Identifiers: \t{sm.study_identifiers_recs:N0}");
                _loggingHelper.LogLine($"Study Titles: \t\t{sm.study_titles_recs:N0}");
                if (s.has_study_people is true)
                {
                    _loggingHelper.LogLine($"Study People: \t\t{sm.study_people_recs:N0}");
                }
                if (s.has_study_organisations is true)
                {
                    _loggingHelper.LogLine($"Study Organisations: \t{sm.study_organisations_recs:N0}");
                }
                if (s.has_study_topics is true)
                {
                    _loggingHelper.LogLine($"Study Topics: \t\t{sm.study_topics_recs:N0}");
                }
                if (s.has_study_conditions is true)
                {
                    _loggingHelper.LogLine($"Study Conditions: \t{sm.study_conditions_recs:N0}");
                }
                if (s.has_study_features is true)
                {
                    _loggingHelper.LogLine($"Study Features: \t\t{sm.study_features_recs:N0}");
                }
                if (s.has_study_relationships is true)
                {
                    _loggingHelper.LogLine($"Study Relationships: \t{sm.study_relationships_recs:N0}");
                }
                if (s.has_study_countries is true)
                {
                    _loggingHelper.LogLine($"Study Countries: \t{sm.study_countries_recs:N0}");
                }
                if (s.has_study_locations is true)
                {
                    _loggingHelper.LogLine($"Study Locations: \t{sm.study_locations_recs:N0}");
                }
            }
            _loggingHelper.LogLine($"Data Objects: \t\t{sm.data_object_recs:N0}");
            _loggingHelper.LogLine($"Object Titles: \t\t{sm.object_titles_recs:N0}");
            if (s.has_object_datasets is true)
            {
                _loggingHelper.LogLine($"Datasets: \t\t{sm.object_datasets_recs:N0}");
            }
            if (s.has_object_instances is true)
            {
                _loggingHelper.LogLine($"Object Instances: \t{sm.object_instances_recs:N0}");
            }
            if (s.has_object_dates is true)
            {
                _loggingHelper.LogLine($"Object Dates: \t\t{sm.object_dates_recs:N0}");
            }
            if (s.has_object_people is true)
            {
                _loggingHelper.LogLine($"Object People: \t\t{sm.object_people_recs:N0}");
            }
            if (s.has_object_organisations is true)
            {
                _loggingHelper.LogLine($"Object Organisations: \t{sm.object_organisations_recs:N0}");
            }
            if (s.has_object_topics is true)
            {
                _loggingHelper.LogLine($"Object Topics: \t\t{sm.object_topics_recs:N0}");
            }
            if (s.has_object_identifiers is true)
            {
                _loggingHelper.LogLine($"Object Identifiers: \t{sm.object_identifiers_recs:N0}");
            }
            if (s.has_object_descriptions is true)
            {
                _loggingHelper.LogLine($"Object Descriptions: \t{sm.object_descriptions_recs:N0}");
            }
            if (s.has_object_rights is true)
            {
                _loggingHelper.LogLine($"Object Rights: \t\t{sm.object_rights_recs:N0}");
            }
            if (s.has_object_relationships is true)
            {
                _loggingHelper.LogLine($"Object Relationships: \t{sm.object_relationships_recs:N0}");
            }
            _monDatalayer.StoreSourceSummary(sm);
        }
    }
}

