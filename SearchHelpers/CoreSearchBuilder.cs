namespace MDR_Aggregator;

public class CoreSearchBuilder
{
    private readonly ILoggingHelper _loggingHelper;
    private readonly SearchHelperTables tables_srch;
    private readonly SearchHelperLexemes lexemes_srch;
    private readonly SearchHelperJson json_srch;
    private readonly JSONStudyDataLayer study_repo;
    
    public CoreSearchBuilder(string connString, ILoggingHelper loggingHelper)
    {
        _loggingHelper = loggingHelper;
        tables_srch = new SearchHelperTables(connString, _loggingHelper);
        lexemes_srch = new SearchHelperLexemes(connString, _loggingHelper);
        json_srch = new SearchHelperJson(connString, _loggingHelper);
        study_repo = new JSONStudyDataLayer(connString, _loggingHelper);
    }

    public void CreateJSONObjectData(bool create_table = true, int offset = 0)
    {
        if (create_table)
        {
            tables_srch.CreateObjectDataSearchTables();
        }
        json_srch.LoopThroughObjectRecords(offset);
    }
    
    public void CreateJSONStudyData(bool create_table = true, int offset = 0)
    {
        if (create_table)
        {
            tables_srch.CreateStudyDataSearchTables();
        }
        json_srch.LoopThroughStudyRecords(offset);
    }
    
    public void CreateIdentifierSearchDataTable()
    {
       tables_srch.CreateIdentifierSearchData();
       int res = study_repo.AddDataToIdentsSearchData();
       _loggingHelper.LogLine($"{res} study identifier search records created");
       _loggingHelper.LogBlank();
    }
       
    public void CreatePMIDSearchDataTable()
    {
       tables_srch.CreatePMIDSearchData();
       int res = study_repo.AddDataToPMIDSearchData();
       _loggingHelper.LogLine($"{res} pmid search records created");
       _loggingHelper.LogBlank();
    } 
    
    public void CreateCountrySearchDataTable()
    {
        tables_srch.CreateCountrySearchData();
        int res = study_repo.AddDataToCountrySearchData();
        _loggingHelper.LogLine($"{res} country search records created");
        _loggingHelper.LogBlank();
    }

    public void CreateObjectTypeSearchDataTable()
    {
        tables_srch.CreateObjectTypeSearchData();
        int res = study_repo.AddDataToObjectTypeSearchData();
        _loggingHelper.LogLine($"{res} object type search records created");
        _loggingHelper.LogBlank();
    }


 
    public void CreateLexemeSearchDataTable()
    {
        // Set up the text search configurations, then for both titles and topics, set up
        // temporary tables and do an initial transition to lexemes.
        // Then aggregate to study based text, before indexing.

        lexemes_srch.CreateTSConfig();
        _loggingHelper.LogLine("Text search configuration reconstructed");

        // Obtain relevant data

        int res = lexemes_srch.GenerateTitleData();
        _loggingHelper.LogLine($"{res} temporary title records created");
        res = lexemes_srch.GenerateTopicData();
        _loggingHelper.LogLine($"{res} temporary topic records created");
        res = lexemes_srch.GenerateConditionData();
        _loggingHelper.LogLine($"{res} temporary condition records created");
        
        res = lexemes_srch.GenerateTitleDataByStudy();
        _loggingHelper.LogLine($"{res} title records, by study, created");
        res = lexemes_srch.GenerateTopicDataByStudy();
        _loggingHelper.LogLine($"{res} topic records, by study, created");
        res = lexemes_srch.GenerateConditionDataByStudy();
        _loggingHelper.LogLine($"{res} condition  records, by study, created");
        res = lexemes_srch.CombineTitleAndTopicText();
        _loggingHelper.LogLine($"{res} title and topic text combined");

        tables_srch.CreateSearchLexemesTable();
        lexemes_srch.ProcessLexemeBaseData();
        
        // tidy up
        
        lexemes_srch.DropTempLexTables(); // leave in for now
    }

    public void AddStudyJsonToSearchTables()
    {
        int min_studies_id = study_repo.FetchMinId();
        int max_studies_id = study_repo.FetchMaxId();
        int res = study_repo.UpdateIdentsSearchWithStudyJson(min_studies_id, max_studies_id);
        _loggingHelper.LogLine($"{res} idents search records updated with study json data");
        res = study_repo.UpdatePMIDsSearchWithStudyJson(min_studies_id, max_studies_id);
        _loggingHelper.LogLine($"{res} pmids search records updated with study json data");
        res = study_repo.UpdateLexemesSearchWithStudyJson(min_studies_id, max_studies_id);
        _loggingHelper.LogLine($"{res} lexemes search records updated with study json data");
    }

    public void TileLexemeTable()
    {
        tables_srch.TileLexemeTable();
    }


    public void ClusterSearchTables()
    {
        tables_srch.ClusterTable("countries", "sc_country_id");
        tables_srch.ClusterTable("pmids", "sp_pmid");
        tables_srch.ClusterTable("idents", "si_type_value");
        tables_srch.ClusterTable("object_types", "sb_type_id");
        tables_srch.ClusterTable("lexemes", "bucket_study");
    }

    public void SwitchToNewTables()
    {
        // turn 'new_' tables into correctly named ones
        // Applies to search.new_pmids, search.new_idents, search.new_lexemes, search.new_countries
        // search.new_studies, search.new_studies_json, search.new_objects_json, search.new_objects 

        // for each, drop the one with the target name and rename the new_table as having the target name
        // Also rename PKs and indexes toi keep consistent naming scheme and avoid conflicts

        tables_srch.RenameTable("pmids");
        tables_srch.RenameIndex("sp_study_id");
        tables_srch.RenameIndex("sp_pmid");

        tables_srch.RenameTable("idents");
        tables_srch.RenameIndex("si_study_id");
        tables_srch.RenameIndex("si_type_value");

        tables_srch.RenameTable("countries");
        tables_srch.RenameIndex("sc_study_id");
        tables_srch.RenameIndex("sc_country_id");

        tables_srch.RenameTable("object_types");
        tables_srch.RenameIndex("sb_study_id");
        tables_srch.RenameIndex("sb_type_id");
                
        tables_srch.RenameTable("lexemes");
        tables_srch.RenamePK("lexemes_pkey");
        tables_srch.RenameIndex("cond_search_idx");
        tables_srch.RenameIndex("tt_search_idx");
        tables_srch.RenameIndex("bucket_study");

        tables_srch.RenameTable("objects");
        tables_srch.RenamePK("objects_pkey");
        tables_srch.RenameIndex("os_object_id");

        tables_srch.RenameTable("objects_json");
        tables_srch.RenamePK("objects_json_pkey");        
        tables_srch.RenameIndex("search_objects_json_id");

        tables_srch.RenameTable("studies");
        tables_srch.RenamePK("studies_pkey");
        tables_srch.RenameIndex("ss_alloc_id");
        tables_srch.RenameIndex("ss_phase_id");
        tables_srch.RenameIndex("ss_start_year");
        tables_srch.RenameIndex("ss_status");
        tables_srch.RenameIndex("ss_type");

        tables_srch.RenameTable("studies_json");
        tables_srch.RenamePK("studies_json_pkey");
        tables_srch.RenameIndex("search_studies_json_id");
    }

}