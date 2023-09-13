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

    public void SwitchToNewTables()
    {
        // to do
        // turn new tables into correctly named ones
        // Applies to search_pmids, search_idents, search_lexemes, 
        // search_studies, search_studies_json, search_objects_json, search_objects   ==> rename to search_objects
        // drop old ones
    }

}