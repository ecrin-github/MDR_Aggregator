using MDR_Aggregator.LoggingHelpers.Interfaces;
using MDR_Aggregator.SearchStudyHelpers;

namespace MDR_Aggregator.SearchHelpers;

public class CoreSearchBuilder
{
    private readonly ILoggingHelper _loggingHelper;
    private readonly SearchHelperTables _tablesSrch;
    private readonly SearchHelperLexemes _lexemesSrch;
    private readonly SearchHelperJson _jsonSrch;
    private readonly JsonStudyDataLayer _studyRepo;
    
    public CoreSearchBuilder(string connString, ILoggingHelper loggingHelper)
    {
        _loggingHelper = loggingHelper;
        _tablesSrch = new SearchHelperTables(connString, _loggingHelper);
        _lexemesSrch = new SearchHelperLexemes(connString, _loggingHelper);
        _jsonSrch = new SearchHelperJson(connString, _loggingHelper);
        _studyRepo = new JsonStudyDataLayer(connString, _loggingHelper);
    }

    public void CreateJSONObjectData(bool createTable = true, int offset = 0)
    {
        if (createTable)
        {
            _tablesSrch.CreateObjectDataSearchTables();
        }
        _jsonSrch.LoopThroughObjectRecords(offset);
    }
    
    public void CreateJSONStudyData(bool createTable = true, int offset = 0)
    {
        if (createTable)
        {
            _tablesSrch.CreateStudyDataSearchTables();
        }
        _jsonSrch.LoopThroughStudyRecords(offset);
    }
    
    public void CreateIdentifierSearchDataTable()
    {
       _tablesSrch.CreateIdentifierSearchData();
       int res = _studyRepo.AddDataToIdentsSearchData();
       _loggingHelper.LogLine($"{res} study identifier search records created");
       _loggingHelper.LogBlank();
    }
       
    public void CreatePMIDSearchDataTable()
    {
       _tablesSrch.CreatePMIDSearchData();
       int res = _studyRepo.AddDataToPMIDSearchData();
       _loggingHelper.LogLine($"{res} pmid search records created");
       _loggingHelper.LogBlank();
    } 
    
    public void CreateCountrySearchDataTable()
    {
        _tablesSrch.CreateCountrySearchData();
        int res = _studyRepo.AddDataToCountrySearchData();
        _loggingHelper.LogLine($"{res} country search records created");
        _loggingHelper.LogBlank();
    } 
    
    
    public void CreateLexemeSearchDataTable()
    {
        // Set up the text search configurations, then for both titles and topics, set up
        // temporary tables and do an initial transition to lexemes.
        // Then aggregate to study based text, before indexing.

        _lexemesSrch.CreateTSConfig();
        _loggingHelper.LogLine("Text search configuration reconstructed");

        // Obtain relevant data

        int res = _lexemesSrch.GenerateTitleData();
        _loggingHelper.LogLine($"{res} temporary title records created");
        res = _lexemesSrch.GenerateTopicData();
        _loggingHelper.LogLine($"{res} temporary topic records created");
        res = _lexemesSrch.GenerateConditionData();
        _loggingHelper.LogLine($"{res} temporary condition records created");
        
        res = _lexemesSrch.GenerateTitleDataByStudy();
        _loggingHelper.LogLine($"{res} title records, by study, created");
        res = _lexemesSrch.GenerateTopicDataByStudy();
        _loggingHelper.LogLine($"{res} topic records, by study, created");
        res = _lexemesSrch.GenerateConditionDataByStudy();
        _loggingHelper.LogLine($"{res} condition  records, by study, created");
        res = _lexemesSrch.CombineTitleAndTopicText();
        _loggingHelper.LogLine($"{res} title and topic text combined");
        
        _tablesSrch.CreateSearchLexemesTable();
        _lexemesSrch.ProcessLexemeBaseData();
        
        // tidy up
        
        _lexemesSrch.DropTempLexTables(); // leave in for now
    }

    public void AddStudyJsonToSearchTables()
    {
        int min_studies_id = _studyRepo.FetchMinId();
        int max_studies_id = _studyRepo.FetchMaxId();
        int res = _studyRepo.UpdateIdentsSearchWithStudyJson(min_studies_id, max_studies_id);
        _loggingHelper.LogLine($"{res} idents search records updated with study json data");
        res = _studyRepo.UpdatePMIDsSearchWithStudyJson(min_studies_id, max_studies_id);
        _loggingHelper.LogLine($"{res} pmids search records updated with study json data");
        res = _studyRepo.UpdateLexemesSearchWithStudyJson(min_studies_id, max_studies_id);
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