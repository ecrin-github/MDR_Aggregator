namespace MDR_Aggregator;

public class CoreSearchBuilder
{
    private readonly ILoggingHelper _loggingHelper;
    private readonly SearchHelperStudies studies_srch;
    private readonly SearchHelperLexemes lexemes_srch;
    private readonly SearchHelperObjects objects_srch;

    public CoreSearchBuilder(string connString, ILoggingHelper loggingHelper)
    {
        _loggingHelper = loggingHelper;
        studies_srch = new SearchHelperStudies(connString, _loggingHelper);
        lexemes_srch = new SearchHelperLexemes(connString, _loggingHelper);
        objects_srch = new SearchHelperObjects(connString, _loggingHelper);
    }

    
    public void CreateStudySearchData()
    {
        studies_srch.CreateTableSearchStudies();
        int res = studies_srch.GenerateStudySearchData();
        _loggingHelper.LogLine(res + " study search records created");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudyTypeData();
        _loggingHelper.LogLine(res + " study search records updated - type terms");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudyStatusData();
        _loggingHelper.LogLine(res + " study search records updated - status terms");
        _loggingHelper.LogBlank();    
        
        res = studies_srch.UpdateStudyGenderEligibilityData();
        _loggingHelper.LogLine(res + " study search records updated - gender eligibility terms");
        _loggingHelper.LogBlank();       
        
        res = studies_srch.UpdateStudyMinAgeData();
        _loggingHelper.LogLine(res + " study search records updated - min ages");
        _loggingHelper.LogBlank();      
        
        res = studies_srch.UpdateStudyMaxAgeData();
        _loggingHelper.LogLine(res + " study search records updated - max ages");
        _loggingHelper.LogBlank();
    }

    public void CreateStudyFeatureData()
    {
        int res = studies_srch.UpdateStudySearchDataWithPhaseIdData();
        _loggingHelper.LogLine($"{res} study search records updated with phase data");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudySearchDataWithAllocIdData();
        _loggingHelper.LogLine($"{res} study search records updated with allocation data");  
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudySearchDataWithPurposeData();
        _loggingHelper.LogLine($"{res} study search records updated with purpose data");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudySearchDataWithInterventionData();
        _loggingHelper.LogLine($"{res} study search records updated with intervention data");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudySearchDataWithMaskingData();
        _loggingHelper.LogLine($"{res} study search records updated with masking data");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudySearchDataWithObsModelData();
        _loggingHelper.LogLine($"{res} study search records updated with observational model data");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudySearchDataWithTimePerspData();
        _loggingHelper.LogLine($"{res} study search records updated with time perspective data");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudySearchDataWithBioSpecData();
        _loggingHelper.LogLine($"{res} study search records updated with bio-specimen data");
        _loggingHelper.LogBlank();
    }

    public void CreateStudyHasObjectData()
    {
        studies_srch.CreateStudySearchObjectDataTable();
        
        int res = studies_srch.UpdateStudySearchDataIfHasRegResults();
        _loggingHelper.LogLine($"{res} study search records updated with 'has reg results' data");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudySearchDataIfHasArticle();
        _loggingHelper.LogLine($"{res} study search records updated with 'has article' data");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudySearchDataIfHasProtocol();
        _loggingHelper.LogLine($"{res} study search records updated with 'has protocol' data");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudySearchDataIfHasOverview();
        _loggingHelper.LogLine($"{res} study search records updated with 'has overview' data");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudySearchDataIfHasPIF();
        _loggingHelper.LogLine($"{res} study search records updated with 'has PIS' data");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudySearchDataIfHasECRFs();
        _loggingHelper.LogLine($"{res} study search records updated with 'has eCRFs' data");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudySearchDataIfHasManual();
        _loggingHelper.LogLine($"{res} study search records updated with 'has manual' data");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudySearchDataIfHasSAP();
        _loggingHelper.LogLine($"{res} study search records updated with 'has SAP' data");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudySearchDataIfHasCSR();
        _loggingHelper.LogLine($"{res} study search records updated with 'has CSR' data");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudySearchDataIfHasDataDesc();
        _loggingHelper.LogLine($"{res} study search records updated with 'has data description' data");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudySearchDataIfHasIPD();
        _loggingHelper.LogLine($"{res} study search records updated with 'has IPD' data");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudySearchDataIfHasOthRes();
        _loggingHelper.LogLine($"{res} study search records updated with 'has other study resource' data");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudySearchDataIfHasOthInfo();
        _loggingHelper.LogLine($"{res} study search records updated with 'has other info. resource' data");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudySearchDataIfHasWebSite();
        _loggingHelper.LogLine($"{res} study search records updated with 'has web site' data");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudySearchDataIfHasSoftware();
        _loggingHelper.LogLine($"{res} study search records updated with 'has software' data");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudySearchDataIfHasOther();
        _loggingHelper.LogLine($"{res} study search records updated with 'has other objects' data");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudySearchDataIfHasSamples();
        _loggingHelper.LogLine($"{res} study search records updated with 'has samples' data");
        _loggingHelper.LogBlank();
        
        // Transfer to bitmap...
        
        studies_srch.CreateBitMapData();
        studies_srch.DropStudySearchObjectDataTable();
        _loggingHelper.LogLine($"Objects data transferred to bitmap in search_studies table");
        _loggingHelper.LogBlank();
    }

    public void CreateStudyCompositeFieldData()
    {
        int res = studies_srch.UpdateStudySearchDataWithCountryData();
        _loggingHelper.LogLine($"{res} study search records updated with lists of countries");
        _loggingHelper.LogBlank();
        
        res = studies_srch.UpdateStudySearchDataWithConditionData();
        _loggingHelper.LogLine($"{res} study search records updated with lists of conditions");
        _loggingHelper.LogBlank();
    }
    
    public void CreateIdentifierSearchData()
    {
       int res = objects_srch.CreateIdentifierSearchData();
       _loggingHelper.LogLine($"{res} study identifier search records created");
       _loggingHelper.LogBlank();
    }
       
    public void CreatePMIDSearchData()
    {
       int res = objects_srch.CreatePMIDSearchData();
       _loggingHelper.LogLine($"{res} pmid search records created");
       _loggingHelper.LogBlank();
    } 
    
    public void CreateObjectSearchData()
    {
        int res = objects_srch.CreateObjectSearchData();
        _loggingHelper.LogLine($"{res} data object summary records created with instance data");
        _loggingHelper.LogBlank();
        
        res = objects_srch.UpdateObjectSearchData();
        _loggingHelper.LogLine($"{res} data object records updated with object data");
        _loggingHelper.LogBlank();
        
        res = objects_srch.UpdateObjectSearchDataWithType();
        _loggingHelper.LogLine($"{res} data object summary updated with type data");
        _loggingHelper.LogBlank();
        
        res = objects_srch.UpdateObjectSearchDataWithAccessIcon();
        _loggingHelper.LogLine($"{res} data object summary updated with access icon data");   
        _loggingHelper.LogBlank();
        
        res = objects_srch.UpdateObjectSearchDataWithResourceIcon();
        _loggingHelper.LogLine($"{res} data object summary updated with resource icon data");
        _loggingHelper.LogBlank();
        
        res = objects_srch.UpdateObjectSearchDataWithJson();
        _loggingHelper.LogLine($"{res} data object summary updated with json equivalent");
        _loggingHelper.LogBlank();
    }
    
    public void CreateLexemeSearchData()
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
        
        lexemes_srch.CreateSearchLexemesTable();
        lexemes_srch.ProcessLexemeBaseData();
        
        // tidy up
        
        lexemes_srch.DropTempLexTables(); // leave in for now
    }
    
    public void CompleteStudySearchData()
    {
        int res = studies_srch.CreateFeatureString();
        _loggingHelper.LogLine($"{res} study search records updated with feature list data");
        res = studies_srch.ObtainObjectJsonData();
        _loggingHelper.LogLine($"{res} study search records updated with object json data");
        res = studies_srch.CreateStudyJsonData();
        _loggingHelper.LogLine($"{res} study search records updated study json data");
    }
    
    public void AddStudyJsonToSearchTables()
    {
        int res = studies_srch.UpdateIdentsSearchWithStudyJson();
        _loggingHelper.LogLine($"{res} idents search records updated with study json data");
        res = studies_srch.UpdatePMIDsSearchWithStudyJson();
        _loggingHelper.LogLine($"{res} pmids search records updated with study json data");
        res = studies_srch.UpdateLexemesSearchWithStudyJson();
        _loggingHelper.LogLine($"{res} lexemes search records updated with study json data");
    }

}