namespace MDR_Aggregator;

public class CoreSearchBuilder
{
    private readonly ILoggingHelper _loggingHelper;
    private readonly SearchTableBuilder srch_builder;
    private readonly SearchHelperStudies studies_srch;
    private readonly SearchHelperLexemes lexemes_srch;
    private readonly SearchHelperObjects objects_srch;

    public CoreSearchBuilder(string connString, ILoggingHelper loggingHelper)
    {
        _loggingHelper = loggingHelper;
        srch_builder = new SearchTableBuilder(connString, _loggingHelper);
        studies_srch = new SearchHelperStudies(connString, _loggingHelper);
        lexemes_srch = new SearchHelperLexemes(connString, _loggingHelper);
        objects_srch = new SearchHelperObjects(connString, _loggingHelper);
    }

    public void CreateSearchTables()
    {
        //srch_builder.create_table_search_studies();
        //srch_builder.create_table_search_lexemes();
        //srch_builder.create_table_search_pmids();
        //srch_builder.create_table_search_idents();
        //srch_builder.create_table_search_objects();
    }

    public void CreateStudySearchData()
    {
        int res = studies_srch.GenerateStudySearchData();
        _loggingHelper.LogLine(res + " study search records created");
        res = studies_srch.UpdateStudyTypeData();
        _loggingHelper.LogLine(res + " study search records updated - type terms");
        res = studies_srch.UpdateStudyStatusData();
        _loggingHelper.LogLine(res + " study search records updated - status terms");
        res = studies_srch.UpdateStudyGenderEligibilityData();
        _loggingHelper.LogLine(res + " study search records updated - gender eligibility terms");
        res = studies_srch.UpdateStudyMinAgeData();
        _loggingHelper.LogLine(res + " study search records updated - min ages");
        res = studies_srch.UpdateStudyMaxAgeData();
        _loggingHelper.LogLine(res + " study search records updated - max ages");
    }

    public void CreateStudyFeatureData()
    {
        int res = studies_srch.UpdateStudySearchDataWithPhaseIdData();
        _loggingHelper.LogLine($"{res} study search records updated with phase data");
        res = studies_srch.UpdateStudySearchDataWithAllocIdData();
        _loggingHelper.LogLine($"{res} study search records updated with allocation data");     
        res = studies_srch.UpdateStudySearchDataWithPurposeData();
        _loggingHelper.LogLine($"{res} study search records updated with purpose data");
        res = studies_srch.UpdateStudySearchDataWithInterventionData();
        _loggingHelper.LogLine($"{res} study search records updated with intervention data");
        res = studies_srch.UpdateStudySearchDataWithMaskingData();
        _loggingHelper.LogLine($"{res} study search records updated with masking data");
        res = studies_srch.UpdateStudySearchDataWithObsModelData();
        _loggingHelper.LogLine($"{res} study search records updated with observational model data");
        res = studies_srch.UpdateStudySearchDataWithTimePerspData();
        _loggingHelper.LogLine($"{res} study search records updated with time perspective data");
        res = studies_srch.UpdateStudySearchDataWithBioSpecData();
        _loggingHelper.LogLine($"{res} study search records updated with bio-specimen data");

    }

    public void CreateStudyHasObjectData()
    {
        studies_srch.CreateStudySearchObjectDataTable();
        int res = studies_srch.UpdateStudySearchDataIfHasRegResults();
        _loggingHelper.LogLine($"{res} study search records updated with 'has reg results' data");
        res = studies_srch.UpdateStudySearchDataIfHasArticle();
        _loggingHelper.LogLine($"{res} study search records updated with 'has article' data");
        res = studies_srch.UpdateStudySearchDataIfHasProtocol();
        _loggingHelper.LogLine($"{res} study search records updated with 'has protocol' data");
        res = studies_srch.UpdateStudySearchDataIfHasOverview();
        _loggingHelper.LogLine($"{res} study search records updated with 'has overview' data");
        res = studies_srch.UpdateStudySearchDataIfHasPIF();
        _loggingHelper.LogLine($"{res} study search records updated with 'has PIS' data");
        res = studies_srch.UpdateStudySearchDataIfHasECRFs();
        _loggingHelper.LogLine($"{res} study search records updated with 'has eCRFs' data");
        res = studies_srch.UpdateStudySearchDataIfHasManual();
        _loggingHelper.LogLine($"{res} study search records updated with 'has manual' data");
        res = studies_srch.UpdateStudySearchDataIfHasSAP();
        _loggingHelper.LogLine($"{res} study search records updated with 'has SAP' data");
        res = studies_srch.UpdateStudySearchDataIfHasCSR();
        _loggingHelper.LogLine($"{res} study search records updated with 'has CSR' data");
        res = studies_srch.UpdateStudySearchDataIfHasDataDesc();
        _loggingHelper.LogLine($"{res} study search records updated with 'has data description' data");
        res = studies_srch.UpdateStudySearchDataIfHasIPD();
        _loggingHelper.LogLine($"{res} study search records updated with 'has IPD' data");
        res = studies_srch.UpdateStudySearchDataIfHasOthRes();
        _loggingHelper.LogLine($"{res} study search records updated with 'has other study resource' data");
        res = studies_srch.UpdateStudySearchDataIfHasOthInfo();
        _loggingHelper.LogLine($"{res} study search records updated with 'has other info. resource' data");
        res = studies_srch.UpdateStudySearchDataIfHasWebSite();
        _loggingHelper.LogLine($"{res} study search records updated with 'has web site' data");
        res = studies_srch.UpdateStudySearchDataIfHasSoftware();
        _loggingHelper.LogLine($"{res} study search records updated with 'has software' data");
        res = studies_srch.UpdateStudySearchDataIfHasOther();
        _loggingHelper.LogLine($"{res} study search records updated with 'has other objects' data");
        
        // Transfer to bitmap...
        
        studies_srch.CreateBitMapData();
        studies_srch.DropStudySearchObjectDataTable();
        _loggingHelper.LogLine($"'Has objects data transferred to bitmap in search_studies table");
    }

    public void CreateStudyCompositeFieldData()
    {
        int res = studies_srch.UpdateStudySearchDataWithCountryData();
        _loggingHelper.LogLine($"{res} study search records updated with lists of countries");
        res = studies_srch.UpdateStudySearchDataWithConditionData();
        _loggingHelper.LogLine($"{res} study search records updated with lists of conditions");
        
    }
    
    public void CreateIdentifierSearchData()
    {
        int res = objects_srch.CreateIdentifierSearchData();
        _loggingHelper.LogLine($"{res} study identifier search records created");
    }
    
    public void CreatePMIDSearchData()
    {
        int res = objects_srch.CreatePMIDSearchData();
        _loggingHelper.LogLine($"{res} pmid search records created");
    }
    
    public void CreateObjectSearchData()
    {
        int res = objects_srch.CreateObjectSearchData();
        _loggingHelper.LogLine($"{res} data object summary records created with instance data");
        res = objects_srch.UpdateObjectSearchData();
        _loggingHelper.LogLine($"{res} data object records updated with object data");
        res = objects_srch.UpdateObjectSearchDataWithType();
        _loggingHelper.LogLine($"{res} data object summary updated with type data");
        res = objects_srch.UpdateObjectSearchDataWithAccessIcon();
        _loggingHelper.LogLine($"{res} data object summary updated with access icon data");        
        res = objects_srch.UpdateObjectSearchDataWithResourceIcon();
        _loggingHelper.LogLine($"{res} data object summary updated with resource icon data");
    }
    
    public void CreateLexemeSearchData()
    {
        // Set up the text search configurations, then for both titles and topics, set up
        // temporary tables and do an initial transition to lexemes.
        // Then aggregate to study based text, before indexing.

        lexemes_srch.CreateTSConfig1(); // ensure test search configs up to date
        _loggingHelper.LogLine("Text search configuration reconstructed");

        // Obtain relevant data

        //int res = lexemes_srch.GenerateTitleData();
        //_loggingHelper.LogLine($"{res} temporary title records created");
        //res = lexemes_srch.GenerateTopicData();
        // _loggingHelper.LogLine($"{res} temporary topic records created");
        //res = lexemes_srch.GenerateConditionData();
        //_loggingHelper.LogLine($"{res} temporary condition records created");
        
        /*
        res = lexemes_srch.GenerateTitleLexemeStrings();
        _loggingHelper.LogLine($"{res} title lexeme records created");
        res = lexemes_srch.GenerateTopicLexemeStrings();
        _loggingHelper.LogLine($"{res} topic lexeme records created");
        res = lexemes_srch.GenerateConditionLexemeStrings();
        _loggingHelper.LogLine($"{res} condition lexeme records created");
        */
        
        //res = lexemes_srch.GenerateTitleDataByStudy();
        //_loggingHelper.LogLine($"{res} title records, by study, created");
        //res = lexemes_srch.GenerateTopicDataByStudy();
        //_loggingHelper.LogLine($"{res} topic records, by study, created");
        //res = lexemes_srch.GenerateConditionDataByStudy();
        //_loggingHelper.LogLine($"{res} condition  records, by study, created");
        //res = lexemes_srch.CombineTitleAndTopicText();
        //_loggingHelper.LogLine($"{res} title and topic text combined");
        
        //int res = lexemes_srch.CreateSearchLexemesTable();
        //_loggingHelper.LogLine($"{res} study lexeme records established");
        
        //lexemes_srch.CleanCollectedTTData();
        //_loggingHelper.LogLine($"tt data records - preliminary clean done");
        //lexemes_srch.CleanCollectedConditionData();
        //_loggingHelper.LogLine($"condition data records - preliminary clean done");

        lexemes_srch.CleanNumbersInTTData();
        lexemes_srch.CleanNumbersInTTData();
        
        int res = lexemes_srch.TransferTitleLexDataByStudy();
        _loggingHelper.LogLine($"{res} title lexeme records, by study, stored");
        res = lexemes_srch.TransferConditionLexDataByStudy();
        _loggingHelper.LogLine($"{res} condition lexeme records, by study, stored");
        
        
        lexemes_srch.IndexTTLexText();
        _loggingHelper.LogLine("title lexeme indices created");
        //lexemes_srch.IndexTopicLexText();
        //_loggingHelper.LogLine("topic lexeme indices created");
        lexemes_srch.IndexConditionLexText();
        _loggingHelper.LogLine("condition lexeme indices created");

        // tidy up
        
        //lexemes_srch.DropTempLexTables(); - leave in for now
    }


    public void CreateLexemeIndices()
    {
        lexemes_srch.IndexTTLexText();
        //lexemes_srch.IndexTopicLexText();
        lexemes_srch.IndexConditionLexText();
        
    }

}