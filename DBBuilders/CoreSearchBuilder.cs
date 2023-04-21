namespace MDR_Aggregator;

public class CoreSearchBuilder
{
    private readonly ILoggingHelper _loggingHelper;
    private readonly CoreSearchHelper core_srch;

    public CoreSearchBuilder(string connString, ILoggingHelper loggingHelper)
    {
        _loggingHelper = loggingHelper;
        core_srch = new CoreSearchHelper(connString, _loggingHelper);
    }

    public void CreateStudyFeatureSearchData()
    {
        // Set up

        core_srch.SetupSearchMinMaxes(); // set up parameters for calls
        int res = core_srch.GenerateStudySearchData();
        _loggingHelper.LogLine(res.ToString() + " study search records created");


        res = core_srch.UpdateStudySearchDataWithPhaseData();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with phase data");
        res = core_srch.UpdateStudySearchDataWithPurposeData();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with purpose data");
        res = core_srch.UpdateStudySearchDataWithAllocationData();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with allocation data");
        res = core_srch.UpdateStudySearchDataWithInterventionData();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with intervention data");
        res = core_srch.UpdateStudySearchDataWithMaskingData();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with masking data");
        res = core_srch.UpdateStudySearchDataWithObsModelData();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with observational model data");
        res = core_srch.UpdateStudySearchDataWithTimePerspData();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with time perspective data");
        res = core_srch.UpdateStudySearchDataWithBioSpecData();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with biospecimen data");

    }

    public void CreateStudyObjectSearchData()
    {
        core_srch.CreateStudySearchObjectDataTable();
        int res = core_srch.UpdateStudySearchDataIfHasRegEntry();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with has reg entry data");
        res = core_srch.UpdateStudySearchDataIfHasRegResults();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with has reg results data");
        res = core_srch.UpdateStudySearchDataIfHasArticle();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with has article data");
        res = core_srch.UpdateStudySearchDataIfHasProtocol();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with has protocol data");
        res = core_srch.UpdateStudySearchDataIfHasOverview();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with has overview data");
        res = core_srch.UpdateStudySearchDataIfHasPIF();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with has PIF data");
        res = core_srch.UpdateStudySearchDataIfHasECRFs();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with has eCRFs data");
        res = core_srch.UpdateStudySearchDataIfHasManual();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with has manual data");
        res = core_srch.UpdateStudySearchDataIfHasSAP();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with has SAP data");
        res = core_srch.UpdateStudySearchDataIfHasCSR();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with has CSR data");
        res = core_srch.UpdateStudySearchDataIfHasDataDesc();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with has data description data");
        res = core_srch.UpdateStudySearchDataIfHasIPD();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with has IPD data");
        res = core_srch.UpdateStudySearchDataIfHasAggData();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with has aggregate data");
        res = core_srch.UpdateStudySearchDataIfHasOthRes();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with has other resource data");
        res = core_srch.UpdateStudySearchDataIfHasConfMat();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with has conference material data");
        res = core_srch.UpdateStudySearchDataIfHasOthArt();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with has other article data");
        res = core_srch.UpdateStudySearchDataIfHasChapter();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with has chapter data");
        res = core_srch.UpdateStudySearchDataIfHasOthInfo();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with has other info data");
        res = core_srch.UpdateStudySearchDataIfHasWebSite();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with has web site data");
        res = core_srch.UpdateStudySearchDataIfHasSoftware();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with has software data");
        res = core_srch.UpdateStudySearchDataIfHasOther();
        _loggingHelper.LogLine(res.ToString() + " study search records updated with has other data");
        core_srch.DropStudySearchObjectDataTable();

    }

    public void CreateStudyTextSearchData()
    {
        // Set up the text search configurations, then for both titles and topics, set up
        // temporary tables and do an initial transition to lexemes.
        // Then aggregate to study based text, before indexing.

        core_srch.CreateTSConfig1(); // ensure test search configs up to date
        _loggingHelper.LogLine("Text search configuration 1 reconstructed");
        core_srch.CreateTSConfig2();
        _loggingHelper.LogLine("Text search configuration 2 reconstructed");

        // titles

        int res = core_srch.GenerateTempTitleData();
        _loggingHelper.LogLine(res.ToString() + " temporary title records created");

        res = core_srch.GenerateTitleLexemeStrings();
        _loggingHelper.LogLine(res.ToString() + " title lexeme records created");

        res = core_srch.GenerateTitleDataByStudy();
        _loggingHelper.LogLine(res.ToString() + " title lexeme records, by study, created");

        res = core_srch.TransferTitleDataByStudy();
        _loggingHelper.LogLine(res.ToString() + " records created");

        core_srch.IndexTitleText();
        _loggingHelper.LogLine("title lexeme indices created");

        // tgopics

        res = core_srch.GenerateTopicData();
        _loggingHelper.LogLine(res.ToString() + " temporary topic records created");

        res = core_srch.GenerateTopicLexemeStrings();
        _loggingHelper.LogLine(res.ToString() + " title lexeme records created");

        res = core_srch.GenerateTopicDataByStudy();
        _loggingHelper.LogLine(res.ToString() + " topic lexeme records, by study, created");

        res = core_srch.TransferTopicDataByStudy();
        _loggingHelper.LogLine(res.ToString() + " topic lexeme records created");

        core_srch.IndexTopicText();
        _loggingHelper.LogLine("topic lexeme indices created");

        // tidy up

        core_srch.DropTempSearchTables();
    }

}