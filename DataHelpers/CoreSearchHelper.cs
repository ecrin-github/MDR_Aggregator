namespace MDR_Aggregator;

public class CoreSearchHelper
{
    string _connString;
    DBUtilities db;
    ILoggingHelper _loggingHelper;
    int min_studies_id, max_studies_id;
    int min_titles_id, max_titles_id;
    int min_topics_id, max_topics_id;

    public CoreSearchHelper(string connString, ILoggingHelper loggingHelper)
    {
        _loggingHelper = loggingHelper;
        _connString = connString;
        db = new DBUtilities(_connString, _loggingHelper);
    }
    
    public int GenerateStudySearchData()
    {
        string sql_string = @"INSERT INTO core.study_search(id, display_title, study_start_year, study_start_month, 
                              study_type_id, study_status_id, study_gender_elig_id, 
                              min_age, min_age_units_id, max_age, max_age_units_id)
                              select id, display_title, study_start_year, study_start_month, 
                              study_type_id, study_status_id, study_gender_elig_id, 
                              min_age, min_age_units_id, max_age, max_age_units_id
                              from core.studies ";

        return db.ExecuteCoreTransferSQL(sql_string, "where" , "study_search");
    }

    public void SetupSearchMinMaxes() 
    {
        // IDs for these tables assessed now
        // the temp tables derived from them will 
        // have exactly the same IDs

        min_studies_id = db.GetAggMinId("core.studies");
        max_studies_id = db.GetAggMaxId("core.studies");

        min_titles_id = db.GetAggMinId("core.study_titles");
        max_titles_id = db.GetAggMaxId("core.study_titles");

        min_topics_id = db.GetAggMinId("core.study_topics");
        max_topics_id = db.GetAggMaxId("core.study_topics");
    }


    public int UpdateStudySearchDataWithPhaseData()
    {
        string sql_string = @"UPDATE core.study_search s
               set phase_value = feature_value_id from core.study_features sf
               where s.id = sf.study_id and feature_type_id = 20 ";

        return db.TransferFeatureData(sql_string, "phase", min_studies_id, max_studies_id);
    }

    public int UpdateStudySearchDataWithPurposeData()
    {
        string sql_string = @"UPDATE core.study_search s
               set purpose_value = feature_value_id from core.study_features sf
               where s.id = sf.study_id and feature_type_id = 21 ";

        return db.TransferFeatureData(sql_string, "study purpose", min_studies_id, max_studies_id);
    }

    public int UpdateStudySearchDataWithAllocationData()
    {
        string sql_string = @"UPDATE core.study_search s
               set allocation_value = feature_value_id from core.study_features sf
               where s.id = sf.study_id and feature_type_id = 22 ";

        return db.TransferFeatureData(sql_string, "allocation", min_studies_id, max_studies_id);
    }

    public int UpdateStudySearchDataWithInterventionData()
    {
        string sql_string = @"UPDATE core.study_search s
               set intervention_value = feature_value_id from core.study_features sf
               where s.id = sf.study_id and feature_type_id = 23 ";

        return db.TransferFeatureData(sql_string, "intervention", min_studies_id, max_studies_id);
    }

    public int UpdateStudySearchDataWithMaskingData()
    {
        string sql_string = @"UPDATE core.study_search s
               set masking_value = feature_value_id from core.study_features sf
               where s.id = sf.study_id and feature_type_id = 24 ";

        return db.TransferFeatureData(sql_string, "masking", min_studies_id, max_studies_id);
    }

    public int UpdateStudySearchDataWithObsModelData()
    {
        string sql_string = @"UPDATE core.study_search s
               set obsmodel_value = feature_value_id from core.study_features sf
               where s.id = sf.study_id and feature_type_id = 30 ";

        return db.TransferFeatureData(sql_string, "observation model", min_studies_id, max_studies_id);
    }

    public int UpdateStudySearchDataWithTimePerspData()
    {
        string sql_string = @"UPDATE core.study_search s
               set timepersp_value = feature_value_id from core.study_features sf
               where s.id = sf.study_id and feature_type_id = 31 ";

        return db.TransferFeatureData(sql_string, "time perspective", min_studies_id, max_studies_id);
    }

    public int UpdateStudySearchDataWithBioSpecData()
    {
        string sql_string = @"UPDATE core.study_search s
               set biospec_value = feature_value_id from core.study_features sf
               where s.id = sf.study_id and feature_type_id = 32 ";

        return db.TransferFeatureData(sql_string, "biospecimen", min_studies_id, max_studies_id);
    }

    public void CreateStudySearchObjectDataTable()
    {
        string sql_string = @"CREATE TABLE core.temp_searchobjects
            ( id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
			  , study_id INT); ";
        db.ExecuteSQL(sql_string);
    }

    public void DropStudySearchObjectDataTable()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.temp_searchobjects; ";
        db.ExecuteSQL(sql_string);
    }

    public int UpdateStudySearchDataIfHasRegEntry()
    {
        return db.TransferObjectData("b.object_type_id in (13)", "reg_entry");
    }

    public int UpdateStudySearchDataIfHasRegResults()
    {
        return db.TransferObjectData("b.object_type_id in (28)", "reg_results");
    }

    public int UpdateStudySearchDataIfHasArticle()
    {
        return db.TransferObjectData("b.object_type_id in (12, 100, 117, 135, 152)", "article");
    }

    public int UpdateStudySearchDataIfHasProtocol()
    {
        return db.TransferObjectData("b.object_type_id in (11, 42, 74, 75, 76)", "protocol");
    }

    public int UpdateStudySearchDataIfHasOverview()
    {
        return db.TransferObjectData("b.object_type_id in (38)", "overview");
    }

    public int UpdateStudySearchDataIfHasPIF()
    {
        return db.TransferObjectData("b.object_type_id in (18, 19, 75, 76)", "pif");
    }

    public int UpdateStudySearchDataIfHasECRFs()
    {
        return db.TransferObjectData("b.object_type_id in (21, 30, 40)", "ecrfs");
    }

    public int UpdateStudySearchDataIfHasManual()
    {
        return db.TransferObjectData("b.object_type_id in (35, 36)", "manual");
    }

    public int UpdateStudySearchDataIfHasSAP()
    {
        return db.TransferObjectData("b.object_type_id in (22, 29, 43, 74, 76)", "sap");
    }

    public int UpdateStudySearchDataIfHasCSR()
    {
        return db.TransferObjectData("b.object_type_id in (26, 27, 79, 85)", "csr");
    }

    public int UpdateStudySearchDataIfHasDataDesc()
    {
        return db.TransferObjectData("b.object_type_id in (20, 31, 32, 81, 82)", "data_desc");
    }

    public int UpdateStudySearchDataIfHasIPD()
    {
        return db.TransferObjectData("b.object_type_id in (80, 153) or b.object_type_id between 51 and 68", "ipd");
    }

    public int UpdateStudySearchDataIfHasAggData()
    {
       return db.TransferObjectData("b.object_type_id in (69, 70, 71, 72, 73, 154)", "agg_data");
    }

    public int UpdateStudySearchDataIfHasOthRes()
    {
        return db.TransferObjectData("b.object_type_id in (14, 15, 16, 17, 23, 24, 25, 33, 34, 39, 77, 78, 83, 84, 86)", "other_studyres");
    }
          
    public int UpdateStudySearchDataIfHasConfMat()
    {
        return db.TransferObjectData("b.object_type_id in (106, 107, 108, 109)", "conf_material");
    }
    
    public int UpdateStudySearchDataIfHasOthArt()
    {
        return db.TransferObjectData("b.object_type_id in (119, 121, 122)", "other_article");
    }

    public int UpdateStudySearchDataIfHasChapter()
    {
        return db.TransferObjectData("b.object_type_id in (101, 102, 103, 104, 105, 113)", "chapter");
    }

    public int UpdateStudySearchDataIfHasOthInfo()
   {
        return db.TransferObjectData("b.object_type_id in (112, 120, 123, 126, 127, 128)", "other_info");
    }

    public int UpdateStudySearchDataIfHasWebSite()
    {
        return db.TransferObjectData("b.object_type_id in (134)", "website");
    }

    public int UpdateStudySearchDataIfHasSoftware()
    {
        return db.TransferObjectData("b.object_type_id in (166, 167, 168, 169, 170)", "software");
    }

    public int UpdateStudySearchDataIfHasOther()
    {
        return db.TransferObjectData("b.object_type_id in (37, 151, 171, 110, 111, 114, 115, 118, 124, 125, 129, 130, 131, 132, 133) or b.object_type_id between 155 and 165",
                                  "other");
    }


    public void CreateTSConfig1()
    {
        string sql_string = @"DROP TEXT SEARCH CONFIGURATION IF EXISTS core.mdr_english_config;
             DROP TEXT SEARCH DICTIONARY IF EXISTS core.mdr_english_dict;
             DROP TEXT SEARCH DICTIONARY IF EXISTS core.mdr_synonyms;
             DROP TEXT SEARCH DICTIONARY IF EXISTS core.mdr_thesaurus;";

        db.ExecuteSQL(sql_string);

        sql_string = @"CREATE TEXT SEARCH CONFIGURATION core.mdr_english_config
             (copy = english);

             CREATE TEXT SEARCH DICTIONARY core.mdr_english_dict
             (TEMPLATE = pg_catalog.simple, STOPWORDS = mdr_english_phase1);

             CREATE TEXT SEARCH DICTIONARY core.mdr_synonyms 
             (TEMPLATE = synonym, SYNONYMS = mdr_synonyms);

             CREATE TEXT SEARCH DICTIONARY core.mdr_thesaurus (
               TEMPLATE = thesaurus,
               DictFile = mdr_thesaurus,
               Dictionary = core.mdr_english_dict
        );";

        db.ExecuteSQL(sql_string);

        sql_string = @" ALTER TEXT SEARCH CONFIGURATION core.mdr_english_config
             alter mapping for asciihword, asciiword, hword, word, int, uint, numword, numhword
             WITH core.mdr_thesaurus, core.mdr_synonyms, core.mdr_english_dict;

             ALTER TEXT SEARCH CONFIGURATION core.mdr_english_config
             DROP MAPPING FOR float, sfloat;";

        db.ExecuteSQL(sql_string);
    }


    public void CreateTSConfig2()
    {
        string sql_string = @"DROP TEXT SEARCH CONFIGURATION IF EXISTS core.mdr_english_config2;
            DROP TEXT SEARCH DICTIONARY IF EXISTS core.mdr_english_dict2;";

        db.ExecuteSQL(sql_string);

        sql_string = @"CREATE TEXT SEARCH CONFIGURATION core.mdr_english_config2
             (copy = english);

             CREATE TEXT SEARCH DICTIONARY core.mdr_english_dict2
             (TEMPLATE = pg_catalog.simple, STOPWORDS = mdr_english_phase2);";

        db.ExecuteSQL(sql_string);

        sql_string = @" ALTER TEXT SEARCH CONFIGURATION core.mdr_english_config2
               ALTER MAPPING FOR asciihword, asciiword, hword, word, numword, numhword
               WITH core.mdr_english_dict2;

               ALTER TEXT SEARCH CONFIGURATION core.mdr_english_config2
               DROP MAPPING FOR int, uint, float, sfloat;";
        db.ExecuteSQL(sql_string);

    }

    public int GenerateTempTitleData()
    {
        string sql_string = @"drop table if exists core.temp_titles;
        create table core.temp_titles
        (
          id INT PRIMARY KEY
        , study_id INT
        , lang_code VARCHAR
        , title VARCHAR
        , lex_string VARCHAR
        );
        CREATE INDEX temp_titles_study_id ON core.temp_titles(study_id);";

        db.ExecuteSQL(sql_string);

        sql_string = @"drop table if exists core.temp_titles_by_study;
        create table core.temp_titles_by_study
        (
          study_id INT PRIMARY KEY
        , lex_string VARCHAR
        , lexemes VARCHAR
        );";
        db.ExecuteSQL(sql_string);
        
        sql_string = @"INSERT INTO core.temp_titles
        (id, study_id, lang_code, title)
        SELECT id, study_id, lang_code, lower(title_text) 
        from core.study_titles ";
        return db.ExecuteCoreTransferSQL(sql_string, "where", "titles for search");

    }


    public int GenerateTitleLexemeStrings()
    {

        string sql_string = @"UPDATE core.temp_titles s
        set lex_string = array_to_string(
                               tsvector_to_array(
                                    to_tsvector('core.mdr_english_config', title)
                               ), 
                         ' ') ";
        return db.SearchUpdateSQL(sql_string, "temp_titles lex_string",
                                       min_titles_id, max_titles_id);

    }


    public int GenerateTitleDataByStudy()
    {

        string sql_string = @"insert into core.temp_titles_by_study(study_id, lex_string)
                     select study_id, string_agg(lex_string, ' ')
                     from core.temp_titles s ";
        return db.SearchStudyUpdateSQL(sql_string, "temp_titles_by_study",
                                       min_studies_id, max_studies_id);
    }
           


    public int TransferTitleDataByStudy()
    {
        string sql_string = @"UPDATE core.study_search ss
        set title_lexemes = to_tsvector('core.mdr_english_config2', s.lex_string)
        from core.temp_titles_by_study s 
        where ss.id = s.study_id ";
        return db.TransferSearchDataByStudy(sql_string, "temp_titles_by_study",
                                       min_studies_id, max_studies_id);
    }


    public int IndexTitleText()
    {
        // TO DO
        return 0;
    }


    public int GenerateTopicData()
    {
        string sql_string = @"drop table if exists core.temp_topics;
        create table core.temp_topics
        (
          id INT PRIMARY KEY
        , study_id INT
        , mesh_value VARCHAR
        , original_value VARCHAR
        , lex_string VARCHAR
        );
        CREATE INDEX temp_topics_study_id ON core.temp_topics(study_id);";

        db.ExecuteSQL(sql_string);

        sql_string = @"drop table if exists core.temp_topics_by_study;
        create table core.temp_topics_by_study
        (
          study_id INT PRIMARY KEY
        , lex_string VARCHAR
        , lexemes VARCHAR
        );";

        db.ExecuteSQL(sql_string);
        
        sql_string = @"INSERT INTO core.temp_topics
        (id, study_id, mesh_value, original_value)
        SELECT id, study_id, lower(mesh_value), lower(original_value)
        from core.study_topics ";

        return db.ExecuteCoreTransferSQL(sql_string, "where", "topics for search");
    }


    public int GenerateTopicLexemeStrings()
    {

        string sql_string = @"UPDATE core.temp_topics s
        set lex_string = array_to_string(
                               tsvector_to_array(
                                    to_tsvector('core.mdr_english_config', original_value ||' '|| coalesce(mesh_value, ''))
                               ), 
                         ' ') ";
        return db.SearchUpdateSQL(sql_string, "temp_topics lex_string",
                                       min_topics_id, max_topics_id);
    }

    public int GenerateTopicDataByStudy()
    {
        string sql_string = @"insert into core.temp_topics_by_study(study_id, lex_string)
                     select study_id, string_agg(lex_string, ' ')
                     from core.temp_topics s ";
        return db.SearchStudyUpdateSQL(sql_string, "temp_topics_by_study",
                                           min_studies_id, max_studies_id);
    }

    public int TransferTopicDataByStudy()
    {
        string sql_string = @"UPDATE core.study_search ss
        set topic_lexemes = to_tsvector('core.mdr_english_config2', s.lex_string)
        from core.temp_topics_by_study s 
        where ss.id = s.study_id ";

        return db.TransferSearchDataByStudy(sql_string, "temp_topics_by_study",
                                       min_studies_id, max_studies_id);
    }

    public int IndexTopicText()
    {
        // TO DO
        return 0;
    }

    public void DropTempSearchTables()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.temp_titles;
                       DROP TABLE IF EXISTS core.temp_titles;
                       DROP TABLE IF EXISTS core.temp_titles_by_study;
                       DROP TABLE IF EXISTS core.temp_topics_by_study;";
        db.ExecuteSQL(sql_string);
    }

}
