namespace MDR_Aggregator;

public class SearchHelperLexemes
{
    private readonly DBUtilities db;
    int min_studies_id, max_studies_id;
    int min_titles_id, max_titles_id;
    int min_topics_id, max_topics_id;

    public SearchHelperLexemes(string connString, ILoggingHelper loggingHelper)
    {
        db = new DBUtilities(connString, loggingHelper);
        
        min_studies_id = db.GetAggMinId("core.studies");
        max_studies_id = db.GetAggMaxId("core.studies");
        min_titles_id = db.GetAggMinId("core.study_titles");
        max_titles_id = db.GetAggMaxId("core.study_titles");
        min_topics_id = db.GetAggMinId("core.study_topics");
        max_topics_id = db.GetAggMaxId("core.study_topics");
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
        //return db.SearchStudyUpdateSQL(sql_string, "temp_titles_by_study",
         //                              min_studies_id, max_studies_id);
         return 0;  // for now
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
       // return db.SearchStudyUpdateSQL(sql_string, "temp_topics_by_study",
             //                              min_studies_id, max_studies_id);
             return 0;  // for now
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
