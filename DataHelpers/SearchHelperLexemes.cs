namespace MDR_Aggregator;

public class SearchHelperLexemes
{
    private readonly DBUtilities db;
    private readonly int min_studies_id, max_studies_id;
    private readonly string[] number_suffixes, roman_suffixes;
    public SearchHelperLexemes(string connString, ILoggingHelper loggingHelper)
    {
        db = new DBUtilities(connString, loggingHelper);
        min_studies_id = db.GetAggMinId("core.studies");
        max_studies_id = db.GetAggMaxId("core.studies");
        
        number_suffixes = new string[]
        {
            " 1 ", " 2 "," 3 "," 4 "," 5 "," 6 "," 7 "," 8 "," 9 "," 10 ",
            " 11 ", " 12 "," 13 "," 14 "," 15 "," 16 "," 17 "," 18 "," 19 "," 20 "
        };
        roman_suffixes = new string[]
        {
            " i ", " ii "," iii "," iv "," v "," vi "," vii "," viii "," ix "," x ",
            " xi ", " xii "," xiii "," xiv "," xv "," xvi "," xvii "," xviii "," xix "," xx "
        };
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
             (TEMPLATE = pg_catalog.simple, STOPWORDS = mdr_english);

             CREATE TEXT SEARCH DICTIONARY core.mdr_synonyms 
             (TEMPLATE = synonym, SYNONYMS = mdr_synonyms);";
        /*
         CREATE TEXT SEARCH DICTIONARY core.mdr_thesaurus (

            TEMPLATE = thesaurus,
            DictFile = mdr_thesaurus,
            Dictionary = core.mdr_english_dict);
            
        WITH core.mdr_thesaurus, core.mdr_synonyms, core.mdr_english_dict;
            */
        db.ExecuteSQL(sql_string);

        sql_string = @" ALTER TEXT SEARCH CONFIGURATION core.mdr_english_config
             alter mapping for asciihword, asciiword, hword, word, int, uint, numword, numhword
             WITH core.mdr_synonyms, core.mdr_english_dict;

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
               ALTER MAPPING FOR asciihword, asciiword, hword, word, hword_asciipart, hword_part, numword, numhword
               WITH core.mdr_english_dict2;

               ALTER TEXT SEARCH CONFIGURATION core.mdr_english_config2
               DROP MAPPING FOR int, uint, float, sfloat;";
        db.ExecuteSQL(sql_string);

    }

    public int GenerateTitleData()
    {
        string sql_string = @"drop table if exists core.temp_titles;
        create table core.temp_titles
        (
          id INT PRIMARY KEY
        , study_id INT
        , title_text VARCHAR
        );
        CREATE INDEX temp_titles_study_id ON core.temp_titles(study_id);";

        db.ExecuteSQL(sql_string);

        sql_string = @"INSERT INTO core.temp_titles
        (id, study_id, title_text)
        SELECT id, study_id, title_text 
        from core.study_titles ";
        return db.ExecuteCoreTransferSQL(sql_string, "where", "core.study_titles");
    }
    
    public int GenerateTopicData()
    {
        string sql_string = @"drop table if exists core.temp_topics;
        create table core.temp_topics
        (
          id INT PRIMARY KEY
        , study_id INT
        , topic_text VARCHAR
        );
        CREATE INDEX temp_topics_study_id ON core.temp_topics(study_id);";

        db.ExecuteSQL(sql_string);

        sql_string = @"INSERT INTO core.temp_topics
        (id, study_id, topic_text)
        SELECT id, study_id, original_value ||' '|| coalesce(mesh_value, '')
        from core.study_topics ";

        return db.ExecuteCoreTransferSQL(sql_string, "where", "core.study_topics");
    }

    public int GenerateConditionData()
    {
        string sql_string = @"drop table if exists core.temp_conditions;
        create table core.temp_conditions
        (
          id INT PRIMARY KEY
        , study_id INT
        , condition_text VARCHAR
        );
        CREATE INDEX temp_conditions_study_id ON core.temp_conditions(study_id);";

        db.ExecuteSQL(sql_string);

        sql_string = @"INSERT INTO core.temp_conditions
        (id, study_id, condition_text)
        SELECT id, study_id, original_value ||' '|| coalesce(icd_name, '')
        from core.study_conditions ";

        return db.ExecuteCoreTransferSQL(sql_string, "where", "core.study_conditions");
    }

    /*
    public int GenerateTitleLexemeStrings()
    {
        string sql_string = @"UPDATE core.temp_titles s
                        set lex_string = array_to_string(
                               tsvector_to_array(
                                    to_tsvector('core.mdr_english_config', title)
                               ), 
                         ' ') ";
        return db.CreateLexSQL(sql_string, "temp_titles lex_string", "core.study_titles");

    }
    public int GenerateTopicLexemeStrings()
    {
        string sql_string = @"UPDATE core.temp_topics s
        set lex_string = array_to_string(
                               tsvector_to_array(
                                    to_tsvector('core.mdr_english_config', original_value ||' '|| coalesce(mesh_value, ''))
                               ), 
                         ' ') ";
        return db.CreateLexSQL(sql_string, "temp_topics lex_string", "core.study_topics");
    }
    
    public int GenerateConditionLexemeStrings()
    {

        string sql_string = @"UPDATE core.temp_conditions s
        set lex_string = array_to_string(
                               tsvector_to_array(
                                    to_tsvector('core.mdr_english_config', original_value ||' '|| coalesce(icd_name, ''))
                               ), 
                         ' ') ";
        return db.CreateLexSQL(sql_string, "temp_conditions lex_string", "core.study_conditions");
    }
    */
    
    public int GenerateTitleDataByStudy()
    {
        string sql_string = @"drop table if exists core.temp_titles_by_study;
        create table core.temp_titles_by_study
        (
          study_id INT PRIMARY KEY
        , title_concat VARCHAR
        );";

        db.ExecuteSQL(sql_string);
        
        sql_string = @"insert into core.temp_titles_by_study(study_id, title_concat)
                     select study_id, string_agg(title_text, ' ')
                     from core.temp_titles s ";
        return db.AggregateLexDataByStudy(sql_string, "temp_titles_by_study",
                               min_studies_id, max_studies_id);
    }
          
    public int GenerateTopicDataByStudy()
    {
        string sql_string = @"drop table if exists core.temp_topics_by_study;
        create table core.temp_topics_by_study
        (
          study_id INT PRIMARY KEY
        , topic_concat VARCHAR
        );";

        db.ExecuteSQL(sql_string);
        
        sql_string = @"insert into core.temp_topics_by_study(study_id, topic_concat)
                     select study_id, string_agg(topic_text, ' ')
                     from core.temp_topics s ";
         return db.AggregateLexDataByStudy(sql_string, "temp_topics_by_study",
                                  min_studies_id, max_studies_id);
    }
    
    public int GenerateConditionDataByStudy()
    {
        string sql_string = @"drop table if exists core.temp_conditions_by_study;
        create table core.temp_conditions_by_study
        (
          study_id INT PRIMARY KEY
        , condition_concat VARCHAR
        );";

        db.ExecuteSQL(sql_string);
        sql_string = @"insert into core.temp_conditions_by_study(study_id, condition_concat)
                     select study_id, string_agg(condition_text, ' ')
                     from core.temp_conditions s ";
        
         return db.AggregateLexDataByStudy(sql_string, "temp_conditions_by_study",
                                    min_studies_id, max_studies_id);
    }

    public int CombineTitleAndTopicText()
    {
        string sql_string = @"Update core.temp_titles_by_study s
                              set title_concat = title_concat ||' '||t.topic_concat 
                              from core.temp_topics_by_study t
                              where t.topic_concat is not null
                              and s.study_id = t.study_id ";
        return db.TransferSearchDataByStudy(sql_string, "title-topic combined ", min_studies_id, max_studies_id);
    }
    

    public int CreateSearchLexemesTable()
    {
        string sql_string = @"drop table if exists core.search_lexemes;
        create table core.search_lexemes
        (
          study_id            int               primary key  not null
        , study_name          varchar           null 
        , tt                  varchar           null   
        , tt_lex   		      tsvector          null
        , tt_lex2  		      tsvector          null
        , conditions 	      varchar           null   
        , conditions_lex   	  tsvector          null
        , conditions_lex2     tsvector          null
        ) ";

        db.ExecuteSQL(sql_string);
        
        sql_string = @"insert into core.search_lexemes(study_id, study_name, tt, conditions)
                     select s.id, s.display_title, t.title_concat, c.condition_concat
                     from core.studies s 
                     left join core.temp_titles_by_study t
                     on s.id = t.study_id
                     left join core.temp_conditions_by_study c
                     on s.id = c.study_id ";
        return db.ExecuteCoreTransferSQL(sql_string, " where ", "core.studies");
    }
    
    // Get the data and clean it 
    public void CleanCollectedTTData()
    {
        string sql_string = @"Update core.search_lexemes ss
                              set tt = replace(tt,'&', ' and ')
                              where tt is not null ";
        
        db.UpdateSearchStudyData(sql_string, "tt ampersands", min_studies_id, max_studies_id);

        sql_string = @"Update core.search_lexemes ss
                              set tt = replace(tt,'''', '')
                              where tt is not null ";
        db.UpdateSearchStudyData(sql_string, "tt apostrophes", min_studies_id, max_studies_id);

        sql_string = @"Update core.search_lexemes ss
                              set tt = lower(replace(tt,'.', ''))
                              where tt is not null 
                              ";
        db.UpdateSearchStudyData(sql_string, "tt periods", min_studies_id, max_studies_id);
    }

    public void CleanCollectedConditionData()
    {
        string sql_string = @"Update core.search_lexemes ss
                              set conditions = replace(conditions,'&', ' and ')
                              where conditions is not null";
        db.UpdateSearchStudyData(sql_string, "conditions ampersands", min_studies_id, max_studies_id);
        
        sql_string = @"Update core.search_lexemes ss
                              set conditions = replace(conditions,'''', '')
                              where conditions is not null";
        db.UpdateSearchStudyData(sql_string, "conditions apostrophes", min_studies_id, max_studies_id);

        sql_string = @"Update core.search_lexemes ss
                              set conditions = lower(replace(conditions,'.', ''))
                              where conditions is not null
                              ";
        db.UpdateSearchStudyData(sql_string, "conditions periods", min_studies_id, max_studies_id);
    }

    public void CleanNumbersInTTData()
    {
        string sql_string, to_find, to_replace;
        for (int n = 0; n < 20; n++)
        {
            to_find = roman_suffixes[n];
            to_replace = number_suffixes[n][1..];
            sql_string = $@"Update core.search_lexemes ss
                           set tt = replace(tt,'{to_find}', '{to_replace}')
                           where tt is not null 
                           and tt like '%{to_find}$'";
            db.ExecuteSQL(sql_string);
            
            to_find = number_suffixes[n];
            to_replace = number_suffixes[n][1..];
            sql_string = $@"Update core.search_lexemes ss
                           set tt = replace(tt,'{to_find}', '{to_replace}')
                           where tt is not null 
                           and tt like '%{to_find}$'";
            db.ExecuteSQL(sql_string);
        }
    }
    
    public void CleanNumnbersInConditionData()
    {
        string sql_string, to_find, to_replace;
        for (int n = 0; n < 20; n++)
        {
            to_find = roman_suffixes[n];
            to_replace = number_suffixes[n][1..];
            sql_string = $@"Update core.search_lexemes ss
                           set conditions = replace(conditions,'{to_find}', '{to_replace}')
                           where conditions is not null 
                           and conditions like '%{to_find}$'";
            db.ExecuteSQL(sql_string);

            to_find = number_suffixes[n];
            to_replace = number_suffixes[n][1..];
            sql_string = $@"Update core.search_lexemes ss
                           set conditions = replace(conditions,'{to_find}', '{to_replace}')
                           where conditions is not null 
                           and conditions like '%{to_find}$'";
            db.ExecuteSQL(sql_string);
        }
    }

    // should ALL BE ...min_studies_id, max_studies_id)

    public int TransferTitleLexDataByStudy()
    {
        string sql_string = @"UPDATE core.search_lexemes ss
        set tt = s.title_concat,
        tt_lex = to_tsvector('core.mdr_english_config', s.title_concat)
        from core.temp_titles_by_study s 
        where ss.study_id = s.study_id ";
        return db.TransferSearchDataByStudy(sql_string, "titles",
                                       1, 40000);
    }
    
    /*
    public int TransferTopicLexDataByStudy()
    {
        string sql_string = @"UPDATE core.search_lexemes ss
        set topics_lex = to_tsvector('core.mdr_english_config', s.lex_string)
        from core.temp_topics_by_study s 
        where ss.study_id = s.study_id ";

        return db.TransferSearchDataByStudy(sql_string, "topics",
                                       min_studies_id, max_studies_id);
    }
    */
    
    public int TransferConditionLexDataByStudy()
    {
        string sql_string = @"UPDATE core.search_lexemes ss
        set conditions = s.condition_concat,
        conditions_lex = to_tsvector('core.mdr_english_config', s.condition_concat)
        from core.temp_conditions_by_study s 
        where ss.study_id = s.study_id ";

        return db.TransferSearchDataByStudy(sql_string, "conditions",
            1, 40000);
    }
    
    
    public void IndexTTLexText()
    {
        string sql_string = @"DROP INDEX IF EXISTS core.tt_search_idx;
        CREATE INDEX tt_search_idx ON core.search_lexemes USING GIN (tt_lex);";
        db.ExecuteSQL(sql_string);
    }
    
    public void IndexConditionLexText()
    {
        string sql_string = @"DROP INDEX IF EXISTS core.cond_search_idx;
        CREATE INDEX cond_search_idx ON core.search_lexemes USING GIN (conditions_lex);";
        db.ExecuteSQL(sql_string);
    }
    
    public void DropTempLexTables()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.temp_titles;
                       DROP TABLE IF EXISTS core.temp_topics;
                       DROP TABLE IF EXISTS core.temp_conditions;
                       DROP TABLE IF EXISTS core.temp_titles_by_study;
                       DROP TABLE IF EXISTS core.temp_topics_by_study;
                       DROP TABLE IF EXISTS core.temp_conditions_by_study;";
        db.ExecuteSQL(sql_string);
    }
}
