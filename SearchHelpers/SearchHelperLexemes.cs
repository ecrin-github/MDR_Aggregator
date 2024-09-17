using System.Text.RegularExpressions;
using Dapper;
using MDR_Aggregator.AggDataHelpers;
using MDR_Aggregator.LoggingHelpers.Interfaces;
using Npgsql;

namespace MDR_Aggregator.SearchHelpers;

public partial class SearchHelperLexemes
{
    private readonly DbUtilities _db;
    private readonly int _minStudiesId, _maxStudiesId;
    private readonly string _connString;
    private readonly ILoggingHelper _loggingHelper;
    private readonly Dictionary<string, string> _romanSuffixes;

    public SearchHelperLexemes(string connString, ILoggingHelper loggingHelper)
    {
        _db = new DbUtilities(connString, loggingHelper);
        _minStudiesId = _db.GetAggMinId("core.studies");
        _maxStudiesId = _db.GetAggMaxId("core.studies");
        _connString = connString;
        _loggingHelper = loggingHelper;

        _romanSuffixes = new()
        {
            {" i ", "1 "}, {" ii ", "2 "},{" iii ", "3 "},{" iv ", "4 "},{" v ", "5 "},
            {" vi ", "6 "},{" vii ", "7 "},{" viii ", "8 "},{" ix ", "9 "},{" x ", "10 "},
            {" xi ", "11 "}, {" xii ", "12 "},{" xiii ", "13 "},{" xiv ", "14 "},{" xv ", "15 "},
            {" xvi ", "16 "},{" xvii ", "17 "},{" xviii ", "18 "},{" xix ", "19 "},{" xx ", "20 " },
            {" xxi ", "21 "},{" xxii ", "22 "},{" xxiii ", "23 "},{" xxiv ", "24 "},{" xxv ", "25 " },
            {" xxvi ", "26 "},{" xxvii ", "27 "},{" xxviii ", "28 "},
            {" xxix ", "29 "},{" xxx ", "30 " }
        };
    }

    public void CreateTSConfig()
    {
        string sql_string = @"DROP TEXT SEARCH CONFIGURATION IF EXISTS core.mdr_english_config2;
               DROP TEXT SEARCH DICTIONARY IF EXISTS core.mdr_synonyms;
               DROP TEXT SEARCH DICTIONARY IF EXISTS core.mdr_english_stemmer;";

        _db.ExecuteSql(sql_string);

        sql_string = @"CREATE TEXT SEARCH CONFIGURATION core.mdr_english_config2
             (copy = english);

             CREATE TEXT SEARCH DICTIONARY core.mdr_english_stemmer (
                TEMPLATE = snowball,
                Language = english,
                StopWords = mdr_english); 
        
              CREATE TEXT SEARCH DICTIONARY core.mdr_synonyms 
               (TEMPLATE = synonym, SYNONYMS = mdr_synonyms);";

        _db.ExecuteSql(sql_string);

        sql_string = @" ALTER TEXT SEARCH CONFIGURATION core.mdr_english_config2
               ALTER MAPPING FOR numhword, hword, numword, hword_asciipart, hword_part, hword_numpart, 
               asciihword, asciiword, word
               WITH core.mdr_synonyms, core.mdr_english_stemmer;
;
               ALTER TEXT SEARCH CONFIGURATION core.mdr_english_config2
               DROP MAPPING FOR float, sfloat, int, uint;";
        _db.ExecuteSql(sql_string);
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

        _db.ExecuteSql(sql_string);

        sql_string = @"INSERT INTO core.temp_titles
        (id, study_id, title_text)
        SELECT id, study_id, title_text 
        from core.study_titles ";
        return _db.ExecuteCoreTransferSql(sql_string, "where", "core.study_titles");
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

        _db.ExecuteSql(sql_string);

        sql_string = @"INSERT INTO core.temp_topics
        (id, study_id, topic_text)
        SELECT id, study_id, original_value ||' '|| coalesce(mesh_value, '')
        from core.study_topics ";

        return _db.ExecuteCoreTransferSql(sql_string, "where", "core.study_topics");
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

        _db.ExecuteSql(sql_string);

        sql_string = @"INSERT INTO core.temp_conditions
        (id, study_id, condition_text)
        SELECT id, study_id, original_value ||' '|| coalesce(icd_name, '')
        from core.study_conditions ";

        return _db.ExecuteCoreTransferSql(sql_string, "where", "core.study_conditions");
    }

    public int GenerateTitleDataByStudy()
    {
        string sql_string = @"drop table if exists core.temp_titles_by_study;
        create table core.temp_titles_by_study
        (
          study_id INT PRIMARY KEY
        , title_concat VARCHAR
        );";

        _db.ExecuteSql(sql_string);

        sql_string = @"insert into core.temp_titles_by_study(study_id, title_concat)
                     select study_id, string_agg(title_text, ' ')
                     from core.temp_titles s ";
        return _db.AggregateLexDataByStudy(sql_string, "temp_titles_by_study",
                               _minStudiesId, _maxStudiesId);
    }

    public int GenerateTopicDataByStudy()
    {
        string sql_string = @"drop table if exists core.temp_topics_by_study;
        create table core.temp_topics_by_study
        (
          study_id INT PRIMARY KEY
        , topic_concat VARCHAR
        );";

        _db.ExecuteSql(sql_string);

        sql_string = @"insert into core.temp_topics_by_study(study_id, topic_concat)
                     select study_id, string_agg(topic_text, ' ')
                     from core.temp_topics s ";
        return _db.AggregateLexDataByStudy(sql_string, "temp_topics_by_study",
                                 _minStudiesId, _maxStudiesId);
    }

    public int GenerateConditionDataByStudy()
    {
        string sql_string = @"drop table if exists core.temp_conditions_by_study;
        create table core.temp_conditions_by_study
        (
          study_id INT PRIMARY KEY
        , condition_concat VARCHAR
        );";

        _db.ExecuteSql(sql_string);
        sql_string = @"insert into core.temp_conditions_by_study(study_id, condition_concat)
                     select study_id, string_agg(condition_text, ' ')
                     from core.temp_conditions s ";

        return _db.AggregateLexDataByStudy(sql_string, "temp_conditions_by_study",
                                   _minStudiesId, _maxStudiesId);
    }

    public int CombineTitleAndTopicText()
    {
        string sql_string = @"Update core.temp_titles_by_study s
                              set title_concat = title_concat ||' '||t.topic_concat 
                              from core.temp_topics_by_study t
                              where t.topic_concat is not null
                              and s.study_id = t.study_id ";
        return _db.TransferSearchDataByStudy(sql_string, "title-topic combined ", _minStudiesId, _maxStudiesId);
    }


    public void ProcessLexemeBaseData()
    {
        for (int i = _minStudiesId; i <= _maxStudiesId; i += 10000)
        {
            int start_id = i; int end_id = i + 10000;
            string sql_string = $@"Select s.id as study_id, 
                     s.display_title as study_name, t.title_concat as tt, 
                     c.condition_concat as conditions
                     from core.studies s 
                     left join core.temp_titles_by_study t
                     on s.id = t.study_id
                     left join core.temp_conditions_by_study c
                     on s.id = c.study_id  
                     where s.id >= {start_id} and s.id < {end_id} ";

            using var conn = new NpgsqlConnection(_connString);
            List<LexemeBase> lexbases = (conn.Query<LexemeBase>(sql_string)).ToList();
            if (lexbases.Count == 0) continue;
            foreach (LexemeBase lex_base in lexbases)
            {
                string? tt = lex_base.tt;
                if (tt is not null)
                {
                    lex_base.tt = CleanStudyString(tt);
                }

                string? cn = lex_base.conditions;
                if (cn is not null)
                {
                    lex_base.conditions = CleanStudyString(cn);
                }
            }
            conn.Open();
            CopyHelpers.lexeme_base_helper.SaveAll(conn, lexbases);
            _loggingHelper.LogLine($"lexeme base data created for ids {start_id} to {end_id}");

            sql_string = $@"UPDATE core.new_search_lexemes s
                    set tt_lex = strip(to_tsvector('core.mdr_english_config2', tt))
                    where s.study_id >= {start_id} and s.study_id < {end_id} ";
            _db.ExecuteSql(sql_string);
            _loggingHelper.LogLine($"lex fields created for tt for ids {start_id} to {end_id}");

            sql_string = $@"UPDATE core.new_search_lexemes s
                    set conditions_lex = strip(to_tsvector('core.mdr_english_config2', conditions))
                    where s.study_id >= {start_id} and s.study_id < {end_id} ";
            _db.ExecuteSql(sql_string);
            _loggingHelper.LogLine($"lex fields created for conditions for ids {start_id} to {end_id}");
        }
    }

    private string CleanStudyString(string inString)
    {
        string new_st = inString.ToLower() + " ";  // ' ' added to catch final numbers below
        new_st = new_st.Replace("'", "");      // simplifies things
        new_st = new_st.Replace("‘", "");
        new_st = new_st.Replace("’", "");
        new_st = new_st.Replace(".", "");      // simplifies things
        new_st = new_st.Replace("/", " ");     // split these words up
        new_st = new_st.Replace("&", " and ");   // ampersands have special meaning
        if (new_st.Contains("in "))
        {
            new_st = new_st.Replace("in situ", "insitu");
            new_st = new_st.Replace("in vivo", "invivo");
            new_st = new_st.Replace("in vitro", "invitro");
            new_st = new_st.Replace("in silico", "insilico");
            new_st = new_st.Replace("in utero", "inutero");
        }
        if (new_st.Contains(" a "))
        {
            new_st = new_st.Replace("hepatitis a ", "hepatitis-a ");
            new_st = new_st.Replace("influenza a ", "influenza-a ");
            new_st = new_st.Replace("philia a ", "philia-a ");
            new_st = new_st.Replace("globin a ", "globin-a ");
            new_st = new_st.Replace("virus a ", "virus-a ");
            new_st = new_st.Replace("protein a ", "protein-a ");
            new_st = new_st.Replace("group a ", "group-a ");
            new_st = new_st.Replace("type a ", "type-a ");
            new_st = new_st.Replace("vitamin a ", "vitamin-a ");
            new_st = new_st.Replace("factor a ", "factor-a ");
            new_st = new_st.Replace("family a ", "family-a ");
            new_st = new_st.Replace("coenzyme a ", "coenzyme-a ");
            new_st = new_st.Replace("kinase a ", "kinase-a ");
            new_st = new_st.Replace("ferase a ", "ferase-a ");
            new_st = new_st.Replace("tidase a ", "tidase-a ");
            new_st = new_st.Replace("tidases a ", "tidases-a ");
            new_st = new_st.Replace("kinin a ", "kinin-a ");
        }
        if (new_st.Contains(" term "))
        {
            new_st = new_st.Replace("short term ", "short-term ");
            new_st = new_st.Replace("medium term ", "medium-term ");
            new_st = new_st.Replace("long term ", "long-term ");
        }
        if (MyRegex().IsMatch(new_st))
        {
            while (MyRegex1().IsMatch(new_st))   // may be more than one  
            {
                string to_replace = Regex.Match(new_st, @" \d{1,2} ").Value;
                string new_value;
                new_value = MyRegex2().IsMatch(new_st) ? to_replace[..^1] : // may be a dosage figure, usually mg; 
                    to_replace[1..]; // more often a suffix
                new_st = new_st.Replace(to_replace, new_value);
            }
        }
        if (MyRegex3().IsMatch(new_st))
        {
            while (MyRegex4().IsMatch(new_st))
            {
                string to_replace = MyRegex5().Match(new_st).Value;
                string new_value = _romanSuffixes[to_replace];
                new_st = new_st.Replace(to_replace, new_value);
            }
        }

        if (!new_st.Contains("phase ")) return new_st.Trim(); // some phase types not picked up above
        new_st = new_st.Replace("phase 1a ", "phase1a ");
        new_st = new_st.Replace("phase 1b ", "phase1b ");
        new_st = new_st.Replace("phase 1/2 ", "phase12 ");
        new_st = new_st.Replace("phase i/ii ", "phase12 ");
        new_st = new_st.Replace("phase 2/3 ", "phase23 ");
        new_st = new_st.Replace("phase ii/iii ", "phase23 ");
        new_st = new_st.Replace("phase 2a ", "phase2a ");
        new_st = new_st.Replace("phase 2b ", "phase2b ");
        new_st = new_st.Replace("phase 3/4 ", "phase34 ");
        new_st = new_st.Replace("phase iii/iv ", "phase34 ");
        return new_st.Trim();
    }

    public void DropTempLexTables()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.temp_titles;
                       DROP TABLE IF EXISTS core.temp_topics;
                       DROP TABLE IF EXISTS core.temp_conditions;
                       DROP TABLE IF EXISTS core.temp_titles_by_study;
                       DROP TABLE IF EXISTS core.temp_topics_by_study;
                       DROP TABLE IF EXISTS core.temp_conditions_by_study;";
        _db.ExecuteSql(sql_string);
    }

    [GeneratedRegex(@" \d{1,2} ")]
    private static partial Regex MyRegex();
    [GeneratedRegex(@" \d{1,2} ")]
    private static partial Regex MyRegex1();
    [GeneratedRegex(@" \d{1,2} mg")]
    private static partial Regex MyRegex2();
    [GeneratedRegex(@" (?=[xvi])(x{0,2})(i[xv]|v?i{0,3}) ")]
    private static partial Regex MyRegex3();
    [GeneratedRegex(@" (?=[xvi])(x{0,2})(i[xv]|v?i{0,3}) ")]
    private static partial Regex MyRegex4();
    [GeneratedRegex(@" (?=[xvi])(x{0,2})(i[xv]|v?i{0,3}) ")]
    private static partial Regex MyRegex5();
}
