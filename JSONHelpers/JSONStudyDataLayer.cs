using Dapper;
using Microsoft.Extensions.Configuration;
using Npgsql;
using NpgsqlTypes;
namespace MDR_Aggregator;

public class JSONStudyDataLayer
{
    private readonly string _connString;
    private readonly string _study_json_folder;
    private readonly ILoggingHelper _loggingHelper;

    // These strings are used as the base of each query.
    // They are constructed once in the class constructor,
    // and can then be applied for each object constructed,
    // by adding the id parameter at the end of the string.

    private string? study_query_string, study_identifier_query_string, study_title_query_string;
    private string? study_object_link_query_string, study_relationship_query_string;
    private string? study_feature_query_string, study_topics_query_string;

    public JSONStudyDataLayer(ILoggingHelper logginghelper, string connString)
    {
        _loggingHelper = logginghelper;
        _connString = connString;

        IConfigurationRoot settings = new ConfigurationBuilder()
        .SetBasePath(AppContext.BaseDirectory)
        .AddJsonFile("appsettings.json")
        .Build();

        _study_json_folder = settings["study json folder"]!;

        ConstructStudyQueryStrings();

    }

    public string ConnString => _connString;
    public string StudyJsonFolder => _study_json_folder;

    public int ExecuteSQL(string sql_string)
    {
        using var conn = new NpgsqlConnection(_connString);
        try
        {
            return conn.Execute(sql_string);
        }
        catch (Exception e)
        {
            _loggingHelper.LogError("In ExecuteSQL; " + e.Message + ", \nSQL was: " + sql_string);
            return 0;
        }
    }

    public int FetchMinId()
    {
        string sql_string = @"select min(id) from core.studies";
        using var conn = new NpgsqlConnection(_connString);
        return conn.ExecuteScalar<int>(sql_string);
    }

    public int FetchMaxId()
    {
        string sql_string = @"select max(id) from core.studies";
        using var conn = new NpgsqlConnection(_connString);
        return conn.ExecuteScalar<int>(sql_string);
    }

    public IEnumerable<int> FetchIds(int n, int batch)
    {
        string sql_string = @"select id from core.studies
                 where id between " + n + @" 
                 and " + (n + batch - 1);
        using var conn = new NpgsqlConnection(_connString);
        return conn.Query<int>(sql_string);
    }

    private void ConstructStudyQueryStrings()
    {
        // study query string
        study_query_string = @"Select s.id, display_title, title_lang_code,
            brief_description, data_sharing_statement, 
            study_type_id, st.name as study_type,
            study_status_id, ss.name as study_status,
            study_enrolment, 
            study_gender_elig_id, ge.name as study_gender_elig, 
            min_age, min_age_units_id, tu1.name as min_age_units,
            max_age, max_age_units_id, tu2.name as max_age_units,
            provenance_string
            from core.studies s
            left join context_lup.study_types st on s.study_type_id = st.id
            left join context_lup.study_statuses ss on s.study_status_id = ss.id
            left join context_lup.gender_eligibility_types ge on s.study_gender_elig_id = ge.id
            left join context_lup.time_units tu1 on s.min_age_units_id = tu1.id
            left join context_lup.time_units tu2 on s.max_age_units_id = tu2.id
            where s.id ";


        // study identifier query string 
        study_identifier_query_string = @"select
            si.id, identifier_value,
            identifier_type_id, it.name as identifier_type,
            source_id, source, source_ror_id,
            identifier_date, identifier_link
            from core.study_identifiers si
            left join context_lup.identifier_types it on si.identifier_type_id = it.id
            where study_id ";


        //study title query string
        study_title_query_string = @"select
            st.id, title_type_id, tt.name as title_type, title_text,
            lang_code, comments
            from core.study_titles st
            left join context_lup.title_types tt on st.title_type_id = tt.id
            where study_id ";


        // study topics query string
        study_topics_query_string = @"select
            st.id, topic_type_id, tt.name as topic_type, 
            mesh_coded, mesh_code, mesh_value, 
            original_ct_id, original_ct_code, original_value
            from core.study_topics st
            left join context_lup.topic_types tt on st.topic_type_id = tt.id
            where study_id ";


        // study feature query string
        study_feature_query_string = @"select
            sf.id, sf.feature_type_id, ft.name as feature_type,
            sf.feature_value_id, fv.name as feature_value
            from core.study_features sf
            inner join context_lup.study_feature_types ft on sf.feature_type_id = ft.id
            left join context_lup.study_feature_categories fv on sf.feature_value_id = fv.id
            where study_id ";


        // study_relationship query string
        study_relationship_query_string = @"select
            sr.id, relationship_type_id, rt.name as relationship_type,
            target_study_id
            from core.study_relationships sr
            left join context_lup.study_relationship_types rt 
            on sr.relationship_type_id = rt.id
            where study_id ";


        // study object link query string
        study_object_link_query_string = @"select object_id
            from core.study_object_links
            where study_id ";

    }


    public DBStudy FetchDbStudy(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = study_query_string + " = " + id;
        return Conn.QueryFirstOrDefault<DBStudy>(sql_string);
    }


    public IEnumerable<DBStudyIdentifier> FetchDbStudyIdentifiers(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = study_identifier_query_string + " = " + id;
        return Conn.Query<DBStudyIdentifier>(sql_string);
    }


    public IEnumerable<DBStudyTitle> FetchDbStudyTitles(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = study_title_query_string + " = " + id;
        return Conn.Query<DBStudyTitle>(sql_string);
    }


    public IEnumerable<DBStudyFeature> FetchDbStudyFeatures(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = study_feature_query_string + " = " + id;
        return Conn.Query<DBStudyFeature>(sql_string);
    }


    public IEnumerable<DBStudyTopic> FetchDbStudyTopics(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = study_topics_query_string + " = " + id;
        return Conn.Query<DBStudyTopic>(sql_string);
    }


    public IEnumerable<DBStudyRelationship> FetchDbStudyRelationships(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = study_relationship_query_string + " = " + id;
        return Conn.Query<DBStudyRelationship>(sql_string);
    }


    public IEnumerable<DBStudyObjectLink> FetchDbStudyObjectLinks(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = study_object_link_query_string + " = " + id;
        return Conn.Query<DBStudyObjectLink>(sql_string);
    }


    public void StoreJSONStudyInDB(int id, string study_json)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        Conn.Open();

        // To insert the string into a json field the parameters for the 
        // command have to be explicitly declared and typed

        using var cmd = new NpgsqlCommand();
        cmd.CommandText = "INSERT INTO core.studies_json (id, json) VALUES (@id, @p)";
        cmd.Parameters.Add(new NpgsqlParameter("@id", NpgsqlDbType.Integer) {Value = id });
        cmd.Parameters.Add(new NpgsqlParameter("@p", NpgsqlDbType.Json) {Value = study_json });
        cmd.Connection = Conn;
        cmd.ExecuteNonQuery();
        Conn.Close();
    }
}



