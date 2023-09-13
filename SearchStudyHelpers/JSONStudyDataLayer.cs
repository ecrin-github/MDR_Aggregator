using Dapper;
using Dapper.Contrib.Extensions;
using Npgsql;
using NpgsqlTypes;
namespace MDR_Aggregator;

public class JSONStudyDataLayer
{
    private readonly string _connString;

    // These strings are used as the base of each query.
    // They are constructed once in the class constructor,
    // and can then be applied for each object constructed,
    // by adding the id parameter at the end of the string.

    private string? study_query_string, study_identifier_query_string, study_title_query_string;
    private string? study_object_link_query_string, study_relationship_query_string;
    private string? study_feature_query_string, study_topics_query_string;
    private string? study_people_query_string, study_organisation_query_string;
    private string? study_condition_query_string, study_icd_query_string;
    private string? study_country_query_string, study_location_query_string;
    private readonly DBUtilities db;
    
    public JSONStudyDataLayer(string connString, ILoggingHelper logginghelper)
    {
        _connString = connString;
        db = new DBUtilities(connString, logginghelper);
        ConstructStudyQueryStrings();
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
        study_query_string = @"Select s.id, display_title, title_lang_code,
            brief_description, data_sharing_statement, 
            study_type_id, st.name as study_type,
            study_status_id, ss.name as study_status,
            study_start_year, study_start_month, study_enrolment, 
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
            where s.id = ";

        study_identifier_query_string = @"select
            si.id, identifier_value,
            identifier_type_id, it.name as identifier_type,
            source_id, si.source, source_ror_id,
            identifier_date, identifier_link
            from core.study_identifiers si
            left join context_lup.identifier_types it on si.identifier_type_id = it.id
            where study_id = ";

        study_title_query_string = @"select
            st.id, title_type_id, tt.name as title_type, title_text,
            lang_code, comments
            from core.study_titles st
            left join context_lup.title_types tt on st.title_type_id = tt.id
            where study_id = ";

        study_people_query_string = @"select 
            sp.id, sp.contrib_type_id, ct.name as contrib_type, sp.person_full_name,
            sp.orcid_id, sp.person_affiliation, sp.organisation_id, 
            sp.organisation_name, sp.organisation_ror_id
            from core.study_people sp 
            left join context_lup.contribution_types ct on sp.contrib_type_id = ct.id
            where study_id = ";

        study_organisation_query_string = @"select
            sg.id, sg.contrib_type_id, ct.name as contrib_type, sg.organisation_id, 
            sg.organisation_name, sg.organisation_ror_id
            from core.study_organisations sg 
            left join context_lup.contribution_types ct on sg.contrib_type_id = ct.id
            where study_id = ";
        
        study_topics_query_string = @"select
            st.id, topic_type_id, tt.name as topic_type, original_value,
            original_ct_type_id, tv.name as original_ct_type, original_ct_code,
            mesh_code, mesh_value 
            from core.study_topics st
            left join context_lup.topic_types tt on st.topic_type_id = tt.id
            left join context_lup.topic_vocabularies tv on st.original_ct_type_id = tv.id
            where study_id = ";

        study_feature_query_string = @"select
            sf.id, sf.feature_type_id, ft.name as feature_type,
            sf.feature_value_id, fv.name as feature_value
            from core.study_features sf
            inner join context_lup.study_feature_types ft on sf.feature_type_id = ft.id
            left join context_lup.study_feature_categories fv on sf.feature_value_id = fv.id
            where study_id = ";

        study_condition_query_string = @"select
            sc.id, sc.original_value, sc.original_ct_type_id, 
            tv.name as original_ct_type, sc.original_ct_code
            from core.study_conditions sc
            left join context_lup.topic_vocabularies tv on sc.original_ct_type_id = tv.id
            where study_id = ";

        study_icd_query_string = @"select 
            id, icd_code, icd_code 
            from core.study_icd 
            where study_id = ";
        
        study_country_query_string = @"select 
             sc.id, sc.country_id, sc.country_name, 
             sc.status_id, ss.name as status
             from core.study_countries sc
             left join context_lup.study_statuses ss on sc.status_id = ss.id
             where study_id = ";
        
        study_location_query_string = @"select
             sn.id, sn.facility_org_id, sn.facility, sn.facility_ror_id,
             sn.city_id, sn.city_name, sn.country_id, sn.country_name, 
             sn.status_id, ss.name as status
             from core.study_locations sn
             left join context_lup.study_statuses ss on sn.status_id = ss.id
             where study_id = ";
        
        study_relationship_query_string = @"select
            sr.id, relationship_type_id, rt.name as relationship_type,
            target_study_id
            from core.study_relationships sr
            left join context_lup.study_relationship_types rt 
            on sr.relationship_type_id = rt.id
            where study_id = ";

        study_object_link_query_string = @"select object_id
            from core.study_object_links
            where study_id = ";
    }


    public DBStudy? FetchDbStudy(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = study_query_string + id;
        return Conn.QueryFirstOrDefault<DBStudy>(sql_string);
    }


    public IEnumerable<DBStudyIdentifier> FetchDbStudyIdentifiers(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = study_identifier_query_string + id;
        return Conn.Query<DBStudyIdentifier>(sql_string);
    }


    public IEnumerable<DBStudyTitle> FetchDbStudyTitles(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = study_title_query_string + id;
        return Conn.Query<DBStudyTitle>(sql_string);
    }

    
    public IEnumerable<DBStudyPerson> FetchDbStudyPeople(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = study_people_query_string + id;
        return Conn.Query<DBStudyPerson>(sql_string);
    }

    
    public IEnumerable<DBStudyOrganisation> FetchDbStudyOrganisations(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = study_organisation_query_string + id;
        return Conn.Query<DBStudyOrganisation>(sql_string);
    }
    
    
    public IEnumerable<DBStudyFeature> FetchDbStudyFeatures(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = study_feature_query_string + id;
        return Conn.Query<DBStudyFeature>(sql_string);
    }
    
    
    public IEnumerable<DBStudyTopic> FetchDbStudyTopics(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = study_topics_query_string + id;
        return Conn.Query<DBStudyTopic>(sql_string);
    }


    public IEnumerable<DBStudyCondition> FetchDbStudyConditions(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = study_condition_query_string + id;
        return Conn.Query<DBStudyCondition>(sql_string);
    }

    
    public IEnumerable<DBStudyICD> FetchDbStudyICDs(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = study_icd_query_string + id;
        return Conn.Query<DBStudyICD>(sql_string);
    }
 
    public IEnumerable<DBStudyCountry> FetchDbStudyCountries(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = study_country_query_string + id;
        return Conn.Query<DBStudyCountry>(sql_string);
    }
    
    
    public IEnumerable<DBStudyLocation> FetchDbStudyLocations(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = study_location_query_string + id;
        return Conn.Query<DBStudyLocation>(sql_string);
    }
    
    public IEnumerable<DBStudyRelationship> FetchDbStudyRelationships(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = study_relationship_query_string + id;
        return Conn.Query<DBStudyRelationship>(sql_string);
    }


    public IEnumerable<DBStudyObjectLink> FetchDbStudyObjectLinks(int id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = study_object_link_query_string + id;
        return Conn.Query<DBStudyObjectLink>(sql_string);
    }

    public IEnumerable<JSONSearchResObject>? FetchObjectDetails(int study_id)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        string sql_string = $@"select os.* 
        from core.study_object_links sol
        inner join core.new_search_objects os
        on sol.object_id = os.oid
        where sol.study_id = {study_id}  
        order by os.year_pub";
        
        return Conn.Query<JSONSearchResObject>(sql_string);
    }
    
    public int StoreSearchRecord(StudyToSearchRecord tsr)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        return (int)Conn.Insert(tsr);
    }

    public void StoreJSONStudyInDB(int id, string full_json, string? search_res_json, 
                                   string? open_aire_json, string? c19p_json)
    {
        using NpgsqlConnection Conn = new NpgsqlConnection(_connString);
        Conn.Open();

        // To insert the string into a json field the parameters for the 
        // command have to be explicitly declared and typed
        
        search_res_json ??= "{}";
        open_aire_json ??= "{}";
        c19p_json ??= "{}";

        using var cmd = new NpgsqlCommand();
        cmd.CommandText = @"INSERT INTO core.new_search_studies_json (id, search_res, full_study, open_aire, c19p) 
                            VALUES (@id, @sr, @fs, @oa, @c19)";
        cmd.Parameters.Add(new NpgsqlParameter("@id", NpgsqlDbType.Integer) {Value = id });
        cmd.Parameters.Add(new NpgsqlParameter("@sr", NpgsqlDbType.Json) {Value = search_res_json });
        cmd.Parameters.Add(new NpgsqlParameter("@fs", NpgsqlDbType.Json) {Value = full_json });
        cmd.Parameters.Add(new NpgsqlParameter("@oa", NpgsqlDbType.Json) {Value = open_aire_json });
        cmd.Parameters.Add(new NpgsqlParameter("@c19", NpgsqlDbType.Json) {Value = c19p_json });
        cmd.Connection = Conn;
        cmd.ExecuteNonQuery();
        Conn.Close();
    }
    
    
    public int AddDataToPMIDSearchData()
    {
        string sql_string = @"insert into core.new_search_pmids (pmid, study_id)
        select oi.identifier_value::int, k.study_id from 
        core.object_identifiers oi
        inner join core.study_object_links k
        on oi.object_id = k.object_id
        where identifier_type_id = 16
        order by oi.identifier_value::int ;";
        
        return db.ExecuteSQL(sql_string);
    }    
    
    // idents table.
    
    public int AddDataToIdentsSearchData()
    {
        string top_sql = @"insert into core.new_search_idents (ident_type, ident_value, study_id)
        select identifier_type_id, identifier_value, study_id
        from core.study_identifiers si
        where identifier_type_id not in (1, 90) ";
        string bottom_sql = @" order by identifier_type_id, identifier_value ;";
        
        int min_studies_id = FetchMinId();
        int max_studies_id = FetchMaxId();
        return db.CreateSearchIdentsData(top_sql, bottom_sql, min_studies_id, max_studies_id, "search_idents");
    }
    
    public int AddDataToCountrySearchData()
    {
        string top_sql = @"insert into core.new_search_countries (country_id, study_id)
        select country_id, study_id
        from core.study_countries sc 
        where country_id is not null ";
        
        int min_studies_id = FetchMinId();
        int max_studies_id = FetchMaxId();
        return db.CreateSearchCountriesData(top_sql,min_studies_id, max_studies_id, "search_countries");
    }
    
    public int UpdateIdentsSearchWithStudyJson(int min_studies_id, int max_studies_id)
    {
        string sql_string = @"update core.new_search_idents s
                              set study_json = sj.search_res
                              from core.search_studies_json sj
                              where s.study_id = sj.id ";
        return db.TransferStudyJson(sql_string, min_studies_id, max_studies_id, "idents study json");
    }
    
    public int UpdatePMIDsSearchWithStudyJson(int min_studies_id, int max_studies_id)
    {
        string sql_string = @"update core.new_search_pmids s
                              set study_json = sj.search_res
                              from core.new_search_studies_json sj
                              where s.study_id = sj.id ";
        return db.TransferStudyJson(sql_string, min_studies_id, max_studies_id, "pmids study json");
    }
    
    public int UpdateLexemesSearchWithStudyJson(int min_studies_id, int max_studies_id)
    {
        string sql_string = @"update core.new_search_lexemes s
                              set study_json = sj.search_res
                              from core.new_search_studies_json sj
                              where s.study_id = sj.id ";
        return db.TransferStudyJson(sql_string, min_studies_id, max_studies_id, "lexemes study json");
    }
    
}



