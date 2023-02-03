using Dapper;
using Npgsql;
using PostgreSQLCopyHelper;
using System.Collections.Generic;


namespace MDR_Aggregator
{
    public class StudyDataTransferrer
    {
        string _connString;
        DBUtilities db;
        ILoggingHelper _loggingHelper;

        public StudyDataTransferrer(string connString, ILoggingHelper logginghelper)
        {
            _connString = connString;
            _loggingHelper = logginghelper;
            db = new DBUtilities(connString, _loggingHelper);
        }


        public void SetUpTempStudyIdsTable()
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                string sql_string = @"DROP TABLE IF EXISTS nk.temp_study_ids;
                        CREATE TABLE nk.temp_study_ids(
                        study_id                 INT
                      , source_id                INT
                      , sd_sid                   VARCHAR
                      , is_preferred             BOOLEAN
                      , datetime_of_data_fetch   TIMESTAMPTZ
                      ); ";
                conn.Execute(sql_string);
            }
        }

        public IEnumerable<StudyId> FetchStudyIds(int source_id, string source_conn_string)
        {
            using (var conn = new NpgsqlConnection(source_conn_string))
            {
                string sql_string = @"select " + source_id.ToString() + @" as source_id, 
                          sd_sid, datetime_of_data_fetch
                          from ad.studies";

                return conn.Query<StudyId>(sql_string);
            }
        }


        public ulong StoreStudyIds(PostgreSQLCopyHelper<StudyId> copyHelper, IEnumerable<StudyId> entities)
        {
            // stores the study id data in a temporary table
            using (var conn = new NpgsqlConnection(_connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        public void CheckStudyLinks()
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                // Does any study id correspond to a study already in all_ids_studies 
                // table, that is linked to it via study-study link table.
                // Such a study will match the left hand side of the study-study 
                // link table (the one to be replaced), and take on the study_id 
                // used for the 'preferred' right hand side. This should already exist
                // because addition of studies is donme in the order 'more preferred first'.

                string sql_string = @"UPDATE nk.temp_study_ids t
                           SET study_id = s.study_id, is_preferred = false
                           FROM nk.study_study_links k
                                INNER JOIN nk.all_ids_studies s
                                ON k.preferred_sd_sid = s.sd_sid
                                AND k.preferred_source_id = s.source_id
                           WHERE t.sd_sid = k.sd_sid
                           AND t.source_id =  k.source_id;";
                int res = db.ExecuteSQL(sql_string);
                _loggingHelper.LogLine(res.ToString() + " existing studies found");

                // Also create a small table that has just the study_ids and sd_sids for the 
                // already existing studies (used in the import of any additional data
                // from these studies

                sql_string = @"DROP TABLE IF EXISTS nk.existing_studies;
                               CREATE TABLE nk.existing_studies as 
                                       SELECT sd_sid, study_id
                                       FROM nk.temp_study_ids
                                       WHERE is_preferred = false";
                db.ExecuteSQL(sql_string);

            }
        }


        public void UpdateAllStudyIdsTable(int source_id)
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                // Add the new study id records to the all Ids table

                string sql_string = @"INSERT INTO nk.all_ids_studies
                            (study_id, source_id, sd_sid, 
                             datetime_of_data_fetch, is_preferred)
                             select study_id, source_id, sd_sid, 
                             datetime_of_data_fetch, is_preferred
                             from nk.temp_study_ids";

                conn.Execute(sql_string);

                // Where the study_ids are null they can take on the value of the 
                // record id.

                sql_string = @"UPDATE nk.all_ids_studies
                            SET study_id = id, is_preferred = true
                            WHERE study_id is null
                            AND source_id = " + source_id.ToString();

                conn.Execute(sql_string);

                // 'Back-update' the study temp table using the newly created study_ids
                // now all should be done...

                sql_string = @"UPDATE nk.temp_study_ids t
                           SET study_id = a.study_id, is_preferred = true
                           FROM nk.all_ids_studies a
                           WHERE t.source_id = a.source_id
                           AND t.sd_sid = a.sd_sid
                           AND t.study_id is null;";

                conn.Execute(sql_string);
            }
        }


        
        public int LoadStudies(string schema_name)
        {
            
            // Insert the study data unless it is already in under another 
            // id (i.e. t.is_preferred = false).

            string sql_string = @"INSERT INTO st.studies(id, 
                    display_title, title_lang_code, brief_description, data_sharing_statement, 
                    study_start_year, study_start_month, study_type_id, study_status_id,
                    study_enrolment, study_gender_elig_id, min_age, min_age_units_id,
                    max_age, max_age_units_id)
                    SELECT t.study_id,
                    s.display_title, s.title_lang_code, s.brief_description, s.data_sharing_statement, 
                    s.study_start_year, s.study_start_month, s.study_type_id, s.study_status_id,
                    s.study_enrolment, s.study_gender_elig_id, s.min_age, s.min_age_units_id,
                    s.max_age, s.max_age_units_id
                    FROM nk.temp_study_ids t
                    INNER JOIN " + schema_name + @".studies s
                    on t.sd_sid = s.sd_sid
                    WHERE t.is_preferred = true ";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "studies", "new studies");
            _loggingHelper.LogLine("Loaded records - " + res.ToString() + " studies, new studies");

            // Note that the statement below also updates studies that are not added as new
            // (because they equate to existing studies) but which were new in the 
            // source data.

            db.Update_SourceTable_ExportDate(schema_name, "studies");
            return res;
        }


        public void LoadStudyIdentifiers(string schema_name)
        {
            string destination_field_list = @"study_id, 
                    identifier_type_id, identifier_org_id, 
                    identifier_org, identifier_org_ror_id,
                    identifier_value, identifier_date, identifier_link ";

            string source_field_list = @" 
                    s.identifier_type_id, s.identifier_org_id, 
                    s.identifier_org, s.identifier_org_ror_id,
                    s.identifier_value, s.identifier_date, s.identifier_link ";

            // For 'preferred' study Ids add all identifiers.

            string sql_string = @"INSERT INTO st.study_identifiers(" + destination_field_list + @")
                    SELECT k.study_id, " + source_field_list + @"
                    FROM nk.temp_study_ids k
                    INNER JOIN " + schema_name + @".study_identifiers s
                    on k.sd_sid = s.sd_sid
                    WHERE k.is_preferred = true ";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "study_identifiers", "new studies");
            _loggingHelper.LogLine("Loaded records - " + res.ToString() + " study_identifiers, new studies");

            // For 'existing studies' study Ids add only new identifiers.

            sql_string = @"DROP TABLE IF EXISTS nk.source_data;
                           CREATE TABLE nk.source_data as 
                           SELECT es.study_id, d.* 
                           FROM " + schema_name + @".study_identifiers d
                           INNER JOIN nk.existing_studies es
                           ON d.sd_sid = es.sd_sid";
            db.ExecuteSQL(sql_string);

            sql_string = @"DROP TABLE IF EXISTS nk.existing_data;
                           CREATE TABLE nk.existing_data as 
                           SELECT es.sd_sid, es.study_id, 
                           c.identifier_type_id, c.identifier_value 
                           FROM st.study_identifiers c
                           INNER JOIN nk.existing_studies es
                           ON c.study_id = es.study_id;";
            db.ExecuteSQL(sql_string);

            sql_string = @"INSERT INTO st.study_identifiers(" + destination_field_list + @")
                           SELECT s.study_id, " + source_field_list + @" 
                           FROM nk.source_data s
                           LEFT JOIN nk.existing_data e
                           ON s.sd_sid = e.sd_sid
                           AND s.identifier_type_id = e.identifier_type_id
                           AND s.identifier_value = e.identifier_value
                           WHERE e.study_id is null ";

            res = db.ExecuteTransferSQL(sql_string, schema_name, "study_identifiers", "existing studies");
            _loggingHelper.LogLine("Loaded records - " + res.ToString() + " study_identifiers, existing studies");

            db.ExecuteSQL("DROP TABLE IF EXISTS nk.source_data;");
            db.ExecuteSQL("DROP TABLE IF EXISTS nk.existing_data;");

            db.Update_SourceTable_ExportDate(schema_name, "study_identifiers");
        }


        public void LoadStudyTitles(string schema_name)
        {
            string destination_field_list = @"study_id, 
                    title_type_id, title_text, lang_code, 
                    lang_usage_id, is_default, comments ";

            string source_field_list = @" 
                    s.title_type_id, s.title_text, s.lang_code, 
                    s.lang_usage_id, s.is_default, s.comments ";

            // For 'preferred' study Ids add all titles.

            string sql_string = @"INSERT INTO st.study_titles(" + destination_field_list + @")
                    SELECT k.study_id, " + source_field_list + @"
                    FROM nk.temp_study_ids k
                    INNER JOIN " + schema_name + @".study_titles s
                    on k.sd_sid = s.sd_sid
                    WHERE k.is_preferred = true ";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "study_titles", "new studies");
            _loggingHelper.LogLine("Loaded records - " + res.ToString() + " study_titles, new studies");

            // For 'existing studies' study Ids add only new titles.

            sql_string = @"DROP TABLE IF EXISTS nk.source_data;
                           CREATE TABLE nk.source_data as 
                           SELECT es.study_id, d.* 
                           FROM " + schema_name + @".study_titles d
                           INNER JOIN nk.existing_studies es
                           ON d.sd_sid = es.sd_sid";
            db.ExecuteSQL(sql_string);

            // Also, all non preferred titles must be non-default (default will be from the preferred source)

            sql_string = @"UPDATE nk.source_data 
                           SET is_default = false;";
            db.ExecuteSQL(sql_string);


            sql_string = @"DROP TABLE IF EXISTS nk.existing_data;
                           CREATE TABLE nk.existing_data as 
                           SELECT es.sd_sid, es.study_id, 
                           c.title_text 
                           FROM st.study_titles c
                           INNER JOIN nk.existing_studies es
                           ON c.study_id = es.study_id;";
            db.ExecuteSQL(sql_string);

            // for titles which are the same as some that already exist
            // the comments field should be updated to reflect this...

            sql_string = @"UPDATE st.study_titles t
                           set comments = t.comments || '; ' || s.comments 
                           FROM nk.source_data s
                           WHERE t.study_id = s.study_id
                           AND lower(t.title_text) = lower(s.title_text);";
            db.ExecuteSQL(sql_string);

            // for titles which are new to the study
            // simply add them

            sql_string = @"INSERT INTO st.study_titles(" + destination_field_list + @")
                           SELECT s.study_id, " + source_field_list + @" 
                           FROM nk.source_data s
                           LEFT JOIN nk.existing_data e
                           ON s.sd_sid = e.sd_sid
                           AND lower(s.title_text) = lower(e.title_text)
                           WHERE e.study_id is null ";

            res = db.ExecuteTransferSQL(sql_string, schema_name, "study_titles", "existing studies");
            _loggingHelper.LogLine("Loaded records - " + res.ToString() + " study_titles, existing studies");



            db.ExecuteSQL("DROP TABLE IF EXISTS nk.source_data;");
            db.ExecuteSQL("DROP TABLE IF EXISTS nk.existing_data;");

            db.Update_SourceTable_ExportDate(schema_name, "study_titles");
        }


        public void LoadStudyContributors(string schema_name)
        {
            string destination_field_list = @"study_id, 
            contrib_type_id, is_individual, 
            person_id, person_given_name, person_family_name, person_full_name,
            orcid_id, person_affiliation, organisation_id, 
            organisation_name, organisation_ror_id ";
            
            string source_field_list = @" 
            s.contrib_type_id, s.is_individual, 
            s.person_id, s.person_given_name, s.person_family_name, s.person_full_name,
            s.orcid_id, s.person_affiliation, s.organisation_id, 
            s.organisation_name, s.organisation_ror_id ";

            // For 'preferred' study Ids add all contributors.

            string sql_string = @"INSERT INTO st.study_contributors(" + destination_field_list + @")
                    SELECT k.study_id, " + source_field_list + @"
                    FROM nk.temp_study_ids k
                    INNER JOIN " + schema_name + @".study_contributors s
                    on k.sd_sid = s.sd_sid
                    WHERE k.is_preferred = true ";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "study_contributors", "new studies");
            _loggingHelper.LogLine("Loaded records - " + res.ToString() + " study_contributors, new studies");

            // For 'existing studies' study Ids add only new contributors.
            // Need to do it in two sets to simplify the SQL (to try and avoid time outs)

            sql_string = @"DROP TABLE IF EXISTS nk.source_data;
                           CREATE TABLE nk.source_data as 
                           SELECT es.study_id, d.* 
                           FROM " + schema_name + @".study_contributors d
                           INNER JOIN nk.existing_studies es
                           ON d.sd_sid = es.sd_sid
                           WHERE d.is_individual = false";
            db.ExecuteSQL(sql_string);

            sql_string = @"DROP TABLE IF EXISTS nk.existing_data;
                           CREATE TABLE nk.existing_data as 
                           SELECT es.sd_sid, es.study_id, 
                           c.contrib_type_id, c.organisation_name 
                           FROM st.study_contributors c
                           INNER JOIN nk.existing_studies es
                           ON c.study_id = es.study_id
                           WHERE c.is_individual = false";
            db.ExecuteSQL(sql_string);

            sql_string = @"INSERT INTO st.study_contributors(" + destination_field_list + @")
                           SELECT s.study_id, " + source_field_list + @" 
                           FROM nk.source_data s
                           LEFT JOIN nk.existing_data e
                           ON s.sd_sid = e.sd_sid
                           AND s.contrib_type_id = e.contrib_type_id
                           AND s.organisation_name = e.organisation_name
                           WHERE e.study_id is null ";

            res = db.ExecuteTransferSQL(sql_string, schema_name, "study_contributors", "existing studies");
            _loggingHelper.LogLine("Loaded records - " + res.ToString() + " study_contributors (orgs), existing studies");

            db.ExecuteSQL("DROP TABLE IF EXISTS nk.source_data;");
            db.ExecuteSQL("DROP TABLE IF EXISTS nk.existing_data;");

            sql_string = @"DROP TABLE IF EXISTS nk.source_data;
                           CREATE TABLE nk.source_data as 
                           SELECT es.study_id, d.* FROM " + schema_name + @".study_contributors d
                           INNER JOIN nk.existing_studies es
                           ON d.sd_sid = es.sd_sid
                           WHERE d.is_individual = true";
            db.ExecuteSQL(sql_string);

            sql_string = @"DROP TABLE IF EXISTS nk.existing_data;
                           CREATE TABLE nk.existing_data as 
                           SELECT es.sd_sid, es.study_id, 
                           c.contrib_type_id, c.person_full_name 
                           FROM st.study_contributors c
                           INNER JOIN nk.existing_studies es
                           ON c.study_id = es.study_id
                           WHERE c.is_individual = true";
            db.ExecuteSQL(sql_string);

            sql_string = @"INSERT INTO st.study_contributors(" + destination_field_list + @")
                           SELECT s.study_id, " + source_field_list + @" 
                           FROM nk.source_data s
                           LEFT JOIN nk.existing_data e
                           ON s.sd_sid = e.sd_sid
                           AND s.contrib_type_id = e.contrib_type_id
                           AND s.person_full_name = e.person_full_name
                           WHERE e.study_id is null ";

            res = db.ExecuteTransferSQL(sql_string, schema_name, "study_contributors", "existing studies");
            _loggingHelper.LogLine("Loaded records - " + res.ToString() + " study_contributors (people), existing studies");

            db.ExecuteSQL("DROP TABLE IF EXISTS nk.source_data;");
            db.ExecuteSQL("DROP TABLE IF EXISTS nk.existing_data;");

            db.Update_SourceTable_ExportDate(schema_name, "study_contributors");
        }


        public void LoadStudyTopics(string schema_name)
        {
            string destination_field_list = @"study_id, 
                    topic_type_id, mesh_coded, mesh_code, mesh_value, 
                    original_ct_id, original_ct_code, original_value ";

            string source_field_list = @" 
                    s.topic_type_id, s.mesh_coded, s.mesh_code, s.mesh_value, 
                    original_ct_id, original_ct_code, s.original_value ";

            // For 'preferred' study Ids add all topics.

            string sql_string = @"INSERT INTO st.study_topics(" + destination_field_list + @")
                    SELECT k.study_id, " + source_field_list + @"
                    FROM nk.temp_study_ids k
                    INNER JOIN " + schema_name + @".study_topics s
                    on k.sd_sid = s.sd_sid
                    WHERE k.is_preferred = true ";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "study_topics", "new studies");
            _loggingHelper.LogLine("Loaded records - " + res.ToString() + " mesh coded study_topics, new studies");

            // For 'existing studies' study Ids add only new topics.
            // Do this in two stages - for mesh coded data
            // and then for non-mesh coded data#

            // create existing data once...

            sql_string = @"DROP TABLE IF EXISTS nk.existing_data;
                           CREATE TABLE nk.existing_data as 
                           SELECT es.sd_sid, es.study_id, 
                           c.mesh_value, c.original_value
                           FROM st.study_topics c
                           INNER JOIN nk.existing_studies es
                           ON c.study_id = es.study_id;";
            db.ExecuteSQL(sql_string);

            // look at mesh coded new data

            sql_string = @"DROP TABLE IF EXISTS nk.source_data;
                           CREATE TABLE nk.source_data as 
                           SELECT es.study_id, d.* 
                           FROM " + schema_name + @".study_topics d
                           INNER JOIN nk.existing_studies es
                           ON d.sd_sid = es.sd_sid
                           WHERE mesh_coded = true";
            db.ExecuteSQL(sql_string);


            sql_string = @"INSERT INTO st.study_topics(" + destination_field_list + @")
                           SELECT s.study_id, " + source_field_list + @" 
                           FROM nk.source_data s
                           LEFT JOIN nk.existing_data e
                           ON s.sd_sid = e.sd_sid
                           AND s.mesh_value = e.mesh_value
                           WHERE e.study_id is null ";

            res = db.ExecuteTransferSQL(sql_string, schema_name, "study_topics", "existing studies");
            _loggingHelper.LogLine("Loaded records - " + res.ToString() + " non mesh coded study_topics, existing studies");

            // look at non mesh coded new data

            sql_string = @"DROP TABLE IF EXISTS nk.source_data;
                           CREATE TABLE nk.source_data as 
                           SELECT es.study_id, d.* 
                           FROM " + schema_name + @".study_topics d
                           INNER JOIN nk.existing_studies es
                           ON d.sd_sid = es.sd_sid
                           WHERE mesh_coded = false";
            db.ExecuteSQL(sql_string);


            sql_string = @"INSERT INTO st.study_topics(" + destination_field_list + @")
                           SELECT s.study_id, " + source_field_list + @" 
                           FROM nk.source_data s
                           LEFT JOIN nk.existing_data e
                           ON s.sd_sid = e.sd_sid
                           AND lower(s.original_value) = lower(e.original_value)
                           WHERE e.study_id is null ";

            db.ExecuteSQL("DROP TABLE IF EXISTS nk.source_data;");
            db.ExecuteSQL("DROP TABLE IF EXISTS nk.existing_data;");

            db.Update_SourceTable_ExportDate(schema_name, "study_topics");
        }


        public void LoadStudyFeatures(string schema_name)
        {
            // For 'preferred' study Ids add all features.
            string destination_field_list = @"study_id, 
                    feature_type_id, feature_value_id ";

            string source_field_list = @" 
                    s.feature_type_id, s.feature_value_id ";

            string sql_string = @"INSERT INTO st.study_features(" + destination_field_list + @")
                    SELECT k.study_id, " + source_field_list + @"
                    FROM nk.temp_study_ids k
                    INNER JOIN " + schema_name + @".study_features s
                    on k.sd_sid = s.sd_sid
                    WHERE k.is_preferred = true ";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "study_features", "new studies");
            _loggingHelper.LogLine("Loaded records - " + res.ToString() + " study_features, new studies");

            // For 'existing studies' study Ids add only new feature types.

            sql_string = @"DROP TABLE IF EXISTS nk.source_data;
                           CREATE TABLE nk.source_data as 
                           SELECT es.study_id, d.* 
                           FROM " + schema_name + @".study_features d
                           INNER JOIN nk.existing_studies es
                           ON d.sd_sid = es.sd_sid";
            db.ExecuteSQL(sql_string);

            sql_string = @"DROP TABLE IF EXISTS nk.existing_data;
                           CREATE TABLE nk.existing_data as 
                           SELECT es.sd_sid, es.study_id, 
                           c.feature_type_id
                           FROM st.study_features c
                           INNER JOIN nk.existing_studies es
                           ON c.study_id = es.study_id;";
            db.ExecuteSQL(sql_string);

            sql_string = @"INSERT INTO st.study_features(" + destination_field_list + @")
                           SELECT s.study_id, " + source_field_list + @" 
                           FROM nk.source_data s
                           LEFT JOIN nk.existing_data e
                           ON s.sd_sid = e.sd_sid
                           AND s.feature_type_id = e.feature_type_id
                           WHERE e.study_id is null ";

            res = db.ExecuteTransferSQL(sql_string, schema_name, "study_features", "existing studies");
            _loggingHelper.LogLine("Loaded records - " + res.ToString() + " study_features, existing studies");

            db.Update_SourceTable_ExportDate(schema_name, "study_features");
        }


        public void LoadStudyRelationShips(string schema_name)
        {
            string destination_field_list = @"study_id, 
                    relationship_type_id ";

            string source_field_list = @" 
                    s.relationship_type_id ";

            // For 'preferred' study Ids add all relationships.
            
            string sql_string = @"INSERT INTO st.study_relationships(" + destination_field_list + @")
                    SELECT k.study_id, " + source_field_list + @"
                    FROM nk.temp_study_ids k
                    INNER JOIN " + schema_name + @".study_relationships s
                    on k.sd_sid = s.sd_sid
                    WHERE k.is_preferred = true ";
            
            int res = db.ExecuteTransferSQL(sql_string, schema_name, "study_relationships", "new studies");
            _loggingHelper.LogLine("Loaded records - " + res.ToString() + " study_relationships, new studies");

            // For 'existing studies' study Ids add only new relationships types.
            sql_string = @"DROP TABLE IF EXISTS nk.source_data;
                           CREATE TABLE nk.source_data as 
                           SELECT es.study_id, d.* 
                           FROM " + schema_name + @".study_relationships d
                           INNER JOIN nk.existing_studies es
                           ON d.sd_sid = es.sd_sid";
            db.ExecuteSQL(sql_string);

            sql_string = @"DROP TABLE IF EXISTS nk.existing_data;
                           CREATE TABLE nk.existing_data as 
                           SELECT es.sd_sid, es.study_id, 
                           c.relationship_type_id
                           FROM st.study_relationships c
                           INNER JOIN nk.existing_studies es
                           ON c.study_id = es.study_id;";
            db.ExecuteSQL(sql_string);

            sql_string = @"INSERT INTO st.study_relationships(" + destination_field_list + @")
                           SELECT s.study_id, " + source_field_list + @" 
                           FROM nk.source_data s
                           LEFT JOIN nk.existing_data e
                           ON s.sd_sid = e.sd_sid
                           AND s.relationship_type_id = e.relationship_type_id
                           WHERE e.study_id is null ";

            res = db.ExecuteTransferSQL(sql_string, schema_name, "study_relationships", "existing studies");
            _loggingHelper.LogLine("Loaded records - " + res.ToString() + " study_relationships, existing studies");
            
            // insert target study id, using sd_sid to find it in the temp studies table
            // N.B. These relationships are defined within the same source...
            // (Cross source relationships are defined in the links (nk) schema)

            sql_string = @"UPDATE st.study_relationships r
                    SET target_study_id = tt.target_study_id
                    FROM 
                        (SELECT t.study_id, t2.study_id as target_study_id
                            FROM nk.temp_study_ids t 
                            INNER JOIN " + schema_name + @".study_relationships s 
                            on t.sd_sid = s.sd_sid
                            INNER JOIN nk.temp_study_ids t2
                            on s.target_sd_sid = t2.sd_sid) tt
                    WHERE r.study_id = tt.study_id ";

            res = db.ExecuteTransferSQL(sql_string, schema_name, "study_relationships", "updating target ids");
            _loggingHelper.LogLine("Loaded records - " + res.ToString() + " study_relationships, updating target ids");
            
            db.Update_SourceTable_ExportDate(schema_name, "study_relationships");
        }


        public void DropTempStudyIdsTable()
        {
            string sql_string = @"DROP TABLE IF EXISTS nk.temp_study_ids;
                                  DROP TABLE IF EXISTS nk.existing_studies;";
            db.ExecuteSQL(sql_string);
        }

    }
}
