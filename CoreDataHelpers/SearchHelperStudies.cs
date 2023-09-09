namespace MDR_Aggregator;

public class SearchHelperStudies
{
    private readonly DBUtilities db;
    
    readonly int min_studies_id, max_studies_id;
    private readonly ILoggingHelper _loggingHelper;
    
    public SearchHelperStudies(string connString, ILoggingHelper loggingHelper)
    {
        db = new DBUtilities(connString, loggingHelper);
        _loggingHelper = loggingHelper;
        min_studies_id = db.GetAggMinId("core.studies");
        max_studies_id = db.GetAggMaxId("core.studies");
    }
    
    public void CreateTableSearchStudies()
    {
        string sql_string = @"drop table if exists core.search_studies;
        create table core.search_studies
        (
            study_id                int primary key   not null
            , study_name            varchar           null
            , description           varchar           null
            , dss                   varchar           null
            , start_year            int               null
            , start_month           int               null
            , type_id               int               null
            , type_name             varchar           null
            , status_id             int               null
            , status_name           varchar           null
            , gender_elig_id        int               null
            , gender_elig           varchar           null
            , min_age               varchar           null
            , min_age_units_id      int               null
            , max_age               varchar           null
            , max_age_units_id      int               null
            , phase_id              int               null
            , phase_name            varchar           null
            , alloc_id              int               null
            , alloc_name            varchar           null
            , purpose_name          varchar           null
            , interv_name           varchar           null
            , masking_name          varchar           null
            , obsmodel_name         varchar           null
            , timepersp_name        varchar           null
            , biospec_name          varchar           null
            , feature_list          varchar           null
            , has_objects           bit(17)           default '00000000000000000'
            , country_list          varchar           null
            , condition_list        varchar           null
            , provenance            varchar           null
            , object_json           json              null
            , study_json            json              null
        );
        create index ss_start_year on core.search_studies(start_year);
        create index ss_type on core.search_studies(type_id);
        create index ss_status on core.search_studies(status_id);
        create index ss_phase_id on core.search_studies(phase_id);
        create index ss_alloc_id on core.search_studies(alloc_id);";

        db.ExecuteSQL(sql_string);
    }
    
    public int GenerateStudySearchData()
    {
        string top_sql = @"INSERT INTO core.search_studies(study_id, study_name, description, dss, 
                              start_year, start_month, type_id, status_id, gender_elig_id, 
                              min_age, min_age_units_id, max_age, max_age_units_id,provenance)
                              select id, display_title, brief_description, data_sharing_statement,
                              study_start_year, study_start_month, coalesce(study_type_id, 0), 
                              coalesce(study_status_id, 0), coalesce(study_gender_elig_id, 915), 
                              min_age::varchar, min_age_units_id, 
                              max_age::varchar, max_age_units_id, provenance_string
                              from core.studies s ";
        string bottom_sql = " order by s.id";
        return db.SearchTableTransfer(top_sql, bottom_sql, "id", min_studies_id, max_studies_id,
                               "search_studies", 20000);
    }

    public int UpdateStudyTypeData()
    {
        string sql_string = @"Update core.search_studies ss
                              set type_name = lup.name
                              from context_lup.study_types lup
                              where ss.type_id = lup.id
                              and ss.type_id is not null ";
        return db.UpdateSearchStudyData(sql_string, "search_studies type", min_studies_id, max_studies_id);
    }
    
    public int UpdateStudyStatusData()
    {   
        string sql_string = @"Update core.search_studies ss
                              set status_name = lup.name
                              from context_lup.study_statuses lup
                              where ss.status_id = lup.id
                              and ss.status_id is not null ";
        return db.UpdateSearchStudyData(sql_string, "search_studies status", min_studies_id, max_studies_id);
    }

    public int UpdateStudyGenderEligibilityData()
    {
        string sql_string = @"Update core.search_studies ss
                              set gender_elig = lup.name
                              from context_lup.gender_eligibility_types lup
                              where ss.gender_elig_id = lup.id
                              and ss.gender_elig_id is not null ";
        return db.UpdateSearchStudyData(sql_string, "search_studies gender elig", min_studies_id, max_studies_id);
    }
    /*
    public int UpdateStudyMinAgeData()
    {   
        string sql_string = @"Update core.search_studies ss
                              set min_age = min_age||' '||lup.name 
                              from context_lup.time_units lup
                              where ss.min_age_units_id = lup.id
                              and ss.min_age_units_id is not null
                              and ss.min_age_units_id <> 17 ";
        return db.UpdateSearchStudyData(sql_string, "search_studies min age", min_studies_id, max_studies_id);
    }
    
    public int UpdateStudyMaxAgeData()
    {   
        string sql_string = @"Update core.search_studies ss
                              set max_age = max_age||' '||lup.name 
                              from context_lup.time_units lup
                              where ss.max_age_units_id = lup.id
                              and ss.max_age_units_id is not null
                              and ss.max_age_units_id <> 17 ";
        return db.UpdateSearchStudyData(sql_string, "search_studies max age", min_studies_id, max_studies_id);
    }

    public int UpdateStudySearchDataWithPhaseIdData()
    {
        string sql_string = @"UPDATE core.search_studies ss
               set phase_id = f.feature_value_id,
                   phase_name = f.name
               from 
                   (select sf.study_id, sf.feature_value_id,
                    lup.name from core.study_features sf
                    inner join context_lup.study_feature_categories lup
                    on sf.feature_value_id = lup.id
                    where sf.feature_type_id = 20) f
               where ss.study_id = f.study_id ";

        return db.UpdateSearchFeatureData(sql_string, "phase", min_studies_id, max_studies_id);
    }
    
    public int UpdateStudySearchDataWithAllocIdData()
    {
        string sql_string = @"UPDATE core.search_studies ss
               set alloc_id = f.feature_value_id,
                   alloc_name = f.name
               from 
                   (select sf.study_id, sf.feature_value_id,
                    lup.name from core.study_features sf
                    inner join context_lup.study_feature_categories lup
                    on sf.feature_value_id = lup.id
                    where sf.feature_type_id = 22) f
               where ss.study_id = f.study_id ";
        
        return db.UpdateSearchFeatureData(sql_string, "allocation", min_studies_id, max_studies_id);
    }

    public int UpdateStudySearchDataWithPurposeData()
    {
        string sql_string = @"UPDATE core.search_studies ss
               set purpose_name = f.name
               from 
                   (select sf.study_id, sf.feature_value_id,
                    lup.name from core.study_features sf
                    inner join context_lup.study_feature_categories lup
                    on sf.feature_value_id = lup.id
                    where sf.feature_type_id = 21) f
               where ss.study_id = f.study_id ";
        return db.UpdateSearchFeatureData(sql_string, "study purpose", min_studies_id, max_studies_id);
    }

    public int UpdateStudySearchDataWithInterventionData()
    {
        string sql_string = @"UPDATE core.search_studies ss
               set interv_name = f.name
               from 
                   (select sf.study_id, sf.feature_value_id,
                    lup.name from core.study_features sf
                    inner join context_lup.study_feature_categories lup
                    on sf.feature_value_id = lup.id
                    where sf.feature_type_id = 23) f
               where ss.study_id = f.study_id ";

        return db.UpdateSearchFeatureData(sql_string, "intervention", min_studies_id, max_studies_id);
    }

    public int UpdateStudySearchDataWithMaskingData()
    {
        string sql_string = @"UPDATE core.search_studies ss
               set masking_name = f.name
               from 
                   (select sf.study_id, sf.feature_value_id,
                    lup.name from core.study_features sf
                    inner join context_lup.study_feature_categories lup
                    on sf.feature_value_id = lup.id
                    where sf.feature_type_id = 24) f
               where ss.study_id = f.study_id ";

        return db.UpdateSearchFeatureData(sql_string, "masking", min_studies_id, max_studies_id);
    }

    public int UpdateStudySearchDataWithObsModelData()
    {
        string sql_string = @"UPDATE core.search_studies ss
               set obsmodel_name = f.name
               from 
                   (select sf.study_id, sf.feature_value_id,
                    lup.name from core.study_features sf
                    inner join context_lup.study_feature_categories lup
                    on sf.feature_value_id = lup.id
                    where sf.feature_type_id = 30) f
               where ss.study_id = f.study_id ";

        return db.UpdateSearchFeatureData(sql_string, "observation model", min_studies_id, max_studies_id);
    }

    public int UpdateStudySearchDataWithTimePerspData()
    {
        string sql_string = @"UPDATE core.search_studies ss
               set timepersp_name = f.name
               from 
                   (select sf.study_id, sf.feature_value_id,
                    lup.name from core.study_features sf
                    inner join context_lup.study_feature_categories lup
                    on sf.feature_value_id = lup.id
                    where sf.feature_type_id = 31) f
               where ss.study_id = f.study_id ";

        return db.UpdateSearchFeatureData(sql_string, "time perspective", min_studies_id, max_studies_id);
    }

    public int UpdateStudySearchDataWithBioSpecData()
    {
        string sql_string = @"UPDATE core.search_studies ss
               set biospec_name = f.name
               from 
                   (select sf.study_id, sf.feature_value_id,
                    lup.name from core.study_features sf
                    inner join context_lup.study_feature_categories lup
                    on sf.feature_value_id = lup.id
                    where sf.feature_type_id = 32) f
               where ss.study_id = f.study_id ";

        return db.UpdateSearchFeatureData(sql_string, "bio-specimen", min_studies_id, max_studies_id);
    }
*/
    public int UpdateStudySearchDataWithCountryData()
    {
        string top_sql = @"update core.search_studies ss
        set country_list = c_list
        from 
        (
            select study_id, 
            string_agg(country_name, ', ') as c_list from core.study_countries sc ";
        
        string bottom_sql = @"group by study_id) c
                  where ss.study_id = c.study_id;";
            
        return db.UpdateListData(top_sql, bottom_sql, min_studies_id, max_studies_id, "study country");
    }
    
    public int UpdateStudySearchDataWithConditionData()
    {
        string top_sql = @"update core.search_studies ss
        set condition_list = c_list
        from 
        (
            select study_id, 
            string_agg(original_value, ', ') as c_list from core.study_conditions sc ";
        
        string bottom_sql = @"group by study_id) c
                  where ss.study_id = c.study_id;";
            
        return db.UpdateListData(top_sql, bottom_sql, min_studies_id, max_studies_id, "study condition");
    }
    
    public void CreateStudySearchObjectDataTable()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.temp_searchobjects;
            CREATE TABLE core.temp_searchobjects
            ( id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
			 , study_id INT, bit_pos INT); ";
        db.ExecuteSQL(sql_string);
    }

    public void DropStudySearchObjectDataTable()
    {
        string sql_string = @"DROP TABLE IF EXISTS core.temp_searchobjects; ";
        db.ExecuteSQL(sql_string);
    }

    public int UpdateStudySearchDataIfHasRegResults()
    {
        return db.CollectHasObjectData("b.object_type_id in (28)", 0, "registry results");
    }

    public int UpdateStudySearchDataIfHasArticle()
    {
        return db.CollectHasObjectData(@"b.object_type_id in (12, 100, 117, 135, 152, 201, 
                                       202, 203, 204, 210)", 1, "article");
    }

    public int UpdateStudySearchDataIfHasProtocol()
    {
        return db.CollectHasObjectData("b.object_type_id in (11, 42, 74, 75, 76, 201)", 2, "protocol");
    }
    
    public int UpdateStudySearchDataIfHasOverview()
    {
       return db.CollectHasObjectData("b.object_type_id in (38)", 3, "study overview");
    }
    
    public int UpdateStudySearchDataIfHasPIF()
    {
        return db.CollectHasObjectData("b.object_type_id in (18, 19, 75, 76)", 4, "PIC/IS");
    }
    
    public int UpdateStudySearchDataIfHasECRFs()
    {
        return db.CollectHasObjectData("b.object_type_id in (21, 30, 40)", 5, "CRFs");
    }

    public int UpdateStudySearchDataIfHasManual()
    {
        return db.CollectHasObjectData("b.object_type_id in (35, 36)", 6, "Procedures");
    }

    public int UpdateStudySearchDataIfHasSAP()
    {
        return db.CollectHasObjectData("b.object_type_id in (22, 29, 43, 74, 76)", 7, "SAP");
    }
  
    public int UpdateStudySearchDataIfHasCSR()
    {
        return db.CollectHasObjectData("b.object_type_id in (26, 27, 79, 85)", 8, "CSR");
    }

    public int UpdateStudySearchDataIfHasDataDesc()
    {
        return db.CollectHasObjectData(@"b.object_type_id in (20, 31, 32, 81, 82, 73)", 9, "Data Description");
    }

    public int UpdateStudySearchDataIfHasIPD()
    {
        return db.CollectHasObjectData(@"b.object_type_id in (80, 153, 154) or 
                                       b.object_type_id between 51 and 72", 10, "IPD");
    }
   
    public int UpdateStudySearchDataIfHasOthRes()
    {
        return db.CollectHasObjectData(@"b.object_type_id in (14, 15, 16, 17, 23, 24, 
                                        25, 33, 34, 39, 77, 78, 83, 84, 86, 115, 171)", 11, "Other resource");
    }
          
    public int UpdateStudySearchDataIfHasOthInfo()
    {
        return db.CollectHasObjectData(@"b.object_type_id in (88, 106, 107, 108, 109, 119, 121, 
                                         122, 101, 102, 103, 104, 105, 113, 112, 120, 123, 
                                         126, 127, 128)", 12, "Other info");
    }
    
    public int UpdateStudySearchDataIfHasWebSite()
    {
        return db.CollectHasObjectData("b.object_type_id in (134)", 13, "Website");
    }

    public int UpdateStudySearchDataIfHasSoftware()
    {
        return db.CollectHasObjectData("b.object_type_id in (166, 167, 168, 169, 170)", 14, "Software");
    }
    
    public int UpdateStudySearchDataIfHasOther()
    {
        return db.CollectHasObjectData(@"b.object_type_id in (37, 151, 110, 111, 114,  
                                       118, 124, 125, 129, 130, 131, 132, 133) 
                                       or b.object_type_id between 155 and 165",
                                       15, "Other");
    }
    
    public int UpdateStudySearchDataIfHasSamples()
    {
        return db.CollectHasObjectData("b.object_type_id in (301)", 16, "Samples");
    }

    public void CreateBitMapData()
    {
        // Data is in table int id, int study_id, int bit_pos

        for (int n = 0; n < 17; n++)
        {
            string sql_string = $@"Update core.search_studies ss 
                                set has_objects = set_bit(has_objects, {n}, 1)
                                from core.temp_searchobjects b 
                                where ss.study_id = b.study_id
                                and b.bit_pos = {n} ";
            int res = db.UpdateBitMap(sql_string, n, min_studies_id, max_studies_id);
            string feedback = $"Updated {res} bit map fields, n = {n}";
            _loggingHelper.LogLine(feedback);
        }
    }
    
    public int CreateFeatureString()
    {
        string sql_string = $@"Update core.search_studies ss 
                               set feature_list = 'Randomised: '
                   || case 
                      when alloc_name is null then 'Not provided'
					  when alloc_name = 'Randomised' then 'Yes'
					  when alloc_name = 'Nonrandomised' then 'No'
					  when alloc_name = 'Not applicable' then 'No'
                      else 'Unclear'
                   end 
                  || case 
                      when phase_name is null then ''
                      when phase_name ='Not applicable' then ''
                      else ';  Phase: '|| phase_name
                   end 
                  
                 || case 
                      when purpose_name is null then ''
                      when purpose_name ='Other' then ''
                      else ';  Focus: '|| purpose_name
                   end 
                 || case 
                      when interv_name is null then ''
                      when interv_name ='Other' then ''
                      else ';  Intervention design: '|| interv_name
                   end 
                 || case 
                      when ss.masking_name is null then ''
                      when ss.masking_name ='Not applicable' then ''
                      else '; Masking: '|| masking_name
                  end 
                  where type_id = 11 ";
        
        db.UpdateStudyFeatureList(sql_string, min_studies_id, max_studies_id, "intervention feature");
        
        sql_string = $@"Update core.search_studies ss 
                      set feature_list = 'Time Perspective:  '|| case 
			          when timepersp_name is null then ''
			          when timepersp_name ='Other' then 'Not specified'
			          else timepersp_name
		           end 
		           || case 
			          when obsmodel_name is null then ''
			          when obsmodel_name ='Other' then ''
			          else '; Observation model: '|| obsmodel_name
		           end 
		           || case 
			          when biospec_name is null then ''
			          when biospec_name ='other' then ''
			          else '; '||trim(biospec_name)||' reported as available; 
		           end 
            where type_id = 12 ";
        
        return db.UpdateStudyFeatureList(sql_string, min_studies_id, max_studies_id, "observation feature");
    }

    public int ObtainObjectJsonData()
    {
        return db.UpdateSearchStudyObjectJson(min_studies_id, max_studies_id, "study object json" );
    }
    
    public int CreateStudyJsonData()
    {
        string sql_string = @"Update core.search_studies ss 
             set study_json =  json_build_object ('study_id', study_id, 'study_name', study_name, 
		     'description', description, 'dss', dss, 'start_year', start_year, 'start_month', start_month,
             'type_id', type_id, 'type_name', type_name, 'status_id', status_id, 'status_name', status_name, 
             'gender_elig', gender_elig, 'min_age', min_age, 'max_age', max_age, 'phase_id', phase_id,
             'alloc_id', alloc_id, 'feature_list', feature_list, 'has_objects', has_objects, 'country_list', 
              country_list, 'condition_list', condition_list, 'provenance', provenance, 'objects', object_json) ";
           
        return db.UpdateStudyJson(sql_string, min_studies_id, max_studies_id, "study json");
    }
    
    public int UpdateIdentsSearchWithStudyJson()
    {
        string sql_string = @"update core.search_idents s
                              set study_json = ss.study_json
                              from core.search_studies ss
                              where s.study_id = ss.study_id ";
        return db.TransferStudyJson(sql_string, min_studies_id, max_studies_id, "idents study json");
    }
    
    public int UpdatePMIDsSearchWithStudyJson()
    {
        string sql_string = @"update core.search_pmids s
                              set study_json = ss.study_json
                              from core.search_studies ss
                              where s.study_id = ss.study_id ";
        return db.TransferStudyJson(sql_string, min_studies_id, max_studies_id, "pmids study json");
    }
    
    public int UpdateLexemesSearchWithStudyJson()
    {
        string sql_string = @"update core.search_lexemes s
                              set study_json = ss.study_json
                              from core.search_studies ss
                              where s.study_id = ss.study_id ";
        return db.TransferStudyJson(sql_string, min_studies_id, max_studies_id, "lexemes study json");
    }
    
}
