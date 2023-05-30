namespace MDR_Aggregator;

public class SearchHelperStudies
{
    private readonly DBUtilities db;
    readonly int min_studies_id, max_studies_id;

    public SearchHelperStudies(string connString, ILoggingHelper loggingHelper)
    {
        db = new DBUtilities(connString, loggingHelper);
        min_studies_id = db.GetAggMinId("core.studies");
        max_studies_id = db.GetAggMaxId("core.studies");
    }
    
    public int GenerateStudySearchData()
    {
        string top_sql = @"INSERT INTO core.search_studies(study_id, study_name, description, dss, 
                              start_year, start_month, type_id, status_id, gender_elig_id, 
                              min_age, min_age_units_id, max_age, max_age_units_id,provenance)
                              select id, display_title, brief_description, data_sharing_statement,
                              study_start_year, study_start_month, study_type_id, study_status_id,
                              study_gender_elig_id, min_age::varchar, min_age_units_id, 
                              max_age::varchar, max_age_units_id, provenance_string
                              from core.studies s ";
        string bottom_sql = " order by s.id";
        return db.SearchTableTransfer(top_sql, bottom_sql, "id", min_studies_id, max_studies_id,
                               "search_studies", 50000);
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
               where ss.study_id = f.study_id";

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
                                       202, 203, 294, 210)", 1, "article");
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
        return db.CollectHasObjectData(@"b.object_type_id in (20, 31, 32, 81, 82, 
                                         69, 70, 71, 72, 73, 154)", 9, "Data Summary");
    }

    public int UpdateStudySearchDataIfHasIPD()
    {
        return db.CollectHasObjectData(@"b.object_type_id in (80, 153) or 
                                       b.object_type_id between 51 and 68", 10, "IPD");
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

    public void CreateBitMapData()
    {
        // Data is in table int id, int study_id, int bit_pos

        for (int n = 0; n < 16; n++)
        {
            string sql_string = $@"Update core.search_studies ss 
                                set has_objects = set_bit(has_objects, {n}, 1)
                                from core.temp_searchobjects b 
                                where ss.study_id = b.study_id
                                and b.bit_pos = {n} ";
            db.UpdateBitMap(sql_string, n, min_studies_id, max_studies_id);
        }
    }
    
}
