namespace MDR_Aggregator;

internal class BBMRITransferHelper
{
    private readonly string _schema_name;
    private readonly DBUtilities db;
    private readonly ILoggingHelper _loggingHelper;

    public BBMRITransferHelper(string schema_name, string connString, ILoggingHelper loggingHelper)
    {
        _schema_name = schema_name;
        _loggingHelper = loggingHelper;
        db = new DBUtilities(connString, _loggingHelper);
    }
    
    public void MatchExistingBBMRILinks()
    {
        // Establish a temporary 'working' table and fill it with key details of the sample
        // collection data objects. Load the data and augment using the study_ids table.
        
        string sql_string = @"DROP TABLE IF EXISTS nk.temp_bbmris;
                  CREATE TABLE IF NOT EXISTS nk.temp_bbmris(
                    id                       INT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY
                  , object_id                INT
                  , source_id                INT
                  , sd_oid                   VARCHAR
                  , object_type_id		     INT            
                  , title                    VARCHAR      
                  , is_preferred_object      BOOLEAN
                  , parent_study_source_id   INT 
                  , parent_study_sd_sid      VARCHAR
                  , parent_study_id          INT
                  , is_preferred_study       BOOLEAN
                  , datetime_of_data_fetch   TIMESTAMPTZ
                  , match_status             INT   default 0
                  );
             CREATE INDEX dist_temp_object_ids_objectid ON nk.temp_bbmris(object_id);
             CREATE INDEX dist_temp_object_ids_sdidsource ON nk.temp_bbmris(source_id, sd_oid);
             CREATE INDEX dist_temp_object_ids_parent_study_sdidsource 
                             ON nk.temp_bbmris(parent_study_source_id, parent_study_sd_sid);";
                  
        db.ExecuteSQL(sql_string);
        
        sql_string = $@"Insert into nk.temp_bbmris(source_id, sd_oid, parent_study_sd_sid,
                        object_type_id, title, datetime_of_data_fetch)
                        select 110426, a.sd_oid, a.sd_sid, 301, a.title, a.datetime_of_data_fetch
                        from {_schema_name}.data_objects a ";
        db.ExecuteSQL(sql_string);
        
        // The source id can at the moment be found from the sd_sid as these are unique across registries.
        // If this were to change a more complex method would need to be employed, or it would need to be 
        // discovered and derived from earlier in the extraction process, as an additional table.

        sql_string =  @"Update nk.temp_bbmris b
                       set parent_study_source_id = ids.source_id,
                       parent_study_id = ids.study_id,
                       is_preferred_study = ids.is_preferred
                       from nk.study_ids ids
                       where b.parent_study_sd_sid = ids.sd_sid ";
        db.ExecuteSQL(sql_string);
        
        // Identify the matched records in the temp table. Matching is against sd_oid and study.
        
        sql_string = @"UPDATE nk.temp_bbmris b
        set match_status = 1
        from nk.data_object_ids doi
        where b.parent_study_id = doi.parent_study_id
        and b.sd_oid = doi.sd_oid ";

        int res = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{res} existing objects matched in temp table");

        // Update the matched records in the data object identifier table
        // with the updated date time of data fetch

        sql_string = @"UPDATE nk.data_object_ids doi
        set match_status = 1,
        datetime_of_data_fetch = b.datetime_of_data_fetch
        from nk.temp_bbmris b
        where doi.parent_study_id = b.parent_study_id
        and doi.sd_oid = b.sd_oid ";
        res = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{res} existing BBMRI sample objects matched in identifiers table");

        // Delete the matched records from the temp table
        
        sql_string = @"DELETE from nk.temp_bbmris
        where match_status = 1 ";
        db.ExecuteSQL(sql_string);
    }
    
    
    public void IdentifyNewBBMRILinks()
    {
        // There may be entries in the temp working table that for some reason have not been matched above.
        // These should be deleted
        
        string sql_string = @"Delete from nk.temp_bbmris t
                              where parent_study_id is null";
        int res = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{res} records deleted from BBMRI-study combinations as sd_sid could not be matched");

        // Identify and label completely new objects first. 

        sql_string = @"Drop table if exists nk.new_bbmris;
        Create table nk.new_bbmris as 
                 select b.id
                 from nk.temp_bbmris b
                 left join nk.data_object_ids doi
                 on b.sd_oid = doi.sd_oid
                 where doi.sd_oid is null; ";
        db.ExecuteSQL(sql_string);

        sql_string = @"UPDATE nk.temp_bbmris b
            set match_status = 3
            FROM nk.new_bbmris n
            where b.id = n.id
            and b.match_status = 0 ";
        res = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{res} new sample-study combinations found with completely new sample Ids");

        // Then identify records where the sample exists but the link with that study is not yet in the
        // data_object_ids table. Relatively rare but could occur.

        sql_string = @"Drop table if exists nk.new_bbmri_links;
        Create table nk.new_bbmri_links as 
        select b.id
        from nk.temp_bbmris b
        left join nk.data_object_ids doi
        on b.sd_oid = doi.sd_oid
        and b.parent_study_id = doi.parent_study_id
        and b.match_status = 0 
        where doi.sd_oid is null";

        db.ExecuteSQL(sql_string);

        sql_string = @"UPDATE nk.temp_bbmris b
        set match_status = 2
        from nk.new_bbmri_links n 
        where b.match_status = 0
        and b.id = n.id ";

        res = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{res} new BBMRI-study combinations found for existing BBMRI samples");

    }

    public void AddNewBBMRIStudyLinks()
    {
        // New links found for BBMRI samples already in the system. Get the preferred object id for this BBMRI link.
        // This means that the sample object is only in the system once, even if it has multiple links.

         string sql_string = @"UPDATE nk.temp_bbmris b
                      SET object_id = doi.object_id,
                      is_preferred_object = false
                      FROM nk.data_object_ids doi
                      where b.sd_oid = doi.sd_oid
                      and doi.is_preferred_object = true
                      and b.match_status = 2 ";

         int res = db.ExecuteSQL(sql_string);
         _loggingHelper.LogLine($"{res} new BBMRI sample-study combinations updated");

         sql_string = @"Insert into nk.data_object_ids
         (object_id, source_id, sd_oid, object_type_id, title, is_preferred_object,
                        parent_study_source_id, parent_study_sd_sid,
                        parent_study_id, is_preferred_study, datetime_of_data_fetch, match_status)
         select object_id, source_id, sd_oid, object_type_id, title, is_preferred_object,
                        parent_study_source_id, parent_study_sd_sid,
                        parent_study_id, is_preferred_study, datetime_of_data_fetch, match_status
         FROM nk.temp_bbmris b
         where b.match_status = 2 ";

         res = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{res} new BBMRI-study combinations added");
    }

    public void AddCompletelyNewBBMRIObjects()
    {
        // Use the BBMRI-Study combination with the minimum study_id as the 'preferred' object record
        // for this sample object. This means only one 'new' BBMRI data object record will be identified
        // as such (several study combinations with the same sample may have been added at the same time.)

        string sql_string = @"UPDATE nk.temp_bbmris b
                              SET is_preferred_object = true
                              FROM 
                                  (select sd_oid, min(parent_study_id) as min_study
                                   from nk.temp_bbmris 
                                   where match_status = 3
                                   group by sd_oid) m
                              where b.sd_oid = m.sd_oid
                              and b.parent_study_id = m.min_study
                              and b.match_status = 3 ";
        int res = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{res} objects set as 'preferred' for new BBMRI samples");
        
        // Put the remaining study-sample combinations as non-preferred.

        sql_string = @"UPDATE nk.temp_bbmris b
                             SET is_preferred_object = false
                             where is_preferred_object is null
                             and b.match_status = 3 ";
        res = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{res} objects set as 'non-preferred' for new BBMRI samples");

        // Add in the 'preferred' new sample records. Note that the match status (3) is included.

        sql_string = @"Insert into nk.data_object_ids
        (source_id, sd_oid, object_type_id, title, is_preferred_object,
                 parent_study_source_id, parent_study_sd_sid,
                 parent_study_id, is_preferred_study, 
                 datetime_of_data_fetch, match_status)
         select source_id, sd_oid, object_type_id, title, is_preferred_object,
                 parent_study_source_id, parent_study_sd_sid,
                 parent_study_id, is_preferred_study, 
                 datetime_of_data_fetch, match_status
         FROM nk.temp_bbmris b
         where b.match_status = 3 
         and is_preferred_object = true";

         res = db.ExecuteSQL(sql_string);
         _loggingHelper.LogLine($"{res} new 'preferred' sample-study combinations added for new BBMRI samples");

         // Update newly added records with object ids, if the 'preferred' object record.

         sql_string = @"Update nk.data_object_ids doi
                        set object_id = id
                        where match_status = 3 
                        and source_id = 110426
                        and object_id is null
                        and is_preferred_object = true;";
        res = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{res} object ids created for new samples and applied to 'preferred' objects");

        // Update remaining study-sample combinations with new object id
        // At this stage only the 'preferred' study-BBMRI links have been completed in the doi table

        sql_string = @"UPDATE nk.temp_bbmris b
                       SET object_id = doi.object_id
                       FROM nk.data_object_ids doi
                       where b.sd_oid = doi.sd_oid
                       and b.match_status = 3 
                       and b.is_preferred_object = false ";
        res = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{res} object ids applied to new sample links with 'non-preferred' objects");

        // Add remaining matching study-sample records. Note object_id is included in the add this time.

        sql_string = @"Insert into nk.data_object_ids
        (object_id, source_id, sd_oid, object_type_id, title, is_preferred_object,
                 parent_study_source_id, parent_study_sd_sid,
                 parent_study_id, is_preferred_study, 
                 datetime_of_data_fetch, match_status)
         select object_id, source_id, sd_oid, object_type_id, title, is_preferred_object,
                 parent_study_source_id, parent_study_sd_sid,
                 parent_study_id, is_preferred_study, 
                 datetime_of_data_fetch, match_status
         FROM nk.temp_bbmris b
         where b.match_status = 3 
         and is_preferred_object = false ";

         res = db.ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"{res} new 'non-preferred' sample-study combinations added");
    }

    public void IdentifyBBMRIDataForImport(int source_id)
    {
        string sql_string = $@"DROP TABLE IF EXISTS nk.temp_objects_to_add;
             CREATE TABLE nk.temp_objects_to_add as 
             select distinct object_id, sd_oid
             from nk.data_object_ids doi
             WHERE is_preferred_object = true and 
             source_id = {source_id}";
        db.ExecuteSQL(sql_string);
    }
    
    public void DropTempBBMRITables()
    {
        string sql_string = @"DROP TABLE IF EXISTS nk.new_bbmris;
                              DROP TABLE IF EXISTS nk.new_bbmri_links;
                              DROP TABLE IF EXISTS nk.temp_bbmris;";
        db.ExecuteSQL(sql_string);
    }
}



