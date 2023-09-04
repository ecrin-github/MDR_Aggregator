namespace MDR_Aggregator;

public class LinkTableBuilder
{
    /*
    // ***************************** DO NOT RUN IN CODE ************************************
    // ***************************** DO NOT RUN IN CODE ************************************
    
    // One off builds.
    // Included here only for info - if run will reset ECRIN Identifiers for studies and objects
    // Should be run - rarely if at all  - from Postgres Admin
    
    DROP TABLE IF EXISTS nk.study_ids;
    CREATE TABLE nk.study_ids(
       id                       INT             NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 2000001 INCREMENT BY 1) PRIMARY KEY
     , study_id                 INT             NULL
     , source_id                INT             NULL
     , sd_sid                   VARCHAR         NULL
     , is_preferred             BOOLEAN         NULL
     , datetime_of_data_fetch   TIMESTAMPTZ     NULL
     , date_id_added            TIMESTAMPTZ     NULL DEFAULT Now()
     , match_status             INT             NULL
    );
    CREATE INDEX study_ids_studyid ON nk.study_ids(study_id);
    CREATE INDEX study_ids_sdsidsource ON nk.study_ids(source_id, sd_sid);

    DROP TABLE IF EXISTS nk.data_object_ids;
    CREATE TABLE nk.data_object_ids(
       id                       INT             NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 10000001 INCREMENT BY 1) PRIMARY KEY
     , object_id                INT             NULL
     , source_id                INT             NOT NULL
     , sd_oid                   VARCHAR         NULL
     , object_type_id			INT             NULL
     , title                    VARCHAR         NULL
     , is_preferred_object      BOOLEAN         NULL
     , is_valid_link            BOOLEAN         NULL DEFAULT true
     , parent_study_source_id   INT             NULL
     , parent_study_sd_sid      VARCHAR         NULL
     , parent_study_id          INT             NULL
     , is_preferred_study       BOOLEAN         NULL
     , datetime_of_data_fetch   TIMESTAMPTZ     NULL
     , date_id_added            TIMESTAMPTZ     NULL DEFAULT Now()
     , match_status             INT             NULL	
    );
    CREATE INDEX object_ids_objectid ON nk.data_object_ids(object_id);
    CREATE INDEX object_ids_sdidsource ON nk.data_object_ids(source_id, sd_oid);
        
    // ***************************** DO NOT RUN IN CODE ************************************
    // ***************************** DO NOT RUN IN CODE ************************************
    */
    
}