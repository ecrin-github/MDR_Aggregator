namespace MDR_Aggregator;

public class SearchHelperObjects
{
    private readonly DBUtilities db;
    readonly int min_studies_id, max_studies_id;
    
    public SearchHelperObjects(string connString, ILoggingHelper loggingHelper)
    {
        db = new DBUtilities(connString, loggingHelper);
        min_studies_id = db.GetAggMinId("core.studies");
        max_studies_id = db.GetAggMaxId("core.studies");
    }
    
    // pmid table.
    
    public int CreatePMIDSearchData()
    {
        string sql_string = @"truncate table core.search_pmids;
        insert into core.search_pmids (pmid, study_id)
        select oi.identifier_value::int, k.study_id from 
        core.object_identifiers oi
        inner join core.study_object_links k
        on oi.object_id = k.object_id
        where identifier_type_id = 16
        order by oi.identifier_value::int ;";
        
        return db.ExecuteSQL(sql_string);
    }    
    
    // idents table.
    
    public int CreateIdentifierSearchData()
    {
        string sql_string = @"truncate table core.search_idents; ";
        db.ExecuteSQL(sql_string);

        string top_sql = @"insert into core.search_idents (ident_type, ident_value, study_id)
        select identifier_type_id, identifier_value, study_id
        from core.study_identifiers si
        where identifier_type_id not in (1, 90) ";
        
        string bottom_sql = @" order by identifier_type_id, identifier_value ;";

        return db.CreateSearchIdentsData(top_sql, bottom_sql, min_studies_id, max_studies_id, "search_idents");
    }
        
    // objects table.
   
    public int CreateObjectSearchData()
    {
        string top_sql =
            @"insert into core.search_objects(study_id, object_id, url, resource_type_id)
                select s.study_id, s.object_id, bi.url, bi.resource_type_id
                from core.study_object_links s
                inner join core.object_instances bi
                on s.object_id = bi.object_id ";
        string bottom_sql = " order by s.study_id, s.object_id"; 
       
        return db.SearchTableTransfer(top_sql, bottom_sql, "study_id", min_studies_id, max_studies_id,
            "search_objects", 20000);
        
        /*
        string top_sql =
            @"insert into core.search_objects(study_id, object_id, object_name, type_id, url,
                                              resource_type_id, year_published, access_type_id, provenance)
                select s.study_id, s.object_id, b.display_title, b.object_type_id, bi.url, 
                       bi.resource_type_id, b.publication_year, b.access_type_id, b.provenance_string
                from core.study_object_links s
                inner join core.data_objects b
                on s.object_id = b.id
                inner join core.object_instances bi
                on b.id = bi.object_id ";
       string bottom_sql = " order by s.study_id, s.object_id"; 
       
       return db.SearchTableTransfer(top_sql, bottom_sql, "study_id", min_studies_id, max_studies_id,
                                     "search_objects", 20000);
       */
       
    }
    
    public int UpdateObjectSearchData()
    {
        string sql_string = @"update core.search_objects so
                             set object_name = b.display_title,
                             type_id = b.object_type_id,
                             year_published = b.publication_year, 
                             access_type_id = b.access_type_id, 
                             provenance = b.provenance_string
                             from core.data_objects b
                             where so.object_id = b.id ";

        return db.UpdateObjectSearchData(sql_string, min_studies_id, max_studies_id, " and ", "object types");
    }
    
    public int UpdateObjectSearchDataWithType()
    {
        string sql_string = @"update core.search_objects so
                             set type_name = lup.name
                             from context_lup.object_types lup
                             where so.type_id = lup.id ";

        return db.UpdateObjectSearchData(sql_string, min_studies_id, max_studies_id," and ", "object types");
    }
    
    public int UpdateObjectSearchDataWithAccessIcon()
    {
        string sql_string = @"update core.search_objects so
                set access_icon =
                case 
                   when resource_type_id = 40 and access_type_id = 15 then 'S'
                   when resource_type_id = 40 and access_type_id <> 15 then 'T'
                   when access_type_id in (11, 12, 13, 14, 20) then 'G'
                   when access_type_id in (15, 16, 17, 18, 19) then 'R' 
                   else 'X'
                   end ";
        return db.UpdateObjectSearchData(sql_string, min_studies_id, max_studies_id, " where ", "access icon");
    }
    
    public int UpdateObjectSearchDataWithResourceIcon()
    {
        string sql_string = @"update core.search_objects so
                set resource_icon =
                case 
                   when resource_type_id in (37, 38, 39, 40) then 'WA'
                   when resource_type_id = 35 then 'WO'
                   when resource_type_id = 36 then 'WD'
                   when resource_type_id = 11 then 'P'
                   when resource_type_id in (12, 13) then 'D'
                   when resource_type_id in (14, 15, 16) then 'T'
                   when resource_type_id in (17, 18, 19) then 'S'
                   when resource_type_id between 20 and 34 then 'O'
                   else 'X'
                   end ";
        return db.UpdateObjectSearchData(sql_string, min_studies_id, max_studies_id, " where ","resource icon");
    }
    
    
    
    

    



}
