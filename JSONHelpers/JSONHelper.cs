using System.Text.Encodings.Web;
using System.Text.Json;

namespace MDR_Aggregator;

public class JSONHelper
{
    private readonly string _connString;
    private readonly DBUtilities db;
    private readonly ILoggingHelper _loggingHelper;
    private readonly JsonSerializerOptions? _json_options;
    public JSONHelper(string connString, ILoggingHelper logginghelper)
    {
        _connString = connString;
        _loggingHelper = logginghelper;
        db = new DBUtilities(connString, _loggingHelper);
        _json_options = new()
        {
            AllowTrailingCommas = true,
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };
    }

    public void CreateJSONStudyData(bool create_table = true, int offset = 0)
    {
        JSONStudyDataLayer repo = new JSONStudyDataLayer(_connString);

        if (create_table)
        {
            string sql_string = @"DROP TABLE IF EXISTS core.studies_json;
            CREATE TABLE core.studies_json(
              id                       INT             NOT NULL PRIMARY KEY
            , search_res               JSON            NULL
            , full_study               JSON            NULL
            , open_aire                JSON            NULL
            , c19p                     JSON            NULL 
            );
            CREATE INDEX studies_json_id ON core.studies_json(id);";
            db.ExecuteSQL(sql_string);
        }

        int min_id = repo.FetchMinId();
        int max_id = repo.FetchMaxId();
        LoopThroughStudyRecords(repo, min_id, max_id, offset);
    }

    public void LoopThroughStudyRecords(JSONStudyDataLayer repo, int min_id, int max_id, int offset = 0)
    {
        JSONStudyProcessor processor = new JSONStudyProcessor(repo);
        int batch = 10000;      // Do 10,000 ids at a time
        int k = offset;
        min_id += offset;
        
        for (int n = min_id; n <= max_id; n+= batch)
        {
            if (k > 10)  // for testing
            {
                break;
            }
            
            IEnumerable<int> id_numbers = repo.FetchIds(n, batch);
            foreach (int id in id_numbers)
            {
                // Re-initialise variables and then construct full study object, drawing data from
                // various database tables and serialise to a formatted json string, then store json in the database.
                string? search_res_json = null;
                string? open_aire_json = null;
                string? c19p_json = null;
                
                JSONFullStudy? st = processor.CreateFullStudyObject(id);
                if (st is not null)
                {
                    string full_json = JsonSerializer.Serialize(st, _json_options);
                    
                    // Construct search result and Covid19 Portal objects as subsets of the full study,
                    // and the open aire object by combining elements 
                    
                    JSONSSearchResStudy? st_search_res = processor.CreateStudySearchResObject(st);
                    if (st_search_res is not null)
                    {
                        
                        
                        search_res_json =  JsonSerializer.Serialize(st_search_res, _json_options);
                    }
                    
                    JSONOAStudy? st_open_aire = processor.CreateStudyOAObject(st);
                    if (st_open_aire is not null)
                    {
                        open_aire_json =  JsonSerializer.Serialize(st_open_aire, _json_options);
                    }
                    
                    JSONC19PStudy? st_c19p = processor.CreateStudyC19PStudyObject(st);
                    if (st_c19p is not null)
                    {
                        c19p_json =  JsonSerializer.Serialize(st_c19p, _json_options);
                    }
                    
                    // Add all json strings to the database

                    processor.StoreJSONStudyInDB(id, full_json, search_res_json, open_aire_json, c19p_json);                    
                }
                k++;
                
                if (k > 10)  // for testing
                {
                    break;
                }
                
                if (k % 1000 == 0) _loggingHelper.LogLine(k.ToString() + " records processed");
            }
        }
    }
    
    
    public void CreateJSONObjectData(bool create_table = true, int offset = 0)
    {
        JSONObjectDataLayer repo = new JSONObjectDataLayer(_connString);

        if (create_table)
        {
            string sql_string = @"DROP TABLE IF EXISTS core.objects_json;
            CREATE TABLE core.objects_json(
              id                       INT             NOT NULL PRIMARY KEY
            , search_res               JSON            NULL
            , full                     JSON            NULL
            );
            CREATE INDEX objects_json_id ON core.objects_json(id);";
            db.ExecuteSQL(sql_string);
        }

        int min_id = repo.FetchMinId();
        int max_id = repo.FetchMaxId();

        LoopThroughObjectRecords(repo, min_id, max_id, offset);
    }

    
    public void LoopThroughObjectRecords(JSONObjectDataLayer repo, int min_id, int max_id, int offset)
    {
        JSONObjectProcessor processor = new JSONObjectProcessor(repo, _loggingHelper);
        int batch = 10000;   // Do 10,000 ids at a time
        int k = offset;
        min_id += offset;
        
        for (int n = min_id; n <= max_id; n += batch)
        {
            IEnumerable<int> id_numbers = repo.FetchIds(n, batch);
            foreach (int id in id_numbers)
            {
                // Construct single study object, drawing data from various database tables 
                // and serialise to a formatted json string, then store json in the database.

                JSONDataObject? obj = processor.CreateObject(id);
                if (obj != null)
                {
                    //var linear_json = JsonConvert.SerializeObject(obj);
                    //processor.StoreJSONObjectInDB(id, linear_json);
                }
                k++;
                if (k % 1000 == 0) _loggingHelper.LogLine(k.ToString() + " records processed");
            }
        }
    }
}




