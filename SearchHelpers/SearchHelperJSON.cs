using System;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Xml.Serialization;

namespace MDR_Aggregator;

public class SearchHelperJson
{
    private readonly string _connString;
    private readonly ILoggingHelper _loggingHelper;
    private readonly JsonSerializerOptions? _json_options;
    
    public SearchHelperJson(string connString, ILoggingHelper logginghelper)
    {
        _connString = connString;
        _loggingHelper = logginghelper;
        _json_options = new()
        {
            AllowTrailingCommas = true,
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };
    }

    public void LoopThroughObjectRecords(int offset = 0)
    {
        JSONObjectDataLayer repo = new JSONObjectDataLayer(_connString);
        JSONObjectProcessor processor = new JSONObjectProcessor(repo, _loggingHelper);
        int min_id = repo.FetchMinId();
        int max_id = repo.FetchMaxId();
        
        int batch = 10000;   // Do 10,000 ids at a time
        int k = offset;
        min_id += offset;

        for (int n = min_id; n <= max_id; n += batch)
        {
            IEnumerable<int> id_numbers = repo.FetchIds(n, batch);
            foreach (int id in id_numbers)
            {
                // Construct single data object object, drawing data from various database tables 
                
                JSONFullObject? obj = processor.CreateFullObject(id);
                if (obj != null)
                {
                    string full_json = JsonSerializer.Serialize(obj, _json_options);
                    processor.StoreJSONObjectInDB(id, full_json);   // full object details 
                    
                    List<JSONSearchResObject> ob_search_results = processor.CreateSearchResObjects(obj);
                    if (ob_search_results.Any())
                    {
                        // no need to serialise to json here - just store straight in the DB
                        // as a record - it will be serialised into json as part of the study json later
                        
                        foreach (JSONSearchResObject sres in ob_search_results)
                        {
                            processor.StoreSearchRecord(sres);
                        }
                    }
                }
                k++;
                if (k % 1000 == 0) _loggingHelper.LogLine(k + " records processed");
            }
        }
    }

    public void LoopThroughStudyRecords(int offset = 0)
    {
        JSONStudyDataLayer repo = new JSONStudyDataLayer(_connString, _loggingHelper);
        JSONStudyProcessor processor = new JSONStudyProcessor(repo);
        int min_id = repo.FetchMinId();
        int max_id = repo.FetchMaxId();
        
        int batch = 1000;      // Do 1000 ids at a time
        int k = offset;
        min_id += offset;

        for (int n = min_id; n <= max_id; n+= batch)
        {
            IEnumerable<int> id_numbers = repo.FetchIds(n, batch);
            foreach (int id in id_numbers)
            {

                // Re-initialise variables and then construct full study object, drawing data from various
                // database tables and serialise to a formatted json string, then store json in the database.

                JSONFullStudy? st = processor.CreateFullStudyObject(id);
                if (st is not null)
                {
                    string full_json = JsonSerializer.Serialize(st, _json_options);
                    
                    // Construct to-search record, json search result and Covid19 Portal objects
                    // as subsets of the full study, and the open aire object by combining elements 
                    
                    JSONSSearchResStudy st_search_res = processor.CreateStudySearchResult(st);
                    processor.AddNewStudySearchRecord(st_search_res);
                    string search_res_json = JsonSerializer.Serialize(st_search_res, _json_options);


                    string? open_aire_json = null;
                    JSONOAStudy? st_open_aire = processor.CreateStudyOAObject(st);
                    if (st_open_aire is not null)
                    {
                        open_aire_json =  JsonSerializer.Serialize(st_open_aire, _json_options);
                    }


                    string? c19p_string = null;
                    entry? c19p_entry = processor.CreateStudyC19PStudyObject(st);
                    if (c19p_entry is not null)
                    {
                        XmlSerializer x = new XmlSerializer(c19p_entry.GetType());
                        StringWriter sw = new StringWriter();
                        x.Serialize(sw, c19p_entry);
                        c19p_string = sw.ToString();

                        c19p_string = c19p_string.Replace("dbref", "ref");
                        c19p_string = c19p_string.Replace("<?xml version=\"1.0\" encoding=\"utf-16\"?>", "");
                        c19p_string = c19p_string.Replace(" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"", "");
                        c19p_string = c19p_string.Replace(" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"", "");
                    }
                    
                    // Add all json strings to the database

                    processor.StoreJSONStudyInDB(id, full_json, search_res_json, open_aire_json, c19p_string);                    
                }
                k++;
                if (k % 100 == 0) _loggingHelper.LogLine(k.ToString() + " records processed");
            }
        }
    }
    
    
    
    
    // temp function, obtaining table data back from json (!)
    /*
    public void CreateObjectSearchDataFromJSON(int offset)
    {
        JSONObjectDataLayer repo = new JSONObjectDataLayer(_connString);
        JSONObjectProcessor processor = new JSONObjectProcessor(repo, _loggingHelper);
        repo.CreateObjectSearchDataTable();
        int min_id = repo.FetchMinInstId();
        int max_id = repo.FetchMaxInstId();
        ulong num_stored = 0;
        int batch = 1000; // Do 1,000 ids at a time
        int k = offset;
        min_id += offset;

        for (int n = min_id; n <= max_id; n += batch)
        {
            List<JSONSearchResObject> result_objects = new();
            IEnumerable<string> id_search_strings = repo.FetchSearchStrings(n, batch);
            foreach (string search_string in id_search_strings)
            {
                JSONSearchResObject? r = JsonSerializer.Deserialize<JSONSearchResObject>(search_string);
                if (r is not null)
                {
                    result_objects.Add(r);
                }
                k++;
            }
            num_stored += repo.StoreRecsInObsSearchResTable(result_objects);
            _loggingHelper.LogLine($"{k} records processed, {num_stored} records added");
        }
    }
    */
                    }




