namespace MDR_Aggregator;

public class JSONHelper
{
    string connString;
    DBUtilities db;
    ILoggingHelper _loggingHelper;

    public JSONHelper(string _connString, ILoggingHelper logginghelper)
    {
        connString = _connString;
        _loggingHelper = logginghelper;
        db = new DBUtilities(connString, _loggingHelper);
    }


    public void CreateJSONStudyData(bool create_table = true, int offset = 0)
    {
        JSONStudyDataLayer repo = new JSONStudyDataLayer(_loggingHelper, connString);

        if (create_table)
        {
            string sql_string = @"DROP TABLE IF EXISTS core.studies_json;
            CREATE TABLE core.studies_json(
              id                       INT             NOT NULL PRIMARY KEY
            , json                     JSON            NULL
            );
            CREATE INDEX studies_json_id ON core.studies_json(id);";
            db.ExecuteSQL(sql_string);
        }

        int min_id = repo.FetchMinId();
        int max_id = repo.FetchMaxId();
        LoopThroughOAStudyRecords(repo, min_id, max_id, offset);
    }


    public void CreateJSONObjectData(bool create_table = true, int offset = 0)
    {
        JSONObjectDataLayer repo = new JSONObjectDataLayer(_loggingHelper, connString);

        if (create_table)
        {
            string sql_string = @"DROP TABLE IF EXISTS core.objects_json;
            CREATE TABLE core.objects_json(
              id                       INT             NOT NULL PRIMARY KEY
            , json                     JSON            NULL
            );
            CREATE INDEX objects_json_id ON core.objects_json(id);";
            db.ExecuteSQL(sql_string);
        }

        int min_id = repo.FetchMinId();
        int max_id = repo.FetchMaxId();

        LoopThroughOAObjectRecords(repo, min_id, max_id, offset);
    }


    public void LoopThroughOAStudyRecords(JSONStudyDataLayer repo, int min_id, int max_id, int offset)
    {
        JSONStudyProcessor processor = new JSONStudyProcessor(repo);

        // Do 10,000 ids at a time
        int batch = 10000;
        //int batch = 100;  // testing

        int k = 0;
        for (int n = min_id; n <= max_id; n+= batch)
        {
            IEnumerable<int> id_numbers = repo.FetchIds(n, batch);
            foreach (int id in id_numbers)
            {
                // Construct single study object, drawing data from various database tables 
                // and serialise to a formatted json string, then store json in the database.

                JSONStudy? st = processor.CreateStudyObject(id);
                if (st is not null)
                {
                    //var linear_json = JsonConvert.SerializeObject(st);
                    //processor.StoreJSONStudyInDB(id, linear_json);
                    /*
                    if (also_do_files)
                    {
                        var formatted_json = JsonConvert.SerializeObject(st, Formatting.Indented);
                        string file_name = "study " + id.ToString() + ".json";
                        string full_path = Path.Combine(folder_path, file_name);
                        File.WriteAllText(full_path, formatted_json);
                    }
                    */
                }

                k++;
                if (k % 1000 == 0) _loggingHelper.LogLine(k.ToString() + " records processed");
            }
        }
    }


    public void LoopThroughOAObjectRecords(JSONObjectDataLayer repo, int min_id, int max_id, int offset)
    {
        JSONObjectProcessor processor = new JSONObjectProcessor(repo, _loggingHelper);

        // Do 10,000 ids at a time
        int batch = 10000;
        //int batch = 100;  // testing

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

    /*
    public void UpdateJSONStudyData(bool also_do_files, int offset = 0)
    {
        JSONStudyDataLayer repo = new JSONStudyDataLayer(logging_repo);
        JSONStudyProcessor processor = new JSONStudyProcessor(repo);

        int min_id = repo.FetchMinId();
        int max_id = repo.FetchMaxId(); 

        // Do 10,000 ids at a time
        int batch = 10000;
        //int batch = 100;  // testing

        string folder_path = "";
        int k = 0;
        for (int n = min_id; n <= max_id; n += batch)
        {
            if (also_do_files)
            {
                // get the folder for the next batch, obtaining the parent path from repo

                string folder_name = "studies " + n.ToString() + " to " + (n + batch - 1).ToString();
                folder_path = Path.Combine(repo.StudyJsonFolder, folder_name);
                if (!Directory.Exists(folder_path))
                {
                    Directory.CreateDirectory(folder_path);
                }
            }

            IEnumerable<int> id_numbers = repo.FetchIds(n, batch);
            foreach (int id in id_numbers)
            {
                // Construct single study object, drawing data from various database tables 
                // and serialise to a formatted json string, then store json in the database.

                JSONStudy st = processor.CreateStudyObject(id);
                if (st != null)
                {
                    var linear_json = JsonConvert.SerializeObject(st);
                    processor.StoreJSONStudyInDB(id, linear_json);
                    if (also_do_files)
                    {
                        var formatted_json = JsonConvert.SerializeObject(st, Formatting.Indented);
                        string file_name = "study " + id.ToString() + ".json";
                        string full_path = Path.Combine(folder_path, file_name);
                        File.WriteAllText(full_path, formatted_json);
                    }
                }

                k++;
                if (k % 1000 == 0) logging_repo.LogLine(k.ToString() + " records processed");
            }
        }
    }


    public void UpdateJSONObjectData(bool also_do_files, int offset = 0)
    {
        JSONObjectDataLayer repo = new JSONObjectDataLayer(logging_repo);
        JSONObjectProcessor processor = new JSONObjectProcessor(repo, logging_repo);

        int min_id = repo.FetchMinId();
        int max_id = repo.FetchMaxId();

        // Do 10,000 ids at a time
        int batch = 10000;
        //int batch = 100;  // testing

        string folder_path = "";
        int k = offset;
        min_id += offset;

        for (int n = min_id; n <= max_id; n += batch)
        {
            //if (n > min_id + 200) break;  // testing
            if (also_do_files)
            {
                // get the folder for the next batch, obtaining the parent path from repo

                string folder_name = "objects " + n.ToString() + " to " + (n + batch - 1).ToString();
                folder_path = Path.Combine(repo.ObjectJsonFolder, folder_name);
                if (!Directory.Exists(folder_path))
                {
                    Directory.CreateDirectory(folder_path);
                }
            }

            IEnumerable<int> id_numbers = repo.FetchIds(n, batch);
            foreach (int id in id_numbers)
            {
                // Construct single study object, drawing data from various database tables 
                // and serialise to a formatted json string, then store json in the database.

                JSONDataObject obj = processor.CreateObject(id);
                if (obj != null)
                {
                    var linear_json = JsonConvert.SerializeObject(obj);
                    processor.StoreJSONObjectInDB(id, linear_json);

                    if (also_do_files)
                    {
                        var formatted_json = JsonConvert.SerializeObject(obj, Formatting.Indented);
                        string file_name = "object " + id.ToString() + ".json";
                        string full_path = Path.Combine(folder_path, file_name);
                        File.WriteAllText(full_path, formatted_json);
                    }
                }

                k++;
                if (k % 1000 == 0) logging_repo.LogLine(k.ToString() + " records processed");
            }
        }
    }
    */

}




