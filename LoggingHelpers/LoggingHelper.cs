using Microsoft.Extensions.Configuration;
namespace MDR_Aggregator;

public class LoggingHelper : ILoggingHelper
{
    private readonly string _logfilePath;
    private readonly string _summaryLogfilePath;
    private readonly StreamWriter? _sw;
    
    public LoggingHelper()
    {
        IConfigurationRoot settings = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json")
            .Build();

        string logfileStartOfPath = settings["logFileStartOfPath"] ?? "";
        string summaryLogfileStartOfPath = settings["summaryFileStartOfPath"] ?? "";
        
        string dtString = DateTime.Now.ToString("s", System.Globalization.CultureInfo.InvariantCulture)
            .Replace(":", "").Replace("T", " ");

        string logFolderPath = Path.Combine(logfileStartOfPath, "aggs");
        if (!Directory.Exists(logFolderPath))
        {
            Directory.CreateDirectory(logFolderPath);
        }
        
        string logFileName = "AG " + dtString + ".log";
        _logfilePath = Path.Combine(logFolderPath, logFileName);
        _summaryLogfilePath = Path.Combine(summaryLogfileStartOfPath, logFileName);
        _sw = new StreamWriter(_logfilePath, true, System.Text.Encoding.UTF8);
    }

    // Used to check if a log file with a named source has been created.

    public string LogFilePath => _logfilePath;
   
    
    public void LogCommandLineParameters(Options opts)
    {
        LogHeader("Setup");
        LogLine("transfer data =  " + opts.transfer_data);
        LogLine("create core =  " + opts.create_core);
        LogLine("create json =  " + opts.create_json);
        LogLine("do statistics =  " + opts.do_statistics);
        LogLine("do iec =  " + opts.do_iec);
        LogLine("do indices =  " + opts.do_indexes);
       
    }


    public void LogLine(string message, string identifier = "")
    {
        string dt_prefix = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
        string feedback = dt_prefix + message + identifier;
        Transmit(feedback);
    }


    public void LogStudyHeader(string leadText, string studyName)
    {
        string dividerLine = new string('-', 70);
        LogLine("");
        LogLine(dividerLine);
        LogLine($"{leadText} {studyName} data".ToUpper());
        LogLine(dividerLine);
        LogLine("");
    }


    public void LogHeader(string message)
    {
        string dt_prefix = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
        string header = dt_prefix + "**** " + message.ToUpper().ToUpper() + " ****";
        Transmit("");
        Transmit(header);
        Transmit("");
    }


    public void LogError(string message)
    {
        string dt_prefix = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
        string error_message = dt_prefix + "***ERROR*** " + message;
        Transmit("");
        Transmit("+++++++++++++++++++++++++++++++++++++++");
        Transmit(error_message);
        Transmit("+++++++++++++++++++++++++++++++++++++++");
        Transmit("");
    }


    public void LogCodeError(string header, string errorMessage, string? stackTrace)
    {
        string dt_prefix = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
        string headerMessage = dt_prefix + "***ERROR*** " + header + "\n";
        Transmit("");
        Transmit("+++++++++++++++++++++++++++++++++++++++");
        Transmit(headerMessage);
        Transmit(errorMessage + "\n");
        Transmit(stackTrace ?? "");
        Transmit("+++++++++++++++++++++++++++++++++++++++");
        Transmit("");
    }

    public void SpacedInformation(string header_text)
    {
        LogLine("");
        LogLine(header_text);
    }
    
    public void LogParseError(string header, string errorNum, string errorType)
    {
        string dt_prefix = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
        string error_message = dt_prefix + "***ERROR*** " + "Error " + errorNum + ": " + header + " " + errorType;
        Transmit(error_message);
    }

/*
    public void LogTableStatistics(Source s, string schema)
    {
        // Gets and logs record count for each table in the sd schema of the database
        // Start by obtaining the connection string, then construct log line for each by 
        // calling db interrogation for each applicable table
        string db_conn = s.db_conn ?? "";

        LogLine("");
        LogLine("TABLE RECORD NUMBERS");

        if (s.has_study_tables is true)
        {
            LogHeader("study tables");
            LogLine("");
            LogLine(StudyTableSummary(db_conn, schema, "studies", false));
            LogLine(StudyTableSummary(db_conn, schema, "study_identifiers"));
            LogLine(StudyTableSummary(db_conn, schema, "study_titles"));

            // These are database dependent.
            
            if (s.has_study_topics is true) LogLine(StudyTableSummary(db_conn, schema, "study_topics"));
            if (s.has_study_features is true) LogLine(StudyTableSummary(db_conn, schema, "study_features"));
            if (s.has_study_conditions is true) LogLine(StudyTableSummary(db_conn, schema, "study_conditions"));
            
            if (s.has_study_people is true) LogLine(StudyTableSummary(db_conn, schema, "study_people"));
            if (s.has_study_organisations is true) LogLine(StudyTableSummary(db_conn, schema, "study_organisations"));
            
            if (s.has_study_references is true) LogLine(StudyTableSummary(db_conn, schema, "study_references"));
            if (s.has_study_relationships is true)
                LogLine(StudyTableSummary(db_conn, schema, "study_relationships"));
            
            if (s.has_study_links is true) LogLine(StudyTableSummary(db_conn, schema, "study_links"));
            if (s.has_study_ipd_available is true)
                LogLine(StudyTableSummary(db_conn, schema, "study_ipd_available"));
            
            if (s.has_study_countries is true) LogLine(StudyTableSummary(db_conn, schema, "study_countries"));
            if (s.has_study_locations is true) LogLine(StudyTableSummary(db_conn, schema, "study_locations"));
            
            if (s.has_study_iec is true) LogLine(StudyTableSummary(db_conn, schema, "study_iec"));
        }

        LogHeader("object tables");
        LogLine("");
        // these common to all databases
        LogLine(ObjectTableSummary(db_conn, schema, "data_objects", false));
        LogLine(ObjectTableSummary(db_conn, schema, "object_instances"));
        LogLine(ObjectTableSummary(db_conn, schema, "object_titles"));

        // these are database dependent		

        if (s.has_object_datasets is true) LogLine(ObjectTableSummary(db_conn, schema, "object_datasets"));
        if (s.has_object_dates is true) LogLine(ObjectTableSummary(db_conn, schema, "object_dates"));
        if (s.has_object_relationships is true)
            LogLine(ObjectTableSummary(db_conn, schema, "object_relationships"));
        if (s.has_object_rights is true) LogLine(ObjectTableSummary(db_conn, schema, "object_rights"));
        if (s.has_object_pubmed_set is true)
        {
            LogLine(ObjectTableSummary(db_conn, schema, "journal_details"));
            LogLine(ObjectTableSummary(db_conn, schema, "object_people"));
            LogLine(ObjectTableSummary(db_conn, schema, "object_organisations"));
            LogLine(ObjectTableSummary(db_conn, schema, "object_topics"));
            LogLine(ObjectTableSummary(db_conn, schema, "object_comments"));
            LogLine(ObjectTableSummary(db_conn, schema, "object_descriptions"));
            LogLine(ObjectTableSummary(db_conn, schema, "object_identifiers"));
            LogLine(ObjectTableSummary(db_conn, schema, "object_db_links"));
            LogLine(ObjectTableSummary(db_conn, schema, "object_publication_types"));
        }
    }

*/
    public void CloseLog()
    {
        if (_sw is not null)
        {
            LogHeader("Closing Log");
            _sw.Flush();
            _sw.Close();
        }

        // Write out the summary file.

        var sw_summary = new StreamWriter(_summaryLogfilePath, true, System.Text.Encoding.UTF8);

        sw_summary.Flush();
        sw_summary.Close();
    }


    private void Transmit(string message)
    {
        _sw?.WriteLine(message);
        Console.WriteLine(message);
    }

/*
    private string StudyTableSummary(string dbConn, string schema, string tableName, bool includeSource = true)
    {
        using NpgsqlConnection conn = new(dbConn);
        string sql_string = "select count(*) from " + schema + "." + tableName;
        int res = conn.ExecuteScalar<int>(sql_string);
        if (includeSource)
        {
            sql_string = "select count(distinct sd_sid) from " + schema + "." + tableName;
            int study_num = conn.ExecuteScalar<int>(sql_string);
            return $"{res} records found in {schema}.{tableName}, from {study_num} studies";
        }
        else
        {
            return $"{res} records found in {schema}.{tableName}";
        }
    }


    private string ObjectTableSummary(string dbConn, string schema, string tableName, bool includeSource = true)
    {
        using NpgsqlConnection conn = new(dbConn);
        string sql_string = "select count(*) from " + schema + "." + tableName;
        int res = conn.ExecuteScalar<int>(sql_string);
        if (includeSource)
        {
            sql_string = "select count(distinct sd_oid) from " + schema + "." + tableName;
            int object_num = conn.ExecuteScalar<int>(sql_string);
            return $"{res} records found in {schema}.{tableName}, from {object_num} objects";
        }
        else
        {
            return $"{res} records found in {schema}.{tableName}";
        }
    }
*/
    public void SendEmail(string errorMessageText)
    {
        // construct txt file with message
        // and place in pickup folder for
        // SMTP service (if possible - may need to change permissions on folder)
    }


    public void SendRes(string resultText)
    {
        // construct txt file with message
        // and place in pickup folder for
        // SMTP service (if possible - may need to change permissions on folder)
    }
    
    /*
    private string GetTableRecordCount(string db_conn, string schema, string table_name)
    {
        string sql_string = "select count(*) from " + schema + "." + table_name;
        using NpgsqlConnection conn = new NpgsqlConnection(db_conn);
        int res = conn.ExecuteScalar<int>(sql_string);
        return res.ToString() + " records found in " + schema + "." + table_name;
    }
    */
}

