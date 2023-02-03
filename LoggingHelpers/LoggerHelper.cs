using Dapper;
using Microsoft.Extensions.Configuration;
using Npgsql;

namespace MDR_Aggregator;


public class LoggingHelper : ILoggingHelper
{
    private readonly string _logfileStartOfPath;
    private readonly string _summaryLogfileStartOfPath;
    private string _logfilePath = "";
    private string _summaryLogfilePath = "";
    private StreamWriter? _sw;

    public LoggingHelper()
    {
        IConfigurationRoot settings = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json")
            .Build();

        _logfileStartOfPath = settings["logfilepath"] ?? "";
        _summaryLogfileStartOfPath = settings["summaryfilepath"] ?? "";
    }

    // Used to check if a log file with a named source has been created.

    public string LogFilePath => _logfilePath;

    public void OpenLogFile(string databaseName)
    {
        string dt_string = DateTime.Now.ToString("s", System.Globalization.CultureInfo.InvariantCulture)
            .Replace(":", "").Replace("T", " ");

        string log_folder_path = Path.Combine(_logfileStartOfPath, databaseName);
        if (!Directory.Exists(log_folder_path))
        {
            Directory.CreateDirectory(log_folder_path);
        }

        string log_file_name = "HV " + databaseName + " " + dt_string + ".log";


        _logfilePath = Path.Combine(log_folder_path, log_file_name);
        _summaryLogfilePath = Path.Combine(_summaryLogfileStartOfPath, log_file_name);
        _sw = new StreamWriter(_logfilePath, true, System.Text.Encoding.UTF8);
    }


    public void OpenNoSourceLogFile()
    {
        string dt_string = DateTime.Now.ToString("s", System.Globalization.CultureInfo.InvariantCulture)
            .Replace(":", "").Replace("T", " ");

        string log_file_name = "HV Source not set " + dt_string + ".log";
        _logfilePath = Path.Combine(_logfileStartOfPath, log_file_name);
        _summaryLogfilePath = Path.Combine(_summaryLogfileStartOfPath, log_file_name);
        _sw = new StreamWriter(_logfilePath, true, System.Text.Encoding.UTF8);
    }


    public void LogCommandLineParameters(Options opts)
    {
        LogHeader("Setup");
        LogLine("transfer data =  " + opts.transfer_data);
        LogLine("create core =  " + opts.create_core);
        LogLine("create json =  " + opts.create_json);
        LogLine("also do json files =  " + opts.also_do_files);
        LogLine("do statistics =  " + opts.do_statistics);
    }


    public void LogLine(string message, string identifier = "")
    {
        string dt_prefix = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
        string feedback = dt_prefix + message + identifier;
        Transmit(feedback);
    }


    public void LogStudyHeader(Options opts, string studyName)
    {
        string dividerLine;
        
        //if (opts.harvest_all_test_data || opts.setup_expected_data_only)
        //{
        //    dividerLine = new string('-', 70);
        //}
        //else
        //{
           // dividerLine = harvest_type is 1 or 2 ? new string('=', 70) : new string('-', 70);
        //}

        LogLine("");
        //LogLine(dividerLine);
        LogLine(studyName);
        //LogLine(dividerLine);
        LogLine("");
    }


    public void LogHeader(string message)
    {
        string dt_prefix = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
        string header = dt_prefix + "**** " + message.ToUpper().ToUpper() + " ****";
        Transmit("");
        Transmit(header);
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


    public void LogParseError(string header, string errorNum, string errorType)
    {
        string dt_prefix = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
        string error_message = dt_prefix + "***ERROR*** " + "Error " + errorNum + ": " + header + " " + errorType;
        Transmit(error_message);
    }


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

            // these are database dependent
            if (s.has_study_topics is true) LogLine(StudyTableSummary(db_conn, schema, "study_topics"));
            if (s.has_study_features is true) LogLine(StudyTableSummary(db_conn, schema, "study_features"));
            if (s.has_study_conditions is true) LogLine(StudyTableSummary(db_conn, schema, "study_conditions"));
            if (s.has_study_iec is true) LogLine(StudyTableSummary(db_conn, schema, "study_iec"));
            if (s.has_study_contributors is true) LogLine(StudyTableSummary(db_conn, schema, "study_contributors"));
            if (s.has_study_references is true) LogLine(StudyTableSummary(db_conn, schema, "study_references"));
            if (s.has_study_relationships is true)
                LogLine(StudyTableSummary(db_conn, schema, "study_relationships"));
            if (s.has_study_links is true) LogLine(StudyTableSummary(db_conn, schema, "study_links"));
            if (s.has_study_ipd_available is true)
                LogLine(StudyTableSummary(db_conn, schema, "study_ipd_available"));
            if (s.has_study_countries is true) LogLine(StudyTableSummary(db_conn, schema, "study_countries"));
            if (s.has_study_locations is true) LogLine(StudyTableSummary(db_conn, schema, "study_locations"));
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
            LogLine(ObjectTableSummary(db_conn, schema, "object_contributors"));
            LogLine(ObjectTableSummary(db_conn, schema, "object_topics"));
            LogLine(ObjectTableSummary(db_conn, schema, "object_comments"));
            LogLine(ObjectTableSummary(db_conn, schema, "object_descriptions"));
            LogLine(ObjectTableSummary(db_conn, schema, "object_identifiers"));
            LogLine(ObjectTableSummary(db_conn, schema, "object_db_links"));
            LogLine(ObjectTableSummary(db_conn, schema, "object_publication_types"));
        }
    }


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

    
    private string GetTableRecordCount(string db_conn, string schema, string table_name)
    {
        string sql_string = "select count(*) from " + schema + "." + table_name;

        using (NpgsqlConnection conn = new NpgsqlConnection(db_conn))
        {
            int res = conn.ExecuteScalar<int>(sql_string);
            return res.ToString() + " records found in " + schema + "." + table_name;
        }
    }
}

