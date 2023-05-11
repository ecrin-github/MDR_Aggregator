using Microsoft.Extensions.Configuration;
namespace MDR_Aggregator;

public class LoggingHelper : ILoggingHelper
{
    private readonly string _logFileStartOfPath;
    private readonly string _summaryLogFileStartOfPath;    
    private string? _logFilePath;    
    private string? _summaryLogFilePath;
    private StreamWriter? _sw;
    
    public LoggingHelper()
    {
        IConfigurationRoot settings = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json")
            .Build();

        _logFileStartOfPath = settings["logFileStartOfPath"] ?? "";
        _summaryLogFileStartOfPath = settings["summaryFileStartOfPath"] ?? "";
    }
    
    public void OpenFile(string[] args)
    {
        string dtString = DateTime.Now.ToString("s", System.Globalization.CultureInfo.InvariantCulture)
            .Replace(":", "").Replace("T", " ");

        string logFolderPath = Path.Combine(_logFileStartOfPath, "aggs");
        if (!Directory.Exists(logFolderPath))
        {
            Directory.CreateDirectory(logFolderPath);
        }

        string logFileName = "AG ";
        if (args.Any())
        {
            foreach (string t in args)
            {
                logFileName += t + " ";
            }
        }
        
        logFileName += dtString + ".log";
        _logFilePath = Path.Combine(logFolderPath, logFileName);
        _summaryLogFilePath = Path.Combine(_summaryLogFileStartOfPath, logFileName);
        _sw = new StreamWriter(_logFilePath, true, System.Text.Encoding.UTF8);
    }

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


    public void LogSummaryStatistics(CoreSummary summ)
    {
        // Logs the record count for each table in the core schema
        
        LogHeader("study tables");
        LogLine($"Studies: {summ.study_recs:n0}");
        LogLine($"Study Identifiers: {summ.study_identifiers_recs:n0}");
        LogLine($"Study Titles: {summ.study_titles_recs:n0}");
            
        LogLine($"Study Topics: {summ.study_topics_recs:n0}");
        LogLine($"Study Features: {summ.study_features_recs:n0}");
        LogLine($"Study Conditions: {summ.study_conditions_recs:n0}");
            
        LogLine($"Study People: {summ.study_people_recs:n0}");
        LogLine($"Study Organisations: {summ.study_organisations_recs:n0}");
        LogLine($"Study Relationships: {summ.study_relationships_recs:n0}");
            
        LogLine($"Study Countries: {summ.study_countries_recs:n0}");
        LogLine($"Study Locations: {summ.study_locations_recs:n0}");

        LogHeader("object tables");

        LogLine($"Data Objects: {summ.data_object_recs:n0}");
        LogLine($"Object Instances: {summ.object_instances_recs:n0}");
        LogLine($"Object titles: {summ.object_titles_recs:n0}");

        LogLine($"Object Datasets: {summ.object_datasets_recs:n0}");
        LogLine($"Object Dates: {summ.object_dates_recs:n0}");
        LogLine($"Object Relationships: {summ.object_relationships_recs:n0}");
        LogLine($"Object Rights: {summ.object_rights_recs:n0}");
        
        LogLine($"Object People: {summ.object_people_recs:n0}");
        LogLine($"Object Organisations: {summ.object_organisations_recs:n0}");
        
        LogLine($"Object Topics: {summ.object_topics_recs:n0}");
        LogLine($"Object Descriptions: {summ.object_descriptions_recs:n0}");
        LogLine($"Object Identifiers: {summ.object_identifiers_recs:n0}");
        
        LogHeader("Study-Object Linkage");
        LogLine($"Study-Object Links: {summ.study_object_link_recs:n0}");
        
    }

    public void LogDataObjectTypeStatistics(int lastAggEventId)
    {
        // Logs the number of each data object type, where n > 10,
        // from the last aggregation event
        
        // input a list of object type - total objects, obtained from a
        // DB call to the monitor DB layer. Loop through and log each line.
        
        LogLine("");
        LogLine("DATA OBJECT NUMBERS");
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

        var sw_summary = new StreamWriter(_summaryLogFilePath!, true, System.Text.Encoding.UTF8);

        sw_summary.Flush();
        sw_summary.Close();
    }


    private void Transmit(string message)
    {
        _sw?.WriteLine(message);
        Console.WriteLine(message);
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

