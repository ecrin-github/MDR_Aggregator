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
        LogBlank();
        LogLine(dividerLine);
        LogLine($"{leadText} {studyName} data".ToUpper());
        LogLine(dividerLine);
        LogBlank();
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
    
    public void LogBlank()
    {
        Transmit("");
    }

    public void SpacedInformation(string header_text)
    {
        LogBlank();
        LogLine(header_text);
    }
    
    public void LogParseError(string header, string errorNum, string errorType)
    {
        string dt_prefix = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
        string error_message = dt_prefix + "***ERROR*** " + "Error " + errorNum + ": " + header + " " + errorType;
        Transmit(error_message);
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

