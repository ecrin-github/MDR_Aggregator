using System;
using System.Collections.Generic;
using System.Text;

namespace MDR_Aggregator
{
    public interface ILoggingHelper
    {
        string LogFilePath { get; }

        void OpenLogFile(string database_name);
        void OpenNoSourceLogFile();

        void LogLine(string message, string identifier = "");
        void LogHeader(string header_text);
        void LogError(string message);
        void LogCodeError(string header, string errorMessage, string? stackTrace);
        void LogParseError(string header, string errorNum, string errorType);
        void CloseLog();
    
        void LogCommandLineParameters(Options opts);
        void LogStudyHeader(Options opts, string dbline);
        void LogTableStatistics(Source s, string schema);
    }
}
