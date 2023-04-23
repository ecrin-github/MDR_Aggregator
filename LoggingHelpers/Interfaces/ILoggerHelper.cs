namespace MDR_Aggregator;

public interface ILoggingHelper
{
    void LogLine(string message, string identifier = "");
    void LogHeader(string header_text);
    void LogError(string message);
    void LogCodeError(string header, string errorMessage, string? stackTrace);
    void LogParseError(string header, string errorNum, string errorType);
    void CloseLog();
    void LogCommandLineParameters(Options opts);
    void LogStudyHeader(string leadText, string studyName);
}

