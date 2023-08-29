using CommandLine;
namespace MDR_Aggregator;

internal class ParametersChecker
{
    private readonly ILoggingHelper _loggingHelper;

    public ParametersChecker(ILoggingHelper logginghelper)
    {
        _loggingHelper = logginghelper;
    }

    public ParamsCheckResult CheckParams(IEnumerable<string>? args)
    {
        // Calls the CommandLine parser. If an error in the initial parsing, log it and return an error.
        // If parameters can be passed, check their validity and if invalid log and return an error,
        // otherwise return the parameters, processed as an instance of the Options class.

        var parsedArguments = Parser.Default.ParseArguments<Options>(args);
        if (parsedArguments.Errors.Any())
        {
            LogParseError(((NotParsed<Options>)parsedArguments).Errors);
            return new ParamsCheckResult(true, false, null);
        }

        var opts = parsedArguments.Value;
        return CheckArgumentValuesAreValid(opts);
    }

    private ParamsCheckResult CheckArgumentValuesAreValid(Options opts)
    {
        // 'opts' is passed by reference and may be changed by the checking mechanism.
        
        try
        {
            if (opts is { transfer_data: false, create_core: false, create_json: false, 
                               do_statistics: false, do_iec: false, do_indexes: false })
            {
                // Need at least one of D, C, J or S to be true

                throw new Exception("None of the allowed parameters appear to be present!");
            }

            // Parameters valid - return opts and the source.

            return new ParamsCheckResult(false, false, opts);
        }

        catch (Exception e)
        {
            _loggingHelper.LogHeader("INVALID PARAMETERS");
            _loggingHelper.LogCommandLineParameters(opts);
            _loggingHelper.LogCodeError("MDR_Aggregator application aborted", e.Message, e.StackTrace ?? "");
            _loggingHelper.CloseLog();
            return new ParamsCheckResult(false, true, null);
        }
    }

    
    private void LogParseError(IEnumerable<Error> errs)
    {
        //_loggingHelper.OpenNoSourceLogFile();
        _loggingHelper.LogHeader("UNABLE TO PARSE PARAMETERS");
        _loggingHelper.LogLine("Error in the command line arguments - they could not be parsed");

        int n = 0;
        foreach (Error e in errs)
        {
            n++;
            _loggingHelper.LogParseError("Error {n}: Tag was {Tag}", n.ToString(), e.Tag.ToString());
            if (e.GetType().Name == "UnknownOptionError")
            {
                _loggingHelper.LogParseError("Error {n}: Unknown option was {UnknownOption}", n.ToString(), 
                    ((UnknownOptionError)e).Token);
            }
            if (e.GetType().Name == "MissingRequiredOptionError")
            {
                _loggingHelper.LogParseError("Error {n}: Missing option was {MissingOption}", n.ToString(), 
                    ((MissingRequiredOptionError)e).NameInfo.NameText);
            }
            if (e.GetType().Name == "BadFormatConversionError")
            {
                _loggingHelper.LogParseError("Error {n}: Wrongly formatted option was {MissingOption}", n.ToString(), 
                    ((BadFormatConversionError)e).NameInfo.NameText);
            }
        }
        _loggingHelper.LogLine("MDR_Aggregator application aborted");
        _loggingHelper.CloseLog();
    }
}


public class Options
{
    // Lists the command line arguments and options
    [Option('D', "transfer and aggregate data", Required = false, HelpText = "Indicates that data should be imported from source systems and aggregate st, ob, nk tables constructed ")]
    public bool transfer_data { get; set; }

    [Option('C', "create core table data", Required = false, HelpText = "Indicates that the core tables should be crated and filled from the aggregate tables.")]
    public bool create_core { get; set; }

    [Option('J', "create json", Required = false, HelpText = "Indicates json fields should be constructed from the core table data.")]
    public bool create_json { get; set; }

    [Option('S', "do statistics", Required = false, HelpText = "Summarises record numbers, of each sort, in different sources and in the summary and core tables")]
    public bool do_statistics { get; set; }
    
    [Option('X', "do indexes", Required = false, HelpText = "Re-establishes text indexes on title and topic fields, for searching")]
    public bool do_indexes { get; set; }
    
    [Option('I', "do IEC data", Required = false, HelpText = "Aggregates the inclusion / exclusion data into a separate database ('iec')")]
    public bool do_iec { get; set; }

}

  
public class ParamsCheckResult
{
    internal bool ParseError { get; set; }
    internal bool ValidityError { get; set; }
    internal Options? Pars { get; set; }

    internal ParamsCheckResult(bool _ParseError, bool _ValidityError, Options? _Pars)
    {
        ParseError = _ParseError;
        ValidityError = _ValidityError;
        Pars = _Pars;
    }
}