using CommandLine;

namespace MDR_Aggregator
{
    internal class ParametersChecker : IParametersChecker
    {
        private ILoggingHelper _loggingHelper;
        private ILoggingHelper _logging_helper;

        public ParametersChecker(ILoggingHelper logginghelper, ILoggingHelper logginghelper_helper)
        {
            _loggingHelper = logginghelper;
            _logging_helper = logginghelper_helper;
        }

        // Parse command line arguments and return true only if no errors.
        // Otherwise log errors and return false.

        public Options ObtainParsedArguments(string[] args)
        {
            var parsedArguments = Parser.Default.ParseArguments<Options>(args);
            if (parsedArguments.Tag.ToString() == "NotParsed")
            {
                HandleParseError(((NotParsed<Options>)parsedArguments).Errors);
                return null;
            }
            else
            {
                return ((Parsed<Options>)parsedArguments).Value;
            }
        }

        // Parse command line arguments and return true if values are valid.
        // Otherwise log errors and return false.

        public bool ValidArgumentValues(Options opts)
        {
            try
            {

                if (opts.testing)
                {
                    // no particular requirement here
                    // can drop straight through to run the program
                    // but set the other parameters as true so all functions are tested

                    opts.transfer_data = true;
                    opts.create_core = true;
                    opts.create_json = true;
                    opts.do_statistics = true;
                }
                else if ((opts.transfer_data == false)
                    && (opts.create_core == false)
                    && (opts.create_json == false)
                    && (opts.do_statistics == false))
                {
                    // If not testing need at least one of D, C, J or S to be true

                    throw new Exception("None of the allowed optional parameters appear to be present!");
                }
                else if (opts.also_do_files)
                {
                    // F only valid if J is present (but F option being dropped)

                    if(opts.create_json == false)
                    {
                        throw new Exception("F parameter can only be provided if J paramewter also provided");
                    }
                }
    
                return true;    // OK the program can run!
            }

            catch (Exception e)
            {
                _loggingHelper.LogError(e.Message);
                _loggingHelper.LogError(e.StackTrace);
                _loggingHelper.LogLine("Harvester application aborted");
                _loggingHelper.LogHeader("Closing Log");
                return false;
            }

        }


        private void HandleParseError(IEnumerable<Error> errs)
        {
            // log the errors
            _loggingHelper.LogError("Error in the command line arguments - they could not be parsed");
            int n = 0;
            foreach (Error e in errs)
            {
                n++;
                _loggingHelper.LogParseError("Error {n}: Tag was {Tag}", n.ToString(), e.Tag.ToString());
                if (e.GetType().Name == "UnknownOptionError")
                {
                    _loggingHelper.LogParseError("Error {n}: Unknown option was {UnknownOption}", n.ToString(), ((UnknownOptionError)e).Token);
                }
                if (e.GetType().Name == "MissingRequiredOptionError")
                {
                    _loggingHelper.LogParseError("Error {n}: Missing option was {MissingOption}", n.ToString(), ((MissingRequiredOptionError)e).NameInfo.NameText);
                }
                if (e.GetType().Name == "BadFormatConversionError")
                {
                    _loggingHelper.LogParseError("Error {n}: Wrongly formatted option was {MissingOption}", n.ToString(), ((BadFormatConversionError)e).NameInfo.NameText);
                }
            }
            _loggingHelper.LogLine("Harvester application aborted");
            _loggingHelper.LogHeader("Closing Log");
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

        [Option('F', "create json files", Required = false, HelpText = "Indicates json files should also be constructed from the core table data. Only has an effect if -J parameter present ")]
        public bool also_do_files { get; set; }

        [Option('S', "do statistics", Required = false, HelpText = "Summarises record numbers, of each sort, in different sources and in the summary and core tables")]
        public bool do_statistics { get; set; }

        [Option('T', "use test data", Required = false, HelpText = "Carry out D, C, S and J but usiung test data only, in the test database")]
        public bool testing { get; set; }
    }

}
      
