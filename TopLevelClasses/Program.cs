using System.Reflection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using MDR_Aggregator;

// Set up file based configuration environment.

string assemblyLocation = Assembly.GetExecutingAssembly().Location;
string? basePath = Path.GetDirectoryName(assemblyLocation);
if (string.IsNullOrWhiteSpace(basePath))
{
    return -1;
}

var configFiles = new ConfigurationBuilder()
    .SetBasePath(basePath)
    .AddJsonFile("appsettings.json")
    .AddJsonFile($"appsettings.{Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")}.json", true)
    .Build();

// Set up the host for the app, adding the services used in the system to support DI

IHost host = Host.CreateDefaultBuilder()
     .UseContentRoot(basePath)
     .ConfigureAppConfiguration(builder =>
     {
         builder.AddConfiguration(configFiles);
     })
     .ConfigureServices((services) =>
     {
         services.AddSingleton<ICredentials, Credentials>();
         services.AddSingleton<ILoggingHelper, LoggingHelper>();
         services.AddSingleton<IMonDataLayer, MonDataLayer>();
     })
     .Build();

// Create the logging helper and monitor repository singleton instances.
// The logger takes the args as an object array in order to include them in the log title.
// The monitor repo also includes DB credentials if required elsewhere.

ILoggingHelper loggingHelper = ActivatorUtilities.CreateInstance<LoggingHelper>(host.Services);
loggingHelper.OpenFile(args);
IMonDataLayer monDataLayer = ActivatorUtilities.CreateInstance<MonDataLayer>(host.Services);

// Check the command line arguments to ensure they are valid. If they are,
// start the program by instantiating the aggregator object and telling it to run. 

ParametersChecker paramChecker = new(loggingHelper);
ParamsCheckResult paramsCheck = paramChecker.CheckParams(args);
if (paramsCheck.ParseError || paramsCheck.ValidityError)
{
    return -1;   // End program, parameter errors should have been logged
}

// Should be able to proceed - opts are known to be non-null and valid.

try
{
    Options opts = paramsCheck.Pars!;
    Aggregator aggregator = new(loggingHelper, monDataLayer);
    aggregator.AggregateData(opts);
    return 0;
}

catch (Exception e)
{
    // If an error bubbles up to here there is an unexpected issue with the code.
    // A file should normally have been created (but just in case...).
    
    loggingHelper.LogHeader("UNHANDLED EXCEPTION");
    string message = (e.InnerException is null) ? e.Message
        : e.Message + "\nInnerException Message:\n" + e.InnerException.Message;
    loggingHelper.LogCodeError("MDR_Aggregator application aborted", message, e.StackTrace);
    loggingHelper.CloseLog();
    return -1;

}
