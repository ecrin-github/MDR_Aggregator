namespace MDR_Aggregator
{
    internal interface IParametersChecker
    {
        Options ObtainParsedArguments(string[] args);
        bool ValidArgumentValues(Options opts);
    }
}
