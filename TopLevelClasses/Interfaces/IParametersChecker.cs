using System;
using System.Collections.Generic;
using System.Text;

namespace MDR_Aggregator
{
    internal interface IParametersChecker
    {
        Options ObtainParsedArguments(string[] args);
        bool ValidArgumentValues(Options opts);
    }
}
