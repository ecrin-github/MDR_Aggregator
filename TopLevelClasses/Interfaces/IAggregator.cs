using System;
using System.Collections.Generic;
using System.Text;

namespace MDR_Aggregator
{
    public interface IAggregator
    {
        int AggregateData(Options opts);
    }
}
