# MDR_Aggregator
Combines data from different sources, each in a separate database, into the central mdr database.

The program takes all the data within the ad tables in the various source databases and loads it to central tables within the MDR database, dealing with multiple entries for studies and creating the link data information between studies and data objects. The aggregated data is held within tables in the st (study), ob (object) and nk (links) schemas. A fourth schema, 'core' is then populated as a direct import from the others, to provide a single simplified MDR dataset whose data can be more easily exported to other systems and / or used as the target of an API. <br/>
Note that the aggregation process starts from scratch each time - there is no attempt to edit existing data. All tables are re-created during the main aggregation processes. This is to simplify the process and make the system easier to maintain.<br/><br/>
The program represents the fifth and final stage in the 5 stage MDR extraction process:<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Download => Harvest => Import => Coding => **Aggregation**<br/><br/>
For a much more detailed explanation of the extraction process,and the MDR system as a whole, please see the project wiki (landing page at https://ecrin-mdr.online/index.php/Project_Overview).<br/>
In particular, for the Aggregation process, please see:<br/>
https://ecrin-mdr.online/index.php/Aggregating_Data and linked pages.

## Parameters and Usage
The system can take the following parameters:<br/>
**-D:** as a flag. Indicates that data should be imported from source systems and aggregate st, ob, nk tables constructed. <br/>
**-C:** as a flag. Indicates that the core (MDR) tables should be created and filled from the aggregate tables.<br/>
**-J:** as a flag. Indicates json fields should be constructed from the core table data.<br/>
**-S:** as a flag. Summarises record numbers, of each sort, in different sources and in the summary and core tables<br/>
**-T:** as a flag. Carry out D, C, J and S but using test data only, in the test database<br/>
Routine usage, as in the scheduled extraction process, would be to do each of D, C, J and S in succession. <br/>
The flags can be used in combination, but if they are the operations are always carried out in the order D, C, J and finally S <br/>

## Dependencies
The program is written in .Net 7.0. <br/>
It uses the following Nuget packages:
* CommandLineParser 2.9.1 - to carry out initial processing of the CLI arguments
* Npgsql 7.0.0, Dapper 2.0.123 and Dapper.contrib 2.0.78 to handle database connectivity
* PostgresSQLCopyHelper 2.8.0 to support bulk insert of data into Postgres
* Microsoft.Extensions.Configuration 7.0.0, .Configuration.Json 7.0.0, and .Hosting 7.0.0 to read the json settings file and support the initial host setup.

## Provenance
* Author: Steve Canham
* Organisation: ECRIN (https://ecrin.org)
* System: Clinical Research Metadata Repository (MDR)
* Project: EOSC Life
* Funding: EU H2020 programme, grant 824087
