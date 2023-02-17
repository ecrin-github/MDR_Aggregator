# MDR_Aggregator
Combines data from different sources, each in a separate database, into the central mdr database.

The program takes all the data within the ad tables in the various source databases and loads it to central tables within the MDR database, dealing with multiple entries for studies and creating the link data information between studies and data objects. The aggregated data is held within tables in the st (study), ob (object) and nk (links) schemas. A fourth schema, 'core' is then populated as a direct import from the others, to provide a single simplified MDR datasetwhose data can be moreeasily exported to other systems and / or used as the target of an API. <br/>
Note that the aggregation process starts from scratch each time - there is no attempt to edit existing data. All tables are re-created during the main aggregation processes. This is to simplify the process and make the system easier to maintain.<br/><br/>
The program represents the fifth and final stage in the 5 stage MDR extraction process:<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Download => Harvest => Import => Coding => **Aggregation**<br/><br/>
For a much more detailed explanation of the extraction process,and the MDR system as a whole, please see the project wiki (landing page at https://ecrin-mdr.online/index.php/Project_Overview).<br/>
In particular, for the Aggregation process, please see:<br/>
https://ecrin-mdr.online/index.php/Aggregating_Data and linked pages.

## Parameters and Usage
The system can take the following parameters:<br/>
**-s:** expects to be followed by a list of integer source ids, each representing a data source within the system. The data is obtained and added to the store of JSON source files for that source.<br/>
**-t:** followed by an integer. Indicates the type of harvest to be carried out. Types available vary for different source systems, and each type is linked with possible pre-requisites, e.g. cut-off date, start and end pages, filter to be applied, etc. - see below for specific details. The system checks for the presence of appropriate suitable pre-requisites, for the specified type, before proceeding.<br/>
<br/>
<br/>
Routine usage, as in the scheduled extraction process,  <br/>

## Dependencies
The program used the Nuget packages:
* CommandLineParser - to carry out initial processing of the CLI arguments
* Npgsql, Dapper and Dapper.contrib to handle database connectivity
* Microsoft.Extensions.Configuration, .Configuration.Json, and .Hosting to read the json settings file and support the initial host setup.

## Provenance
* Author: Steve Canham
* Organisation: ECRIN (https://ecrin.org)
* System: Clinical Research Metadata Repository (MDR)
* Project: EOSC Life
* Funding: EU H2020 programme, grant 824087
