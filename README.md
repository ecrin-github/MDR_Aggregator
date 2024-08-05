# MDR_Aggregator
It combines data from different sources, each in a separate database, into the central MDR database.

The program takes all the data within the ad tables in the various source databases.

It loads it to central tables within the MDR database, dealing with multiple entries for studies and creating the link 
data information between studies and data objects.

The aggregated data is held within tables in the `st` (study), `ob` (object) and `nk` (links) schemas. A fourth schema, 
'core', is then populated as a direct import from the others to provide a single simplified MDR dataset whose data can 
be more easily exported to other systems and/or used as the target of an API.


Note that the aggregation process starts from scratch each time - there is no attempt to edit existing data.

All tables are re-created during the main aggregation processes to simplify the process and facilitate system maintainability.

The program represents the fifth and final stage of the MDR extraction process:

>Download => Harvest => Import => Coding => **Aggregation**

For a much more detailed explanation of the extraction process and the MDR system as a whole, please see the project 
wiki (landing page at https://ecrin-mdr.online/index.php/Project_Overview).

In particular, for the Aggregation process, please see:
- https://ecrin-mdr.online/index.php/Aggregating_Data

and linked pages.


## Parameters and Usage

The table below shows the allowed parameters.

| Parameter | Function                    | Description |
|:----------|:----------------------------|:-------------|
| **-D**   | transfer and aggregate data | Indicates data import from source systems and aggregate st, ob, nk tables constructed.       
| **-C**   | create core table data      | Indicates that the core (MDR) tables should be created and filled from the aggregate tables. 
| **-J**   | create json                 | Indicates JSON fields building from the core table data.                                     
| **-S**   | do statistics               | Summarises record numbers of each sort in different sources and the summary and core tables. 
| **-I**   | do IEC data                 | Aggregates the inclusion / exclusion data into a separate database (`iec`).                       
| **-X**    | Create indexes              | Re-establishes text indexes on title and topic fields, for searching.

As in the scheduled extraction process, routine usage would be to do each of D, C, J and S in succession.
The flags can be combined, but if they are, the operations are always carried out in the order D, C, J and S.

## Dependencies
The program is written in `.Net 7.0`.

It uses the following `Nuget` packages:

- `CommandLineParser 2.9.1` - to carry out the initial processing of the CLI arguments

- `Npgsql 7.0.0`, `Dapper 2.0.123` and `Dapper.contrib 2.0.78` to handle database connectivity

- `PostgreSQLCopyHelper 2.8.0` to support fast bulk inserts into Postgres 

- `Microsoft.Extensions.Configuration 7.0.0`, `.Configuration.Json 7.0.0`, and `.Hosting 7.0.0` to read the json settings 
file and support the initial host setup.


## Provenance
* Author: Steve Canham
* Contributor: Michele Scarlato
* Organisation: ECRIN (https://ecrin.org)
* System: Clinical Research Metadata Repository (MDR)
* Project: EOSC Life
* Funding: EU H2020 programme, grant 824087
