# MDR_Aggregator
Combines data from different sources, each in a separate database, into the central mdr database.

The program takes all the data within the ad tables in the various source databases and loads it to central tables within the mdr database, dealing with multiple entries for studies and creating the link data information between studies and data objects. The aggregated data is held within tables in the st (study), ob (object) and nk (links) schemas. A fourth schema, 'core' is then populated as a direct import from the others, to provide a single simplified mdr dataset that can be exported to other systems. <br/>
Note that the aggregation process starts from scratch each time - there is no attempt to edit existing data. All tables are re-created during the main aggregation processes. This is to simplify the process and make the system easier to maintain.<br/><br/>
The program represents the fourth stage in the 4 stage MDR extraction process:<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Download => Harvest => Import => **Aggregation**<br/><br/>
For a much more detailed explanation of the extraction process,and the MDR system as a whole, please see the project wiki (landing page at https://ecrin-mdr.online/index.php/Project_Overview).<br/>
In particular, for the Aggregation process, please see:<br/>
https://ecrin-mdr.online/index.php/Aggregating_Data<br/>
http://ecrin-mdr.online/index.php/Identifying_Links_between_Studies<br/>
and linked pages.

### Provenance
* Author: Steve Canham
* Organisation: ECRIN (https://ecrin.org)
* System: Clinical Research Metadata Repository (MDR)
* Project: EOSC Life
* Funding: EU H2020 programme, grant 824087
