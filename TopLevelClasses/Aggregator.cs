namespace MDR_Aggregator;
 
public class Aggregator
{
    private readonly ILoggingHelper _loggingHelper;
    private readonly IMonDataLayer _monDatalayer;
    private readonly ICredentials _credentials;
    
    private readonly int agg_event_id;

    public Aggregator(ILoggingHelper loggingHelper, IMonDataLayer monDatalayer)
    {
        _loggingHelper = loggingHelper;
        _monDatalayer = monDatalayer;
        _credentials = monDatalayer.Credentials;
        agg_event_id = _monDatalayer.GetNextAggEventId();
    }
    
    public int AggregateData(Options opts)
    {
        _loggingHelper.LogCommandLineParameters(opts);

        if (opts.transfer_data)
        {
            // Obtain connection string to the database where aggregation will occur - the 'aggs'.
            // aggregating database. Set up the context DB as two schemas of foreign
            // tables (ctx & lup), 
                     
            string agg_conn_string = _credentials.GetConnectionString("aggs");
            List<string> context_schemas = new(){"ctx", "lup"};
            _monDatalayer.SetUpTempFTWs(_credentials, agg_conn_string, "nk", "context", context_schemas);
            _loggingHelper.LogLine("Context data established as FTWs in aggs DB");

            // Derive a new table of inter-study relationships - First get a list of all
            // the study sources and ensure it is sorted correctly, then use the study link
            // builder to create a record of all current study - study links. At the end include
            // additional links that have been identified using non-registry identifiers.
          
            List<Source> sources = _monDatalayer.RetrieveDataSources()
                                       .OrderBy(s => s.preference_rating).ToList();
            _loggingHelper.LogLine("Sources obtained");
            StudyLinkBuilder slb = new StudyLinkBuilder(_monDatalayer, _loggingHelper, 
                                                        _credentials, agg_conn_string);
            slb.CollectStudyStudyLinks(sources);
            _loggingHelper.LogBlank();
            slb.CheckStudyStudyLinks(sources);
            _loggingHelper.LogBlank();
            slb.ProcessStudyStudyLinks();
            _loggingHelper.LogLine("Study-study links identified");
            _loggingHelper.LogBlank();            
            
            // Only then establish new tables and construct a new aggregation event record.

            AggregationEvent agg_event = new AggregationEvent(agg_event_id);

            _monDatalayer.DeleteSameEventDBStats(agg_event.id);  // in case needed            
            SchemaBuilder sb = new SchemaBuilder(agg_conn_string);
            sb.BuildNewStudyTables();
            sb.BuildNewObjectTables();
            _loggingHelper.LogLine("Study, object and link aggregate tables recreated");
            _loggingHelper.LogBlank();

            
            // Loop through the study sources (in preference order).
            // In each case establish and then drop the source AD tables as an FTW.

            _loggingHelper.LogHeader("Data Transfer");
            int num_studies_imported = 0;
            int num_objects_imported = 0;
            
            foreach (Source source in sources)
            {
                // Schema name references the ad tables in a source FTW - i.e. <db name>_ad.
                // Credentials are used here to also get the connection string for the source database. 
                
                string db_name = source.database_name!;
                source.db_conn = _credentials.GetConnectionString(db_name);
                List<string> source_schemas = new List<string>{"ad"};
                _monDatalayer.SetUpTempFTWs(_credentials, agg_conn_string, "nk", db_name, source_schemas);
                string ftw_schema_name = $"{db_name}_ad";

                _loggingHelper.LogStudyHeader("Aggregating", db_name);
                DataTransferBuilder dtb = new DataTransferBuilder(source, ftw_schema_name, 
                                             agg_conn_string, _monDatalayer, _loggingHelper);
                SourceSummary srce_summary = new (agg_event_id, db_name);
                
                if (source.has_study_tables is true)
                {
                    _loggingHelper.LogHeader("Process study Ids");
                    dtb.ProcessStudyIds();
                    _loggingHelper.LogHeader("Transfer study data");
                    num_studies_imported += dtb.TransferStudyData(srce_summary);
                    _loggingHelper.LogHeader("Process object Ids");
                    dtb.ProcessStudyObjectIds();
                }
                else
                {
                    _loggingHelper.LogHeader("Process object Ids");
                    dtb.ProcessStandaloneObjectIds(); // for now, just PubMed and BBMRI
                }
                
                _loggingHelper.LogHeader("Transfer object data");
                num_objects_imported += dtb.TransferObjectData(srce_summary);

                // store summary record, and remove FTW, for this source.
                
                dtb.StoreSourceSummaryStatistics(srce_summary);
                _monDatalayer.DropTempFTWs(agg_conn_string, db_name, source_schemas);
            }
            
            // Get the ICD data - simpler if all condition data to be added first
            // So this is done after the loop through the various sources

            slb.LoadStudyICDs(agg_conn_string);

            //Use the study groups data to insert additional study_relationship records.

            slb.AddStudyStudyRelationshipRecords();

            // Update and store aggregation event record.

            agg_event.num_studies_imported = num_studies_imported;
            agg_event.num_objects_imported = num_objects_imported;
            agg_event.num_total_studies = _monDatalayer.GetAggregateRecNum("study_ids", "nk");
            agg_event.num_total_objects = _monDatalayer.GetAggregateRecNum("data_object_ids", "nk");
            agg_event.num_total_study_object_links = _monDatalayer.GetAggregateRecNum("data_object_ids", "nk");
            _monDatalayer.StoreAggregationEvent(agg_event);
  
            // Also store summary statistics for 1-to-1 and 1-to-n linked records, before dropping FTWs
            
            slb.StoreStudyLinkStatistics(agg_event.id);
            _monDatalayer.DropTempFTWs(agg_conn_string, "context", context_schemas);
        }           


        if (opts.create_core)
        {
            // Obtain connection strings to the final core schema in the mdr database.
            // Then create FTW tables for source aggs schemas and mon (sf) monitoring data schema,
            // before using the core builder class to recreate the core tables.

            string core_conn_string = _credentials.GetConnectionString("mdr");
            List<string> aggs_schemas = new(){"st", "ob", "nk"};
            _monDatalayer.SetUpTempFTWs(_credentials, core_conn_string, "core", "aggs", aggs_schemas);
            List<string> mon_schemas = new(){"sf"};
            _monDatalayer.SetUpTempFTWs(_credentials, core_conn_string, "core", "mon", mon_schemas);
            _loggingHelper.LogLine("FTW tables recreated");

            CoreBuilder cb = new CoreBuilder(core_conn_string, _loggingHelper);
            cb.BuildNewCoreTables();
            _loggingHelper.LogLine("Core tables recreated");

            // Transfer data to core tables
            
            int last_agg_event_id = _monDatalayer.GetLastAggEventId();
            _monDatalayer.DeleteSameEventSummaryStats(last_agg_event_id);  // ensure one record per event!
            
            CoreSummary core_summ = new (last_agg_event_id);
            CoreTransferBuilder ctb = new CoreTransferBuilder(core_conn_string, _monDatalayer, _loggingHelper);
            _loggingHelper.LogHeader("Transferring study data");
            ctb.TransferCoreStudyData(core_summ);
            _loggingHelper.LogHeader("Transferring object data");
            ctb.TransferCoreObjectData(core_summ);
            _loggingHelper.LogHeader("Transferring link data");
            ctb.TransferCoreLinkData(core_summ);
            
            // Generate data provenance strings. 
            
            ctb.GenerateProvenanceData(); 
            _loggingHelper.LogHeader("Generating provenance data");

            // Store summary statistics - needs a reference to context schemas
            
            List<string> context_schemas = new(){"lup", "ctx"};
            _monDatalayer.SetUpTempFTWs(_credentials, core_conn_string, "core", "context", context_schemas);
            ctb.StoreCoreSummaryStatistics(core_summ);
            ctb.StoreDataObjectStatistics(last_agg_event_id);
            _monDatalayer.DropTempFTWs(core_conn_string, "context", context_schemas);
            
            // Drop FTW schemas.
            
            _monDatalayer.DropTempFTWs(core_conn_string, "aggs", aggs_schemas);
            _monDatalayer.DropTempFTWs(core_conn_string, "mon", mon_schemas);
            _loggingHelper.LogLine("FTW tables dropped");
        }
        
        if (opts.do_indexes)
        {
            // There are two aspects of setting up search data. One is to create searchable
            // tables to respond to queries and filters. The other is to set up
            // suitable JSON fields to return to the UI in response to those queries.
            // The lup schemas from context, as well as the aggs schemas, are required.
            
            string core_conn_string = _credentials.GetConnectionString("mdr");
            List<string> aggs_schemas = new(){"st", "ob", "nk"};
            _monDatalayer.SetUpTempFTWs(_credentials, core_conn_string, "core", "aggs", aggs_schemas);
            List<string> ctx_schemas = new(){"lup"};
            _monDatalayer.SetUpTempFTWs(_credentials, core_conn_string, "core", "context", ctx_schemas);
            _loggingHelper.LogLine("FTW tables recreated");
            
            // Initial task is to create JSON versions of the object data (as part of this will be
            // incorporated into study json and tables, from where it can be returned when necessary).
            // Querying and filtering is almost always done against studies rather than objects - the
            // exception being PMIDs and even then it is studies that are returned.
            // Preparing object data is therefore focused on creating the JSON required. The routine
            // below generates both a 'full' JSON image of each object.
            
            CoreSearchBuilder csb = new CoreSearchBuilder(core_conn_string, _loggingHelper);
            
            //_loggingHelper.LogHeader("Creating JSON object data");
            csb.CreateJSONObjectData();  
                
            // Tables are then created to hold data for querying in various ways
            
            _loggingHelper.LogHeader("Setting up study search tables");

            csb.CreateIdentifierSearchDataTable();
            csb.CreatePMIDSearchDataTable();
            csb.CreateCountrySearchDataTable();
            csb.CreateObjectTypeSearchDataTable();

            csb.CreateLexemeSearchDataTable();

            // The study data json objects are then created

            _loggingHelper.LogHeader("Creating JSON study data");
            csb.CreateJSONStudyData();
            
            _loggingHelper.LogHeader("Creating data in search tables");
            csb.AddStudyJsonToSearchTables();
            csb.TileLexemeTable();
            csb.ClusterSearchTables();
            csb.SwitchToNewTables();
            
            // Drop FTW schemas.
            
            _monDatalayer.DropTempFTWs(core_conn_string, "aggs", aggs_schemas);
            _monDatalayer.DropTempFTWs(core_conn_string, "context", ctx_schemas);
            _loggingHelper.LogLine("FTW tables dropped");
        }


        if (opts.do_statistics)
        {
            // Instantiate a Statistics Builder class and use that to generate the 
            // three main sets of statistics. To be run after creation of a new core database.
            
            StatisticsBuilder sb = new StatisticsBuilder(_monDatalayer, _loggingHelper);
            sb.WriteOutCoreDataSummary();
            sb.WriteOutCoreObjectTypes();
            sb.WriteOutSourceDataTableSummaries();
            sb.WriteOutStudyLinkData();
        }
        

        if (opts.do_iec)
        {
            // Get connection string for destination DB and re-establish IEC tables.
            // Set up a new IEC Aggravation Event record.
            
            string iec_conn_string = _credentials.GetConnectionString("iec");
            IECTransferrer ieh = new IECTransferrer(iec_conn_string, _loggingHelper);
            
            ieh.BuildNewIECTables();
            _loggingHelper.LogLine("IEC tables recreated");

            int iec_agg_id = _monDatalayer.GetNextIECAggEventId();
            IECAggregationEvent iec_agg_event = new (iec_agg_id);
            
            // Get a list of sources that have IEC data, and transfer the data from each one. Use the 
            // source's db_conn property for the FTW schema name, which will be in the form <db_name>_ad.
            
            IEnumerable<Source> sources = _monDatalayer.RetrieveIECDataSources();
            _loggingHelper.LogLine("Sources obtained");
            foreach (Source source in sources)
            {
                string db_name = source.database_name!;
                List<string> source_schemas = new List<string>{"ad"};
                _monDatalayer.SetUpTempFTWs(_credentials, iec_conn_string, "dv", db_name, source_schemas);
                source.db_conn = $"{db_name}_ad";
                _loggingHelper.LogStudyHeader("Aggregating", db_name);
                Int64 res = ieh.TransferIECData(source);
                _monDatalayer.StoreSourceIECData(iec_agg_id, source, res);
                _monDatalayer.DropTempFTWs(iec_conn_string, db_name, source_schemas);
            }  
            
            // update statistics about aggregation and store the final aggregation record
            
            _monDatalayer.UpdateIECAggregationEvent(iec_agg_event, iec_conn_string);
            _monDatalayer.StoreIECAggregationEvent(iec_agg_event);
            
            // Update aggregated IEC records with study Ids from aggs DB still required.
            // First import the key study data from the aggs db, by using two aggs schemas
            // as FTWs. Then update the IEC records, first from lup tables, then internally.
            
            _loggingHelper.LogHeader("Adding study data");
            List<string> agg_schemas = new List<string>{"nk", "st"};
            _monDatalayer.SetUpTempFTWs(_credentials, iec_conn_string, "dv", "aggs", agg_schemas);
            ieh.TransferKeyStudyIdData("aggs_nk");
            ieh.TransferKeyStudyRecordData("aggs_st");
            _loggingHelper.LogLine("Key study data imported from the aggs database");
            _monDatalayer.DropTempFTWs(iec_conn_string, "aggs", agg_schemas);
            
            _loggingHelper.LogHeader("Decoding Study data");
            ieh.DecodeStudyData();
            _loggingHelper.LogLine("Key study data updated with lookup decodes");
            
            _loggingHelper.LogHeader("Updating IEC data");
            ieh.UpdateIECWithStudyIds();
            _loggingHelper.LogLine("IECdata updated with study ids");
        }
        
        _loggingHelper.CloseLog();
        return 0;
        
    }
}

