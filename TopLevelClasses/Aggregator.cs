namespace MDR_Aggregator;
 
public class Aggregator
{
    private readonly ILoggingHelper _loggingHelper;
    private readonly IMonDataLayer _monDatalayer;
    private readonly ICredentials _credentials;
    
    private readonly int agg_event_id;

    public Aggregator(ILoggingHelper logginghelper, IMonDataLayer monDatalayer)
    {
        _loggingHelper = logginghelper;
        _monDatalayer = monDatalayer;
        _credentials = monDatalayer.Credentials;
        agg_event_id = _monDatalayer.GetNextAggEventId();
    }
    
    public int AggregateData(Options opts)
    {
        try
        {
            _loggingHelper.LogCommandLineParameters(opts);

            if (opts.transfer_data)
            {
                // Obtain connection string to the database where aggregation will occur - the 'aggs'.
                // aggregating database (unless testing). Set up the context DB as two schemas of foreign
                // tables (ctx & lup), and finally establish new tables for the three schemas st, ob, nk
                // and construct a new aggregation event record.
                         
                string agg_conn_string = _credentials.GetConnectionString("aggs", opts.testing);
                List<string> source_schemas = new(){"ctx", "lup"};
                _monDatalayer.SetUpTempFTWs(_credentials, agg_conn_string, "nk", "context", source_schemas);
                _loggingHelper.LogLine("Context data established as FTWs in aggs DB");
                SchemaBuilder sb = new SchemaBuilder(agg_conn_string);
                sb.BuildNewStudyTables();
                sb.BuildNewObjectTables();
                sb.BuildNewLinkTables();
                _loggingHelper.LogLine("Study, object and link aggregate tables recreated");
                _loggingHelper.LogLine("");
                AggregationEvent agg_event = new AggregationEvent(agg_event_id);

                // Derive a new table of inter-study relationships - First get a list of all
                // the study sources and ensure it is sorted correctly, then use the study link
                // builder to create a record of all current study - study links
              
                List<Source> sources = _monDatalayer.RetrieveDataSources()
                                           .OrderBy(s => s.preference_rating).ToList();
                _loggingHelper.LogLine("Sources obtained");
                StudyLinkBuilder slb = new StudyLinkBuilder(_loggingHelper, _credentials, 
                                                          agg_conn_string, opts.testing);
                slb.CollectStudyStudyLinks(sources);
                _loggingHelper.LogLine("");
                slb.CheckStudyStudyLinks(sources);
                _loggingHelper.LogLine("");
                slb.ProcessStudyStudyLinks();
                _loggingHelper.LogLine("Study-study links identified");
                _loggingHelper.LogLine("");

                // Loop through the study sources (in preference order).
                // In each case establish and then drop the source AD tables as an FTW.

                _loggingHelper.LogHeader("Data Transfer");
                int num_studies_imported = 0;
                int num_objects_imported = 0;
                foreach (Source source in sources)
                {
                    // in normal (non-testing) environment, schema name references the ad tables in a source FTW
                    // - i.e. <db name>_ad. Credentials are used here to also get the connection string for the 
                    // source database. Process in a testing environment to be revised!
                    
                    string db_name = source.database_name!;
                    source.db_conn = _credentials.GetConnectionString(db_name, opts.testing);
                    source_schemas = new List<string>{"ad"};
                    _monDatalayer.SetUpTempFTWs(_credentials, agg_conn_string, "nk", db_name, source_schemas);
                    string ftw_schema_name = $"{db_name}_ad";

                    _loggingHelper.LogStudyHeader("Aggregating", db_name);
                    DataTransferBuilder tb = new DataTransferBuilder(source, ftw_schema_name, 
                                                 agg_conn_string, _monDatalayer, _loggingHelper);
                    SourceSummary srce_summ = new (agg_event_id, db_name);
                    
                    if (source.has_study_tables is true)
                    {
                        _loggingHelper.LogHeader("Process study Ids");
                        tb.ProcessStudyIds();
                        _loggingHelper.LogHeader("Transfer study data");
                        num_studies_imported += tb.TransferStudyData(srce_summ);
                        tb.ProcessStudyObjectIds();
                    }
                    else
                    {
                        tb.ProcessStandaloneObjectIds(); // for now, just PubMed
                    }
                    _loggingHelper.LogHeader("Transfer object data");
                    num_objects_imported += tb.TransferObjectData(srce_summ);
                    
                    // store summary record, and remove FTW, for this source.
                    
                    tb.StoreSourceSummaryStatistics(srce_summ);
                    _monDatalayer.DropTempFTWs(agg_conn_string, db_name, source_schemas);
                }

                // Use the study groups data to insert additional study_relationship records.
                
                slb.AddStudyStudyRelationshipRecords();

                // Update aggregation event record.

                agg_event.num_studies_imported = num_studies_imported;
                agg_event.num_objects_imported = num_objects_imported;
                agg_event.num_total_studies = _monDatalayer.GetAggregateRecNum("studies", "st", agg_conn_string);
                agg_event.num_total_objects = _monDatalayer.GetAggregateRecNum("data_objects", "ob", agg_conn_string);
                agg_event.num_total_study_object_links = _monDatalayer.GetAggregateRecNum("data_object_ids", 
                                                                                      "nk", agg_conn_string);

                if (!opts.testing)
                {
                    _monDatalayer.StoreAggregationEvent(agg_event);
                }
            }


            if (opts.create_core)
            {
                // Obtain connection strings to the final core schema in the mdr database.
                // Then create FTW tables for source aggs schemas and mon (sf) monitoring data schema,
                // before using the core builder class to recreate the core tables.

                string core_conn_string = _credentials.GetConnectionString("mdr", opts.testing);
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
                ctb.StoreStudyStudyLinkStatistics(last_agg_event_id);
                _monDatalayer.DropTempFTWs(core_conn_string, "context", context_schemas);
                
                // Drop FTW schemas.
                
                _monDatalayer.DropTempFTWs(core_conn_string, "aggs", aggs_schemas);
                _monDatalayer.DropTempFTWs(core_conn_string, "mon", mon_schemas);
                _loggingHelper.LogLine("FTW tables dropped");
            }

            
            if (opts.do_indexes)
            {
                // Set up study search data. The lup schemas from context, as well as the
                // aggs databases required to set up these tables.
                
                string core_conn_string = _credentials.GetConnectionString("mdr", opts.testing);
                List<string> aggs_schemas = new(){"st", "ob", "nk"};
                _monDatalayer.SetUpTempFTWs(_credentials, core_conn_string, "core", "aggs", aggs_schemas);
                List<string> ctx_schemas = new(){"lup"};
                _monDatalayer.SetUpTempFTWs(_credentials, core_conn_string, "core", "context", ctx_schemas);
                _loggingHelper.LogLine("FTW tables recreated");
                
                CoreSearchBuilder csb = new CoreSearchBuilder(core_conn_string, _loggingHelper);
                _loggingHelper.LogHeader("Setting up Study Text Search data");
                
                //csb.CreateStudySearchData();
                //csb.CreateStudyFeatureData();
                //csb.CreateStudyHasObjectData();
                //csb.CreateStudyCompositeFieldData();
                //csb.CreateIdentifierSearchData();
                //csb.CreatePMIDSearchData();
                //csb.CreateObjectSearchData();
                //csb.CreateLexemeSearchData();
                //csb.CompleteStudySearchData();
                csb.AddStudyJsonToSearchTables();
                
                // Drop FTW schemas.
                
                _monDatalayer.DropTempFTWs(core_conn_string, "aggs", aggs_schemas);
                _monDatalayer.DropTempFTWs(core_conn_string, "context", ctx_schemas);
                _loggingHelper.LogLine("FTW tables dropped");
            }


            if (opts.do_statistics)
            {
                // Find last agg event id. Use that to locate and write out most recent
                // aggregate summary record to log, and then to obtain and write out breakdown 
                // of data objects, and study-study links.
               
                CoreSummary? core_summ = _monDatalayer.GetLatestCoreSummary();
                if (core_summ is not null)
                {
                    _loggingHelper.LogHeader("TABLE RECORD NUMBERS");
                    _loggingHelper.LogSummaryStatistics(core_summ);
                }

                List<AggregationObjectNum>? objectNumbers = _monDatalayer.GetLatestObjectNumbers();
                if (objectNumbers?.Any() is true)
                {
                    int small_cats = 0;
                    _loggingHelper.LogHeader("OBJECT TYPE NUMBERS");
                    foreach (AggregationObjectNum a in objectNumbers)
                    {
                        if (a.number_of_type >= 30)
                        {
                            _loggingHelper.LogLine($"\t{a.object_type_id}\t{a.object_type_name}: {a.number_of_type:n0}");
                        }
                        else
                        {
                            small_cats += a.number_of_type;
                        }
                    }
                    _loggingHelper.LogLine($"\tXX\tTotal of smaller categories (n<30): {small_cats:n0}");
                }
            }
            

            if (opts.do_iec)
            {
                // Get connection string for destination DB and re-establish IEC tables.
                // Set up a new IEC Aggravation Event record.
                
                string iec_conn_string = _credentials.GetConnectionString("iec", opts.testing);
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
            
            
            if (opts.create_json)
            {
                //string conn_string = _credentials.GetConnectionString("mdr", opts.testing);
                //JSONHelper jh = new JSONHelper(conn_string, _loggingHelper);

                // Create json fields. If tables are to be left as they are, add false as an
                // additional boolean (default = true). if tables are to have further data appended
                // add an integer offset that represents the records to skip (default = 0)

                //_loggingHelper.LogHeader("Creating JSON study data");
                //jh.CreateJSONStudyData();
                //jh.LoopThroughOAStudyRecords();
                //_loggingHelper.LogHeader("Creating JSON object data");
                //jh.CreateJSONObjectData();
                //jh.LoopThroughOAObjectRecords();
            }

            
            _loggingHelper.CloseLog();
            return 0;
        }

        catch(Exception e)
        {
            _loggingHelper.LogCodeError("Error detected in main Aggregator class", e.Message, e.StackTrace);
            throw;
        }
    }
}

