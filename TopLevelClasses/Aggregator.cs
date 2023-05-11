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

            // Obtain connection strings to the database where aggregation will occur and the final location
            // of the 'core' mdr database - the mdr DB.  If not testing, agg_conn_string points to the 'aggs'
            // aggregating database (if testing it points to the test database). Then set up the context DB
            // as two schemas of foreign tables (ctx & lup) as that data is used in several places. 
            
            string agg_conn_string = _credentials.GetConnectionString("aggs", opts.testing);
            string core_conn_string = _credentials.GetConnectionString("mdr", opts.testing);
            _monDatalayer.SetUpTempContextFTWs(_credentials, agg_conn_string);
            _monDatalayer.SetUpTempContextFTWs(_credentials, core_conn_string);
            
            if (opts.transfer_data)
            {
                // Set up the context DB as two schemas of foreign tables (ctx & lup) and then  
                // establish new tables for the three schemas st, ob, nk
                
                SchemaBuilder sb = new SchemaBuilder(agg_conn_string);
                sb.BuildNewStudyTables();
                sb.BuildNewObjectTables();
                sb.BuildNewLinkTables();
                _loggingHelper.LogLine("Study, object and link aggregate tables recreated");
                _loggingHelper.LogLine("");
                
                // construct the aggregation event record
                AggregationEvent agg_event = new AggregationEvent(agg_event_id);

                // Derive a new table of inter-study relationships - First get a list of all
                // the study sources and ensure it is sorted correctly.
              
                List<Source> sources = _monDatalayer.RetrieveDataSources()
                                           .OrderBy(s => s.preference_rating).ToList();
                _loggingHelper.LogLine("Sources obtained");

                // Then use the study link builder to create a record of all current study - study links

                StudyLinkBuilder slb = new StudyLinkBuilder(_loggingHelper, _credentials, 
                                                          agg_conn_string, opts.testing);
                slb.CollectStudyStudyLinks(sources);
                _loggingHelper.LogLine("");
                slb.CheckStudyStudyLinks(sources);
                _loggingHelper.LogLine("");
                slb.ProcessStudyStudyLinks();
                _loggingHelper.LogLine("Study-study links identified");
                _loggingHelper.LogLine("");

                // Start the data transfer process. Loop through the study sources (in preference order).
                // In each case establish and then drop the source AD tables as an FTW.

                _loggingHelper.LogHeader("Data Transfer");
                int num_studies_imported = 0;
                int num_objects_imported = 0;
                if (opts.testing)
                {
                    // ensure all AD tables are present for use in the test db
                    //_test_repo.BuildNewADTables();
                }

                foreach (Source source in sources)
                {
                    string db_name = source.database_name!;
                    string source_conn_string = _credentials.GetConnectionString(db_name, opts.testing);
                    source.db_conn = source_conn_string;

                    // in normal (non-testing) environment, schema name references the ad tables in a source FTW
                    // - i.e. <db name>_ad. Credentials are used here to get the connection string for the source
                    // database. In a testing environment source schema name will simply be 'ad', and the
                    // ad table data all has to be transferred from adcomp before each source transfer...to revise!

                    string ftw_schema_name;
                    if (opts.testing)
                    {
                        ftw_schema_name = "ad";
                        //_test_repo.TransferADTableData(source);
                    }
                    else
                    {
                        ftw_schema_name = _monDatalayer.SetUpTempCoreFTW(_credentials, db_name, agg_conn_string);
                    }

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
                    
                    // store summary record for that database...
                    if (!opts.testing)
                    {
                        tb.StoreSourceSummaryStatistics(srce_summ);
                    }

                    _monDatalayer.DropTempCoreFTW(db_name, agg_conn_string);
                }

                // Also use the study groups data to insert additional study_relationship records.
                
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
                // Create core tables and establish necessary additional foreign tables for transfer.

                CoreBuilder cb = new CoreBuilder(core_conn_string, _loggingHelper);
                cb.BuildNewCoreTables();
                _loggingHelper.LogLine("Core tables recreated");
                _monDatalayer.SetUpTempAggsFTW(_credentials, core_conn_string);
                _monDatalayer.SetUpTempCoreFTW(_credentials, "mon", core_conn_string);
                _loggingHelper.LogLine("FTW tables recreated");
                
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
                
                // Store summary statistics
 
                ctb.StoreCoreSummaryStatistics(core_summ);
                ctb.StoreDataObjectStatistics(last_agg_event_id);
                ctb.StoreStudyStudyLinkStatistics(last_agg_event_id);
                
                // Drop FTW schemas.
                
                _monDatalayer.DropTempAggsFTW(core_conn_string);
                _monDatalayer.DropTempCoreFTW("mon", core_conn_string);
                _loggingHelper.LogLine("FTW tables dropped");
            }

            
            if (opts.do_indexes)
            {
                // Set up study search data.

                CoreSearchBuilder csb = new CoreSearchBuilder(core_conn_string, _loggingHelper);
                _loggingHelper.LogHeader("Setting up Study Text Search data");
                csb.CreateStudyFeatureSearchData();
                csb.CreateStudyObjectSearchData();
                csb.CreateStudyTextSearchData();
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
                // Get connection string for destination DB and re-establish IEC tables 
                
                string dest_conn_string = _credentials.GetConnectionString("iec", opts.testing);
                IECTransferrer ieh = new IECTransferrer(dest_conn_string, _loggingHelper);
                ieh.BuildNewIECTables();
                _loggingHelper.LogLine("IEC tables recreated");
                AggregationEvent agg_event = new AggregationEvent(agg_event_id);

                // Get a list of sources that have IEC data, and transfer the data from each one. Use the 
                // source's db_conn property for the FTW schema name, which will be in the form <db_name>_ad.
                
                IEnumerable<Source> sources = _monDatalayer.RetrieveIECDataSources();
                _loggingHelper.LogLine("Sources obtained");
                foreach (Source source in sources)
                {
                    source.db_conn = _monDatalayer.SetUpTempIECFTW(_credentials, 
                                         source.database_name!, dest_conn_string);
                    ieh.TransferIECData(source);
                    _monDatalayer.DropTempIECFTW(source.database_name!, dest_conn_string);
                }  
                
                // update statistics about aggregation (?)
                
                _monDatalayer.StoreAggregationEvent(agg_event);
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

            _monDatalayer.DropTempContextFTWs(core_conn_string);
            _monDatalayer.DropTempContextFTWs(agg_conn_string);
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

