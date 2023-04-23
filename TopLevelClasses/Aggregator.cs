namespace MDR_Aggregator;

public class Aggregator
{
    private readonly ILoggingHelper _loggingHelper;
    private readonly IMonDataLayer _mon_repo;
    private readonly ICredentials _credentials;
    
    private readonly int agg_event_id;

    public Aggregator(ILoggingHelper logginghelper, IMonDataLayer mon_repo)
    {
        _loggingHelper = logginghelper;
        _mon_repo = mon_repo;
        _credentials = mon_repo.Credentials;
        agg_event_id = _mon_repo.GetNextAggEventId();
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
            

            if (opts.transfer_data)
            {
                // Set up the context DB as two schemas of foreign tables (ctx & lup) and then  
                // establish new tables for the three schemas st, ob, nk
                
                _mon_repo.SetUpTempContextFTWs(_credentials, agg_conn_string);
                SchemaBuilder sb = new SchemaBuilder(agg_conn_string);
                sb.BuildNewStudyTables();
                sb.BuildNewObjectTables();
                sb.BuildNewLinkTables();
                _loggingHelper.LogLine("Study, object and link aggregate tables recreated");

                // construct the aggregation event record
                AggregationEvent agg_event = new AggregationEvent(agg_event_id);

                // Derive a new table of inter-study relationships - First get a list of all
                // the study sources and ensure it is sorted correctly.

                List<Source> sources = _mon_repo.RetrieveDataSources()
                                           .OrderBy(s => s.preference_rating).ToList();
                _loggingHelper.LogLine("Sources obtained");

                // Then use the study link builder to create a record of all current study - study links

                StudyLinkBuilder slb = new StudyLinkBuilder(_loggingHelper, _credentials, 
                                                            agg_conn_string, opts.testing);
                slb.CollectStudyStudyLinks(sources);
                slb.CheckStudyStudyLinks(sources);
                slb.ProcessStudyStudyLinks();
                _loggingHelper.LogLine("Study-study links identified");

                // Start the data transfer process. Loop through the study sources
                // (in preference order). In each case establish and then drop the
                // source tables as a foreign table wrapper

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
                    string source_conn_string = _credentials.GetConnectionString(source.database_name!, opts.testing);
                    source.db_conn = source_conn_string;

                    // in normal (non-testing) environment, schema name references the ad tables in a source FTW
                    // - i.e. <db name>_ad. Credentials are used here to get the connection string for the source
                    // database. In a testing environment source schema name will simply be 'ad', and the
                    // ad table data all has to be transferred from adcomp before each source transfer...to revise!

                    string schema_name;
                    if (opts.testing)
                    {
                        schema_name = "ad";
                        //_test_repo.TransferADTableData(source);
                    }
                    else
                    {
                        schema_name = _mon_repo.SetUpTempFTW(_credentials, source.database_name!, agg_conn_string);
                    }

                    _loggingHelper.LogStudyHeader("Aggregating", source.database_name!);
                    DataTransferBuilder tb = new DataTransferBuilder(source, schema_name, 
                                                                     agg_conn_string, _loggingHelper);
                    if (source.has_study_tables is true)
                    {
                        _loggingHelper.LogHeader("Process study Ids");
                        tb.ProcessStudyIds();
                        _loggingHelper.LogHeader("Transfer study data");
                        num_studies_imported += tb.TransferStudyData();
                        tb.ProcessStudyObjectIds();
                    }
                    else
                    {
                        tb.ProcessStandaloneObjectIds(); // for now, just PubMed
                    }
                    _loggingHelper.LogHeader("Transfer object data");
                    num_objects_imported += tb.TransferObjectData();
                    _mon_repo.DropTempFTW(source.database_name!, agg_conn_string);
                }

                // Also use the study groups data to insert additional study_relationship records.
                
                slb.AddStudyStudyRelationshipRecords();

                // Update aggregation event record.

                agg_event.num_studies_imported = num_studies_imported;
                agg_event.num_objects_imported = num_objects_imported;

                agg_event.num_total_studies = _mon_repo.GetAggregateRecNum("studies", "st", agg_conn_string);
                agg_event.num_total_objects = _mon_repo.GetAggregateRecNum("data_objects", "ob", agg_conn_string);
                agg_event.num_total_study_object_links = _mon_repo.GetAggregateRecNum("all_ids_data_objects", 
                                                                                      "nk", agg_conn_string);

                if (!opts.testing)
                {
                    _mon_repo.StoreAggregationEvent(agg_event);
                }
                _mon_repo.DropTempContextFTWs(agg_conn_string);
            }


            if (opts.create_core)
            {
                // Create core tables.
                
                CoreBuilder cb = new CoreBuilder(core_conn_string, _loggingHelper);
                _loggingHelper.LogHeader("Recreating core tables");
                cb.BuildNewCoreTables();
                _loggingHelper.LogLine("Core tables recreated");

                // Transfer data to core tables.
                
                CoreTransferBuilder ctb = new CoreTransferBuilder(core_conn_string, _loggingHelper);
                _loggingHelper.LogHeader("Transferring study data");
                ctb.TransferCoreStudyData();
                _loggingHelper.LogHeader("Transferring object data");
                ctb.TransferCoreObjectData();
                _loggingHelper.LogHeader("Transferring link data");
                ctb.TransferCoreLinkData();

                // Generate data provenance strings. Need an additional temporary FTW link to mon.

                _loggingHelper.LogHeader("Generating provenance data");
                _mon_repo.SetUpTempFTW(_credentials, "mon", core_conn_string);
                ctb.GenerateProvenanceData();
                _mon_repo.DropTempFTW("mon", core_conn_string);
                
                // Set up study search data.

                CoreSearchBuilder csb = new CoreSearchBuilder(core_conn_string, _loggingHelper);
                _loggingHelper.LogHeader("Setting up Study Text Search data");
                csb.CreateStudyFeatureSearchData();
                csb.CreateStudyObjectSearchData();
                csb.CreateStudyTextSearchData();
            }


            if (opts.do_statistics)
            {
                
                int last_agg_event_id = _mon_repo.GetLastAggEventId();
                _mon_repo.SetUpTempFTW(_credentials, "mon", core_conn_string);
                StatisticsBuilder stb = new StatisticsBuilder(last_agg_event_id, _mon_repo, 
                                              _loggingHelper, opts.testing);
                if (!opts.testing)
                {
                    stb.GetStatisticsBySource();
                }
                stb.GetSummaryStatistics();
                _mon_repo.DropTempFTW("mon", core_conn_string);
            }

            
            if (opts.do_iec)
            {
                // Get connection string for destination DB and re-establish IEC tables 
                
                _loggingHelper.LogHeader("Establishing IEC tables");
                //IECSchemaBuilder sb = new SchemaBuilder(dest_conn_string);
                //sb.BuildNewIECTables();
                _loggingHelper.LogLine("IEC tables recreated");

                // construct the aggregation event record  (??)
                AggregationEvent agg_event = new AggregationEvent(agg_event_id);
                
                // Loop through the study sources (in preference order)
                // N.B. Study links table already obtained.
                // In each case establish and then drop the source tables   
                // in a foreign table wrapper

                int num_studies_imported = 0;
                
                List<Source> sources = _mon_repo.RetrieveDataSources()
                    .OrderBy(s => s.preference_rating).ToList();
                _loggingHelper.LogLine("Sources obtained");

                foreach (Source source in sources)
                {
                    string source_conn_string = _credentials.GetConnectionString(source.database_name!, opts.testing);
                    source.db_conn = source_conn_string;

                    // in normal (non-testing) environment, schema name references the ad tables in a FTW
                    // - i.e. <db name>_ad. Credentials are here to get the connection string for the source
                    // database. In a testing environment source schema name will simply be 'ad', and the
                    // ad table data all has to be transferred from adcomp before each source transfer...
                   
                    string schema_name = _mon_repo.SetUpTempFTW(_credentials, 
                                              source.database_name!, core_conn_string);
                    
                    //IECTransferBuilder iecb = new IECTransferBuilder(source, schema_name, 
                    //                                 dest_conn_string, _loggingHelper);
                    //if (source.has_study_tables is true)
                    //{
                    //    iecb.ProcessStudyIds();
                    //    num_studies_imported += iecb.TransferStudyData();
                    //}
                    
                    // update statistics about aggregation
                    agg_event.num_studies_imported = num_studies_imported;
                    _mon_repo.StoreAggregationEvent(agg_event);
                }

            }
            
            
            if (opts.create_json)
            {
                string conn_string = _credentials.GetConnectionString("mdr", opts.testing);
                JSONHelper jh = new JSONHelper(conn_string, _loggingHelper);

                // Create json fields. If tables are to be left as they are, add false as an
                // additional boolean (default = true). if tables are to have further data appended
                // add an integer offset that represents the records to skip (default = 0)

                _loggingHelper.LogHeader("Creating JSON study data");
                //jh.CreateJSONStudyData();
                //jh.LoopThroughOAStudyRecords();
                _loggingHelper.LogHeader("Creating JSON object data");
                //jh.CreateJSONObjectData();
                //jh.LoopThroughOAObjectRecords();
            }

            _mon_repo.DropTempContextFTWs(core_conn_string);

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

