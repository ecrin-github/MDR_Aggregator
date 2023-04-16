namespace MDR_Aggregator;

public class TestingDataLayer : ITestingDataLayer
{

    ILoggingHelper _loggingHelper;
    ADStudyTableBuilder _study_builder;
    ADObjectTableBuilder _object_builder;

    public TestingDataLayer(ILoggingHelper logginghelper, ICredentials credentials)
    {
        _loggingHelper = logginghelper;
        string _db_conn = credentials.GetConnectionString("test", true);
        _study_builder = new ADStudyTableBuilder(_db_conn);
        _object_builder = new ADObjectTableBuilder(_db_conn);
    }


    public void BuildNewADTables()
    {
        // create ALL study tables
        // ensures that they are present, to be truncated / refilled
        // whatever previous actions

        _study_builder.create_ad_schema();
        _study_builder.create_table_studies();
        _study_builder.create_table_study_identifiers();
        _study_builder.create_table_study_titles();
        _study_builder.create_table_study_topics();
        _study_builder.create_table_study_features();
        _study_builder.create_table_study_contributors();
        _study_builder.create_table_study_references();
        _study_builder.create_table_study_relationships();
        _study_builder.create_table_study_links();
        _study_builder.create_table_ipd_available();

        _loggingHelper.LogLine("Rebuilt test AD study tables");

        // Similarly, create ALL study tables

        _object_builder.create_table_data_objects();
        _object_builder.create_table_object_instances();
        _object_builder.create_table_object_titles();
        _object_builder.create_table_object_hashes();

        _object_builder.create_table_object_datasets();
        _object_builder.create_table_object_dates();
        _object_builder.create_table_object_relationships();
        _object_builder.create_table_object_rights();
        
        _object_builder.create_table_object_contributors();
        _object_builder.create_table_object_topics();
        _object_builder.create_table_object_comments();
        _object_builder.create_table_object_descriptions();
        _object_builder.create_table_object_identifiers();
        _object_builder.create_table_object_db_links();
        _object_builder.create_table_object_publication_types();
       
        _loggingHelper.LogLine("Rebuilt test AD Object tables");
    }


    public void TransferADTableData(Source source)
    {
        RetrieveADDataBuilder tdb = new RetrieveADDataBuilder(source);
        tdb.DeleteExistingADStudyData();
        tdb.DeleteExistingADObjectData();
        _loggingHelper.LogLine("Any existing AD test data for source " + source.id + " removed from AD tables");

        tdb.RetrieveStudyData();
        tdb.RetrieveObjectData();
        _loggingHelper.LogLine("New AD test data for source " + source.id + " added to AD tables");
    }
}