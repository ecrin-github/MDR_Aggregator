using MDR_Aggregator.LoggingHelpers.Interfaces;

namespace MDR_Aggregator.CoreDBBuilders;

public class CoreBuilder
{
    private readonly CoreStudyTableBuilder _coreStudytablebuilder;
    private readonly CoreObjectTableBuilder _coreObjecttablebuilder;
    
    public CoreBuilder(string connString, ILoggingHelper loggingHelper)
    {
        _coreStudytablebuilder = new CoreStudyTableBuilder(connString, loggingHelper);
        _coreObjecttablebuilder = new CoreObjectTableBuilder(connString, loggingHelper);
    }
   
    public void BuildNewCoreTables()
    {
        _coreStudytablebuilder.create_table_studies();
        _coreStudytablebuilder.create_table_study_identifiers();
        _coreStudytablebuilder.create_table_study_titles();
        _coreStudytablebuilder.create_table_study_topics();
        _coreStudytablebuilder.create_table_study_conditions();
        _coreStudytablebuilder.create_table_study_icd();
        _coreStudytablebuilder.create_table_study_features();
        _coreStudytablebuilder.create_table_study_people();
        _coreStudytablebuilder.create_table_study_organisations();
        _coreStudytablebuilder.create_table_study_countries();
        _coreStudytablebuilder.create_table_study_locations();
        _coreStudytablebuilder.create_table_study_relationships();
        
        _coreObjecttablebuilder.create_table_data_objects();
        _coreObjecttablebuilder.create_table_object_instances();
        _coreObjecttablebuilder.create_table_object_titles();
        _coreObjecttablebuilder.create_table_object_datasets();
        _coreObjecttablebuilder.create_table_object_dates();
        _coreObjecttablebuilder.create_table_object_relationships();
        _coreObjecttablebuilder.create_table_object_rights();
        _coreObjecttablebuilder.create_table_object_people();
        _coreObjecttablebuilder.create_table_object_organisations();
        _coreObjecttablebuilder.create_table_object_topics();
        _coreObjecttablebuilder.create_table_object_descriptions();
        _coreObjecttablebuilder.create_table_object_identifiers();

        _coreObjecttablebuilder.create_table_study_object_links();
        
        _coreStudytablebuilder.create_table_study_search();
    }
}