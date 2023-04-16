namespace MDR_Aggregator;

public class CoreBuilder
{
    private readonly CoreTableBuilder core_tablebuilder;

    public CoreBuilder(string _connString)
    {
        core_tablebuilder = new CoreTableBuilder(_connString);
    }
   

    public void BuildNewCoreTables()
    {
        core_tablebuilder.create_table_studies();
        core_tablebuilder.create_table_study_identifiers();
        core_tablebuilder.create_table_study_titles();
        core_tablebuilder.create_table_study_topics();
        core_tablebuilder.create_table_study_conditions();
        core_tablebuilder.create_table_study_features();
        core_tablebuilder.create_table_study_people();
        core_tablebuilder.create_table_study_organisations();
        core_tablebuilder.create_table_study_countries();
        core_tablebuilder.create_table_study_locations();
        core_tablebuilder.create_table_study_relationships();
        
        core_tablebuilder.create_table_data_objects();
        core_tablebuilder.create_table_object_instances();
        core_tablebuilder.create_table_object_titles();
        core_tablebuilder.create_table_object_datasets();
        core_tablebuilder.create_table_object_dates();
        core_tablebuilder.create_table_object_relationships();
        core_tablebuilder.create_table_object_rights();
        core_tablebuilder.create_table_object_people();
        core_tablebuilder.create_table_object_organisations();
        core_tablebuilder.create_table_object_topics();
        core_tablebuilder.create_table_object_descriptions();
        core_tablebuilder.create_table_object_identifiers();

        core_tablebuilder.create_table_study_object_links();
    }
}