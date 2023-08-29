namespace MDR_Aggregator;

public class CoreBuilder
{
    private readonly CoreStudyTableBuilder core_studytablebuilder;
    private readonly CoreObjectTableBuilder core_objecttablebuilder;
    
    public CoreBuilder(string _connString, ILoggingHelper loggingHelper)
    {
        core_studytablebuilder = new CoreStudyTableBuilder(_connString, loggingHelper);
        core_objecttablebuilder = new CoreObjectTableBuilder(_connString, loggingHelper);
    }
   
    public void BuildNewCoreTables()
    {
        core_studytablebuilder.create_table_studies();
        core_studytablebuilder.create_table_study_identifiers();
        core_studytablebuilder.create_table_study_titles();
        core_studytablebuilder.create_table_study_topics();
        core_studytablebuilder.create_table_study_conditions();
        core_studytablebuilder.create_table_study_icd();
        core_studytablebuilder.create_table_study_features();
        core_studytablebuilder.create_table_study_people();
        core_studytablebuilder.create_table_study_organisations();
        core_studytablebuilder.create_table_study_countries();
        core_studytablebuilder.create_table_study_locations();
        core_studytablebuilder.create_table_study_relationships();
        
        core_objecttablebuilder.create_table_data_objects();
        core_objecttablebuilder.create_table_object_instances();
        core_objecttablebuilder.create_table_object_titles();
        core_objecttablebuilder.create_table_object_datasets();
        core_objecttablebuilder.create_table_object_dates();
        core_objecttablebuilder.create_table_object_relationships();
        core_objecttablebuilder.create_table_object_rights();
        core_objecttablebuilder.create_table_object_people();
        core_objecttablebuilder.create_table_object_organisations();
        core_objecttablebuilder.create_table_object_topics();
        core_objecttablebuilder.create_table_object_descriptions();
        core_objecttablebuilder.create_table_object_identifiers();

        core_objecttablebuilder.create_table_study_object_links();
        
        core_studytablebuilder.create_table_study_search();
    }
}