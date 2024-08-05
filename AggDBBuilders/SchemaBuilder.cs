namespace MDR_Aggregator.AggDBBuilders;

public class SchemaBuilder
{
    private readonly StudyTableBuilder _studyTablebuilder;
    private readonly ObjectTableBuilder _objectTablebuilder;

    public SchemaBuilder(string connString)
    {
        _studyTablebuilder = new StudyTableBuilder(connString);
        _objectTablebuilder = new ObjectTableBuilder(connString);
    }
    
    public void BuildNewStudyTables()
    {
        _studyTablebuilder.create_table_studies();
        _studyTablebuilder.create_table_study_identifiers();
        _studyTablebuilder.create_table_study_titles();
        _studyTablebuilder.create_table_study_topics();
        _studyTablebuilder.create_table_study_conditions();
        _studyTablebuilder.create_table_study_icd();
        _studyTablebuilder.create_table_study_features();
        _studyTablebuilder.create_table_study_people();
        _studyTablebuilder.create_table_study_organisations();
        _studyTablebuilder.create_table_study_relationships();
        _studyTablebuilder.create_table_study_countries();
        _studyTablebuilder.create_table_study_locations();
    }

    public void BuildNewObjectTables()
    {
        _objectTablebuilder.create_table_data_objects();
        _objectTablebuilder.create_table_object_instances();
        _objectTablebuilder.create_table_object_titles();

        _objectTablebuilder.create_table_object_datasets();
        _objectTablebuilder.create_table_object_dates();
        _objectTablebuilder.create_table_object_relationships();
        _objectTablebuilder.create_table_object_rights();

        _objectTablebuilder.create_table_object_people();
        _objectTablebuilder.create_table_object_organisations();
        _objectTablebuilder.create_table_object_topics();
        _objectTablebuilder.create_table_object_descriptions();
        _objectTablebuilder.create_table_object_identifiers();
        
        _objectTablebuilder.create_table_study_object_links();
    }
}