

namespace MDR_Aggregator
{
    public class CoreBuilder
    {
        private string connString;
        private CoreTableBuilder core_tablebuilder;

        public CoreBuilder(string _connString)
        {
            connString = _connString;
            core_tablebuilder = new CoreTableBuilder(connString);
        }

        public void DeleteCoreTables()
        {
            // dropping routines include 'if exists'

            core_tablebuilder.drop_table("studies");
            core_tablebuilder.drop_table("study_identifiers");
            core_tablebuilder.drop_table("study_titles");
            core_tablebuilder.drop_table("study_contributors");
            core_tablebuilder.drop_table("study_topics");
            core_tablebuilder.drop_table("study_features");
            core_tablebuilder.drop_table("study_relationships"); 
            
            core_tablebuilder.drop_table("data_objects");
            core_tablebuilder.drop_table("object_datasets");
            core_tablebuilder.drop_table("object_dates");
            core_tablebuilder.drop_table("object_instances");
            core_tablebuilder.drop_table("object_titles");
            core_tablebuilder.drop_table("object_contributors");
            core_tablebuilder.drop_table("object_topics");
            core_tablebuilder.drop_table("object_descriptions");
            core_tablebuilder.drop_table("object_identifiers");
            core_tablebuilder.drop_table("object_relationships");
            core_tablebuilder.drop_table("object_rights");

            core_tablebuilder.drop_table("study_object_links");
        }


        public void BuildNewCoreTables()
        {
            core_tablebuilder.create_table_studies();
            core_tablebuilder.create_table_study_identifiers();
            core_tablebuilder.create_table_study_titles();
            core_tablebuilder.create_table_study_topics();
            core_tablebuilder.create_table_study_features();
            core_tablebuilder.create_table_study_contributors();
            core_tablebuilder.create_table_study_relationships();

            core_tablebuilder.create_table_data_objects();
            core_tablebuilder.create_table_object_instances();
            core_tablebuilder.create_table_object_titles();
            core_tablebuilder.create_table_object_datasets();
            core_tablebuilder.create_table_object_dates();
            core_tablebuilder.create_table_object_relationships();
            core_tablebuilder.create_table_object_rights();
            core_tablebuilder.create_table_object_contributors();
            core_tablebuilder.create_table_object_topics();
            core_tablebuilder.create_table_object_descriptions();
            core_tablebuilder.create_table_object_identifiers();

            core_tablebuilder.create_table_study_object_links();
        }
    }
}

