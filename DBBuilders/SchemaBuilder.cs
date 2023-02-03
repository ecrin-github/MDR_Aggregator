

namespace MDR_Aggregator
{
    public class SchemaBuilder
    {
        private string connString;
        private StudyTableBuilder study_tablebuilder;
        private ObjectTableBuilder object_tablebuilder;
        private LinkTableBuilder link_tablebuilder;

        public SchemaBuilder(string _connString)
        {
            connString = _connString;
            study_tablebuilder = new StudyTableBuilder(connString);
            object_tablebuilder = new ObjectTableBuilder(connString);
            link_tablebuilder = new LinkTableBuilder(connString);
        }

        public void DeleteStudyTables()
        {
            // dropping routines include 'if exists'
            // therefore can attempt to drop all of them
            study_tablebuilder.drop_table("studies");
            study_tablebuilder.drop_table("study_identifiers");
            study_tablebuilder.drop_table("study_titles");
            study_tablebuilder.drop_table("study_contributors");
            study_tablebuilder.drop_table("study_topics");
            study_tablebuilder.drop_table("study_features");
            study_tablebuilder.drop_table("study_relationships");
        }


        public void DeleteObjectTables()
        {
            // dropping routines include 'if exists'
            // therefore can attempt to drop all of them
            object_tablebuilder.drop_table("data_objects");
            object_tablebuilder.drop_table("object_datasets");
            object_tablebuilder.drop_table("object_dates");
            object_tablebuilder.drop_table("object_instances");
            object_tablebuilder.drop_table("object_titles");
            object_tablebuilder.drop_table("object_contributors");
            object_tablebuilder.drop_table("object_topics");
            object_tablebuilder.drop_table("object_descriptions");
            object_tablebuilder.drop_table("object_identifiers");
            object_tablebuilder.drop_table("object_relationships");
            object_tablebuilder.drop_table("object_rights");
        }

        public void DeleteLinkTables()
        {
            link_tablebuilder.drop_table("all_ids_data_objects");
            link_tablebuilder.drop_table("all_ids_studies");
            link_tablebuilder.drop_table("all_links");
            link_tablebuilder.drop_table("linked_study_groups");
            link_tablebuilder.drop_table("study_object_links");
            link_tablebuilder.drop_table("study_study_links");
            link_tablebuilder.drop_table("temp_study_ids");
        }
        
        public void BuildNewStudyTables()
        {
            study_tablebuilder.create_table_studies();
            study_tablebuilder.create_table_study_identifiers();
            study_tablebuilder.create_table_study_titles();
            study_tablebuilder.create_table_study_topics();
            study_tablebuilder.create_table_study_features();
            study_tablebuilder.create_table_study_contributors();
            study_tablebuilder.create_table_study_relationships();
        }


        public void BuildNewObjectTables()
        {
            // these common to all databases

            object_tablebuilder.create_table_data_objects();
            object_tablebuilder.create_table_object_instances();
            object_tablebuilder.create_table_object_titles();

            // these are database dependent		

            object_tablebuilder.create_table_object_datasets();
            object_tablebuilder.create_table_object_dates();
            object_tablebuilder.create_table_object_relationships();
            object_tablebuilder.create_table_object_rights();

            object_tablebuilder.create_table_object_contributors();
            object_tablebuilder.create_table_object_topics();
            object_tablebuilder.create_table_object_descriptions();
            object_tablebuilder.create_table_object_identifiers();
        }


        public void BuildNewLinkTables()
        {
            link_tablebuilder.create_table_all_ids_data_objects();
            link_tablebuilder.create_table_all_ids_studies();
            link_tablebuilder.create_table_all_links();
            link_tablebuilder.create_table_linked_study_groups();
            link_tablebuilder.create_table_study_object_links();
            link_tablebuilder.create_table_study_study_links();
            link_tablebuilder.create_table_temp_study_ids();
        }

    }
}

