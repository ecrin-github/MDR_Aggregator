namespace MDR_Aggregator;

public class StudyLink
{
    public int source_1 { get; set; }
    public string? sd_sid_1 { get; set; }
    public string? sd_sid_2 { get; set; }
    public int? source_2 { get; set; }
}


public class StudyId
{
    public int source_id { get; set; }
    public string? sd_sid { get; set; }
    public DateTime? datetime_of_data_fetch { get; set; }
}


public class ObjectId
{
    public int source_id { get; set; }
    public string? sd_oid { get; set; }
    public int? parent_study_source_id { get; set; }
    public string? parent_study_sd_sid { get; set; }
    public DateTime? datetime_of_data_fetch { get; set; }
}


public class PMIDLink
{
    public int source_id { get; set; }
    public string? sd_oid { get; set; }
    public int? parent_study_source_id { get; set; }
    public string? parent_study_sd_sid { get; set; }
    public DateTime? datetime_of_data_fetch { get; set; }
}