namespace MDR_Aggregator;

public class StudyLink
{
    public int source_1 { get; set; }
    public string? sd_sid_1 { get; set; }
    public string? sd_sid_2 { get; set; }
    public int? source_2 { get; set; }
}


public class OldNewLink
{
    public string? new_id { get; set; }
    public string? old_id { get; set; }
}


public class StudyId
{
    public int source_id { get; set; }
    public string? sd_sid { get; set; }
    public DateTime? datetime_of_data_fetch { get; set; }
}

public class ComplexStudy
{
    public int group_number { get; set; }
    public int src_id { get; set; }
    public string sid_id { get; set; }

    public ComplexStudy(int groupNumber, int srcId, string sidId)
    {
        group_number = groupNumber;
        src_id = srcId;
        sid_id = sidId;
    }
}

public class ComplexStudyRow
{
    public int group_source_id { get; set; }
    public string group_sd_sid { get; set; }
    public int member_source_id { get; set; }
    public string member_sd_sid { get; set; }
}

/*
public class ComplexKeyStudy
{
    public int src_id { get; set; }
    public string sid_id { get; set; }
    public bool considered { get; set; }
}


public class ComplexStudy
{
    public int sourceid { get; set; }
    public string sdsid { get; set; }
}
*/
   
public class ComplexLink
{
    public int srce { get; set; }
    public string sdsid { get; set; }
    public int relationship_id { get; set; }
    public int target_srce { get; set; }
    public string target_sdsid { get; set; }

    public ComplexLink(int _srce, string _sdsid, int _relationship_id,
        int _target_srce, string _target_sdsid)
    {
        srce = _srce;
        sdsid = _sdsid;
        relationship_id = _relationship_id;
        target_srce = _target_srce;
        target_sdsid = _target_sdsid;
    }
}

public class IdChecker
{
    public string sd_sid { get; set; }
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