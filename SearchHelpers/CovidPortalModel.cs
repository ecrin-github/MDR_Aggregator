using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace MDR_Aggregator;

public class database
{
    public string name = "ECRIN MDR";
    public string description = "The MDR aggregates metadata describing clinical studies and the data objects associated with them. It derives its data from trial registries, bibliographic resources and data repositories.";

    public string release = "";
    public string release_date = "";
    public int entry_count = 0;
    public string keywords = "clinical research, clinical trials, interventional studies, cohort studies, observational health research";

    public List<entry> entries = new();
}


public class entry
{
    [XmlAttribute]
    public string? id { get; set; }

    public string? name { get; set; } 
    public string? description { get; set; }

    public List<dbref>? cross_references { get; set; }
    public List<date>? dates { get; set; }
    public List<field>? additional_fields { get; set; }

    public entry(string? _id, string? _name, string? _description)
    {
        id = _id;
        name = _name;
        description = _description;
    }

    public entry()
    { }

}

public class dbref
{
    [XmlAttribute]
    public string? dbname; 
    [XmlAttribute]
    public string? dbkey;

    public dbref (string? _dbname, string? _dbkey)
    {
        dbname = _dbname;
        dbkey = _dbkey;
    }

    public dbref()
    { }
}

public class date
{
    [XmlAttribute]
    public string? type; 
    [XmlAttribute]
    public string? value; 

    public date (string? _type, string? _value)
    {
        type = _type;
        value = _value;
    }

    public date()
    { }
}

public class field
{
    [XmlText]
    public string? value;

    [XmlAttribute]
    public string? name;

    public field (string? _name, string? _value)
    {
        name = _name;
        value = _value;
    }

    public field()
    { }
}
