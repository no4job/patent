CREATE VIEW `patent_by_inid_71_72_73_74` AS
(select "inid_71" as key_type, patent_inid_71.inid_71 as inid_71_key, patent_all.*
from patent_all  inner join patent_inid_71 on patent_inid_71.patent_id = patent_all.inid_11_id  order by patent_inid_71.inid_71)
union  all
(select "inid_72" as key_type,patent_inid_72.inid_72 as inid_72_key, patent_all.*
from patent_all  inner join patent_inid_72 on patent_inid_72.patent_id = patent_all.inid_11_id  order by patent_inid_72.inid_72)
union all 
(select "inid_73" as key_type,patent_inid_73.inid_73 as inid_73_key, patent_all.*
from patent_all  inner join patent_inid_73 on patent_inid_73.patent_id = patent_all.inid_11_id  order by patent_inid_73.inid_73)
union all
(select "inid_74" as key_type,patent_inid_74.inid_74 as inid_74_key, patent_all.*
from patent_all  inner join patent_inid_74 on patent_inid_74.patent_id = patent_all.inid_11_id  order by patent_inid_74.inid_74)