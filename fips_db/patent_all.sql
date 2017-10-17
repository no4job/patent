drop view if exists patent_all;
select group_concat(column_name order by grp,column_name_casted,column_name SEPARATOR ',') from 
(
select grp,column_name,cast(replace(replace(column_name,'inid_',''),'f_','') AS UNSIGNED) as column_name_casted from 
(select 1 as grp,'id' as column_name union all
select 2 as grp,column_name from information_schema.COLUMNS where TABLE_NAME like 'patent'and column_name like '%_id'  union all
select 3 as grp,column_name from information_schema.COLUMNS where TABLE_NAME like 'patent%'and column_name not like '%id'  and column_name like '%inid%' union all
select 4 as grp,column_name from information_schema.COLUMNS where TABLE_NAME like 'patent%'and column_name not like '%id'  and column_name like 'f_%' union all
select 5 as grp,'time_stamp' as column_name
)all_columns
)all_columns_casted into @cols;

set @first_part = 'create view patent_all as select ';
set @third_part = ' from patent left outer join 
(select patent_id,GROUP_CONCAT(inid_15 order by inid_15 asc separator "; " ) as  inid_15  from patent_inid_15 group by patent_id) inid_15 on inid_11_id=inid_15.patent_id left outer join 
(select patent_id,GROUP_CONCAT(inid_21 order by inid_21 asc separator "; " ) as  inid_21  from patent_inid_21 group by patent_id) inid_21 on inid_11_id=inid_21.patent_id left outer join 
(select patent_id,GROUP_CONCAT(inid_22 order by inid_22 asc separator "; " ) as  inid_22  from patent_inid_22 group by patent_id) inid_22 on inid_11_id=inid_22.patent_id left outer join 
(select patent_id,GROUP_CONCAT(inid_23 order by inid_23 asc separator "; " ) as  inid_23  from patent_inid_23 group by patent_id) inid_23 on inid_11_id=inid_23.patent_id left outer join 
(select patent_id,GROUP_CONCAT(inid_30 order by inid_30 asc separator "; " ) as  inid_30  from patent_inid_30 group by patent_id) inid_30 on inid_11_id=inid_30.patent_id left outer join 
(select patent_id,GROUP_CONCAT(inid_31 order by inid_31 asc separator "; " ) as  inid_31  from patent_inid_31 group by patent_id) inid_31 on inid_11_id=inid_31.patent_id left outer join 
(select patent_id,GROUP_CONCAT(inid_32 order by inid_32 asc separator "; " ) as  inid_32  from patent_inid_32 group by patent_id) inid_32 on inid_11_id=inid_32.patent_id left outer join 
(select patent_id,GROUP_CONCAT(inid_51 order by inid_51 asc separator "; " ) as  inid_51  from patent_inid_51 group by patent_id) inid_51 on inid_11_id=inid_51.patent_id left outer join 
(select patent_id,GROUP_CONCAT(inid_56 order by inid_56 asc separator "; " ) as  inid_56  from patent_inid_56 group by patent_id) inid_56 on inid_11_id=inid_56.patent_id left outer join 
(select patent_id,GROUP_CONCAT(inid_71 order by inid_71 asc separator "; " ) as  inid_71  from patent_inid_71 group by patent_id) inid_71 on inid_11_id=inid_71.patent_id left outer join 
(select patent_id,GROUP_CONCAT(inid_72 order by inid_72 asc separator "; " ) as  inid_72  from patent_inid_72 group by patent_id) inid_72 on inid_11_id=inid_72.patent_id left outer join 
(select patent_id,GROUP_CONCAT(inid_73 order by inid_73 asc separator "; " ) as  inid_73  from patent_inid_73 group by patent_id) inid_73 on inid_11_id=inid_73.patent_id left outer join
(select patent_id,GROUP_CONCAT(inid_74 order by inid_74 asc separator "; " ) as  inid_74  from patent_inid_74 group by patent_id) inid_74 on inid_11_id=inid_74.patent_id left outer join
(select patent_id,GROUP_CONCAT(f_125 order by f_125 asc separator "; " ) as  f_125  from patent_f_125 group by patent_id) f_125 on inid_11_id=f_125.patent_id left outer join 
(select patent_id,GROUP_CONCAT(f_126 order by f_126 asc separator "; " ) as  f_126  from patent_f_126 group by patent_id) f_126 on inid_11_id=f_126.patent_id'
;

set @sql = concat(@first_part,@cols,@third_part);
#signal SQLSTATE 'Break';
prepare stmt1 from @sql;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;