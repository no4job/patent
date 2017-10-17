drop view if exists soft_all;
select group_concat(column_name order by grp,column_name_casted,column_name SEPARATOR ',') from 
(
select grp,column_name,cast(replace(replace(column_name,'inid_',''),'f_','') AS UNSIGNED) as column_name_casted from 
(select 1 as grp,'id' as column_name union all
select 2 as grp,column_name from information_schema.COLUMNS where TABLE_NAME like 'soft'and column_name like '%_id'  union all
select 3 as grp,column_name from information_schema.COLUMNS where TABLE_NAME like 'soft%'and column_name not like '%id'  and column_name like 'f_%' union all
select 4 as grp,'time_stamp' as column_name
)all_columns
)all_columns_casted into @cols;

set @first_part = 'create view soft_all as select ';
set @third_part = ' from soft left outer join 
(select soft_id,GROUP_CONCAT(f_72 order by f_72 asc separator "; " ) as  f_72  from soft_f_72 group by soft_id) f_72 on f_11_id=f_72.soft_id left outer join 
(select soft_id,GROUP_CONCAT(f_73 order by f_73 asc separator "; " ) as  f_73  from soft_f_73 group by soft_id) f_73 on f_11_id=f_73.soft_id'
;
set @sql = concat(@first_part,@cols,@third_part);
#select @sql;
#signal SQLSTATE 'Break';
prepare stmt1 from @sql;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;
#select * from soft_all;