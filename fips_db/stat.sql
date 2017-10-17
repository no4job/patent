DROP TABLE IF EXISTS `tech_data`.`stat`;
USE `tech_data`;
CREATE OR REPLACE VIEW `stat` AS
select 'Страна','Юридическое/Физическое лицо' ,'Полное наименование/ФИО',  'Короткое наименование', 'Автор патента','Патентообладатеь',
'Автор ПО/БД','Правообладатель ПО/БД', 'Всего'  from  dual where false
union all  
select * from (
select country,ref_type,full_name,short_name, 
#sum(if(doc_type = 'patent' and role = 'заявитель' and inid_12 like "%заявка%",1,0) ) as patent_applicant,
sum(if(doc_type = 'patent' and role = 'автор'  and inid_12 not like "%заявка%" and not relevance = 0,1,0)) as patent_author,
sum(if(doc_type = 'patent' and role = 'патентообладатель' and not relevance = 0,1,0)) as patent_owner,
sum(if(doc_type = 'soft' and role = 'автор' and not relevance = 0,1,0)) as soft_author,
sum(if(doc_type = 'soft' and role = 'правообладатель' and not relevance = 0,1,0)) as soft_owner,
sum(if(inid_12 not like "%заявка%" and not relevance = 0,1,0)) as total
from (
select tbl.*,reference_book.ref_type,organization.full_name,organization.short_name,organization.country,review.relevance,patent.inid_12
 from (
select *, "заявитель" as role,"patent" as doc_type from patent_inid_71 union all
select *, "автор" as role,"patent" as doc_type from patent_inid_72 union all
select *,"патентообладатель" as role,"patent" as doc_type from patent_inid_73 
#select * from soft_f_72 union all
#select * from soft_f_73
) as tbl left outer join reference_book on uuid_id = reference_book.source_record_id
inner join organization on reference_book.reference_id = organization.id  and reference_book.ref_type = 'organization'
left outer join review on review.ref_id=patent_id
left outer join patent on patent.inid_11_id=patent_id
where review.ref_id not like '%@%'
union all

select tbl.*,reference_book.ref_type,person.FIO,"",person.country,review.relevance,patent.inid_12
 from (
select *, "заявитель" as role,"patent" as doc_type from patent_inid_71 union all
select *, "автор" as role,"patent" as doc_type from patent_inid_72 union all
select *,"патентообладатель" as role,"patent" as doc_type from patent_inid_73 
#select * from soft_f_72 union all
#select * from soft_f_73
) as tbl left outer join reference_book on uuid_id = reference_book.source_record_id
inner join person on reference_book.reference_id = person.id  and reference_book.ref_type = 'person'
left outer join review on review.ref_id=patent_id
left outer join patent on patent.inid_11_id=patent_id
where review.ref_id not like '%@%'
union all

select tbl.*,reference_book.ref_type,organization.full_name,organization.short_name,organization.country,review.relevance,soft.f_12
 from (
#select *, "заявитель" as role,"patent" as doc_type from patent_inid_71 union all
#select *, "автор" as role,"patent" as doc_type from patent_inid_72 union all
#select *,"правообладатель" as role,"patent" as doc_type from patent_inid_73 
select *, "автор" as role,"soft" as doc_type from soft_f_72 union all
select *,"правообладатель" as role,"soft" as doc_type from soft_f_73
) as tbl left outer join reference_book on uuid_id = reference_book.source_record_id
inner join organization on reference_book.reference_id = organization.id  and reference_book.ref_type = 'organization'
left outer join review on review.ref_id=soft_id
left outer join soft on soft.f_11_id=soft_id
where review.ref_id not like '%@%'
union all
#signal SQLSTATE 'Break';

select tbl.*,reference_book.ref_type,person.FIO,"",person.country,review.relevance,soft.f_12
 from (
#select *, "заявитель" as role,"patent" as doc_type from patent_inid_71 union all
#select *, "автор" as role,"patent" as doc_type from patent_inid_72 union all
#select *,"правообладатель" as role,"patent" as doc_type from patent_inid_73 
select *, "автор" as role,"soft" as doc_type from soft_f_72 union all
select *,"правообладатель" as role,"soft" as doc_type from soft_f_73
) as tbl left outer join reference_book on uuid_id = reference_book.source_record_id
inner join person on reference_book.reference_id = person.id  and reference_book.ref_type = 'person'
left outer join review on review.ref_id=soft_id
left outer join soft on soft.f_11_id=soft_id
where review.ref_id not like '%@%'
) tbl2 group by full_name order by total desc
)tbl3
