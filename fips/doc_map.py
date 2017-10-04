__author__ = 'mdu'
import itertools
from collections import defaultdict

FIPS_PATENT_DOC_MAP = [
    {'doc_field':'11','field_id':'11','inid':True,'field_type':'string','column_name':'inid_11','table':'patent','ins_order':1,'ref_table':'','ref_field':'','fk':'','split_pattern':''},
    {'doc_field':'12','field_id':'12','inid':True,'field_type':'string','column_name':'inid_12','table':'patent','ins_order':1,'ref_table':'','ref_field':'','fk':'','split_pattern':''},
    {'doc_field':'13','field_id':'13','inid':True,'field_type':'string','column_name':'inid_13','table':'patent','ins_order':1,'ref_table':'','ref_field':'','fk':'','split_pattern':''},
    {'doc_field':'19','field_id':'19','inid':True,'field_type':'string','column_name':'inid_19','table':'patent','ins_order':1,'ref_table':'','ref_field':'','fk':'','split_pattern':''},
    {'doc_field':'24','field_id':'24','inid':True,'field_type':'date','column_name':'inid_24','table':'patent','ins_order':1,'ref_table':'','ref_field':'','fk':'','split_pattern':''},
    {'doc_field':'33','field_id':'33','inid':True,'field_type':'string','column_name':'inid_33','table':'patent','ins_order':1,'ref_table':'','ref_field':'','fk':'','split_pattern':''},
    {'doc_field':'41','field_id':'41','inid':True,'field_type':'date','column_name':'inid_41','table':'patent','ins_order':1,'ref_table':'','ref_field':'','fk':'','split_pattern':''},
    {'doc_field':'43','field_id':'43','inid':True,'field_type':'date','column_name':'inid_43','table':'patent','ins_order':1,'ref_table':'','ref_field':'','fk':'','split_pattern':''},
    {'doc_field':'45','field_id':'45','inid':True,'field_type':'date','column_name':'inid_45','table':'patent','ins_order':1,'ref_table':'','ref_field':'','fk':'','split_pattern':''},
    {'doc_field':'46','field_id':'46','inid':True,'field_type':'date','column_name':'inid_46','table':'patent','ins_order':1,'ref_table':'','ref_field':'','fk':'','split_pattern':''},
    {'doc_field':'54','field_id':'54','inid':True,'field_type':'string','column_name':'inid_54','table':'patent','ins_order':1,'ref_table':'','ref_field':'','fk':'','split_pattern':''},
    {'doc_field':'85','field_id':'85','inid':True,'field_type':'date','column_name':'inid_85','table':'patent','ins_order':1,'ref_table':'','ref_field':'','fk':'','split_pattern':''},
    {'doc_field':'86_date','field_id':'86_date','inid':True,'field_type':'date','column_name':'inid_86_date','table':'patent','ins_order':1,'ref_table':'','ref_field':'','fk':'','split_pattern':''},
    {'doc_field':'86_number','field_id':'86_number','inid':True,'field_type':'string','column_name':'inid_86_number','table':'patent','ins_order':1,'ref_table':'','ref_field':'','fk':'','split_pattern':''},
    {'doc_field':'87_date','field_id':'87_date','inid':True,'field_type':'date','column_name':'inid_87_date','table':'patent','ins_order':1,'ref_table':'','ref_field':'','fk':'','split_pattern':''},
    {'doc_field':'87_number','field_id':'87_number','inid':True,'field_type':'string','column_name':'inid_87_number','table':'patent','ins_order':1,'ref_table':'','ref_field':'','fk':'','split_pattern':''},
    {'doc_field':'98','field_id':'98','inid':False,'field_type':'string','column_name':'f_98','table':'patent','ins_order':1,'ref_table':'','ref_field':'','fk':'','split_pattern':''},
    {'doc_field':'11_href','field_id':'121','inid':False,'field_type':'string','column_name':'f_121','table':'patent','ins_order':1,'ref_table':'','ref_field':'','fk':'','split_pattern':''},
    {'doc_field':'21_href','field_id':'122','inid':False,'field_type':'string','column_name':'f_122','table':'patent','ins_order':1,'ref_table':'','ref_field':'','fk':'','split_pattern':''},
    {'doc_field':'43_href','field_id':'123','inid':False,'field_type':'string','column_name':'f_123','table':'patent','ins_order':1,'ref_table':'','ref_field':'','fk':'','split_pattern':''},
    {'doc_field':'45_href','field_id':'124','inid':False,'field_type':'string','column_name':'f_124','table':'patent','ins_order':1,'ref_table':'','ref_field':'','fk':'','split_pattern':''},
    {'doc_field':'126','field_id':'126','inid':False,'field_type':'string','column_name':'f_126','table':'patent','ins_order':1,'ref_table':'','ref_field':'','fk':'','split_pattern':''},
    {'doc_field':'15','field_id':'15','inid':True,'field_type':'string','column_name':'inid_15','table':'patent_inid_15','ins_order':2,'ref_table':'patent','ref_field':'id','fk':'patent_id','split_pattern':';'},
    {'doc_field':'21','field_id':'21','inid':True,'field_type':'string','column_name':'inid_21','table':'patent_inid_21','ins_order':2,'ref_table':'patent','ref_field':'id','fk':'patent_id','split_pattern':';'},
    {'doc_field':'22','field_id':'22','inid':True,'field_type':'date','column_name':'inid_22','table':'patent_inid_22','ins_order':2,'ref_table':'patent','ref_field':'id','fk':'patent_id','split_pattern':';'},
    {'doc_field':'23','field_id':'23','inid':True,'field_type':'date','column_name':'inid_23','table':'patent_inid_23','ins_order':2,'ref_table':'patent','ref_field':'id','fk':'patent_id','split_pattern':';'},
    {'doc_field':'30','field_id':'30','inid':True,'field_type':'string','column_name':'inid_30','table':'patent_inid_30','ins_order':2,'ref_table':'patent','ref_field':'id','fk':'patent_id','split_pattern':';'},
    {'doc_field':'31','field_id':'31','inid':True,'field_type':'string','column_name':'inid_31','table':'patent_inid_31','ins_order':2,'ref_table':'patent','ref_field':'id','fk':'patent_id','split_pattern':';'},
    {'doc_field':'32','field_id':'32','inid':True,'field_type':'date','column_name':'inid_32','table':'patent_inid_32','ins_order':2,'ref_table':'patent','ref_field':'id','fk':'patent_id','split_pattern':';'},
    {'doc_field':'51','field_id':'51','inid':True,'field_type':'string','column_name':'inid_51','table':'patent_inid_51','ins_order':2,'ref_table':'patent','ref_field':'id','fk':'patent_id','split_pattern':';'},
    {'doc_field':'56','field_id':'56','inid':True,'field_type':'string','column_name':'inid_56','table':'patent_inid_56','ins_order':2,'ref_table':'patent','ref_field':'id','fk':'patent_id','split_pattern':';'},
    {'doc_field':'71','field_id':'71','inid':True,'field_type':'string','column_name':'inid_71','table':'patent_inid_71','ins_order':2,'ref_table':'patent','ref_field':'id','fk':'patent_id','split_pattern':';'},
    {'doc_field':'73','field_id':'73','inid':True,'field_type':'string','column_name':'inid_73','table':'patent_inid_73','ins_order':2,'ref_table':'patent','ref_field':'id','fk':'patent_id','split_pattern':';'},
    {'doc_field':'74','field_id':'74','inid':True,'field_type':'string','column_name':'inid_74','table':'patent_inid_74','ins_order':2,'ref_table':'patent','ref_field':'id','fk':'patent_id','split_pattern':';'},
    {'doc_field':'125','field_id':'125','inid':False,'field_type':'string','column_name':'f_125','table':'patent_f_125','ins_order':2,'ref_table':'patent','ref_field':'id','fk':'patent_id','split_pattern':';'},
    {'doc_field':'status','field_id':'127','inid':False,'field_type':'string','column_name':'f_127','table':'patent','ins_order':1,'ref_table':'','ref_field':'','fk':'','split_pattern':''}
]


class fips_map:
    def __init__(self, mapping):
        self.mapping=mapping
        self.doc_map_by_doc_field = self.doc_map_by_attribute('doc_field')
        self.doc_map_by_field_id=self.doc_map_by_attribute('field_id')
        self.doc_map_by_column_name=self.doc_map_by_attribute('column_name')
        self.all_table_ordered = self.get_all_table_ordered()
        self.all_table_field = self.get_all_table_field()
        self.all_table_fk = self.get_all_table_fk()


    def doc_map_by_attribute(self,attribute):
            if not attribute in ['doc_field','field_id','column_name']:
                return None
            doc_map = {}
            for element in self.mapping:
                doc_map[element[attribute]] = element
            return doc_map


    def get_attribute(self,k_attribute_type,k_attribute_value):
        if not k_attribute_type in ['doc_field','field_id','column_name']:
            return None
        self.doc_map_by_attribute(k_attribute_type)[k_attribute_value]

    def  all_table(self):
        table_list = set()
        for doc_field in self.doc_map_by_doc_field.keys():
            table_list.update([self.doc_map_by_doc_field[doc_field]["table"]])
        return table_list


    def  get_all_table_ordered(self):
        table_ordered ={}
        table_list = []
        for doc_field in self.doc_map_by_doc_field.keys():
            table_list.append([self.doc_map_by_doc_field[doc_field]['table'],
                               self.doc_map_by_doc_field[doc_field]['ins_order']])
        table_list.sort()
        table_list = list(table_list for table_list,_ in itertools.groupby(table_list))
        for table in  table_list:
            table_ordered[table[0]]=table[1]
        return table_ordered

    def  get_all_table_field(self):
        table_field = defaultdict(list)
        for doc_field in self.doc_map_by_doc_field.keys():
            table = self.doc_map_by_doc_field[doc_field]["table"]
            table_field[table].append(doc_field)
        return table_field

    def  get_all_table_fk(self):
        all_table_fk ={}
        table_fk = []
        for doc_field in self.doc_map_by_doc_field.keys():
            table_fk.append([ self.doc_map_by_doc_field[doc_field]['table'],
                                self.doc_map_by_doc_field[doc_field]['ref_table'],
                                self.doc_map_by_doc_field[doc_field]['ref_field'],
                                self.doc_map_by_doc_field[doc_field]['fk']]
            )
        table_fk.sort()
        table_fk = list(table_fk for table_fk,_ in itertools.groupby(table_fk))
        for table in  table_fk:
            all_table_fk[table[0]]={'ref_table':table[1],'ref_field':table[2],'fk':table[3]}
        return all_table_fk

    def get_attribute_doc_field(self,key,attribute):
        return self.doc_map_by_doc_field[key][attribute]
    @staticmethod
    def get_attribute_field_id(self,key,attribute):
        return self.doc_map_by_field_id[key][attribute]
    @staticmethod
    def get_attribute_column_name(self,key,attribute):
        return self.doc_map_by_column_name[key][attribute]


