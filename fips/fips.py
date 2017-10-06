__author__ = 'mdu'
import json
from io import StringIO
import re
import csv
import csv_tools
import os
import db_connect
import common_config
from datetime import datetime
import hashlib
import time
import doc_map
from doc_map import fips_map
import itertools
import copy
import mysql.connector

from contextlib import suppress
try:
    from lxml import etree
    from lxml.html import fromstring
except ImportError:
    print("lxml import error")
    raise
INID_CODES = ['11','12','13','15','19',
              '21','22','23','24','25','26','27',
              '30','31','32','33','34',
              '41','42','43','44','45','46','47','48',
              '51','52','54','56','57','58',
              '61','62','63','64','65','66','67','68',
              '71','72','73','74','75','76',
              '81','83','84','85','86_number','86_date','87_number','87_date','88',
              '91','92','93','94','95','96','97']
OTHER_FIELDS = {
    "98":"98",
    "11_href":"121",
    "21_href":"122",
    "43_href":"123",
    "45_href":"124",
    "125":"125",
    "126":"126",
    "status":"127",
}
FIELDS_TO_TABLE_MAP = {
    '11':'patent','12':'patent','13':'patent','19':'patent',
    '24':'patent',
    '33':'patent',
    '41':'patent','43':'patent','45':'patent','46':'patent',
    '54':'patent',
    '85':'patent','86_date':'patent','86_number':'patent','87_date':'patent','87_number':'patent',
    '98':'patent',
    '121':'patent','122':'patent','123':'patent','124':'patent','126':'patent',
    '15':'patent_inid_15',
    '21':'patent_inid_21',
    '22':'patent_inid_22',
    '23':'patent_inid_23',
    '30':'patent_inid_30',
    '31':'patent_inid_31',
    '32':'patent_inid_32',
    '51':'patent_inid_51',
    '56':'patent_inid_56',
    '71':'patent_inid_71',
    '73':'patent_inid_73',
    '74':'patent_inid_74',
    '125':'patent_f_125',
    '127':'patent_f_127'
}
INID_CODES_IN_LINKED_TABLES = [
              '15',
              '21','22','23',
              '30','31','32',
              '51','56',
              '71','73','74']

OTHER_FIELDS_IN_LINKED_TABLES = {
    "125":"125",
    "126":"126"
}
TABLE_LIST = ["patent","patent_f_125","patent_f_126","patent_inid_15","patent_inid_21","patent_inid_22", \
              "patent_inid_23","patent_inid_30","patent_inid_31","patent_inid_32","patent_inid_51","patent_inid_56", \
              "patent_inid_71","patent_inid_73","patent_inid_74"]
INID_CODES_IN_PATENT = ['11','12','13','19',
                        '24',
                        '33',
                        '41','43','45','46',
                        '54',
                        '85','86','87'
                        ]
OTHER_FIELDS_IN_PATENT = {
    "98":"98",
    "11_href":"121",
    "21_href":"122",
    "43_href":"123",
    "45_href":"124",
    "status":"127"
}
FIELD_FORMAT = {'11':'string','12':'string','13':'string','15':'string','19':'string',
                '21':'string','22':'date','23':'date','24':'date','25':'string','26':'string','27':'string',
                '30':'string','31':'string','32':'date','33':'string','34':'string',
                '41':'date','42':'date','43':'date','44':'date','45':'date','46':'date','47':'date','48':'date',
                '51':'string','52':'string','54':'string','56':'string','57':'string','58':'string',
                '61':'string/date','62':'string/date','63':'string/date','64':'string','65':'string',
                '66':'string/date','67':'string/date','68':'string',
                '71':'string','72':'string','73':'string','74':'string','75':'string','76':'string',
                '81':'string','83':'string','84':'string','85':'date',
                '86_nummer':'string','86_date':'date','87_nummer':'string','87_date':'date','88':'date',
                '91':'date','92':'string/date','93':'string/date','94':'date','95':'string',
                '96':'string','97':'date/string/string','98':'string',
                '121':'string','122':'string','123':'string','124':'string',
                '125':'string','126':'string','127':'string','128':'string'
                }

class auto_id_wrapper:
    def __init__(self, auto_id=None):
        self.auto_id=auto_id
    def __int__(self):
        return self.auto_id
    def __long__(self):
        return self.auto_id
    def set(self,auto_id):
        self.auto_id=auto_id
    def get(self):
        return self.auto_id

class dialect_tab(csv.excel):
    delimiter = '\t'

class ParseException(Exception):
    pass

INID_CODE_LIST_FILE = "inid.txt"
FIPS_PATENT = 1
FIPS_SOFT = 2

def get_html(json_file):
    parsed =  json.load(json_file)
    html = parsed["result"]["html"]
    # print(html)
    html=re.sub("(\\r\\n)+|(\\n)+"," ",html)
    # print(html)
    return html

def test_for_fips_patent_file(file):
    with open(file,mode = "r",encoding = "utf8") as input_file:
        str = re.sub("(\\r\\n)+|(\\n)+"," ",input_file.read())
        result = re.search("\\(19\\).+\\(11\\).+\\(13\\)",str)
    if result:
        return True
    else:
        return False

def test_for_fips_soft_file(file):
    with open(file,mode = "r",encoding = "utf8") as input_file:
        str = re.sub("(\\r\\n)+|(\\n)+"," ",input_file.read())
        result_1 = re.search("\\(12\\).+ГОСУДАРСТВЕННАЯ РЕГИСТРАЦИЯ ПРОГРАММЫ ДЛЯ ЭВМ",str)
        result_2 = re.search("\\(12\\).+ГОСУДАРСТВЕННАЯ РЕГИСТРАЦИЯ БАЗЫ ДАННЫХ",str)
    if result_1 or result_2:
        return True
    else:
        return False
def parse_fields_fips_patent(html):
    html=re.sub("(\\r\\n)+|(\\n)+"," ",html)
    parser = etree.HTMLParser()
    # doc_tree = etree.parse(StringIO(html),parser)#parse string cleaned from \n as io stream
    # root = doc_tree.getroot()
    root = fromstring(html)
    # print(type(root))
    body = root.xpath("descendant-or-self::body")[0]
    # print(type(body))
    # body = doc_tree.find('body')
    # fields=body.xpath("descendant-or-self::*[@class]")
    ########################################################################
    ####  subitems of item 12 patent description   ########################
    #######################################################################
    inid_search_pattern_str = "^\\s*"+inid_search_pattern()+".*"
    paragraph=body.xpath("descendant-or-self::*/p")
    patent_field={}
    for element in paragraph:
        # if element.text != None and re.match(inid_search_pattern_str,element.text):
        if element.text != None:
            searched = re.search(inid_search_pattern_str,element.text)
            if searched != None:
                field_code = searched.group(1)
                # paragraf_with_field.append(element)
                if field_code=="43":
                    with suppress(Exception):
                        patent_field["43_href"]=element.xpath("descendant::b[position()=1]/a")[0].attrib["href"]
                if field_code=="45":
                    with suppress(Exception):
                        patent_field["45_href"]=element.xpath("descendant::b[position()=1]/a")[0].attrib["href"]
                # if field_code=="21" or field_code=="22":
                #     patent_field["21_href"]=element.xpath("descendant::a")[0].attrib["href"]
                bold_ = element.xpath("descendant-or-self::*/b")
                if len(bold_) > 0:
                    bold= bold_[0]
                else:
                    continue
                bold_child=bold.xpath("child::*")
                if len(bold_child) == 0 and bold.text != None:
                    if field_code in ["86","87"]:
                        fips_num_date = fips_get_num_date(bold.text.strip())
                        patent_field[field_code+"_number"]=fips_num_date[0]
                        patent_field[field_code+"_date"]=fips_num_date[1]
                    else:
                        patent_field[field_code] = bold.text.strip()
                    continue
                if len(bold_child) == 0 and bold.text == None:
                    continue
                if field_code=="21" or field_code=="22":
                    with suppress(Exception):
                        patent_field["21_href"]=element.xpath("descendant::a")[0].attrib["href"]
                    for child in bold_child:
                        if child.text != None:
                            patent_field["21"]=child.text.strip()
                            if child.tail != None:
                                patent_field["22"]=re.sub("^\s*,","",child.tail.strip())
                            break
                    continue
                if bold.text != None:
                    all_text = bold.text
                else:
                    all_text=""
                # print (etree.tostring(bold, pretty_print=True))
                for child in bold_child:
                    if child.text != None:
                        all_text+=" " + child.text.strip()
                    if child.tail != None:
                        all_text+=" " + child.tail.strip()
                if field_code in ["86","87"]:
                    fips_num_date = fips_get_num_date(all_text)
                    patent_field[field_code+"_number"]=fips_num_date[0]
                    patent_field[field_code+"_date"]=fips_num_date[1]
                    continue
                patent_field[field_code]=all_text
    ########################################################################
    ####  top items    ########################
    #######################################################################
    with suppress(Exception):
        patent_field["19"]=body.xpath("descendant-or-self::*[@id='top2']")[0].text.strip()
    with suppress(Exception):
        patent_field["13"]=body.xpath("descendant-or-self::*[@id='top6']")[0].text.strip()
    with suppress(Exception):
        patent_field["11"]=body.xpath("descendant-or-self::*[@id='top4']/*[.!='']")[0].text.strip()
    with suppress(Exception):
        patent_field["12"]=body.xpath("descendant-or-self::*[@id='NameDoc']/b[.!='']")[0].text.strip()
    with suppress(Exception):
        all_codes=body.xpath("descendant-or-self::*[@class='top7']/*[@class='ipc']/li/a")
        patent_field["51"]="; ".join([code.text_content().strip() for code in all_codes])
    with suppress(Exception):
        patent_field["98"]=body.xpath("descendant-or-self::*[contains(.,'Адрес для переписки')]/b")[0].text.strip()
    with suppress(Exception):
        patent_field["11_href"]=body.xpath("descendant-or-self::*[@id='top4']/a")[0].attrib["href"]
    with suppress(Exception):
        patent_field["21_href"]=body.xpath("descendant-or-self::*[@id='top4']/a")[0].attrib["href"]
    with suppress(Exception):
        patent_field["21_href"]=body.xpath("descendant-or-self::*[@id='top4']/a")[0].attrib["href"]
    with suppress(Exception):
        left_column=body.xpath("descendant-or-self::*[@id='StatusL']")[0]
        left_column_rows = get_rows(left_column)
        right_column=body.xpath("descendant-or-self::*[@id='StatusR']")[0]
        right_column_rows = get_rows(right_column)
        patent_field["status"]= concat_columns_by_row(left_column_rows,right_column_rows," ",";")

    return patent_field

def fips_get_num_date(str):
    number=re.sub("\\(.+\\)\\s*","",str).strip()
    date=re.search("^[^\\(^\\)]*\\((.+)\\)s*",str).group(1).strip()
    return [number,date]


def fips_inid_fields_convert(code):
    # INID_CODES = ['10','11','12','13','15','19',
    #               '21','22','23','24','25','26','27',
    #               '30','31','32','33','34',
    #               '40','41','42','43','44','45','46','47','48',
    #               '50','51','52','54','56','57','58',
    #               '60','61','62','63','64','65','66','67','68',
    #               '70','71','72','73','74','75','76',
    #               '80','81','83','84','85','86_number','86_date','87_number','87_date','88',
    #               '90','91','92','93','94','95','96','97']
    # OTHER_FIELDS = {
    #     "98":"98",
    #     "11_href":"121",
    #     "21_href":"122",
    #     "43_href":"123",
    #     "45_href":"124",
    #     "125":"125",
    #     "126":"126",
    #     "status":"127"
    # }
    if code.strip() in INID_CODES:
        return "inid_"+code.strip()
    elif code.strip() in OTHER_FIELDS.keys():
        return "f_"+OTHER_FIELDS[code.strip()]
    else:
        return code.strip()


def parse_fields_fips_soft(html):
    html=re.sub("(\\r\\n)+|(\\n)+"," ",html)
    parser = etree.HTMLParser()
    root = fromstring(html)
    body = root.xpath("descendant-or-self::body")[0]

    ########################################################################
    ####  subitems of item 12 patent description   ########################
    #######################################################################
    inid_search_pattern_str = "^\\s*"+inid_search_pattern()+".*"
    paragraph=body.xpath("descendant-or-self::*/p")
    patent_field={}
    ########################################################################
    ####  top items    ########################
    #######################################################################
    with suppress(Exception):
        patent_field["Идентификация органа, регистрирующего программу ЭВМ/базу данных"]=body.xpath("descendant-or-self::*[@id='top2']")[0].text.strip()
    # with suppress(Exception):
    #     patent_field["11"]=body.xpath("descendant-or-self::*[@id='top4']/*[.!='']")[0].text.strip()
    with suppress(Exception):
        patent_field["Cловесное обозначение вида документа"]=body.xpath("descendant-or-self::*[@id='NameDoc']/b[.!='']")[0].text.strip()
    with suppress(Exception):
        xpath_str="descendant::table[@id='bib']/descendant::p[contains(.,'Номер регистрации (свидетельства)')]/descendant::a"
        patent_field["Номер регистрации (свидетельства)"]=body.xpath(xpath_str)[0].text.strip()
        patent_field["Номер регистрации (свидетельства)_href"]=body.xpath(xpath_str)[0].attrib["href"]
    with suppress(Exception):
        xpath_str="descendant::table[@id='bib']/descendant::p[contains(.,'Дата регистрации')]/descendant::b"
        patent_field["Дата регистрации"]=body.xpath(xpath_str)[0].text.strip()
    with suppress(Exception):
        xpath_str="descendant::table[@id='bib']/descendant::p[contains(.,'Номер и дата поступления заявки')]/descendant::b"
        all_text = re.split(" ",body.xpath(xpath_str)[0].text.strip())
        patent_field["Номер заявки"]=all_text[0]
        patent_field["Дата поступления заявки"]=all_text[1]
    with suppress(Exception):
        xpath_str="descendant::table[@id='bib']/descendant::p[contains(.,'Дата публикации')]/descendant::a"
        patent_field["Дата публикации"]=body.xpath(xpath_str)[0].text.strip()
        patent_field["Дата публикации_href"]=body.xpath(xpath_str)[0].attrib["href"]
    with suppress(Exception):
        xpath_str="descendant::table[@id='bib']/descendant::p[contains(.,'Контактные реквизиты')]/descendant::b"
        patent_field["Контактные реквизиты"]=body.xpath(xpath_str)[0].text.strip()
    with suppress(Exception):
        xpath_str="descendant::table[@id='bib']/descendant::p[contains(.,'Автор')]/descendant::b"
        element =body.xpath(xpath_str)[0]
        rows = get_rows(element)
        patent_field["Автор"]="; ".join(map(trim_signes,rows))
    with suppress(Exception):
        xpath_str="descendant::table[@id='bib']/descendant::p[contains(.,'Правообладател')]/descendant::b"
        element =body.xpath(xpath_str)[0]
        rows = get_rows(element)
        patent_field["Правообладатель"]="; ".join(map(trim_signes,rows))
    with suppress(Exception):
        xpath_str="descendant::p[contains(.,'создана') and contains(.,'государственному') and contains(.,'контракту')]"
        if len(body.xpath(xpath_str))>0:
            patent_field["Программа для ЭВМ/база данных создана по государственному контракту"]="ГК"
    with suppress(Exception):
        xpath_str="descendant::p[contains(.,'Название базы данных')]/descendant::b"
        patent_field["Название программы ЭВМ/базы данных"]=body.xpath(xpath_str)[0].text_content().strip()
    with suppress(Exception):
        xpath_str="descendant::p[contains(.,'Название программы для ЭВМ')]/descendant::b"
        patent_field["Название программы ЭВМ/базы данных"]=body.xpath(xpath_str)[0].text_content().strip()
    with suppress(Exception):
        xpath_str="descendant::b[contains(.,'Тип реализующей ЭВМ')]"
        patent_field["Тип реализующей ЭВМ"]=body.xpath(xpath_str)[0].tail.strip()
    with suppress(Exception):
        xpath_str="descendant::b[contains(.,'Вид и версия системы управления базой данных')]"
        patent_field["Вид и версия системы управления базой данных"]=body.xpath(xpath_str)[0].tail.strip()
    with suppress(Exception):
        xpath_str="descendant::b[contains(.,'Вид и версия операционной системы')]"
        patent_field["Вид и версия операционной системы"]=body.xpath(xpath_str)[0].tail.strip()
    with suppress(Exception):
        xpath_str="descendant::b[contains(.,'Объем базы данных')]"
        patent_field["Объем программы для ЭВМ/базы данных"]=body.xpath(xpath_str)[0].tail.strip()
    with suppress(Exception):
        xpath_str="descendant::b[contains(.,'Язык программирования')]"
        patent_field["Язык программирования"]=body.xpath(xpath_str)[0].tail.strip()
    with suppress(Exception):
        xpath_str="descendant::b[contains(.,'Объем программы для ЭВМ')]"
        patent_field["Объем программы для ЭВМ/базы данных"]=body.xpath(xpath_str)[0].tail.strip()
    with suppress(Exception):
        xpath_str="descendant::p[@class='NameIzv']"
        izv_type=body.xpath(xpath_str)[0].text.strip()
        xpath_str="descendant::p[@class='NameIzv']/following-sibling::p[contains(@class,'izv')]"
        izv_rows=body.xpath(xpath_str)
        patent_field["Извещения об изменениях сведений"] = izv_type + "; " + "; ".join([row.text_content().strip() for row in izv_rows])
        # print (patent_field["Извещения об изменениях сведений"])
        xpath_str="descendant::p[@class='NameIzv']/following-sibling::p[contains(@class,'izv')]/descendant::a"
        izv_href=body.xpath(xpath_str)
        patent_field["Извещения об изменениях сведений_href"] = "; ".join([element.attrib["href"] for element in izv_href])

    return patent_field

def filter_blank(str):
    return re.sub("\\s\\s\\s*"," ",str).strip()
def trim_leading_signes(str):
    return re.sub("^\s*[,;]+","",str)
def trim_trailing_signes(str):
    return re.sub("[,;]+\s*$","",str)
def trim_signes(str):
    return trim_trailing_signes(trim_leading_signes(str))
def get_rows(element):
    element_str=etree.tostring(element).decode("utf-8")
    element_str = re.sub("<br>|<br/>"," #@# ",element_str)
    element_ = fromstring(element_str)
    all_text = element_.text_content()
    all_rows = re.split("#@#",all_text)
    all_rows = [row.strip() for row in all_rows]
    return all_rows

def concat_columns_by_row(list_l,list_r, column_separator,row_separator):
    rows = map(column_separator.join,zip(list_l,list_r))
    out_str = row_separator.join(rows)
    return out_str

def inid_search_pattern():
    pattern=""
    for code in inid_code_list():
        if pattern!="":
            # pattern = pattern+"|\\("+code+"\\)"
            pattern = pattern+"|"+code
        else:
            # pattern = "\\("+code+"\\)"
            pattern = code
    pattern = "\\("+"("+pattern+")"+"\\)"
    return pattern

def inid_code_list():
    with open (INID_CODE_LIST_FILE,mode = "r",encoding = "utf8") as file:
        # inid_code_list = csv.DictReader(file, dialect=dialect_tab)
        inid_code_list_ = csv.reader(file, dialect=dialect_tab)
        inid_code_list={}
        for element in list(inid_code_list_)[1:]:
            inid_code_list[element[0]] = element[1]
        return inid_code_list

def get_file_list(dir_path,data_type):
    file_list=[]
    if data_type == "FIPS_PATENT":
        pattern = "^index_[0-9]*\\.html$"
        test_function = test_for_fips_patent_file
    if data_type == "FIPS_SOFT":
        pattern = "^index_[0-9]*\\.html$"
        test_function = test_for_fips_soft_file
    for subdir, dirs, files in os.walk(dir_path):
        for file in files:
            file_path = os.path.join(subdir, file)
            # for file in os.listdir(dir_path):
            if re.match(pattern,file) and test_function(file_path):
                file_list.append(file_path)
    return file_list

def parse_files(file_list,data_type):
    if data_type == "FIPS_PATENT":
        parse_function =  parse_fields_fips_patent
    if data_type == "FIPS_SOFT":
        parse_function =  parse_fields_fips_soft
    patent_list={}
    for file in file_list:
        with open(file,mode = "r",encoding = "utf8") as input_file:
            # patent_list.append(parse_function(input_file.read()))
            result_ = parse_function(input_file.read())
            result=result_
            # patent_list[file]=parse_function(input_file.read())
            patent_list[file]=result
    return patent_list

def save_csv(patent_list,csv_file,append = 0):
    out_csv = csv_tools.csvLog(csv_file,append)
    header_list = sort_headers(list(get_field_type_list(patent_list)))
    if append == 0:
        out_csv.add_row(header_list)
    for patent in patent_list:
        csv_row = []
        for header in header_list:
            value = filter_blank(patent.get(header) or "")
            value = trim_leading_signes(value)
            trim_leading_signes
            csv_row.append(value)
        out_csv.add_row(csv_row)
        # if value !=None:
        #     csv_row.append(value)
        # else:
        #     csv_row.append(value)
    return out_csv


def save_db(db,document_list,mapping,append=0):
    if append == 0:
        db.query("set foreign_key_checks=0")
        db.truncate_table(mapping.all_table())
        db.query("set foreign_key_checks=1")
    # header_list = sort_headers(list(get_field_type_list(patent_list)))
    for file,document_ in document_list.items():
        document = copy.deepcopy(document_)
        transaction=[]
        ref_table= {}
        splitted_doc_field = None
        splitt_result = []
        ref_table_all_fields = {}
        for table in document_table_list_ordered(document,mapping):
            split_count = 1
            if table in mapping.all_table_split_field:
                splitted_doc_field = mapping.all_table_split_field[table]['doc_field']
                split_pattern = mapping.all_table_split_field[table]['split_pattern']
                split_result = split_field(document.get(splitted_doc_field) or "",split_pattern)
                split_count = len(split_result)
            for count in range(split_count):
                if split_count>1:
                    document[splitted_doc_field]=split_result[count]
                action={}
                action["func"]=db.insert
                data={}
                data["table"]=table
                params = {}

                ref_table[table] = auto_id_wrapper()
                data["auto_id"]=ref_table[table]
                if not mapping.all_table_fk[table]["fk"]== "":
                    # id = ref_table[mapping.all_table_fk[table]["ref_table"]]
                    id = ref_table_all_fields[ mapping.all_table_fk[table]["ref_field"]]
                    column = mapping.all_table_fk[table]["fk"]
                    params[column] = id
                    # ref_field = mapping.all_table_fk[table]["ref_field"]
                # for doc_field in document.keys():
                for doc_field in table_field_list(document,table,mapping):
                    table_field_list(document,table,mapping)
                    value = filter_blank(document.get(doc_field) or "")
                    value = trim_leading_signes(value)
                    # column = fips_inid_fields_convert(doc_field)
                    # field_number=re.sub("^\w+_","",column)
                    field_type = mapping.get_attribute_doc_field(doc_field,"field_type")
                    if field_type=="date":
                        if not value =="":
                            value = datetime.strptime(value, '%d.%m.%Y').strftime('%Y-%m-%d')
                        else:
                            value = None
                    column = mapping.get_attribute_doc_field(doc_field,"column_name")
                    params[column]=value
                    if mapping.all_table_ordered[table]==1:
                        ref_table_all_fields[column] = value
                    if mapping.doc_map_by_doc_field[doc_field]["as_id"] == "1":
                        value = re.sub("\s+","",value)
                        column = column+"_id"
                        params[column]=value
                    if mapping.all_table_ordered[table]==1:
                        ref_table_all_fields[column] = value


                    # if header.strip() in INID_CODES_IN_PATENT or header.strip() in OTHER_FIELDS_IN_PATENT.keys():
                    # if  FIELDS_TO_TABLE_MAP == "patent":
                    #     params[column]=value
                    # if doc_field.strip() in INID_CODES_IN_LINKED_TABLES or doc_field.strip() in OTHER_FIELDS_IN_LINKED_TABLES.keys():
                    #
                    # else:
                    #     raise ParseException("Unknown parsed patent/aplication field")
                data["data"]=params
                action["data"]=data
                transaction.append(action)
        try:
            db.transaction(transaction,sql_strings=False)
        except mysql.connector.IntegrityError as e:
            if not e.args[0] == 1062:
                raise
            else:
                print ("MY ERROR 1062: " + e.args[1])
                if mapping.solve_duplicate(transaction,db)=="repeat":
                    db.transaction(transaction,sql_strings=False)
        # db.transaction(transaction,sql_strings=False)
def split_field(str,pattern):
    if str == "" or str == None:
        return str
    result = re.split(pattern,str)
    if result == None:
        return result
    result =[row for row in result if not row.strip()==""]
    # for row in result:
    #     if row.strip()=="":

    return result

def sort_headers(header_list):
    not_href = [header for header in header_list if not re.match(".*href",header)]
    href = [header for header in header_list if re.match(".*href",header)]
    sorted_headers = sorted(not_href)+sorted(href)
    return sorted_headers

def get_field_type_list(patent_list):
    field_type_list = set()
    for patent in patent_list:
        field_type_list.update(patent.keys())
    return field_type_list

def document_table_list(document,mapping):
    table_list = set()
    for doc_field in document.keys():
        table_list.update([mapping.doc_map_by_doc_field[doc_field]['table']])
    return table_list


def table_field_list(document,table,mapping):
    field_list = set()
    for doc_field in document.keys():
        if mapping.doc_map_by_doc_field[doc_field]['table']==table:
            field_list.update([doc_field])
    return field_list


def document_table_list_ordered(document,mapping):
    table_order=[]
    table_list =document_table_list(document,mapping)
    for table in table_list:
        table_order.append([table,mapping.all_table_ordered[table]])
    # table_order_list.sort()
    # list(k for k,_ in itertools.groupby(k))
    table_order=sorted(table_order, key=lambda table: table[1])
    table_order_list = [table[0] for table in table_order]
    return table_order_list


def get_table(doc_field):
    return fips_map.get_attribute_doc_field(doc_field,"table")

def import_to_db(data_folder,db,data_type):
    mapping = doc_map.fips_map(data_type)
    file_list = get_file_list(data_folder,data_type)
    document_list = parse_files(file_list,data_type)
    save_db(db,document_list,mapping)


def import_to_csv(data_folder,output_file,data_type):
    file_list = get_file_list(data_folder,data_type)
    document_list = parse_files(file_list,data_type)
    save_csv(document_list,output_file)


if __name__ == "__main__":
    # FIPS_PATENT_FOLDER = "..\\fips_input_data\\patent"
    FIPS_PATENT_FOLDER = "..\\..\\data_patent"
    FIPS_SOFT_FOLDER = "..\\..\\data_soft"
    # INPUT_JSON_FILE = "..\\fips_input_data\\index13"
    PATENT_OUTPUT_FILE = "..\\fips_out_data\\out_patent.csv"
    SOFT_OUTPUT_FILE = "..\\fips_out_data\\out_soft.csv"
    # data_type = FIPS_SOFT
    # # data_type = FIPS_PATENT
    # if data_type == FIPS_PATENT:
    #     data_folder = FIPS_PATENT_FOLDER
    #     output_file = PATENT_OUTPUT_FILE
    #     mapping = FIPS_PATENT_DOC_MAP
    # if data_type == FIPS_SOFT:
    #     data_folder = FIPS_SOFT_FOLDER
    #     output_file = SOFT_OUTPUT_FILE
    #     mapping = FIPS_SOFT_DOC_MAP
    # file_list = get_file_list(data_folder,data_type)
    # document_list = parse_files(file_list,data_type)
    # # save_csv(patent_list,output_file)
    with db_connect.Database(**common_config.DATABASES['default']) as db:
        data_type = "FIPS_PATENT"
        data_folder = FIPS_PATENT_FOLDER
        output_file = PATENT_OUTPUT_FILE
        import_to_db(data_folder,db,data_type)
        # import_to_csv(data_folder,output_file,data_type)

        data_type = "FIPS_SOFT"
        data_folder = FIPS_SOFT_FOLDER
        output_file = SOFT_OUTPUT_FILE
        import_to_db(data_folder,db,data_type)
        # import_to_csv(data_folder,output_file,data_type)
    exit(0)
