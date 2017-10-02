__author__ = 'mdu'
import json
from io import StringIO
import re
import csv
import csv_tools
import os
# import db_connect

from contextlib import suppress
try:
    from lxml import etree
    from lxml.html import fromstring
except ImportError:
    print("lxml import error")
    raise

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
                    patent_field[field_code+"_number"]=re.sub("\\(.+\\)\\s*","",all_text).strip()
                    patent_field[field_code+"_date"]=re.search("^[^\\(^\\)]*(\\(.+\\))s*").group(1).strip()
                    break
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



def fips_inid_fields_convert(code):
    INID_CODES = ['10','11','12','13','15','19',
                  '21','22','23','24','25','26','27',
                  '30','31','32','33','34',
                  '40','41','42','43','44','45','46','47','48',
                  '50','51','52','54','56','57','58',
                  '60','61','62','63','64','65','66','67','68',
                  '70','71','72','73','74','75','76',
                  '80','81','83','84','85','86','87','88',
                  '90','91','92','93','94','95','96','97']
    OTHER_FIELDS = {
        "98":"98",
        "11_href":"121",
        "21_href":"122",
        "43_href":"123",
        "45_href":"124",
        "status":"127"
    }
    if code.strip in INID_CODES:
        return "inid_"+code.strip
    elif code.strip in OTHER_FIELDS:
        return "f_"+OTHER_FIELDS[code.strip]
    else:
        return code.strip


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
        patent_field["19"]=body.xpath("descendant-or-self::*[@id='top2']")[0].text.strip()
    with suppress(Exception):
        patent_field["11"]=body.xpath("descendant-or-self::*[@id='top4']/*[.!='']")[0].text.strip()
    with suppress(Exception):
        patent_field["12"]=body.xpath("descendant-or-self::*[@id='NameDoc']/b[.!='']")[0].text.strip()
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
        patent_field["Номер поступления заявки"]=all_text[0]
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
        xpath_str="descendant::p[contains(.,'Название базы данных')]/descendant::b"
        patent_field["Название базы данных"]=body.xpath(xpath_str)[0].text_content().strip()
    with suppress(Exception):
        xpath_str="descendant::p[contains(.,'Название программы для ЭВМ')]/descendant::b"
        patent_field["Название программы для ЭВМ"]=body.xpath(xpath_str)[0].text_content().strip()
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
        patent_field["Объем базы данных"]=body.xpath(xpath_str)[0].tail.strip()
    with suppress(Exception):
        xpath_str="descendant::b[contains(.,'Язык программирования')]"
        patent_field["Язык программирования"]=body.xpath(xpath_str)[0].tail.strip()
    with suppress(Exception):
        xpath_str="descendant::b[contains(.,'Объем программы для ЭВМ')]"
        patent_field["Объем программы для ЭВМ"]=body.xpath(xpath_str)[0].tail.strip()
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
    if data_type == FIPS_PATENT:
        pattern = "^index_[0-9]*\\.html$"
        test_function = test_for_fips_patent_file
    if data_type == FIPS_SOFT:
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
    if data_type == FIPS_PATENT:
        parse_function =  parse_fields_fips_patent
    if data_type == FIPS_SOFT:
        parse_function =  parse_fields_fips_soft
    patent_list=[]
    for file in file_list:
        with open(file,mode = "r",encoding = "utf8") as input_file:
            patent_list.append(parse_function(input_file.read()))
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


def save_db(db,patent_list,csv_file,append = 0):
    # out_csv = csv_tools.csvLog(csv_file,append)
    # header_list = sort_headers(list(get_field_type_list(patent_list)))
    table_list = ["patent","patent_f125","patent_f126","patent_inid_15","patent_inid_21","patent_inid_22", \
                  "patent_inid_23","patent_inid_31","patent_inid_32","patent_inid_51","patent_inid_56", \
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
    if append == 0:
        db.query("set foreign_key_checks=0")
        db.truncate_table(table_list)
        db.query("set foreign_key_checks=1")
    header_list = sort_headers(list(get_field_type_list(patent_list)))
    sql={}
    sql["func"]="insert"
    for patent in patent_list:
        data={}
        data["table"]="patent"
        for header in patent.keys:
            value = filter_blank(patent.get(header) or "")
            value = trim_leading_signes(value)
            column = fips_inid_fields_convert( value)
            if column in INID_CODES_IN_PATENT or column in OTHER_FIELDS_IN_PATENT.values():
                data[column]=patent[header]
            else:
                raise ParseException("Unknown parsed patent/aplication field")
            sql["data"]=data


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

if __name__ == "__main__":
    # FIPS_PATENT_FOLDER = "..\\fips_input_data\\patent"
    FIPS_PATENT_FOLDER = "..\\..\\data_patent"
    FIPS_SOFT_FOLDER = "..\\..\\data_soft"
    # INPUT_JSON_FILE = "..\\fips_input_data\\index13"
    PATENT_OUTPUT_FILE = "..\\fips_out_data\\out_patent.csv"
    SOFT_OUTPUT_FILE = "..\\fips_out_data\\out_soft.csv"
    # data_type = FIPS_SOFT
    data_type = FIPS_PATENT
    if data_type == FIPS_PATENT:
        data_folder = FIPS_PATENT_FOLDER
        output_file = PATENT_OUTPUT_FILE
    if data_type == FIPS_SOFT:
        data_folder = FIPS_SOFT_FOLDER
        output_file = SOFT_OUTPUT_FILE
    file_list = get_file_list(data_folder,data_type)
    patent_list = parse_files(file_list,data_type)
    save_csv(patent_list,output_file)
    exit(0)


    exit(0)