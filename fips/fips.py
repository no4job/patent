__author__ = 'mdu'
import json
from io import StringIO
import re
import csv
import csv
from contextlib import suppress
try:
    from lxml import etree
    from lxml.html import fromstring
except ImportError:
    print("lxml import error")
    raise

class dialect_tab(csv.excel):
    delimiter = '\t'

INID_CODE_LIST_FILE = "inid.txt"

def get_html(json_file):
    parsed =  json.load(json_file)
    html = parsed["result"]["html"]
    # print(html)
    html=re.sub("(\\r\\n)+|(\\n)+"," ",html)
    # print(html)
    return html

def parse_fields(html):
    parser = etree.HTMLParser()
    # doc_tree = etree.parse(StringIO(html),parser)#parse string cleaned from \n as io stream
    # root = doc_tree.getroot()
    root = fromstring(html)
    print(type(root))
    body = root.xpath("descendant-or-self::body")[0]
    print(type(body))
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
                    patent_field[field_code] = bold.text
                    continue
                if len(bold_child) == 0 and bold.text == None:
                    continue
                if field_code=="21" or field_code=="22":
                    with suppress(Exception):
                        patent_field["21_href"]=element.xpath("descendant::a")[0].attrib["href"]
                    for child in bold_child:
                        if child.text != None:
                            patent_field["21"]=child.text
                            if child.tail != None:
                                patent_field["22"]=child.tail
                            break
                    continue
                all_text=""
                print (etree.tostring(bold, pretty_print=True))
                for child in bold_child:
                    if child.text != None:
                        all_text+=" " + child.text
                    if child.tail != None:
                        all_text+=" " + child.tail
                patent_field[field_code]=all_text
    ########################################################################
    ####  top items    ########################
    #######################################################################
    with suppress(Exception):
        patent_field["19"]=body.xpath("descendant-or-self::*[@id='top2']")[0].text
    with suppress(Exception):
        patent_field["13"]=body.xpath("descendant-or-self::*[@id='top6']")[0].text
    with suppress(Exception):
        patent_field["11"]=body.xpath("descendant-or-self::*[@id='top4']/*[.!='']")[0].text
    with suppress(Exception):
        patent_field["12"]=body.xpath("descendant-or-self::*[@id='NameDoc']/b[.!='']")[0].text.strip()
    with suppress(Exception):
        all_codes=body.xpath("descendant-or-self::*[@class='top7']/*[@class='ipc']/li/a")
        patent_field["51"]="; ".join([code.text_content() for code in all_codes])
    with suppress(Exception):
        patent_field["98"]=body.xpath("descendant-or-self::*[contains(.,'Адрес для переписки')]/b")[0].text
    with suppress(Exception):
        patent_field["11_href"]=body.xpath("descendant-or-self::*[@id='top4']/a")[0].attrib["href"]
    with suppress(Exception):
        patent_field["21_href"]=body.xpath("descendant-or-self::*[@id='top4']/a")[0].attrib["href"]
    with suppress(Exception):
        patent_field["21_href"]=body.xpath("descendant-or-self::*[@id='top4']/a")[0].attrib["href"]
    return patent_field




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

if __name__ == '__main__':
    INPUT_JSON_FILE = "..\\fips_input_data\\index13"
    OUTPUT_CSV_FILE = "..\\fips_out_data\\out.csv"
    with open(INPUT_JSON_FILE,mode = "r",encoding = "utf8") as input_json_file:
        html = get_html(input_json_file)
        patent_fields = parse_fields(html)
        print(patent_fields)
    exit(0)
