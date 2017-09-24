#coding windows-1251
__author__ = 'mdu'
#import os
from copy import deepcopy
import io
import re
import Fragments
from common_config import *
'''
try:
    import Fragments
except ImportError:
    try:
        #from tomita_tools import Fragments
    except:
        raise
'''
#from tomita_tools import Fragments
from io import StringIO
from datetime import datetime


try:
    from lxml import etree
except ImportError:
 print("lxml import error")
 raise
class ParametersForMarkup():
    def __init__(self, **kwargs):
        #absolute or relative path to input markuped html file
        self.inputMarkupedHTMLFile = kwargs.get('INPUT_MARKUPED_HTML_FILE','')
        #absolute or relative path to output text file
        self.outputTextFile = kwargs.get('OUTPUT_TEXT_FILE',"markuped_text.txt")
        #absolute or relative path to output xml file
        self.outputXMLFile = kwargs.get('OUTPUT_XML_FILE',"markuped_fragments.xml")
        #input file encoding
        self.inputFileEncoding = kwargs.get('INPUT_FILE_ENCODING',DEFAULT_INPUT_HTML_FILE_ENCODING)
        #output text file encoding
        self.outputTextFileEncoding = kwargs.get('OUTPUT_TEXT_FILE_ENCODING',DEFAULT_OUTPUT_TXT_FILE_ENCODING)
        #output file encoding
        self.outputXMLFileEncoding = kwargs.get('OUTPUT_XML_FILE_ENCODING',DEFAULT_OUTPUT_XML_FILE_ENCODING)
#*********************************************************************************
#***function return list of lists where each list contain 2 elements:
#  [text or tail , nearest outer element with markup css class attribute]
#  texts/tails ordered as in the parsed document
def iterateTextElements(element,textElements,markupedElements):
    #print(element.tag+"\n")
    textElement=[]
    if element.text != None:
        if "&clubs" in  element.text:
            #***create list of 2 elemets - [text of element ,nearest outer element with markup css class attribute]
            textElement.append(re.sub("&clubs","",element.text))
            textElement.append(markupedElements.get(element))
            #***append above list to list of lists
            textElements.append(textElement)
            textElement=[]
            textElement.append("\n")
            textElement.append(None)
            textElements.append(textElement)
            element.text= re.sub("&clubs","",element.text)
        else:
            #***create list of 2 elemets - [text of element ,nearest outer element with markup css class attribute]
            textElement.append(element.text)
            textElement.append(markupedElements.get(element))
            #if markupedElements.get(element)!=None and markupedElements.get(element).attrib["class"]== "WordSection1":
            #    print (element)
            #***append above list to list of lists
            textElements.append(textElement)
    #***recursive iterate over child elements
    for childElement in element:
        iterateTextElements(childElement,textElements,markupedElements)
        textElement=[]
        if childElement.tail != None:
            if "&clubs" in  childElement.tail:
                #***create list of 2 elemets - [tail of element ,nearest outer element with markup css class attribute]
                textElement.append(re.sub("&clubs","",childElement.tail))
                textElement.append(markupedElements.get(element))
                #***append above list to list of lists
                textElements.append(textElement)
                textElement=[]
                textElement.append("\n")
                textElement.append(None)
                textElements.append(textElement)
                childElement.tail= re.sub("&clubs","",childElement.tail)
            else:
                #***create list of 2 elemets - [tail of element ,nearest outer element with markup css class attribute]
                textElement.append(childElement.tail)
                textElement.append(markupedElements.get(element))
                #***append above list to list of lists
                textElements.append(textElement)

def createXMLWithReplacements(replacements,xmlEncoding):
    root = etree.XML('<fdo_objects><document url="" di="" bi="" date=""><facts></facts></document></fdo_objects>')
    root.set('date',datetime.now().strftime('%Y-%m-%d'))
    #facts = root.find("facts")
    facts = root.xpath("descendant-or-self::facts")[0]
    for replacement in replacements:
        addReplacementToXML(facts,replacement)
    return etree.tostring(root, xml_declaration=True,encoding=xmlEncoding).decode(xmlEncoding)

def addReplacementToXML(facts,replacement):
    fact = etree.SubElement(facts, replacement.markupStyleID,pos=str(replacement.position), len=str(replacement.length))
    replacementText = etree.SubElement(fact, 'ReplacementText',val = replacement.text)
#******************************************************************************************
#@profile
def markup(parameters: ParametersForMarkup)-> ParametersForMarkup:

    start_time=datetime.now()
    print("Start processing: "+start_time.strftime("%d.%m.%Y %H:%M:%S.%f"))
    # ******* read word markup style names from config file
    styleList={} # list of lists [markup style name, marup style ID]
    with io.open(file="StylesList.txt", encoding=DEFAULT_STYLE_LIST_FILE_ENCODING,mode="rt") as f:
        for s in f.read().splitlines():
            styleList[s.split(",")[0]]=s.split(",")[1]
    #******* map markup css class names and markup style names in html file
    s=""
    #inputFileName = "Greka_marked.htm"
    #inputFileName = "test.htm"
    #inputFileName = "Truba_markup.htm"
    #inputFileName = parameters.inputMarkupedHTMLFile
    with open(parameters.inputMarkupedHTMLFile,encoding=parameters.inputFileEncoding, mode="r") as f:
        s=f.read()
    styleClassMap={}#***dictionary {markup style name, markup css class name}
    classStyleMap={}#***dictionary {markup css class name,markup style name}
    #styleSection = re.findall('<\s*style\s*>.*<\-\-.*\-\->.*<\s*/\s*style\s*>', s,flags=re.DOTALL | re.IGNORECASE)[0]
    styleSection = re.findall(r"<\s*style\s*>.*<\!--.*-->\s*<\s*/\s*style\s*>", s,flags=re.DOTALL | re.IGNORECASE)[0]
    for style in styleList:
        #className = re.findall('(\w+)[\s\n]*\{[^\{]*'+style[0].replace("/","\\\\/")+'\W*;', s,flags=re.DOTALL |
        # re.IGNORECASE)
        #className = re.findall('(\w+)[\s\n]*\{[^\{]*'+style.replace("/","\\\\/")+'\W*;', s,flags=re.DOTALL | re.IGNORECASE)
        className = re.findall('(\w+)[\s\n]*\{[^\{]*'+style.replace("/","\\\\/")+'\W*;', styleSection,flags=re.DOTALL | re.IGNORECASE)
        #styleClassMap[style[0]]=className[0] if len(className)> 0 else ""
        styleClassMap[style]=className[0] if len(className)> 0 else ""
        if len(className)>0:
            #classStyleMap[className[0]]= style[0]
            classStyleMap[className[0]]= style
    #print(styleClassMap)
    #******* map text element and parent markup css class
    #******* step 1: clear html  remove \n e.c.t.
    #with open(parameters.inputMarkupedHTMLFile, "r") as f:
    #    s=f.read()

    #*** special case 1: remove \n inside tag replcacing with " "
    searchPattern=re.compile("<[^<^>]*[\n][^<^>]*>")
    s=re.sub(searchPattern,lambda m: m.group(0).replace("\n"," "),s)
    # while True:
    #     tagBreaks=re.findall("(<[^<^>]*)([\n])([^<^>]*>)",s)
    #     if len(tagBreaks)== 0:
    #         break
    #     for tagBreak in tagBreaks:
    #         searchPattern = tagBreak[0]+tagBreak[1]+tagBreak[2]
    #         s=s.replace(searchPattern,tagBreak[0]+" "+tagBreak[2])
#*** special case 2: remove \n inside text elements replcacing with " "
    searchPattern=re.compile("(>[^<^>]*)([\n])([^<^>]*<)")
    s=re.sub(searchPattern,lambda m: m.group(0).replace("\n"," "),s)
    # while True:
    #     tagBreaks=re.findall("(>[^<^>]*)([\n])([^<^>]*<)",s)
    #     if len(tagBreaks)== 0:
    #         break
    #     for tagBreak in tagBreaks:
    #         searchPattern = tagBreak[0]+tagBreak[1]+tagBreak[2]
    #         s=s.replace(searchPattern,tagBreak[0]+" "+tagBreak[2])

#*** common case: remove \n between tag replcacing with ""
    #print (s)
    s=re.sub("[\n]+","",s)
    s=re.sub(" </p>","&clubs</p>",s)
#    print (s)

    #******* step 2: get <body> element from clean string
    parser = etree.HTMLParser()
    #doc_tree = etree.parse(os.path.join(os.getcwd(),"test.htm"),parser)
    doc_tree = etree.parse(StringIO(s),parser)#parse string cleaned from \n as io stream
    root = doc_tree.getroot()
    body = doc_tree.find('body')
    #print (etree.tostring(body, pretty_print=True))
    #******* step 3: create string with search pattern from css markup class names
    cssMarkupClassNamePattern=""
    for className in styleClassMap:
        cssMarkupClassNamePattern+=(styleClassMap[className]+"|")
    cssMarkupClassNamePattern=cssMarkupClassNamePattern[:-1]
    cssMarkupClassNamePattern="^("+cssMarkupClassNamePattern+")$"

    #******* step 4: create  css markup dictionary {element, nearest outer element with markup css class attribute}
    elementsWithCSSClass=body.xpath("descendant-or-self::*[@class]")#list of all elements with css class attribute
    elementsWithMarkupCSSClass=[]#***list of elements with markup css class attribute
    for element in elementsWithCSSClass:
        if re.match(cssMarkupClassNamePattern,element.attrib["class"]):
            elementsWithMarkupCSSClass.append(element)


    #*** merge splited styles(two neigbor elements with same markup css class with no symbol between)
    elementsForDelete=[]
    for elementWithMarkupCSSClass in elementsWithMarkupCSSClass:
        nextElement = elementWithMarkupCSSClass
        while True:
            nextElement = nextElement.getnext()
            #print(nextElement)
            if nextElement!=None and "class" in nextElement.attrib and \
                    elementWithMarkupCSSClass.attrib["class"]== nextElement.attrib["class"] \
                    and elementWithMarkupCSSClass.tail == None and  "---x---" not in elementWithMarkupCSSClass.attrib \
                    and  "---x---" not in nextElement.attrib:
                nextElement.attrib["---x---"]="1"
                for childElement in nextElement:
                    if elementWithMarkupCSSClass[-1].tail != None and nextElement.text != None:
                        elementWithMarkupCSSClass[-1].tail+=nextElement.text
                    elif elementWithMarkupCSSClass[-1].tail == None and nextElement.text != None:
                        elementWithMarkupCSSClass[-1].tail=nextElement.text
                    elementWithMarkupCSSClass.append(deepcopy(childElement))
                    #print(etree.tostring(childElement, encoding='unicode',method='html', pretty_print=True))
                    elementsForDelete.append(nextElement)
                        #print(nextElement)
                #print(etree.tostring(nextElement))
                #print(etree.tostring(elementWithMarkupCSSClass))

                if nextElement.tail != None:
                    elementWithMarkupCSSClass.tail=nextElement.tail
                    break
            else:
                break
    #print(etree.tostring(body, encoding='unicode',method='html', pretty_print=True))
    for element in elementsForDelete:
        element.getparent().remove(element)
    #print(etree.tostring(body, encoding='unicode',method='html', pretty_print=True))
    #elementsWithCSSClass=body.xpath("descendant-or-self::*[@---x--- ]")
    #******* rebuild  css markup dictionary after merging}
    elementsWithCSSClass =[]
    elementsWithCSSClass=body.xpath("descendant-or-self::*[@class]")#list of all elements with css class attribute
    elementsWithMarkupCSSClass=[]#***list of elements with markup css class attribute
    for element in elementsWithCSSClass:
        if re.match(cssMarkupClassNamePattern,element.attrib["class"]):
        #if re.match("^("+cssMarkupClassNamePattern+")$",element.attrib["class"]):
            #print(element.attrib["class"])
            elementsWithMarkupCSSClass.append(element)
    markupedElements={}#***dictionary {element, nearest outer element with markup css class attribute}
    #markupedElement={}
    for elementWithMarkupCSSClass in elementsWithMarkupCSSClass:
        #print(etree.tostring(elementWithMarkupCSSClass, encoding='unicode',method='html', pretty_print=True))
        for element in elementWithMarkupCSSClass.xpath("descendant-or-self::*"):
            markupedElements[element]=elementWithMarkupCSSClass
            #markupedElements[element]=elementWithMarkupCSSClass.attrib["class"]

    #******* step 5: extract text  from markuped elements and map them with css markup elements


    #for element in  markupedElements:
    #if markupedElements.get(element)!=None and markupedElements.get(element).attrib["class"]== "WordSection1":
    #    print (element.attrib["class"])
    textElements=[]#***list of lists where each list contain text or tail in the order as in the parsed document
    iterateTextElements(body,textElements,markupedElements)

    #******* step 6: create output text file
    s=""
    for element in textElements:
        s=s+element[0]
    #with open("out_text.txt", "w") as f:
    with open(parameters.outputTextFile,encoding=parameters.outputTextFileEncoding,mode= "w") as f:
        f.write(s)


    #******* step 7: create list of objects with description of extracted text fragments marked as replacements

    replacements=[]
    length=0
    position = 1
    #print (len("\n"))
    last_element = None
    for element in textElements:
    #    if element[1] != None and element[1].attrib["class"]== "WordSection1":
    #        print (element)
        if element[1] != None and element[1] is not last_element:
            r=Fragments.ManualReplacement()
            r.text=element[0]
            #r.length=len(element[0])
            r.length=len(re.sub("\n","",element[0]))+len(re.findall("\n",element[0]))*2
            r.position=position-1
            #position+=len(re.sub("\n","",element[0]))
            position+=r.length
            #print(element[1].attrib["class"])
            r.markupStyleName=classStyleMap.get(element[1].attrib["class"])
            r.markupStyleID=styleList[r.markupStyleName]
            r.markupCSSClassName=element[1].attrib["class"]
            r.pathToElementWithCSSMarkup=doc_tree.getpath(element[1])
            replacements.append(r)
        elif element[1] != None and element[1] is last_element:
            replacements[-1].text+=element[0]
            delta= len(re.sub("\n","",element[0]))+len(re.findall("\n",element[0]))*2
            replacements[-1].length+=delta
            position+=delta
        else:
            #position+=len(re.sub("\n","",element[0]))
            position+=len(re.sub("\n","",element[0]))+len(re.findall("\n",element[0]))*2
        last_element = element[1]
    #******* step 8: create output XML file with replacemtnts


    #with open("out_xml.xml", "w") as f:
    with open(parameters.outputXMLFile, encoding=parameters.outputXMLFileEncoding,mode="w") as f:
        s= createXMLWithReplacements(replacements,parameters.outputXMLFileEncoding)
        f.write(s)

    end_time=datetime.now()
    print("End processing: "+end_time.strftime("%d.%m.%Y %H:%M:%S.%f"))
    duration = end_time-start_time;
    duration_str = "%d.%d" % (duration.seconds, duration.microseconds / 1000)
    print("Duration: "+duration_str+"s" )
    print("Markups found: "+str(len(replacements)) )
    return
    #exit(0)


if __name__ == '__main__':
    p =  ParametersForMarkup()
    p.inputMarkupedHTMLFile = "C:\\tomita_project\\Address\\input\\test_0068.htm"
    p.outputTextFile = "C:\\tomita_project\\Address\\input\\test_0068.txt"
    p.outputXMLFile = "C:\\tomita_project\\Address\\input\\test_0068.xml"
    markup(p)
    exit(0)
