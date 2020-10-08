 
# Developed By : 
# Syed Faizan Ali  SP17-BSE-110
# Muhammad Hammad  SP17-BSE-074
# Section : G2
# Submitted To : Dr Salman Khan
# Title : Web Of Science

import pyodbc 
import os

def c1(research,cursor,author_dict,source_author,affliation,C1,r_id):
    
    affliation = affliation.split('\n')
    for one in affliation:
        flag=0
        if('[' in one):
            ones = one.split(']')
            if(';' in ones[0][1:]):
                authors = ones[0][1:].split(';')
                if('Dept' in ones[-1][1:]):
                    uni_dept = ones[-1][1:].split("Dept")
                    uni = uni_dept[0]
                    dept = uni_dept[-1]
                else:
                    uni = ones[-1][1:].split(",")[0]
                    dept = "NO Dept"
            else:
                authors = ones[0][1:]
                if('Dept' in ones[-1][1:]):
                    uni_dept = ones[-1][1:].split("Dept")
                    uni = uni_dept[0]
                    dept = uni_dept[-1]
                else:
                    uni = ones[-1][1:].split(",")[0]
                    dept = "NO Dept"
                
                flag=1  
                author = authors    
                if(author_dict.__contains__(author+uni)):
                    a_id = author_dict[author+uni]
                    cursor.execute("Insert into Research_Authors values(?,?)",(r_id,a_id))
                else:
                    cursor.execute("Insert into Authors values(?,?,?,?)",(author,uni,dept,source_author))
                    cursor.execute("SELECT Author_ID AS LastID FROM Authors WHERE Author_ID = @@Identity")
                    a_id=int(cursor.fetchone()[0])
                    cursor.execute("Insert into Research_Authors values(?,?)",(r_id,a_id))
                    author_dict[author+uni]=a_id

        else:
            authors= fieldData(research,'AF ',['TI ']).split('\n')    
            if('Dept' in C1):
                uni_dept = C1.split("Dept")
                uni = uni_dept[0]
                dept = uni_dept[-1]
            else:
                uni = C1.split(",")[0]
                dept = "NO Dept"

        if(uni==''):
            uni="NO uni"

        if(flag==0):
            for author in authors:
                if(author_dict.__contains__(author+uni)):
                    a_id = author_dict[author+uni]
                    cursor.execute("Insert into Research_Authors values(?,?)",(r_id,a_id))
                else:
                    cursor.execute("Insert into Authors values(?,?,?,?)",(author,uni,dept,source_author))
                    cursor.execute("SELECT Author_ID AS LastID FROM Authors WHERE Author_ID = @@Identity")
                    a_id=int(cursor.fetchone()[0])
                    cursor.execute("Insert into Research_Authors values(?,?)",(r_id,a_id))
                    author_dict[author+uni]=a_id




def check(line,end):
    for e in end:
        if(line.startswith(e)):
            return True
    return False

def fieldData(research,start,end):
    copy = False
    data=''
    for l in research:
        if l.startswith(start):
            copy = True
            data = l[3:]
            continue
        elif check(l,end):
            copy = False
            break
        elif copy:
            data = data + l[3:]
    return data[:-2]

def filterResearch(research,cursor,author_dict,source_author):

    language="";doc_type="";no_of_ref=0;time_cited=0;download180=0;download2003=0
    publish_year='';start_page=0;end_page=0;reprint_author='';email='';C1=''

    for l in research:
        if l.startswith('LA '):
            language = l[3:].rstrip('\n')
            continue
        elif l.startswith('DT '):
            doc_type = l[3:].rstrip('\n')
            continue
        elif l.startswith('NR '):
            no_of_ref = l[3:].rstrip('\n')
            continue
        elif l.startswith('TC '):
            time_cited = l[3:].rstrip('\n')
            continue
        elif l.startswith('U1 '):
            download180 = l[3:].rstrip('\n')
            continue
        elif l.startswith('U2 '):
            download2003 = l[3:].rstrip('\n')
            continue
        elif l.startswith('PY '):
            publish_year = l[3:].rstrip('\n')
            continue
        elif l.startswith('BP '):
            start_page = l[3:].rstrip('\n')
            continue
        elif l.startswith('EP '):
            end_page = l[3:].rstrip('\n')
            continue
        elif l.startswith('RP '):
            reprint_author = l[3:].rstrip('\n')
            continue
        elif l.startswith('EM '):
            email = l[3:].rstrip('\n')
            continue
        elif l.startswith('C1 '):
            C1 = l[3:]
            continue

    # authors_names = fieldData(research,"AF ",['TI'])
    title = fieldData(research,'TI ',['SO '])
    journal = fieldData(research,'SO ',['LA '])
    authors_keyword = fieldData(research,'DE ',['ID '])
    keywords_plus = fieldData(research,'ID ',['AB '])
    abstract = fieldData(research,'AB ',['C1 '])
    affliation = fieldData(research,'C1 ',['RP ','EM ','RI ','OI ','CR ','NR '])
    web_science = fieldData(research,'WC ',['SC '])
    research_areas = fieldData(research,'SC ',['GA '])

#   Research Table------------
    
    cursor.execute("INSERT INTO Research VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", 
        (title,journal,language,doc_type,abstract,no_of_ref,time_cited,download180,download2003,publish_year,start_page,end_page))
    cursor.execute("SELECT R_ID AS LastID FROM Research WHERE R_ID = @@Identity")
    r_id=int(cursor.fetchone()[0])

#   Research Area Table------------

    research_areas = research_areas.split(';')
    for area in research_areas:
        cursor.execute("Insert into Research_Area values(?,?)",(r_id,area))

#   Authors keywords Table------------

    authors_keyword = authors_keyword.split(';')
    for keyword in authors_keyword:
        cursor.execute("Insert into Authors_Keywords values(?,?)",(r_id,keyword))

#   Keywords pluus Table------------

    keywords_plus = keywords_plus.split(';')
    for keyword in keywords_plus:
        cursor.execute("Insert into Keywords_Plus values(?,?)",(r_id,keyword))

#   web of Science Table------------
    
    web_science = web_science.split(';')
    for keyword in web_science:
        cursor.execute("Insert into web_of_science values(?,?)",(r_id,keyword))

#   Reprint Author Table------------

    
    author = reprint_author
    author_dep = author.split('(reprint author)')
    authors = author_dep[0]

    if('Dept' in author_dep[-1][1:]):
        uni_dept = author_dep[-1][1:].split("Dept")
        uni = uni_dept[0]
        dept = uni_dept[-1].split(",")[0]
    else:
        uni = author_dep[-1][1:].split(",")[0]
        dept = "NO Dept"

    if(uni==''):
        uni='No University'    
    if(dept==''):
        dept='No Dept'          
    if(email==''):
        email='No Email'
    
        
    cursor.execute("Insert into Reprint_Author values(?,?,?,?,?)",(r_id,authors,uni,dept,email))


#   Authors Table------------

    c1(research,cursor,author_dict,source_author,affliation,C1,r_id)

# start
author_dict = {}
for i in range(1,13):
    path = r"B:\COMSATS_DATA\SEMESTER_6\Data_Warehousing\Web_of_Science_V1.2\data ("+str(i)+")"
    dirListing = os.listdir(path)
    length = len(dirListing)
    length +=1

    for j in range(1,length):
        print("Folder "+str(i),": File "+str(j))
        research = []
        conn = pyodbc.connect('Driver={SQL Server};'
                            'Server=FAIZANSHAH;'
                            'Database=db_webscience;'
                            'Trusted_Connection=yes;')
        cursor = conn.cursor()
        # where Author_Name=? AND Institute_Name=?",(author,uni)

        with open("B:\COMSATS_DATA\SEMESTER_6\Data_Warehousing\Web_of_Science_V1.2\data ("+str(i)+")/input("+str(j)+").txt", encoding="utf8") as infile:
            copy = False
            count = 1
            for line in infile:
                count +=1
                if (line.startswith("PT J") or line.startswith("nullPT J")):
                    copy = True
                    continue
                elif line.startswith("ER"):
                    copy = False
                    source_author = str(i)+"_"+str(j)+"_"+str(count)
                    filterResearch(research,cursor,author_dict,source_author)
                    research.clear()
                    continue
                elif copy:
                    research.append(line)

        conn.commit() 
    

