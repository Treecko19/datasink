// Austin Suarez Data sink assignment 

import requests
import sqlite3
from bs4 import BeautifulSoup

db_name ='datasink.db'
cxn = sqlite3.connect(db_name)
cc = cxn.cursor()

url = "https://en.wikipedia.org/wiki/List_of_best-selling_hip_hop_albums_of_the_2010s_in_the_United_States"
page = requests.get(url)

r = requests.get(url)

print(r.status_code)

soup = BeautifulSoup(page.text, 'html.parser')
table = soup.find('table')

alblum = []

cc.execute("""CREATE TABLE finalalblum (   year INTEGER, 
                                    artist varchar ,
                                    alblum varchar,
                                    Label varchar,
                                    units INTEGEr
                                   
                                    )""")


for i in range(len(table.find_all('tr'))-1):
    
    for row in table.find_all('tr')[i+1]:
        alblum.append(row.getText().strip())
        while "" in alblum:
            alblum.remove("")
    alblum.pop()

    print(alblum[0], "|", alblum[1], "|", alblum[2], "|", alblum[3], "|", alblum[4])

    sql = """INSERT INTO finalalblum      (year,
                                        artist,
                                        alblum,
                                        Label,
                                        units
                                        
                                        )                       
            VALUES(?,?,?,?,?);"""
    cc.execute(sql, alblum)

    alblum = []

cxn.commit()
cxn.close()
