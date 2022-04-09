from bs4 import BeautifulSoup
import mysql.connector
import requests
import json
import csv
import os

### ---Connect to database--- ###
try:
    mydb = mysql.connector.connect(
      host="localhost",
      user="root",
      password="",
      database="3ddd_database"
    )
    mycursor = mydb.cursor()
except:
    
    print('Connection can not be estiblished with the database, please check xampp server to make sure it is running...\n\nCode Execution stoped. Please fix the issue and try again!')
    exit()
### ---Connect to database--- ###

cookies = {
    'PHPSESSID': '9b8e549bd08820c2151b9a44e2d54db0',
    'CookieConsent_ru': '1%7C1677524156',
    '_gid': 'GA1.2.110129187.1648826683',
    'frontsrv': 'k230',
    '_ga_6V3ZTRGYKW': 'GS1.1.1649012551.13.1.1649014966.0',
    '_ga': 'GA1.2.1601563660.1648579996',
    '_gat': '1',
}

headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9,it;q=0.8',
    'Connection': 'keep-alive',
    # Already added when you pass json=
    # 'Content-Type': 'application/json',
    # Requests sorts cookies= alphabetically
    # 'Cookie': 'PHPSESSID=9b8e549bd08820c2151b9a44e2d54db0; CookieConsent_ru=1%7C1677524156; _gid=GA1.2.110129187.1648826683; frontsrv=k230; _ga_6V3ZTRGYKW=GS1.1.1649012551.13.1.1649014966.0; _ga=GA1.2.1601563660.1648579996; _gat=1',
    'DNT': '1',
    'Origin': 'https://3ddd.ru',
    'Referer': 'https://3ddd.ru/3dmodels',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.60 Safari/537.36',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="100", "Google Chrome";v="100"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}


print("Scraper started...\n")

page = 1
data = []
row = []
tags_str = ""

while page <= 2:

    print(f"Scraping Page#{page}...")

    json_data = {
        'page': page,
        'categories': [
        '3d_panel',
        ],
    }
    
    response = requests.post('https://3ddd.ru/api/models', headers=headers, cookies=cookies, json=json_data)

    json_3ddd = json.loads(response.text)

    for model in json_3ddd['data']['models']:
        
        model_name = model['title_en']
        model_cat = model['category_parent']['title_en']
        model_sub_cat = model['category']['title_en']
        img_hash = model['images'][0]['file_name']
        slug = model['slug']

        detail_page = f"https://3ddd.ru/3dmodels/show/{slug}"
        response_2 = requests.get(detail_page,  headers=headers, cookies=cookies)
        soup = BeautifulSoup(response_2.text, features="html.parser")

        try:
        
            num_tags = len(soup.find('p',{'class':'model-tags-text'}).findAll('a'))
            
            for tag_text in soup.find('p',{'class':'model-tags-text'}).findAll('a'):
                if num_tags != 1:
                    tags_str = tags_str + str (tag_text.get_text()) + ", "
                else:
                    tags_str = tags_str + str (tag_text.get_text())
                num_tags = num_tags - 1
            

            sql = "INSERT INTO model (name, img_hash, tags, sub_cat_name, cat) VALUES (%s, %s ,%s ,%s ,%s)"
            val = (model_name, img_hash, tags_str, model_sub_cat, model_cat)
            mycursor.execute(sql, val)

            mydb.commit()

            print(mycursor.rowcount, "record inserted.")
            
            row.append(str(model_cat))
            row.append(str(model_sub_cat))
            row.append(str(model_name))
            row.append(str(img_hash))
            row.append(str(tags_str))
            
            data.append(row)
            tags_str = ""
            row = []

   
        except:
            
            sql = "INSERT INTO model (model_name, img_hash, tags, sub_cat_name, cat) VALUES (%s, %s ,%s ,%s ,%s)"
            val = (model_name, img_hash, tags_str, model_sub_cat, model_cat)
            mycursor.execute(sql, val)

            mydb.commit()

            print(mycursor.rowcount, "record inserted.")

            row.append(str(model_cat))
            row.append(str(model_sub_cat))
            row.append(str(model_name))
            row.append(str(img_hash))
            row.append(str(tags_str))
            
            data.append(row)
            tags_str = ""
            row = []

            
    page = page + 1


### The following code snippet Generate CSV ###

'''
#Check and Remove the existing file
if open('3ddd_data.csv', 'a', newline=''):
    os.remove('3ddd_data.csv')
            
#open a file for writing 
with open('3ddd_data.csv', 'a', encoding="utf-8", newline='') as outputfile:
    #create the csv writer object 
    csv_writer = csv.writer(outputfile) 
                    
    #headers to the CSV file       
    header=['Category', 'Sub Category', 'Model Name', 'Image Hash', "Tags"]
    #Writing headers of CSV file 
    csv_writer.writerow(header) 
                    
    for row in data:
        csv_writer.writerow(row)
        #print(dict_item.values())
            
outputfile.close()

print("3ddd CSV generated succesfully!")
'''


