#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#Importing Necessary Libraries
from bs4 import BeautifulSoup as soup
import requests
import lxml
import re
import pandas as pd
from threading import Thread
from elasticsearch import Elasticsearch

if __name__== "__main__":
    main_url='https://en.wikipedia.org/wiki/List_of_universities_in_England'
    source=requests.get(my_url).text
    page_soup=soup(source,'lxml')
    containers=page_soup.find('div',{'class':"mw-parser-output"})
    rows= containers.table.find_all('tr') 
    data=[]
    header_name=[]
    x=0
    type_name=re.split('https://en.wikipedia.org/wiki/',my_url)[1]
    # csv_file=open(csv_name[1],'w')

    for row in rows:
        x+=1
        if x==1:
            header=row.find_all('th')
            for hedr in header:
                header_name.append(hedr.text[0:-1])
            header_name.append('Url')
            data.append(header_name)
        else:
            cols = row.find_all('td')
            link='https://en.wikipedia.org'+cols[0].a.get('href')
            cols = [ele.text.strip() for ele in cols]
            cols.append(link)
            data.append([ele for ele in cols if ele])

    df = pd.DataFrame.from_records(data[1:], columns=data[0])
    urls=list(df['Url'])
    sub_agg_df = pd.DataFrame(columns=['University','Former names','Detailed_Location','Students','Undergraduates','Postgraduates','Url'])
    print(sub_agg_df)

    # defining the function for scraping individual website 
    def sub_scrapping(url,indx,U_name):
        print(url,indx)
        my_url=url
        try:
            print('Executing for '+my_url)
            source=requests.get(my_url).text
            page_soup=soup(source,'lxml')
            containers=page_soup.find('table',{'class':"infobox vcard"})
            indivual_data={}
            sub_headers=['Former names','Location','Students','Undergraduates','Postgraduates']

            for sh in range(1,6):
                sub_container=containers.find_all('tr')
                for ele in sub_container:
                    try:
                        if ele.th.text==sub_headers[sh-1]:
                            indivual_data.update({sub_headers[sh-1]:ele.td.text})
                    except:
                        pass
                if len(indivual_data) < sh:
                    indivual_data.update({sub_headers[sh-1]:None})


            df_sub=pd.DataFrame([[U_name,indivual_data['Former names'],str(indivual_data['Location']),indivual_data['Students'],indivual_data['Undergraduates'],indivual_data['Postgraduates'],my_url]],columns=['University','Former names','Detailed_Location','Students','Undergraduates','Postgraduates','Url'],index=[indx])
            global sub_agg_df
            sub_agg_df=sub_agg_df.append(df_sub)
        except:
            print('Error in fetching url '+my_url)

        threadlist=[]
    for x in df.itertuples():
        td= Thread(target=sub_scrapping,args=(x.Url,x.Index,x.University))
        td.start()
        threadlist.append(td)
    for b in threadlist:
        b.join()
    df.drop('Url', axis=1, inplace=True)
    final_df=pd.merge(sub_agg_df, df, on='University')
    # Optional: Saving the table in CSV format for analysis purspose in excel:
    final_df.to_csv(type_name+'.csv')
    es= Elasticsearch('http://localhost:9200')
    es.indices.delete(index='universities')

    # {
    #     "settings":{
    #         "analysis":{
    #             "analyzer":{
    #                 "my_analyzer":{
    #                     "type":"keyword",
    #                 }
    #             }
    #         }
    #     }

    #     "mappings":{
    #         "doc":{
    #             "dynamic": "strict",
    #             "properties":{
    #                 "University":{
    #                     "type":"text",
    #                     "fields":{
    #                         "keyword":{
    #                             "type":"keyword"
    #                         }
    #                     },
    #                     "analyzer":"my_analyzer"
    #                 },
    #                 "Former names":{
    #                     "type":"text",
    #                     "fields":{
    #                         "keyword":{
    #                             "type":"keyword"
    #                         }
    #                     },
    #                     "analyzer":"my_analyzer"
    #                 },
    #                 "Detailed_Location":{
    #                     "type":"text",
    #                     "fields":{
    #                         "keyword":{
    #                             "type":"keyword"
    #                         }
    #                     },
    #                     "analyzer":"my_analyzer"
    #                 },
    #                 "Students":{
    #                     "type":"integer"
    #                 },
    #                 "Undergraduates":{
    #                     "type":"integer"
    #                 },
    #                 "Postgraduates":{
    #                     "type":"integer"
    #                 },
    #                 "Url":{
    #                     "type":"text",
    #                     "fields":{
    #                         "keyword":{
    #                             "type":"keyword"
    #                         }
    #                     },
    #                     "analyzer":"my_analyzer"
    #                 },
    #                 "Location":{
    #                     "type":"text",
    #                     "fields":{
    #                         "keyword":{
    #                             "type":"keyword"
    #                         }
    #                     },
    #                     "analyzer":"my_analyzer"
    #                 },
    #                 "Established":{
    #                     "type":"integer"
    #                 },
    #                 "Number of students":{
    #                     "type":"integer"
    #                 },
    #                 "Tuition fee":{
    #                     "type":"integer"
    #                 },
    #                 "Degree powers":{
    #                     "type":"text",
    #                     "fields":{
    #                         "keyword":{
    #                             "type":"keyword"
    #                         }
    #                     },
    #                     "analyzer":"my_analyzer"
    #                 }
    #             }
    #         }
    #     }
    # }
    es.indices.create(index='universities',ignore=400)
    for x in final_df.iterrows():
        es.index(index='universities',doc_type=type_name,id=x[0] ,body=dict(x[1]))
    print('!!!!!!!!----Data Transfer Completed-----!!!!!!!')

    es= Elasticsearch('http://localhost:9200')
    res=es.search(index='universities',body={"from":0, "size":1,"query":{"match":{"University":"Harper Adams University"}}})
    res          
    # Execting queries
    res=es.search(index="universities",body={"from":0, "size":2, "query":{"bool":{"must":{"match":{"University":"Harper Adams University"}},"must":{"match":{"Former names":"None"}}}}})           
    res

