#!/usr/bin/env python
# coding: utf-8

# In[11]:


import pandas as pd
import fuzzywuzzy
from pandas import ExcelWriter
from fuzzywuzzy import fuzz

df1=pd.read_excel(r'C:\Users\Menda Jawahar\Desktop\F1000 companies_clevel.xlsx')
df1['FullName']=df1['firstName'] + ' ' + df1['lastName']

df1.groupby(['ck_nid','FullName']).ck_nid.transform('idxmin')
df2=df1[df1.duplicated(subset=['ck_nid','FullName'], keep=False)]
df2_new=df1[df1.duplicated(subset=['ck_nid','FullName'], keep='first')]
df2.groupby(['company_name','ck_nid']).count()

df2.sort_values(by=['company_name','FullName'],inplace=True)
df2.reset_index(drop=True,inplace=True)
df3=df2.drop(labels=['FullName','Unnamed: 6'],axis=1)

df4=pd.read_csv(r'C:\Users\Menda Jawahar\Desktop\Popularity_Data.csv')
df5=pd.read_csv(r'C:\Users\Menda Jawahar\Desktop\Popularity_Data1.csv')

df5=df5.rename(columns={'executiveId':'executive_id'})
df6=pd.merge(df4,df5,how='right',on='executive_id')
df6.fillna(value=0,inplace=True)

df6['Tot_Count']=df6.pageViewCount_x + df6.pageViewCount_y
df6.drop(columns=['pageViewCount_x','pageViewCount_y'],axis=1,inplace=True)
df7=pd.merge(df3,df6,how='left',on='executive_id')
df7['Tot_Count'].fillna(0,inplace=True)
df7=pd.read_excel(r'C:\Users\Menda Jawahar\Desktop\df7.xlsx')


# In[10]:


# file1=ExcelWriter(r'C:\Users\Menda Jawahar\Desktop\F1000 companies_clevel_OnlyDuplicates.xlsx')
# df2_new.to_excel(file1,'sheet1',index=False)
# file1.save()

# file=ExcelWriter(r'C:\Users\Menda Jawahar\Desktop\df7.xlsx')
# df7.to_excel(file,'sheet1',index=False)
# file.save()


# In[12]:


scores=[]
i=0
while(i<=len(df7)):
    try:
        if df7['company_name'][i]==df7['company_name'][i+1]:
            if df7['firstName'][i] + df7['lastName'][i] == df7['firstName'][i+1] + df7['lastName'][i+1]:
                fuzz_score=fuzz.token_set_ratio(df7['title'][i],df7['title'][i+1])
                scores.append(fuzz_score)
                i+=1
            else:
                i+=1
        else:
            i+=1
    except:
        break


df8=df7.dropna(subset=['Tot_Count'])
df8.reset_index(drop=True,inplace=True)
dfnew=pd.DataFrame(columns=df8.columns)
cols=[]
l1=[]
df7['Popularity Weight']=' '
j=0
pos=0
while(j<=len(df7)):
    pos=0
    try:
        if df7['company_name'][j]==df7['company_name'][j+1]:
            if df7['firstName'][j] + df7['lastName'][j] == df7['firstName'][j+1] + df7['lastName'][j+1]:
                count1=df7['Tot_Count'][j]
                count2=df7['Tot_Count'][j+1]
                count3=max(count1,count2)
                df7['Popularity Weight'].loc[df7['Tot_Count'] == count3]=0.3
                j+=1
            else:
                count3=min(count1,count2)
                df7['Popularity Weight'].loc[df7['Tot_Count'] == count3]=0.0
                j+=1
        else:
            j+=1
    except:
        break


# In[39]:


df8=df7[df7['Count'].apply(lambda x:x<=2)]
df9=df7[df7['Count'].apply(lambda x:x>2 and x<=3)]
df10=df7[df7['Count'].apply(lambda x:x>=4)]
l=list(df7.emp_id)
l1=[]
l2=[]
l3=[]
i=0
for i in range(len(df7)):
    try:
        if df7['Count'][i]==4:
            l1.append(i)
            i+=1
        elif df7['Count'][i]==3 and df7['Count'][i-1]!=4:
            l2.append(i)
            i+=1
        elif df7['Count'][i]==2 and df7['Count'][i+2]==2:
            l3.append(i)
            i+=1
    except:
        break


# In[37]:


l1


# In[1]:


import pandas as pd
import math
df1=pd.read_excel(r'C:\Users\Menda Jawahar\Desktop\F1000 companies_clevel.xlsx')
df1.dropna(subset=['Actual Company Name'],inplace=True)
df1.reset_index(drop=True,inplace=True)

l1=[]
l2=[]

for i in range(len(df1)):
    if (df1['Actual Company Name'][i]==df1['Current Company'][i]) and (df1['Actual Position Name'][i]==df1['Current Position'][i]):
        l1.append(df1['ck_nid'][i])
    else:
        l2.append(df1['ck_nid'][i])

Match_Cases = len(l1)
MisMatch_Cases = len(l2)

df1_currempwithprevemployer=df1.loc[df1['Unnamed: 50']=='Current Employment detail with Previous Employer']
df1_divisioncompany=df1.loc[df1['Division']=='Yes']

MisMatch_Cases_Percentage = MisMatch_Cases/340
Match_Cases_Percentage = Match_Cases/340

print('currempwithprevemployer:{}'.format(len(df1_currempwithprevemployer)))
print('divisioncompany:{}'.format(len(df1_divisioncompany)))
print('Match_Cases_Percentage:{}'.format(Match_Cases_Percentage*100))
print('MisMatch_Cases_Percentage:{}'.format(MisMatch_Cases_Percentage*100))


# In[95]:


df_source=pd.read_csv(r'C:\Users\Menda Jawahar\Desktop\SourceData.csv')
df_source.rename(columns={'id':'emp_id'},inplace=True)
df_final=pd.merge(df7,df_source,on=['emp_id'],how='inner')
df_final.drop(labels=['executive_id_y','firstname','lastname','title_y'],axis=1,inplace=True)

df_UpdatedModifiedData=pd.read_csv(r'C:\Users\Menda Jawahar\Desktop\Updated&Modified_Data.csv')
df_UpdatedModifiedData.rename(columns={'id':'executive_id_x'},inplace=True)

df_final1=pd.merge(df_final,df_UpdatedModifiedData,on=['executive_id_x'],how='left')

file2=ExcelWriter(r'C:\Users\Menda Jawahar\Desktop\Updatedsourcepopularitydate.xlsx')
df_final1.to_excel(file2,'sheet1',index=False)
file2.save()

df_final1['updated_date_weight']=' '
df_final1['updadate'].fillna('0',inplace=True)
df_final1.rename(columns={'executive_id_x':'executive_id','title_x':'title'},inplace=True)

h=0
while(h<=len(df_final1)):
    try:
        if df_final1['company_name'][h]==df_final1['company_name'][h+1]:
            if df_final1['firstName'][h] + df_final1['lastName'][h] == df_final1['firstName'][h+1] + df_final1['lastName'][h+1]:
                max_date=max(str(df_final1['updadate'][h]),str(df_final1['updadate'][h+1]))
                min_date=min(str(df_final1['updadate'][h]),str(df_final1['updadate'][h+1]))
                if df_final1['updadate'][h]==df_final1['updadate'][h+1]:
                    df_final1['updated_date_weight'][h]=0.0
                    df_final1['updated_date_weight'][h+1]=0.0
                    h+=1
                elif df_final1['updadate'][h]==max_date:
                    df_final1['updated_date_weight'][h]=0.4
                    df_final1['updated_date_weight'][h+1]=0.0
                    h+=1
                elif df_final1['updadate'][h]==min_date:
                    df_final1['updated_date_weight'][h]=0.0
                    df_final1['updated_date_weight'][h+1]=0.4
                    h+=1
            else:
                h+=1
        else:
            h+=1
    except:
        break

df_final1['source_count']=' '
df_final2=df_final1.loc[df_final1['source'].isna()==False]
df_final2.reset_index(drop=True,inplace=True)
df_final2.rename(columns={'executive_id_x':'executive_id','title_x':'title'},inplace=True)
a=0
for a in range(len(df_final2)):
    if str(df_final2['source'][a]).count(',')==0:
        df_final2['source_count'][a]=1
    else:
        df_final2['source_count'][a]=str(df_final2['source'][a]).count(',')+1
        a+=1

df_final3=pd.merge(df_final1,df_final2,on=['emp_id'],how='left')


# In[157]:


df_final4=pd.merge(df_final3,df7,on=['emp_id'],how='inner')


# In[158]:


file1=ExcelWriter(r'C:\Users\Menda Jawahar\Desktop\mjrp.xlsx')
df_final4.to_excel(file1,'sheet1',index=False)
file1.save()


# In[150]:


df_final3['source_count_y'].fillna(0,inplace=True)
df_final3['source_count_weight']=' '
j=0
while(j<=len(df_final3)):
    pos=0
    try:
        if df_final3['company_name_x'][j]==df_final3['company_name_x'][j+1]:
            if df_final3['firstName_x'][j] + df_final3['lastName_x'][j] == df_final3['firstName_x'][j+1] + df_final3['lastName_x'][j+1]:
                count1=df_final3['source_count_y'][j]
                count2=df_final3['source_count_y'][j+1]
                count3=max(count1,count2)
                df_final3['source_count_weight'].loc[df_final3['source_count_y'] == count3]=0.3
                j+=1
            else:
                count3=min(count1,count2)
                df_final3['source_count_weight'].loc[df_final3['source_count_y'] == count3]=0.0
                j+=1
        else:
            j+=1
    except:
        break


# In[ ]:


# lastupdateddate-Max Weight
# no of sources-equal weights
# source
# popularity

# findout LI URL and run crowdflower job
# Have to combine both sources and popularity data 


# In[159]:


import pandas as pd


# In[186]:


df_f1000final=pd.read_excel(r'C:\Users\Menda Jawahar\Desktop\F1000_Final.xlsx')


# In[187]:


df_f1000final['comments']=''


# In[188]:


df_f1000final['comments'].loc[df_f1000final['Total_Weight']==1.0]='Perfect'


# In[13]:


df_f1000final


# In[15]:


df7


# In[1]:


import pandas as pd
from pandas import ExcelWriter
df1=pd.read_excel(r'C:\Users\Menda Jawahar\Desktop\F1000 companies_clevel.xlsx')

df1=df1['ck_nid'].drop_duplicates(keep='first')

fileeeee=ExcelWriter(r'C:\Users\Menda Jawahar\Desktop\shashank.xlsx')
df1.to_excel(fileeeee,'sheet1',index=False)
fileeeee.save()

df1=df1[df1.linkedin_profile.isna()==True]


# In[1]:


import pandas as pd
import fuzzywuzzy
from fuzzywuzzy import fuzz


# In[2]:


df1=pd.read_csv(r'C:\Users\Menda Jawahar\Desktop\full_result_196018646.csv')


# In[5]:


df1['contact_company_latest_dump1_title'].value_counts().head()


# In[6]:


s1='tolaram group'
s2='Tolaram Pte group'


# In[79]:


import pandas as pd
df1=pd.read_csv(r'C:\Users\Menda Jawahar\Desktop\CrowdFlow_Title.csv')


# In[82]:


df1.head(n=2)


# In[ ]:




