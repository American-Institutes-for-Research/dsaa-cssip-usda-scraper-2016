import pandas as pd 
import numpy as np
import time
from datetime import date
from scipy import array, concatenate
import numpy as np
from pandas import DataFrame

path = 'H:\CSSIP\Data\CRIS_Project'.replace("\\" , "/")
path_2 = 'H:\CSSIP\Data\CRIS_Project\Old_Data'.replace('\\' , '/')
test   = 'H:\CSSIP\Data\CRIS_Project\explore'.replace("\\" , "/")
print path
print test


cris_df = pd.read_csv(path+'/4_26_2016_main_info_USDA_awards.csv')
# print cris_df.head()


#USDA_grant_agency Table
USDA_grant_agency_2 = cris_df[['Accession No.' , 'Sponsoring Institution'  , 'Funding Source' , 'Program Code' ,  'Animal Health Component' , 'Basic' , 'Applied' , 'Developmental']]
print USDA_grant_agency_2.tail()
print 'Length of unique Accession No. before dropping duplicates is:' ,str(len(USDA_grant_agency_2['Accession No.'].unique()))
#Check for duplicates
USDA_grant_agency_2['is_duplicate'] = USDA_grant_agency_2.duplicated('Accession No.')  #removing duplicates by accesion number 
USDA_grant_agency_2['is_duplicate'] = USDA_grant_agency_2['is_duplicate'].astype(int)  # converting true and false to int 
print 'Duplicate Table:' , str(USDA_grant_agency_2.groupby('is_duplicate').count())
#Removing duplicates 
USDA_grant_agency_2_nodup = USDA_grant_agency_2[USDA_grant_agency_2['is_duplicate'] == 0]  #Keeping unique observations only
print USDA_grant_agency_2_nodup.head()
print 'Length of unique Accession No. after dropping duplicates is:' ,str(len(USDA_grant_agency_2_nodup['Accession No.']))

USDA_grant_agency_2_nodup = USDA_grant_agency_2_nodup.drop('is_duplicate', 1)
USDA_grant_agency_2_nodup.rename(columns={'Accession No.': 'accession_number', 'Sponsoring Institution': 'sponsor_institution' ,  'Funding Source' : 'funding_source', 'Program Code' : 'program_code'  , \
									'Animal Health Component' : 'animal_health_component' , 'Basic' : 'basic' , "Applied": 'applied' , "Developmental": "developmental"} , inplace=True)

USDA_grant_agency_2_nodup['year'] = 1 
print USDA_grant_agency_2_nodup.head()
# USDA_grant_agency_2_nodup.to_csv(test+'/'+'USDA_grant_agency_2_nodup.csv')



#Old Data
path_2 = 'H:\CSSIP\Data\CRIS_Project\Old_Data'.replace('\\' , '/')
USDA_grant_agency_1 = pd.read_csv(path_2+ '/USDA_grant_agency.csv')
print USDA_grant_agency_1.head()
print 'Length of unique accession_number is:',str(len(USDA_grant_agency_1['accession_number']))

print "*******Appending Data**********"
USDA_grant_agency_combined = USDA_grant_agency_1.append(USDA_grant_agency_2_nodup , ignore_index=True)
USDA_grant_agency_combined['is_duplicate'] = USDA_grant_agency_combined.duplicated('accession_number')
USDA_grant_agency_combined['is_duplicate'] = USDA_grant_agency_combined['is_duplicate'].astype(int) 
print 'Duplicate Table for appended data:' , str(USDA_grant_agency_combined.groupby('is_duplicate').count())

# USDA_grant_agency_combined.to_csv(test+'/'+'USDA_grant_agency_combined.csv')
USDA_grant_agency_combined['animal_health_component'] = str(USDA_grant_agency_combined['animal_health_component'])


USDA_grant_agency_combined_2 = USDA_grant_agency_combined[(USDA_grant_agency_combined['animal_health_component'] != "(N/A)") & (USDA_grant_agency_combined['applied'] !="(N/A)" ) & (USDA_grant_agency_combined['basic'] != "(N/A)")  \
& (USDA_grant_agency_combined['developmental'] != "(N/A)")  & (USDA_grant_agency_combined['is_duplicate'] != 1 ) ]

# USDA_grant_agency_combined_2 = USDA_grant_agency_combined.loc[ (USDA_grant_agency_combined['animal_health_component'] != "(N/A)") & (USDA_grant_agency_combined['applied'] !="(N/A)" ) & (USDA_grant_agency_combined['basic'] != "(N/A)")  \
# & (USDA_grant_agency_combined['developmental'] != "(N/A)")    & (USDA_grant_agency_combined['is_duplicate'] != 1 )  , :   ]





USDA_grant_agency_combined_2.to_csv(test+'/'+'USDA_grant_agency_combined_cl3.csv')


"""
# print USDA_grant_agency_combined.head()
print USDA_grant_agency_combined.groupby('is_duplicate').count()
print "BBBBBBBBBBBBBBBB"
print 'length of accesion_number before dropping dups:' , str(len(USDA_grant_agency_combined['accession_number']))
print 'lenght of unique accession_number before dropping dups:' , str(len(USDA_grant_agency_combined['accession_number' ].unique()))
#Removing duplicates
unique_USDA_grant_agency_combined_df = USDA_grant_agency_combined[USDA_grant_agency_combined['is_duplicate'] != 1]
print "DDDDDDDDDD"
print unique_USDA_grant_agency_combined_df.tail()
print "*** Duplicates Dropped ***"
print 'Length of unique_USDA_grant_agency_combined_df is:' , str(len(unique_USDA_grant_agency_combined_df['accession_number']))
print 'Length of unique unique_USDA_grant_agency_combined_df is:' , str(len(unique_USDA_grant_agency_combined_df['accession_number'].unique()))
print "ZZZZZZZZZZZZZZZZZ"
print unique_USDA_grant_agency_combined_df.groupby('is_duplicate').count()
unique_USDA_grant_agency_combined_df = unique_USDA_grant_agency_combined_df.drop('is_duplicate', 1)
today = date.today()
print today 
unique_USDA_grant_agency_combined_df2 = unique_USDA_grant_agency_combined_df[['accession_number' , 'sponsor_institution' , 'funding_source' , 'program_code' , 'animal_health_component' , 'basic' , 'applied' , 'developmental']]
unique_USDA_grant_agency_combined_df2.to_csv(path+'/'+str(today) + '_USDA_grant_agency_appended.csv'  )

"""

# Table 2 , USDA_knowledge_area
# print 'New_data'
# USDA_knowledge_area_2 = cris_df[['Accession No.' , 'Knowledge Area' ]]
# print USDA_knowledge_area_2.head()

# #Separating knowledge area into different rows based on 
# USDA_knowledge_area_2 = pd.concat([pd.Series(row['Accession No.'], str(row['Knowledge Area']).split(';'))  
# 	for i, row in USDA_knowledge_area_2.iterrows()]).reset_index()

# USDA_knowledge_area_2 = USDA_knowledge_area_2[[ 0 , 'index']]
# USDA_knowledge_area_2.rename(columns={0: 'accession_number', 'index' :'knowledge_area' } , inplace=True)
# print USDA_knowledge_area_2.head()
# print 'Length of accession_number in new_data:' , str(len(USDA_knowledge_area_2['accession_number']))
# print 'Length of UNIQUE accession_number in new_data:' ,str(len(USDA_knowledge_area_2['accession_number'].unique()))

# USDA_knowledge_area_2['is_duplicate'] = USDA_knowledge_area_2.duplicated(['accession_number' ,  'knowledge_area'])  #duplicates by accesion number + knowledge_area
# USDA_knowledge_area_2['is_duplicate'] = USDA_knowledge_area_2['is_duplicate'].astype(int)  # 1 = True  and 0 = False
# print 'Duplicate Table:' , str(USDA_knowledge_area_2.groupby('is_duplicate').count())
# #Removing duplicates 
# USDA_knowledge_area_2_nodup = USDA_knowledge_area_2[USDA_knowledge_area_2['is_duplicate'] == 0]  #Keeping unique observations only
# print USDA_knowledge_area_2_nodup.head()
# print 'Length of accestion_num after removing duplicates:' , str(len(USDA_knowledge_area_2_nodup['accession_number']))
# print 'Length of UNIQUE accesion_number after removing duplicates:' ,str(len(USDA_knowledge_area_2_nodup['accession_number'].unique()))
# USDA_knowledge_area_2_nodup = USDA_knowledge_area_2_nodup.drop('is_duplicate', 1)
# print "***Duplicated droped"
# print USDA_knowledge_area_2_nodup.tail()


# print "Old_data"
# USDA_knowledge_area_1 = pd.read_csv(path_2+ '/USDA_knowledge_area.csv')
# USDA_knowledge_area_1 = USDA_knowledge_area_1[['accession_number' , 'knowledge_area']]
# print USDA_knowledge_area_1.head()
# print "********"
# print 'Length of USDA_knowledge_area_1 accession_number before appende is:' ,str(len(USDA_knowledge_area_1['accession_number']))
# print 'Length of USDA_knowledge_area_1 unique accession_number before appende is:' , str(len(USDA_knowledge_area_1['accession_number'].unique()))

# #Appending data
# USDA_knowledge_area_appended = USDA_knowledge_area_1.append(USDA_knowledge_area_2_nodup , ignore_index=True)
# print "***** Appended data ******"
# print USDA_knowledge_area_appended.head()

# #Creating a new column wiht the number of the knowledge area 
# USDA_knowledge_area_appended['knowledge_area_no'] = USDA_knowledge_area_appended['knowledge_area'].apply(lambda x: pd.Series(x.split('-')))[0]
# print USDA_knowledge_area_appended.head()

# # #Removing duplicate
# USDA_knowledge_area_appended['is_duplicate'] = USDA_knowledge_area_appended.duplicated(['accession_number' , 'knowledge_area_no']) #duplicates by accesion_num + knowledge_area
# USDA_knowledge_area_appended['is_duplicate'] = USDA_knowledge_area_appended['is_duplicate'].astype(int)  # 1 = True and 0 = False
# print 'Appended data duplicates table' , str(USDA_knowledge_area_appended.groupby('is_duplicate').count())
# USDA_knowledge_area_appended_ = USDA_knowledge_area_appended[USDA_knowledge_area_appended['is_duplicate'] == 0]  #Keeping unique values only 
# print 'Length appended_data accestion_num:' , str(len(USDA_knowledge_area_appended_['accession_number']))
# print 'Length of appended data UNIQUE accesion_number:' ,str(len(USDA_knowledge_area_appended_['accession_number'].unique()))
# USDA_knowledge_area_appended_ = USDA_knowledge_area_appended_.drop('is_duplicate', 1) #removing is_duplicate column 
# USDA_knowledge_area_appended_ = USDA_knowledge_area_appended_[['accession_number'  , 'knowledge_area']]
# USDA_knowledge_area_appended_ = USDA_knowledge_area_appended_[pd.notnull(USDA_knowledge_area_appended_['accession_number'])] #removing the row with missing values 
# #Outputing the appended data
# today = date.today()
# print today 
# USDA_knowledge_area_appended_.to_csv(path+'/'+str(today) + '_USDA_knowledge_area_appended.csv')


# Table 3
# USDA_investigation_subject_2 = cris_df[['Accession No.' , 'Subject Of Investigation' , ]]
# print USDA_investigation_subject_2.tail()

# #Separating subject of investigation into different rows 
# USDA_investigation_subject_2 = pd.concat([pd.Series(row['Accession No.'], str(row['Subject Of Investigation']).split(';'))  
# 	for i, row in USDA_investigation_subject_2.iterrows()]).reset_index()

# USDA_investigation_subject_2 = USDA_investigation_subject_2[[ 0 , 'index']]
# USDA_investigation_subject_2.rename(columns={0: 'accession_number', 'index' :'investigation_subject' } , inplace=True)
# print USDA_investigation_subject_2.tail()

# print 'Length of accession_number in new_data:' , str(len(USDA_investigation_subject_2['accession_number']))
# print 'Length of UNIQUE accession_number in new_data:' ,str(len(USDA_investigation_subject_2['accession_number'].unique()))

# USDA_investigation_subject_2['is_duplicate'] = USDA_investigation_subject_2.duplicated(['accession_number' ,  'investigation_subject'])  #duplicates by accesion number + investigation_subject
# USDA_investigation_subject_2['is_duplicate'] = USDA_investigation_subject_2['is_duplicate'].astype(int)  # 1 = True  and 0 = False
# print 'Duplicate Table:' , str(USDA_investigation_subject_2.groupby('is_duplicate').count())
# #Removing duplicates 
# USDA_investigation_subject_2_nodup = USDA_investigation_subject_2[USDA_investigation_subject_2['is_duplicate'] == 0]  #Keeping unique observations only
# print USDA_investigation_subject_2_nodup.head()
# print 'Length of accestion_num after removing duplicates:' , str(len(USDA_investigation_subject_2_nodup['accession_number']))
# print 'Length of UNIQUE accesion_number after removing duplicates:' ,str(len(USDA_investigation_subject_2_nodup['accession_number'].unique()))
# USDA_investigation_subject_2_nodup = USDA_investigation_subject_2_nodup.drop('is_duplicate', 1)
# print "***Duplicated droped"
# print USDA_investigation_subject_2_nodup.tail()

# print "Old_data"
# USDA_investigation_subject_1 = pd.read_csv(path_2+ '/USDA_investigation_subject.csv')
# USDA_investigation_subject_1 = USDA_investigation_subject_1[['accession_number' , 'investigation_subject']]
# print USDA_investigation_subject_1.head()
# print "********"
# print 'Length of old_data accession_number before appende is:' ,str(len(USDA_investigation_subject_1['accession_number']))
# print 'Length of  old_data unique accession_number before appende is:' , str(len(USDA_investigation_subject_1['accession_number'].unique()))


# #Appending data
# USDA_investigation_subject_appended = USDA_investigation_subject_1.append(USDA_investigation_subject_2_nodup , ignore_index=True)
# print "***** Appended data ******"
# print USDA_investigation_subject_appended.tail()

# #Creating a new column wiht the number of the knowledge area 
# USDA_investigation_subject_appended['investigation_subject_no'] = USDA_investigation_subject_appended['investigation_subject'].apply(lambda x: pd.Series(x.split('-')))[0]
# print USDA_investigation_subject_appended.tail()
# # #Removing duplicate
# USDA_investigation_subject_appended['is_duplicate'] = USDA_investigation_subject_appended.duplicated(['accession_number' , 'investigation_subject_no']) #duplicates by accesion_num + knowledge_area
# USDA_investigation_subject_appended['is_duplicate'] = USDA_investigation_subject_appended['is_duplicate'].astype(int)  # 1 = True and 0 = False
# print 'Appended data duplicates table' , str(USDA_investigation_subject_appended.groupby('is_duplicate').count())
# USDA_investigation_subject_appended_ = USDA_investigation_subject_appended[USDA_investigation_subject_appended['is_duplicate'] == 0]  #Keeping unique values only 
# print 'Length appended_data accestion_num:' , str(len(USDA_investigation_subject_appended_['accession_number']))
# print 'Length of appended data UNIQUE accesion_number:' ,str(len(USDA_investigation_subject_appended_['accession_number'].unique()))
# USDA_investigation_subject_appended_ = USDA_investigation_subject_appended_.drop('is_duplicate', 1) #removing is_duplicate column 
# USDA_investigation_subject_appended_ = USDA_investigation_subject_appended_[['accession_number'  , 'investigation_subject']]
# USDA_investigation_subject_appended_ = USDA_investigation_subject_appended_[pd.notnull(USDA_investigation_subject_appended_['accession_number'])] #removing the row with missing values 
# #Outputing the appended data
# today = date.today()
# print today 
# USDA_investigation_subject_appended_.to_csv(path+'/'+str(today) + '_USDA_investigation_subject_appended.csv')


# #Table 4: USDA_PI_Org

# USDA_PI_Org_2 = cris_df[['Accession No.' , 'Project Director' , 'Recepient Organization' , 'Organization Street' , 'Organization Address' , 'Performing Department']]
# print USDA_PI_Org_2.head()

# USDA_PI_Org_2.rename(columns={'Accession No.': 'accession_number', 'Project Director' :'project_director'  , 'Recepient Organization' : 'organization' , 'Organization Street' : 'organization_street' , \
# 	'Organization Address' :'organization_address'  , 'Performing Department' : 'performing_dept' } , inplace=True)
# print USDA_PI_Org_2.tail()

# print 'Length of accession_number in new_data:' , str(len(USDA_PI_Org_2['accession_number']))
# print 'Length of UNIQUE accession_number in new_data:' ,str(len(USDA_PI_Org_2['accession_number'].unique()))

# USDA_PI_Org_2['is_duplicate'] = USDA_PI_Org_2.duplicated('accession_number')  #duplicates by accesion number + investigation_subject
# USDA_PI_Org_2['is_duplicate'] = USDA_PI_Org_2['is_duplicate'].astype(int)  # 1 = True  and 0 = False


# print 'Duplicate Table:' , str(USDA_PI_Org_2.groupby('is_duplicate').count())
# #Removing duplicates 
# USDA_PI_Org_2_nodup = USDA_PI_Org_2[USDA_PI_Org_2['is_duplicate'] == 0]  #Keeping unique observations only
# print USDA_PI_Org_2_nodup.tail()
# print 'Length of accestion_num after removing duplicates:' , str(len(USDA_PI_Org_2_nodup['accession_number']))
# print 'Length of UNIQUE accesion_number after removing duplicates:' ,str(len(USDA_PI_Org_2_nodup['accession_number'].unique()))
# # USDA_PI_Org_2_nodup = USDA_PI_Org_2_nodup.drop('is_duplicate', 1)
# print "***Duplicated droped"
# print USDA_PI_Org_2_nodup.tail()
# # USDA_PI_Org_2_nodup.to_csv(path+'/testing/'+'USDA_PI_Org_2_testing.csv')

# print "Old_data"
# USDA_PI_Org_1 = pd.read_csv(path_2+ '/USDA_PI_Org.csv')
# print USDA_PI_Org_1.head()
# print "********"
# print 'Length of old_data accession_number before appende is:' ,str(len(USDA_PI_Org_1['accession_number']))
# print 'Length of  old_data unique accession_number before appende is:' , str(len(USDA_PI_Org_1['accession_number'].unique()))

# #Appending data
# USDA_PI_Org_appended = USDA_PI_Org_1.append(USDA_PI_Org_2_nodup , ignore_index=True)
# print "***** Appended data ******"
# print USDA_PI_Org_appended.tail()

# #Removing duplicate
# USDA_PI_Org_appended['is_duplicate'] = USDA_PI_Org_appended.duplicated('accession_number') #duplicates by accesion_num + knowledge_area
# USDA_PI_Org_appended['is_duplicate'] = USDA_PI_Org_appended['is_duplicate'].astype(int)  # 1 = True and 0 = False
# print 'Appended data duplicates table' , str(USDA_PI_Org_appended.groupby('is_duplicate').count())
# USDA_PI_Org_appended_ = USDA_PI_Org_appended[USDA_PI_Org_appended['is_duplicate'] == 0]  #Keeping unique values only 
# print 'Length appended_data accestion_num:' , str(len(USDA_PI_Org_appended_['accession_number']))
# print 'Length of appended data UNIQUE accesion_number:' ,str(len(USDA_PI_Org_appended_['accession_number'].unique()))
# USDA_PI_Org_appended_ = USDA_PI_Org_appended_.drop('is_duplicate', 1) #removing is_duplicate column 
# USDA_PI_Org_appended_ = USDA_PI_Org_appended_[pd.notnull(USDA_PI_Org_appended_['accession_number'])] #removing the row with missing values 
# #Outputing the appended data
# today = date.today()
# print today 
# USDA_PI_Org_appended_.to_csv(path+'/'+str(today) + '_USDA_PI_Org_appended.csv')

# #Table 5: USDA_science_field
# USDA_science_field_2 = cris_df[['Accession No.' , 'Field Of Science']]
# print USDA_science_field_2.tail()

# #Separating science field colum into different rows 
# USDA_science_field_2 = pd.concat([pd.Series(row['Accession No.'], str(row['Field Of Science']).split(';'))  
# 	for i, row in USDA_science_field_2.iterrows()]).reset_index()

# USDA_science_field_2 = USDA_science_field_2[[ 0 , 'index']]
# USDA_science_field_2.rename(columns={0: 'accession_number', 'index' :'science_field' } , inplace=True)
# print USDA_science_field_2.head()
# print 'Length of accession_number in new_data:' , str(len(USDA_science_field_2['accession_number']))
# print 'Length of UNIQUE accession_number in new_data:' ,str(len(USDA_science_field_2['accession_number'].unique()))


# USDA_science_field_2['is_duplicate'] = USDA_science_field_2.duplicated(['accession_number' ,  'science_field'])  #duplicates by accesion number + knowledge_area
# USDA_science_field_2['is_duplicate'] = USDA_science_field_2['is_duplicate'].astype(int)  # 1 = True  and 0 = False
# print 'Duplicate Table:' , str(USDA_science_field_2.groupby('is_duplicate').count())
# #Removing duplicates 
# USDA_science_field_2_nodup = USDA_science_field_2[USDA_science_field_2['is_duplicate'] == 0]  #Keeping unique observations only
# print USDA_science_field_2_nodup.head()
# print 'Length of accestion_num after removing duplicates:' , str(len(USDA_science_field_2_nodup['accession_number']))
# print 'Length of UNIQUE accesion_number after removing duplicates:' ,str(len(USDA_science_field_2_nodup['accession_number'].unique()))
# USDA_science_field_2_nodup = USDA_science_field_2_nodup.drop('is_duplicate', 1)
# print "***Duplicated droped"
# print USDA_science_field_2_nodup.tail()

# print "Old_data"
# USDA_science_field_1 = pd.read_csv(path_2+ '/USDA_science_field.csv')
# USDA_science_field_1 = USDA_science_field_1[['accession_number' , 'science_field']]
# print USDA_science_field_1.head()
# print "********"
# print 'Length of old_data accession_number before appende is:' ,str(len(USDA_science_field_1['accession_number']))
# print 'Length of old_data unique accession_number before appende is:' , str(len(USDA_science_field_1['accession_number'].unique()))

# #Appending data
# USDA_science_field_appended = USDA_science_field_1.append(USDA_science_field_2_nodup , ignore_index=True)
# print "***** Appended data ******"
# print USDA_science_field_appended.head()

# #Creating a new column with the number of the knowledge area 
# USDA_science_field_appended['science_field_no'] = USDA_science_field_appended['science_field'].apply(lambda x: pd.Series(x.split('-')))[0]
# print USDA_science_field_appended.head()


# # #Removing duplicate
# USDA_science_field_appended['is_duplicate'] = USDA_science_field_appended.duplicated(['accession_number' , 'science_field_no']) #duplicates by accesion_num + knowledge_area
# USDA_science_field_appended['is_duplicate'] = USDA_science_field_appended['is_duplicate'].astype(int)  # 1 = True and 0 = False
# print 'Appended data duplicates table' , str(USDA_science_field_appended.groupby('is_duplicate').count())
# USDA_science_field_appended_ = USDA_science_field_appended[USDA_science_field_appended['is_duplicate'] == 0]  #Keeping unique values only 
# print 'Length appended_data accestion_num:' , str(len(USDA_science_field_appended_['accession_number']))
# print 'Length of appended data UNIQUE accesion_number:' ,str(len(USDA_science_field_appended_['accession_number'].unique()))
# USDA_science_field_appended_ = USDA_science_field_appended_.drop('is_duplicate', 1) #removing is_duplicate column 
# USDA_science_field_appended_ = USDA_science_field_appended_[['accession_number'  , 'science_field']]
# USDA_science_field_appended_ = USDA_science_field_appended_[pd.notnull(USDA_science_field_appended_['accession_number'])] #removing the row with missing values 
# #Outputing the appended data
# today = date.today()
# print today 
# USDA_science_field_appended_.to_csv(path+'/'+str(today) + '_USDA_science_field_appended.csv')



# Table 6: Progress Report
# USDA_progress_reports_1= pd.read_csv(path_2+ '/USDA_progress_reports.csv')
# del USDA_progress_reports_1['sequence']
# USDA_progress_reports_1 = USDA_progress_reports_1.rename(columns = {
#     'accession_number' : 0,
#     'periodstartdate' : 1 ,
#     'periodenddate' : 2,
#     'outputs'  : 3,
#     'impacts' : 4 ,
#     'publications' : 5
# })
# print USDA_progress_reports_1.head()



# # USDA_progress_reports_2= pd.read_csv(path+ '/progress_reports_USDA_awards_ps.csv' , header=None , nrows = 300000) 
# USDA_progress_reports_2= pd.read_csv(path+ '/progress_reports_USDA_awards_ps.csv' , header=None , chunksize= 300000 ,error_bad_lines=False , warn_bad_lines = True) 
# USDA_progress_reports_2 = USDA_progress_reports_2.rename(columns = {
#     0 : 'accession_number' ,
#     1 :'periodstartdate',
#     2: 'periodenddate',
#     3: 'outputs' ,
#     4 : 'impacts' ,
#     5 : 'publications'
# })
# for chunk in USDA_progress_reports_2 :
# 	print "************"
# 	# print (chunk)
# 	USDA_progress_reports_appended = USDA_progress_reports_1.append(chunk , ignore_index=True)
# 	print USDA_progress_reports_appended.head()


# # #Removing duplicate
# USDA_progress_reports_appended['is_duplicate'] = USDA_progress_reports_appended.duplicated([0 , 1 , 2]) #duplicates by accesion_num + knowledge_area
# USDA_progress_reports_appended['is_duplicate'] = USDA_progress_reports_appended['is_duplicate'].astype(int)  # 1 = True and 0 = False
# print 'Appended data duplicates table' , str(USDA_progress_reports_appended.groupby('is_duplicate').count())

# USDA_progress_reports_appended_ = USDA_progress_reports_appended[USDA_progress_reports_appended['is_duplicate'] == 0]  #Keeping unique values only 
# print 'Length appended_data accestion_num:' , str(len(USDA_progress_reports_appended_[0]))
# print 'Length of appended data UNIQUE accesion_number:' ,str(len(USDA_progress_reports_appended_[0].unique()))
# USDA_progress_reports_appended_ = USDA_progress_reports_appended_.drop('is_duplicate', 1) #removing is_duplicate column 
# USDA_progress_reports_appended_ = USDA_progress_reports_appended_[pd.notnull(USDA_progress_reports_appended_[0])] #removing the row with missing values 

# USDA_progress_reports_appended_ = USDA_progress_reports_appended_.rename(columns = {
#     0 : 'accession_number' ,
#     1 :'periodstartdate',
#     2: 'periodenddate',
#     3: 'outputs' ,
#     4 : 'impacts' ,
#     5 : 'publications'
# })

# #Outputing the appended data
# today = date.today()
# print today 
# USDA_progress_reports_appended_.to_csv(path+'/'+str(today) + '_USDA_progress_reports_appended.csv')
# print "Finished"


# USDA_progress_reports= pd.read_csv(path+ '/2016-04-29_USDA_progress_reports_appended.csv')
# del USDA_progress_reports['Unnamed: 0']
# print USDA_progress_reports.head(1000)
# USDA_progress_reports['seq_num'] = USDA_progress_reports.groupby('accession_number').cumcount()
# #Outputing the appended data
# today = date.today()
# print today 
# USDA_progress_reports.to_csv(path+'/'+str(today) + '_USDA_progress_reports_appended_v2.csv')
# # print "Finished"

# USDA_progress_reports2= pd.read_csv(path+ '/2016-04-29_USDA_progress_reports_appended_v2.csv')
# print USDA_progress_reports2[['accession_number' ,'seq_num']].head(10000)
