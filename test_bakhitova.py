import pandas as pd
import requests
from sqlalchemy import create_engine
import re

# getting data from url
url = 'http://84.201.129.203:4545/get_structure_course'
response = requests.post(url)

# transforming json to (almost) clean pandas DF
data = pd.read_json(response.text)
data = pd.DataFrame(list(data['blocks'].values))

# adding current date and time
data['updated'] = pd.Timestamp.now().strftime("%d/%m/%Y %I:%M:%S")

data2 = data.copy()
data.children = data.children.astype('str')

# establish connection to the DB
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}:{port}/{db}"
                       .format(user="user2",
                               pw="qtybcgt++H6",
                               host="84.201.129.203",
                               port="32769",
                               db="test"))

# replace table if it already exists
data.to_sql(con=engine, name='test_bakhitova', if_exists='replace', index=False)

# put module name and ID together for more convenient printing
data2['display_name'] = data2['display_name']+' - ' + data2['block_id']
data2 = data2[['display_name', 'id', 'children']]

# every children ID in a new row (requires latest pandas version). we'll need it for joins
data2 = data2.explode('children')

# for each child ID in the column we will recursively join his name and his children
def create_table(df, level, counter=1):
        # check if we still have available children ID to join
        if sum(pd.isna(df.children)) == df.shape[0]:
            # if yes then we'll remove the last column with display_name
            df = df.iloc[:,:-1]
        else:
            # join with original data to get more info
            df = pd.merge(df, data2, how='left',
                          left_on='children', right_on='id',
                          suffixes=('_' + str(counter), ''))
            counter+=1 # counter here is needed only for column renaming
            level-=1 # shows how many iterations we already did

        if level != 0 and sum(pd.isna(df.children)) < df.shape[0]:
            # recursively join data until it is not possible
            df = create_table(df, counter=counter, level=level)

        # return table only with names and ID
        df = df.loc[:, df.columns.str.startswith('display')]
        return df.drop_duplicates()

# start joining from the modules
course_struct = create_table(data2[data2.id=='block-v1:Skillfactory+PYRT-2+10DEC2019+type@course+block@course'],
                 level = 2) # show two additional levels in data

# add tabs and new lines for nice printing
for i in range(1, course_struct.shape[1]):
    course_struct.iloc[:,i] = '\n' + '\t'*i + course_struct.iloc[:,i]

# clean duplicated cells and transform DF to string
course_struct = course_struct.where(~course_struct.apply(pd.Series.duplicated, axis=0), '')\
    .fillna('')\
    .to_string(header = False, index=False)

# remove extra characters and print
course_struct = re.sub("\s?\n\s?|\s+", " ", course_struct).replace('\\n', '\n').replace("\\t", "\t")
print(course_struct)