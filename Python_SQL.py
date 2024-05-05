import sqlite3 as db
import pandas as pd
nyc_data=db.connect(r'M:\OMSA\CSE6040\NYC-311-2M.db')

#df=pd.read_sql_query('SELECT * FROM data LIMIT 10',nyc_data)
#print(df.head())

#query='''SELECT LOWER(ComplaintType), LOWER(Descriptor), LOWER(Agency)
 #        FROM data
  #       GROUP BY LOWER(ComplaintType)'''
#df=pd.read_sql_query(query,nyc_data)
#print(df)

query = '''
       SELECT UPPER(City) AS name,COUNT(*) AS freq
       FROM data
       WHERE name <> 'None'
       GROUP BY name COLLATE NOCASE
       ORDER BY freq DESC
       LIMIT 10
'''
df2_whiny = pd.read_sql_query(query, nyc_data)

top_cities=list(df2_whiny.head(7)['name'])

query2='''
        SELECT LOWER(ComplaintType) AS complaint_type, UPPER(City) AS city_name,COUNT(*) AS complaint_count
        FROM data
        WHERE city_name IN {}
        GROUP BY city_name,complaint_type
        ORDER BY complaint_count DESC
        '''.format(tuple(top_cities))
output=pd.read_sql_query(query2,nyc_data)
df_complaints_by_city=pd.DataFrame(output)


del query # clears any existing `query` variable; you should define it, below!

# Define a variable named `query` containing your solution
query3= '''
       SELECT DISTINCT(LOWER(ComplaintType)) AS type,COUNT(*) AS freq
       FROM data
       GROUP BY type
       ORDER BY freq DESC
'''
df_complaint_freq = pd.read_sql_query(query3,nyc_data)


top_complaints = df_complaint_freq[:25]


# Plot subset of data corresponding to the top complaints
df_plot = top_complaints.merge(df_complaints_by_city,
                               left_on=['type'],
                               right_on=['complaint_type'],
                               how='left')
df_plot.dropna(inplace=True)

#copy for fractional db creations
df_plot_1=df_plot.copy()

#find fractional value of a column
df_plot_1['complaint_frac']=df_plot_1.complaint_count/df_plot_1.freq
del df_plot_1['complaint_count']
print(df_plot_1)

#Number of complaints by hour
query4='''
        SELECT  STRFTIME('%H',CreatedDate) AS hour, COUNT(*) AS count
        FROM data
        GROUP BY hour
        '''
df3=pd.read_sql_query(query4,nyc_data)
print(df3)


#Number of "nose" complaints by hour after filtering data
query5='''
        SELECT STRFTIME('%H',CreatedDate) AS hour,COUNT(*) as count
        FROM data
        WHERE ComplaintType LIKE '%noise%'
        AND STRFTIME('%H:%M:%f', CreatedDate) <> "00:00:00.000"
        GROUP BY hour
        '''

df4=pd.read_sql_query(query5, nyc_data)
print(df4)
    
    