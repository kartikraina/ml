
# coding: utf-8

# In[16]:


import sqlite3

conn = sqlite3.connect("downloads/flights.db")


# In[17]:


# create a Cursor object
cur = conn.cursor()
# execute a query
cur.execute("select * from airlines limit 5;")
# fetching the records
results = cur.fetchall()
print(results)


# In[18]:


# Mapping airports
coords = cur.execute("""
  select cast(longitude as float), 
  cast(latitude as float) 
  from airports;"""
).fetchall()


# In[20]:


from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')

m = Basemap(projection='merc',llcrnrlat=-80,urcrnrlat=80,llcrnrlon=-180,urcrnrlon=180,lat_ts=20,resolution='c')

m.drawcoastlines()
m.drawmapboundary()

x, y = m([l[0] for l in coords], [l[1] for l in coords])

m.scatter(x,y,1,marker='o',color='red')


# In[21]:


import pandas as pd
df = pd.read_sql_query("select * from airlines limit 5;", conn)
df


# In[22]:


df["country"]


# In[23]:


df.columns


# In[24]:


df.head(3)


# In[25]:


df.tail(4)


# In[26]:


routes = pd.read_sql_query("""
                           select cast(sa.longitude as float) as source_lon, 
                           cast(sa.latitude as float) as source_lat,
                           cast(da.longitude as float) as dest_lon,
                           cast(da.latitude as float) as dest_lat
                           from routes 
                           inner join airports sa on
                           sa.id = routes.source_id
                           inner join airports da on
                           da.id = routes.dest_id;
                           """, 
                           conn)
m = Basemap(projection='merc',llcrnrlat=-80,urcrnrlat=80,llcrnrlon=-180,urcrnrlon=180,lat_ts=20,resolution='c')
m.drawcoastlines()
for name, row in routes[:3000].iterrows():
    if abs(row["source_lon"] - row["dest_lon"]) < 90:
        # Draw a great circle between source and dest airports.
        m.drawgreatcircle(
            row["source_lon"], 
            row["source_lat"], 
            row["dest_lon"],
            row["dest_lat"],
            linewidth=1,
            color='b'
        )


# In[27]:


# Inserting rows with Python
cur = conn.cursor()
cur.execute("insert into airlines values (6048, 19846, 'Test flight', '', '', null, null, null, 'Y')")


# In[28]:


conn.commit()
pd.read_sql_query("select * from airlines where id=19846;", conn)


# In[30]:


cur = conn.cursor()
values = ('Test Flight', 'Y')
cur.execute("insert into airlines values (6049, 19847, ?, '', '', null, null, null, ?)", values)
conn.commit()


# In[31]:


# Updating
cur = conn.cursor()
values = ('USA', 19847)
cur.execute("update airlines set country=? where id=?", values)
conn.commit()
pd.read_sql_query("select * from airlines where id=19847;", conn)


# In[32]:


# Deleting rows
cur = conn.cursor()
values = (19847, )
cur.execute("delete from airlines where id=?", values)
conn.commit()
pd.read_sql_query("select * from airlines where id=19847;", conn)


# In[34]:


# Creating Tables
cur = conn.cursor()
cur.execute("create table daily_flights (id integer, departure date, arrival date, number text, route_id integer)")
conn.commit()

cur.execute("insert into daily_flights values (1, '2016-09-28 0:00', '2016-09-28 12:00', 'T1', 1)")
conn.commit()

pd.read_sql_query("select * from daily_flights;", conn)


# In[35]:


# Creating tables with pandas
from datetime import datetime
df = pd.DataFrame(
    [[1, datetime(2016, 9, 29, 0, 0) , datetime(2016, 9, 29, 12, 0), 'T1', 1]], 
    columns=["id", "departure", "arrival", "number", "route_id"]
)
df.to_sql("daily_flights", conn, if_exists="replace")


# In[36]:


pd.read_sql_query("select * from daily_flights;", conn)

