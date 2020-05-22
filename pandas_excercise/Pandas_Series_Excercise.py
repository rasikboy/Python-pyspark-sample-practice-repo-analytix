#!/usr/bin/env python
# coding: utf-8

# In[39]:


about_me = ["handsome", "Charming", "Intelligent", "Brilliant", "Humble"]


# In[40]:


import pandas as pd


# In[41]:


series = pd.Series(about_me)


# In[9]:


series


# In[42]:


series.values # returns an Array of the object


# In[43]:


series.index #indexes how they starts in series 


# In[44]:


series.dtype  # the 'O' noted for stored values data type is object


# #Methods of Panadas 

# In[45]:


prices = [2.99, 4.45, 1.36]


# In[46]:


s=pd.Series(prices)


# In[47]:


s.sum() #Adds all the values in the series. 


# In[48]:


s.product()   #Multiply/Product of all the values in the series. 


# In[49]:


s.mean()    #Mean/Average of all the values in the series. 


# # Parameters and The Arguments
# # Difficulty - Easy Medium Hard
# # Subtitles - True / False

# In[50]:


fruits = ["Apple", "Orange", "plum", "Grape","BlueBerry"]
weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]


# In[51]:


pd.Series(data= fruits, index= weekdays)


# # Import Series using read_csv method. 

# In[109]:


aqd=pd.read_csv("D:\\data\\air-quality-data-continuous.csv", sep=";", header="infer")


# In[53]:


aqd.tail()


# In[55]:


list(aqd)


# In[ ]:


pd.Series()


# In[58]:


sorted(aqd) # the data has string values, else it could sort numeric values in ascending order, same it does for strings, data has been sorted based in alphabets  


# In[57]:


min(aqd) 


# In[56]:


max(aqd)


# In[110]:


pokemon = pd.read_csv("D:\\Big_Data\\Pandas_Data\\Pandas\\pokemon.csv", usecols=["Name"], squeeze=True) # Squeeze will help to convert them into the Series object later 


# In[111]:


pokemon.dtypes


# In[86]:


pokemon.ndim #the number of dimension in the dataframe


# In[87]:


pokemon.shape # How many rows and columns in the pandas dataframe


# In[88]:


pokemon.head() #Shows 5 head/top values 


# In[73]:


pokemon.name="Pokemon New Name" #to assign the new name for you dataframe if you want to


# In[75]:


pokemon.name # verify the name has been set or not


# In[92]:


pokemon.sort_values()  # in series we do not have to give any value in this method.
#this method takes parameter if you are applying over the dataframe as : first parameter for sort the data based on which column we want to do that, axis has facility to change the axis which axis we wanted to achieve after sort the dataframe.


# In[93]:


pokemon.sort_values().head()


# In[112]:


pokemon.sort_values(ascending=False, inplace=True) # Inplace method makes the changes permanent in the dataframe or a Series. 


# In[113]:


pokemon


# In[115]:


pokemon.sort_index() # this method is basically to sort data by index but we can get back the previous state of the any dataframe of Series which had been called inplace as True earlier.


# In[116]:


pokemon.sort_index(inplace=True) # we can call inplace again on the Sorted dataframe/Series as well to sort and persist that stage


# # Index based Data Extraction in Pandas from the dataframe 

# In[2]:


import pandas as pd
pokemon = pd.read_csv("D:\\Big_Data\\Pandas_Data\\Pandas\\pokemon.csv", usecols=["Name"], squeeze=True)


# In[3]:


pokemon[50:100] # fetching values from 50 to 99. the last upper value will be exclusded from the result always. 


# In[6]:


pokemon[:50] # can pass only one maximum index value to get 


# In[8]:


pokemon[60:] #can pass minimum index from where to start and upto last value.


# In[9]:


pokemon[-70:] # this will fetch last 30 values from the dataframe


# In[11]:


pokemon[-70:-34]  # this will fecth value from last 70 to 34 next values


# # Extract Series Values from Index Position of Index Label

# In[17]:


pokemon = pd.read_csv("D:\\Big_Data\\Pandas_Data\\Pandas\\pokemon.csv", index_col="Name", usecols=["Name","Type 1"], squeeze=True)


# In[20]:


pokemon.head(3)


# In[22]:


# fetching from the index
pokemon[500]


# In[24]:


pokemon[[122,333,444,555,666,100]] # Can pass multiple indexes as well to fetch the  values


# In[4]:


import pandas as pd 
pokemon = pd.read_csv("D:\\Big_Data\\Pandas_Data\\Pandas\\pokemon.csv", index_col="Name", usecols=["Name","Type 1"], squeeze=True)


# In[31]:


pokemon


# In[32]:


pokemon[[122,333,444,555,666,100]] # Can pass multiple indexes as well to fetch the  values


# In[5]:


pokemon[["Tangela","Servine","Diancie"]] # can Pass label values as well into it.


# In[6]:


pokemon["Bulbasaur":"HoopaHoopa Unbound"] # can pass String as Index too if we have created them as index


# In[8]:


pokemon["Servine":]# can pass String(bottom, from where to start) as Index too if we have created them as index


# In[10]:


pokemon.reindex(index=["Pickachu", "Digimon","Tepig"])


# In[18]:


pokemon.idxmax #return the max value of index from the Series


# In[20]:


pokemon.idxmin #return the min value of index from the Series


# In[22]:


pokemon.value_counts() #Count the values how many time theese all occered


# In[23]:


pokemon.value_counts().sum() # Sum all the values how many time theese all occered


# In[28]:


pokemon = pd.read_csv("D:\\Big_Data\\Pandas_Data\\Pandas\\pokemon.csv", index_col="Name", usecols=["Name","Total"], squeeze=True)


# In[29]:


pokemon


# In[36]:


def custom_function_to_replace_value_with_desired_string(num):
    if num<350:
        return "OK"
    elif num>=350:
        return "SatifFactory"
    else:
        return "Incredible"


# # use of apply method

# In[40]:


pokemon.apply(custom_function_to_replace_value_with_desired_string).tail(50)  
#we have created 1 method above to acheive functionality of inbuilt apply method  


# In[41]:


pokemon.head()


# In[44]:


pokemon.apply( lambda i:i*2) # we can pass lambda as well into it.


# In[75]:


pokemon_name = pd.read_csv("D:\\Big_Data\\Pandas_Data\\Pandas\\pokemon.csv", usecols=["Name"], squeeze=True) 


# In[76]:


pokemon_name


# In[77]:


pokemon_type = pd.read_csv("D:\\Big_Data\\Pandas_Data\\Pandas\\pokemon.csv", index_col="Name", usecols=["Name","Type 1"], squeeze=True)


# In[78]:


pokemon_name.map(pokemon_type)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




