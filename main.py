# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
import pandas as pd

def main(session: snowpark.Session): 
    schema = "HAKKODA.TECHNICAL_ASSESSMENT"
    session.use_schema(schema)
    #fetching the data
    trans_tb_Name = 'TRANSACTIONS'
    dataframe = session.table(trans_tb_Name)
    #converting into pandas data frame
    df = dataframe.toPandas()

    #--------------------------------------------------
    #deleting duplicates
    df.drop_duplicates(inplace=True)

    #--------------------------------------------------
    #removing invalid characters
    #function to remove invalid characters
    def clean_column(column):
        if column.name == 'EMAIL':
            return column.str.replace('[^\w@]', '')
        elif pd.api.types.is_numeric_dtype(column.dtype):
            return column
        else:
            return column.str.replace('[^\w]', '')
  
    #apply the function to each column
    df = df.apply(clean_column)
    
    #--------------------------------------------------
    #Function to fill the nulls
    def fill_fun(row, dic, idColName):
      id_value = row[idColName]
      for col_name in row.index:
          if pd.isnull(row[col_name]):
              row[col_name] = dic[col_name].replace("X", str(id_value))
      return row

    #Clients
    clientCol = ["CLIENT_ID", "CLIENT_NAME","CLIENT_LASTNAME","EMAIL"]
    clientFormat = {
        "CLIENT_NAME": "Client_X",
        "CLIENT_LASTNAME": "Lastname_X",
        "EMAIL": "client_X.lastname_X@example.com"
    }
    
    clientdf = df[clientCol]
    df[clientCol] = clientdf.apply(fill_fun, axis=1, args=(clientFormat, "CLIENT_ID"))

    #Store
    storeCol = ["STORE_ID","STORE_NAME","LOCATION"]
    storeFormat = {
        "STORE_NAME": "Store_X",
        "LOCATION": "Location_X"
    }
    
    storedf = df[storeCol]
    df[storeCol] = storedf.apply(fill_fun, axis=1, args=(storeFormat, "STORE_ID"))

    #Product
    productCol = ["PRODUCT_ID","PRODUCT_NAME","BRAND"]
    productFormat = {
        "PRODUCT_NAME": "Product_X",
        "BRAND": "Brand_X"
    }
    
    productdf = df[productCol]
    df[productCol] = productdf.apply(fill_fun, axis=1, args=(productFormat, "PRODUCT_ID"))

    #Address
    addressCol = ["ADDRESS_ID","STREET","CITY","STATE"]
    addressFormat = {
        "STREET": "Street_X",
        "CITY": "City_X",
        "STATE": "State_X"
    }
    
    addressdf = df[addressCol]
    df[addressCol] = addressdf.apply(fill_fun, axis=1, args=(addressFormat, "ADDRESS_ID"))
    
    #--------------------------------------------------
    #filling the tables
    session.use_schema("HAKKODA.TECHNICAL_ASSESSMENT")
    
    #Client
    session.table("CLIENT").delete()
    client_df = df[clientCol].drop_duplicates().sort_values(by='CLIENT_ID').rename(columns={'CLIENT_LASTNAME': 'CLIENT_LAST_NAME'})
    session.write_pandas(client_df,"CLIENT")

    #Store
    session.table("STORE").delete()
    store_df = df[storeCol].drop_duplicates().sort_values(by='STORE_ID')
    session.write_pandas(store_df,"STORE")

    #Product
    session.table("PRODUCT").delete()
    product_df = df[productCol+["CATEGORY"]].drop_duplicates().sort_values(by='PRODUCT_ID')
    session.write_pandas(product_df,"PRODUCT")

    #Address
    session.table("ADDRESS").delete()
    address_df = df[addressCol+["ZIP_CODE"]].drop_duplicates().sort_values(by='ADDRESS_ID')
    session.write_pandas(address_df,"ADDRESS")

    #Fact
    session.table("FACT").delete()
    factCol = ["TRANSACTION_ID","CLIENT_ID","STORE_ID","PRODUCT_ID","ADDRESS_ID","DISCOUNT","UNIT_PRICE","QUANTITY_OF_ITEMS_SOLD"]
    fact_df = df[factCol].drop_duplicates().sort_values(by="TRANSACTION_ID")
    session.write_pandas(fact_df,"FACT")
    
    #--------------------------------------------------
    # Return value will appear in the Results tab.
    return session.create_dataframe(df)