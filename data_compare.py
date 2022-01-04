import re
import sys
import os
import string
import traceback
from shutil import copyfile
import io
import tarfile
import datacompy, pandas as pd
from settings import Settings
from io import StringIO
#import snowflake.connector


def main():
     print("Environment Path : ", sys.executable)
     ctx = settings.get_snowflake_connection()
     cs = ctx.cursor()
     try:
         sql = 'select * from "DIM_EMPLOYEE" order by employee_key  limit 10'
         cs.execute(sql)
         df1 = cs.fetch_pandas_all()
         #print(df1.head(10))
         
         sql = 'select * from "DIM_EMPLOYEE" order by employee_key limit 10'
         cs.execute(sql)
         df2 = cs.fetch_pandas_all()
         
         compare = datacompy.Compare(
                   df1,
                   df2,
                   join_columns='employee_key',  #You can also specify a list of columns eg ['employee_name','startdate']
                   abs_tol=0, #Optional, defaults to 0
                   rel_tol=0, #Optional, defaults to 0
                   df1_name='Postgres', #Optional, defaults to 'df1'
                   df2_name='SnowFlake' #Optional, defaults to 'df2'
                 )
         print(compare.report())

         #print(df_2.head(10))
         
     finally:
         cs.close()
     ctx.close()

if __name__ == "__main__":
    settings = Settings()
    main()
    
