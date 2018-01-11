import numpy as np
import pandas as pd

import os

def wget_from_virgodb(username, password, sql_query, save_name='virgodb_result.csv', save_dir='./'):
  '''
  Small function to query the private MyMillennium virgo-db.
  http://virgodb.dur.ac.uk:8080/MyMillennium/Help
  '''
  if os.path.exists(save_dir+save_name):
    print('File {0} apparently already downloaded. Skipping.'.format(save_dir+save_name))
    return
    
  if not os.path.exists(save_dir+'cookie.txt'):
    os.system('touch {0}/cookie.txt'.format(save_dir))
  
  cmd = 'wget --http-user={0} --http-passwd={1} \
         --cookies=on --keep-session-cookies --save-cookies=cookie.txt --load-cookies=cookie.txt \
         -O {2} \
         \"http://galaxy-catalogue.dur.ac.uk:8080/MyMillennium?action=doQuery&SQL={3}\"'.format(username, password, save_dir+save_name, sql_query)
  print(cmd)
  os.system(cmd)
  
def add_columns_to_s3sax(s3sax_fname, newcols_fname, mode='merge'):
  
  new_columns = pd.read_csv(newcols_fname, comment='#')
  s3sax_columns = pd.read_hdf(s3sax_fname, 'table')
  
  s3sax_columns['galaxyid'] = s3sax_columns['galaxyid'].astype(int)

  if 'galaxyID' in new_columns.columns:
    new_columns.rename(columns={'galaxyID' : 'galaxyid'}, inplace=True)
  
  if mode=='merge':
    ret_df = pd.merge(s3sax_columns, new_columns, on='galaxyid')
    return ret_df
  elif mode=='join':
    s3sax_columns = s3sax_columns.join(new_columns, on='galaxyid', how='left', rsuffix='_sdss')
    return s3sax_columns
  
if __name__=='__main__':
  
  delu_sdss_sql = 'select \
                   sdss.galaxyID, sdss.snapnum, u_sdss, g_sdss, r_sdss, i_sdss, z_sdss, J_2mass, H_2mass, K_2mass, redshift \
                   from \
                   MPAGalaxies..DeLucia2006a_SDSS2MASS as sdss, \
                   MPAGalaxies..DeLucia2006a as dl \
                   where \
                   dl.redshift between 0.35 and 3.06 and \
                   dl.galaxyID = sdss.galaxyID'
  
  wget_from_virgodb('mypass', 'myname', delu_sdss_sql, save_name='delucia2006a_sdss2mass.csv')
  
  s3sax_with_sdss = add_columns_to_s3sax('updated_contcut_galaxies_line.h5', 'delucia2006a_sdss2mass.csv', mode='merge')
  s3sax_with_sdss.to_hdf('s3sax_delucia2006a_sdss2mass.h5', key='table')