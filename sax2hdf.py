import numpy as np
import pandas as pd
import time
import pdb
import sys

in_fname = '/local/scratch/harrison/scubed/galaxies_line.csv'
#in_fname = './10k_galaxies_line.csv'
out_fname = '/local/scratch/harrison/scubed/galaxies_line.h5'
#out_fname = './10k_galaxies_line.h5'
continuum_out_fname = './contcut_galaxies_line.h5'

ska1_band1 = {
              'name' : 'ska1_band1-sum',
              'bandwidth' : 700.e6,
              'nu_min' : 350.e0,
              'nu_max' : 1050.e0,
              'delta_nu' : 0.01,
              'nchan' : 64e3,
              'centre_frequency' : 0.7e9,
              's_rms' : 315.e-6,
              'z_min' : 0.5,
              'z_max' : 3.06
              }

ska1_band2 = {
              'name' : 'ska1_band2-sum',
              'bandwidth' : 810.e6,
              'nu_min' : 950.e0,
              'nu_max' : 1400.e0,
              'delta_nu' : 0.01,
              'nchan' : 64e3,
              'centre_frequency' : 1.355e9,
              's_rms' : 187.e-6,
              'z_min' : 0.00,
              'z_max' : 0.58
              }

tstart = time.time()

chunker = pd.read_csv(in_fname, chunksize=1000, dtype=float)
#store = pd.HDFStore(out_fname, 'w')
continuum_store = pd.HDFStore(continuum_out_fname, 'w')
for piece in chunker:
  deltaV = 2.1e0*(1.e0 + piece['zapparent'])
  ska1_band1_snr = (piece['hiintflux']/piece['hiwidthpeak'])/(ska1_band1['s_rms']/np.sqrt(piece['hiwidthpeak']/deltaV))
  ska1_band2_snr = (piece['hiintflux']/piece['hiwidthpeak'])/(ska1_band2['s_rms']/np.sqrt(piece['hiwidthpeak']/deltaV))

  piece['ska1_band1_snr'] = pd.Series(ska1_band1_snr, index=piece.index)
  piece['ska1_band2_snr'] = pd.Series(ska1_band2_snr, index=piece.index)

  continuum_store.append('data', piece[(abs(piece['ra']) < 2.)*(abs(piece['decl']) < 2.)*(np.greater(piece['himass'],piece['zapparent']*pow(10,9.5)))])
  #store.append('data', piece) 

#store.close()
continuum_store.close()


tend = time.time()
print('Time taken: {0}'.format(tend-tstart))

'''
  ra, decl, distance, zapparent, himass, hiintflux, diskpositionangle, diskinclination, hiaxisratio, himajoraxis_halfmass, hilumcenter, hilumpeak, hiwidthpeak, hiwidth50, hiwidth20 = line[3,4,5,6,8,10,21,22,25,30,37,38,39,40,41]
  peakfluxdensity = hiintflux*hilumpeak
  centerfluxdensity = hiintflux*hilumcenter
'''
  
