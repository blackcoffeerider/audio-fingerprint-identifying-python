#!/usr/bin/python

from multiprocessing import set_start_method
from multiprocessing import get_context
# try:
#     set_start_method('spawn')
# except RuntimeError:
#     pass


import os
import sys
import libs
import libs.fingerprint as fingerprint
from itertools import zip_longest
from pathlib import Path

from termcolor import colored
from libs.reader_file import FileReader
# from libs.db_sqlite import SqliteDatabase
from libs.db_pickledb import SoundHashPickleDB
from libs.config import get_config


from multiprocessing import Pool, TimeoutError

import time
import os
from pprint import pprint

def fingerprint_file(filename):
    config = get_config()

    reader = FileReader(filename)
    audio = reader.parse_audio()
    filehash = reader.parse_file_hash()

    db = SoundHashPickleDB(None,filehash)

    song = db.get_song_by_filehash(filehash)
    song_id = db.add_song(filename, filehash)



    msg = ' * %s %s: %s' % (
      colored('id=%s', 'white', attrs=['dark']),       # id
      colored('channels=%d', 'white', attrs=['dark']), # channels
      colored('%s', 'white', attrs=['bold'])           # filename
    )
    print(msg % (song_id, len(audio['channels']), filename))
    # time.sleep(20)
    # return filename, 42


    hashes = set()
    channel_amount = len(audio['channels'])

    for channeln, channel in enumerate(audio['channels']):
      # msg = '   fingerprinting channel %d/%d'
      # print(colored(msg, attrs=['dark']) % (channeln+1, channel_amount))

      # fingerprint.PEAK_SORT = False

      channel_hashes = fingerprint.fingerprint(channel, Fs=audio['Fs'], plots=config['fingerprint.show_plots'])
      channel_hashes = set(channel_hashes)

      # msg = '%s   finished channel %d/%d, got %d hashes'
      # print(colored(msg, attrs=['dark']) % (
      #   filename, channeln+1, channel_amount, len(channel_hashes)
      # ))

      hashes |= channel_hashes

    # msg = '%s   finished fingerprinting, got %d unique hashes'

    values = []
    for lv_hash, offset in hashes:
      values.append((song_id, lv_hash, offset))

    l1 = len(values)
    msg = '%s   storing %d hashes in db' % (filename, l1)
    print(colored(msg, 'green'))

    # pprint((1, (filename, l1)))
    db.store_fingerprints(values)
    l2 = len(db.fprints.getall())
    db.update_song_hashcount(song_id,l2)
    msg = '%s   stored %d unique hashes in db' % (filename, l2)
    print(colored(msg, 'green'))

    del db
    # time.sleep(2)
    # pprint((2, (filename, l2)))
    
    return filename, l2, song_id

def grouper(iterable, n, fillvalue=None):
  for i in range(0, len(iterable), n):
    yield iterable[i:i + n]

def find_files_to_process():
  config = get_config()

  db = SoundHashPickleDB("main")
  path = "mp3"

  # fingerprint all files in a directory
  processfiles = []
  fnps = list(Path(path).rglob('*.mp3'))
  fnps.extend(list(Path(path).rglob('*.flac')))

  fnps = sorted(fnps)
  
  for fnp in fnps:
    filename = str(fnp)

    if ".mp3" in filename or ".flac" in filename:

      reader = FileReader(filename)
      filehash = reader.parse_file_hash()

      song = db.get_song_by_filehash(filehash)
      if not song:
        song_id = db.add_song(filename, filehash)
        song = db.get_song_by_filehash(filehash)
      else:
        song_id = song["filehash"]

      msg = ' * %s %s: %s' % (
        colored('id=%s', 'white', attrs=['dark']),       # id
        colored('channels=%d', 'white', attrs=['dark']), # channels
        colored('%s', 'white', attrs=['bold'])           # filename
      )
      

      if song:
        hash_count = db.get_song_hashes_count(song_id)

        if hash_count > 0:
          msg = '   already exists (%d hashes), skip' % hash_count
          print(colored(msg, 'red'))

          continue
        processfiles.append(filename)
        print(colored(filename+':new song, going to analyze..', 'green'))
  db.close()
  del db
  return processfiles


if __name__ == '__main__':
  set_start_method("spawn")

  psize = 12 # start 6 worker processes

  with get_context("spawn").Pool(processes=psize) as pool:    
    processfiles = find_files_to_process()
    for group in grouper(processfiles, psize):
        r = pool.map_async(fingerprint_file, group) 
        results = r.get()
        # pprint(results)
        dbmain = SoundHashPickleDB("main")
        for _, _, song_id in results:
          dbs = SoundHashPickleDB(None,song_id)
          for k  in dbs.songs.getall():
            dataset = dbs.get_song_by_filehash(k)
            dbmain.update_song_hashcount(k,dataset["hashcount"])
          dbs.close()
          del dbs
        dbmain.close()
        del dbmain
        