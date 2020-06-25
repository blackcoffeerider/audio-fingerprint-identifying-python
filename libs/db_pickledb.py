from .db import Database
from .config import get_config
from pickledb import PickleDB
from itertools import zip_longest
from termcolor import colored
# from threading import Thread
import json
import os
# import gzip
import lzma

# class GZIPPickleDB(PickleDB):
#     def __init__(self, location, auto_dump=False, sig=None):
#         super().__init__(location, auto_dump, sig)
    
#     def _loaddb(self):
#         '''Load or reload the json info from the file'''
#         try: 
#             self.db = json.load(gzip.open(self.loco, 'rt'))
#         except ValueError:
#             if os.stat(self.loco).st_size == 0:  # Error raised because file is empty
#                 self.db = {}
#             else:
#                 raise  # File is not empty, avoid overwriting it

    
#     def dump(self):
#         '''Force dump memory db to file'''

#         json.dump(self.db, gzip.open(self.loco, 'wt'))
#         # self.dthread = Thread(
#         #     target=json.dump,
#         #     args=(self.db, gzip.open(self.loco, 'wt')))
#         # self.dthread.start()
#         # self.dthread.join()
#         return True

class LZMAPickleDB(PickleDB):
    def __init__(self, location, auto_dump=False, sig=None):
        super().__init__(location, auto_dump, sig)
    
    def _loaddb(self):
        '''Load or reload the json info from the file'''
        try: 
            self.db = json.load(lzma.open(self.loco, 'rt'))
        except ValueError:
            if os.stat(self.loco).st_size == 0:  # Error raised because file is empty
                self.db = {}
            else:
                raise  # File is not empty, avoid overwriting it

    
    def dump(self):
        '''Force dump memory db to file'''

        json.dump(self.db, lzma.open(self.loco, 'wt'))
        # self.dthread = Thread(
        #     target=json.dump,
        #     args=(self.db, gzip.open(self.loco, 'wt')))
        # self.dthread.start()
        # self.dthread.join()
        return True


class MultiSoundHashPickleDB():
    def __init__(self,filename=None):
        self.filename = filename
        config = get_config()
        if self.filename == None:
            self.filename = config['db.file']
        self.songsfname =  os.path.join(config['db.dir'],"songs_"+self.filename)
        self.songs = LZMAPickleDB(self.songsfname)
        self.subdbs = {}

        for k in self.songs.getall():
            song = self.songs.get(k)
            self.load_subdb(k, song["name"])

    
    def load_subdb(self, key, filename=None):
        if filename:
            print("loading fingerprints for {}".format(filename))
        else:
            print("loading fingerprints for {}".format(str(key)))
        self.subdbs[key] = SoundHashPickleDB(None,key)

    def find(self, hashes):
        for filekey in self.subdbs.keys():
            for k in hashes:
                if self.subdbs[filekey].fprints.exists(k):
                    for o in self.subdbs[filekey].fprints.get(k).split("|"):
                        yield (k, filekey, int(o))

    def get_song_by_id(self, song_id):
        if self.songs.exists(song_id):
            d = self.songs.get(song_id)
            return d
        return None


class SoundHashPickleDB(Database):
    TABLE_SONGS = 'songs'
    TABLE_FINGERPRINTS = 'fingerprints'

    def __init__(self,filename=None,filehash=None):
        self.filename = filename
        self.filehash = filehash
        self.changed = False
        config = get_config()

        if self.filename == None:
            self.filename = config['db.file']

        if filehash is not None:
            self.songsfname =  os.path.join(config['db.dir'],"songs_"+self.filehash)
            self.fprintsfname = os.path.join(config['db.dir'],"fprints_"+self.filehash)
        else:
            print("Open: "+self.filename)
            self.songsfname =  os.path.join(config['db.dir'],"songs_"+self.filename)
            self.fprintsfname = os.path.join(config['db.dir'],"fprints_"+self.filename)

        self.open = False
        self.connect()

    def connect(self):
        self.songs = LZMAPickleDB(self.songsfname , False)
        self.fprints = LZMAPickleDB(self.fprintsfname , False)
        self.open = True
        # print((colored('pickle:'+self.songsfname+' - connection opened','white',attrs=['dark'])))
        # print((colored('pickle:'+self.fprintsfname+' - connection opened','white',attrs=['dark'])))

    def close(self):
        if self.changed == True and self.open == True:
            self.open = False
            self.songs.dump()
            self.fprints.dump()
        # print((colored('pickle:'+self.songsfname+' - connection has been closed','white',attrs=['dark'])))
        # print((colored('pickle:'+self.fprintsfname+' - connection has been closed','white',attrs=['dark'])))


    def __del__(self):
        if self.open:
            self.close()


    def add_song(self, filename, filehash):
        song = self.get_song_by_filehash(filehash)
        if self.open:
            if not song:
                self.songs.set(filehash, {
                    "name": filename,
                    "filehash": filehash,
                    "hashcount": 0
                })
                song_id = filehash
            else:
                song_id = song["filehash"]

            self.changed = True
            return song_id
        return None
    
    def update_song_hashcount(self, song_id, hashcount):
        if self.open:
            if self.songs.exists(song_id):
                d = self.songs.get(song_id)
                d["hashcount"] = hashcount
                self.songs.set(song_id,d)
                self.changed = True
                return True
            return False


    def get_song_hashes_count(self, song_id):
        if self.songs.exists(song_id):
            d = self.songs.get(song_id)
            return d["hashcount"]
        return None

    def get_song_by_filehash(self, filehash):
        if self.songs.exists(filehash):
            d = self.songs.get(filehash)
            return d
        return None

    def get_song_by_id(self, song_id):
        if self.songs.exists(song_id):
            d = self.songs.get(song_id)
            return d
        return None

    def store_fingerprint(self, song_id, iv_hash, offset):
        if self.open:
            if self.fprints.exists(iv_hash):
                self.fprints.append(iv_hash, "|"+str(offset))
            else:
                self.fprints.set(iv_hash, ""+str(offset))
            self.changed = True
        
    def store_fingerprints(self,sets):
        if self.open:
            for song_id, lv_hash, offset in sets:
                self.store_fingerprint(song_id, lv_hash, offset)
