#!/usr/bin/python
import os
import sys
import libs
import libs.fingerprint as fingerprint
import argparse

from pprint import pprint

from argparse import RawTextHelpFormatter
from itertools import zip_longest
from termcolor import colored
from libs.config import get_config
from libs.reader_microphone import MicrophoneReader
from libs.visualiser_console import VisualiserConsole as visual_peak
from libs.visualiser_plot import VisualiserPlot as visual_plot
from libs.db_pickledb import MultiSoundHashPickleDB
# from libs.db_mongo import MongoDatabase

gv_retlimit = 3
gv_conflimit = 80



def grouper(iterable, n, fillvalue=None):
  args = [iter(iterable)] * n
  return ([_f for _f in values if _f] for values
          in zip_longest(fillvalue=fillvalue, *args))


def find_matches(samples, Fs=fingerprint.DEFAULT_FS):
  hashes = fingerprint.fingerprint(samples, Fs=Fs)
  return return_matches(hashes)

def return_matches(hashes):
  mapper = {}
  values = []
  for hash, offset in hashes:
    mapper[hash] = offset
    values.append(hash)
  # values = list(mapper.keys())

  # for split_values in grouper(values, 1000):
  if 1 == 1:
    split_values = values
    
    x = db.find(split_values)


    matches_found = 0

    for hash, sid, offset in x:
      matches_found += 1
      if hash in mapper:
        yield (sid, offset - mapper[hash])
      else:
        msg = "===> hash_missmatch: %s"
        print(colored(msg, 'red') % (str(hash)))

    if matches_found > 0:
      msg = '   ** found %d hash matches (step %d/%d)'
      print(colored(msg, 'green') % (
        matches_found,
        len(split_values),
        len(values)
      ))
    else:
      msg = '   ** not matches found (step %d/%d)'
      print(colored(msg, 'red') % (
        len(split_values),
        len(values)
      ))


def align_matches(matches):
  diff_counter = {}
  largest = 0
  largest_count = 0
  song_id = -1

  retmatchesdict = {} 

  

  for tup in matches:
    sid, diff = tup

    if diff not in diff_counter:
      diff_counter[diff] = {}

    if sid not in diff_counter[diff]:
      diff_counter[diff][sid] = 0

    diff_counter[diff][sid] += 1

    if diff_counter[diff][sid] > largest_count:
      largest = diff
      largest_count = diff_counter[diff][sid]
      song_id = sid
      retmatchesdict[song_id] = { "song_id": song_id, "largest": largest, "largest_count": largest_count }
  
  retmatches = [ v for k, v in sorted(retmatchesdict.items(), key=lambda item: int(item[1]["largest_count"]), reverse=True) ]

  for idx, match in enumerate(retmatches):
    
    if idx >= gv_retlimit:
      break

    if idx >= 1 and match["largest_count"] < gv_conflimit:
      break


    songM = db.get_song_by_id(match["song_id"])

    nseconds = round(float(match["largest"]) / fingerprint.DEFAULT_FS *
                      fingerprint.DEFAULT_WINDOW_SIZE *
                      fingerprint.DEFAULT_OVERLAP_RATIO, 5)

    yield {
        "SONG_ID" : match["song_id"],
        "SONG_NAME" : songM["name"],
        "CONFIDENCE" : match["largest_count"],
        "OFFSET" : int(match["largest"]),
        "OFFSET_SECS" : nseconds
    }



if __name__ == '__main__':
  config = get_config()

  db = MultiSoundHashPickleDB()

  parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter)
  parser.add_argument('-s', '--seconds', nargs='?')
  args = parser.parse_args()

  if not args.seconds:
    parser.print_help()
    sys.exit(0)

  seconds = int(args.seconds)

  chunksize = 2**12  # 4096
  channels = 2#int(config['channels']) # 1=mono, 2=stereo

  record_forever = False
  visualise_console = bool(config['mic.visualise_console'])
  visualise_plot = bool(config['mic.visualise_plot'])

  reader = MicrophoneReader(None)
  detect_forever = bool(config['mic.detect_forever'])

  detect = True

  while detect:
    detect = detect_forever
    reader.start_recording(seconds=seconds,
      chunksize=chunksize,
      channels=channels)

    # msg = ' * started recording..'
    # print(colored(msg, attrs=['dark']))

    while True:
      bufferSize = int(reader.rate / reader.chunksize * seconds)

      for i in range(0, bufferSize):
        nums = reader.process_recording()

        if visualise_console:
          msg = colored('   %05d', attrs=['dark']) + colored(' %s', 'green')
          print(msg  % visual_peak.calc(nums))
        else:
          pass
          # msg = '   processing %d of %d..' % (i, bufferSize)
          # print(colored(msg, attrs=['dark']))

      if not record_forever: break

    if visualise_plot:
      data = reader.get_recorded_data()[0]
      visual_plot.show(data)

    reader.stop_recording()

    # msg = ' * recording has been stopped'
    # print(colored(msg, attrs=['dark']))

    data = reader.get_recorded_data()

    msg = ' * recorded %d samples'
    print(colored(msg, attrs=['dark']) % len(data[0]))

    # reader.save_recorded('test.wav')


    Fs = fingerprint.DEFAULT_FS
    channel_amount = len(data)

    result = set()
    matches = []


    for channeln, channel in enumerate(data):
      # TODO: Remove prints or change them into optional logging.
      # msg = '   fingerprinting channel %d/%d'
      # print(colored(msg, attrs=['dark']) % (channeln+1, channel_amount))

      matches.extend(find_matches(channel))

      # msg = '   finished channel %d/%d, got %d hashes'
      # print(colored(msg, attrs=['dark']) % (
      #   channeln+1, channel_amount, len(matches)
      # ))

    total_matches_found = len(matches)

    print('')

    if total_matches_found > 0:
      msg = ' ** totally found %d hash matches'
      print(colored(msg, 'green') % total_matches_found)


      songs = align_matches(matches)



      for song in songs:
        msg = ' => song: %s (id=%s)\n'
        msg += '    offset: %d (%d secs)\n'
        msg += '    confidence: %d'
        print(colored(msg, 'green') % (
          song['SONG_NAME'], song['SONG_ID'],
          song['OFFSET'], song['OFFSET_SECS'],
          song['CONFIDENCE']
        ))
    else:
      msg = ' ** not matches found at all'
      print(colored(msg, 'red'))
