# -*- coding:utf-8 -*-
from django.core.management.base import BaseCommand
from django.conf import settings
import os,sys
sys.path.append(settings.COMMANDLIB_DIRS[0])
from __builtin__ import False

import traceback,sys
import time
import re

import threading
import time
import random
from twitterconf import *
from mongoconf import Mongoconf
from radio.models import Tweet_Trends,Broadcast
from hashtag import Hashtag
import json
import traceback,sys
import signal

class timer:
    def __init__(self):
        self.flag = True

    def _handler(self, x, y):
        self.flag = False

    def set(self, sec):
        #signal.signal(signal.SIGALRM, self._handler)
        signal.signal(signal.SIGTERM, self._handler)
        signal.alarm(sec)

    def check(self):
        return self.flag

class Command(BaseCommand):

        def handle(self, *args, **options):
            p = Mongoconf()
            con = p.connect_replica()
            db = con.tweetDB
            col = db.tweets_ja

            twitter = Twitter()
            twitter.test()
            #twitter.get_tweetstreming()
            print "starting worker()"

            tracklist = twitter.create_tracklist()
            print "tracking: %s" % tracklist
            if tracklist is None:
                print "error: create_tracklist()"
                return

            #sys.exit(0)
            #tweetのジェネレータを取得する関数
            res= twitter.get_tweet(tracklist)

            #メインループ
            for r in res:
                #timeout処理
                tmr = timer()
                tmr.set(120)
                tmr.check()
                
                try:
                    #jsonに変換
                    data= json.loads(r)

                    if data['user']['lang'] == 'ja':
                        print data['user']['id'], data['user']['screen_name'], data['text']
                        #mongoDBに入れる
                        p.to_mongo(data,col)
                        #self.to_mongo(data,self.judge(data),ja)

                except ValueError, e:
                    pass
                except:
                    print traceback.format_exc(sys.exc_info()[2])
                    print "OMG! Fuckin' error!"
                    return

                finally:
                    print "fin: self.to_mongo()"

            print "Finishing routine()"
            return

