#-*- coding:utf-8 -*-

from django.core.management.base import BaseCommand
from radio.models import Tweet,Broadcast,Tweet_To_Broadcast,Tweet_User,Tweet_To_Broadcast_Rank
from django.core.exceptions import ObjectDoesNotExist
from django.db import models
import calendar
from datetime import datetime,timedelta
import copy
import time
import sys
import json
import pytz
from cqlengine.query import DoesNotExist
jsonDec = json.decoder.JSONDecoder()

class Command(BaseCommand):
    def change_to_dt(self,created_at):
        t_list = created_at.encode('utf-8').split(" ")
        t_cut = " ".join(t_list[:4] + t_list[5:])

        tdatetime = datetime.strptime(t_cut, "%a %b %d %H:%M:%S %Y")
        ct = datetime(tdatetime.year,
                          tdatetime.month,
                          tdatetime.day,
                          tdatetime.hour,
                          tdatetime.minute,
                          tdatetime.second,
                          000001,
                          tzinfo=pytz.timezone('utc'))
        return ct

    def datetime_to_epoch(self,d):
        return int(time.mktime(d.timetuple()))

    def build(self,broadcast_id,tweet):

        try:
            data = tweet.json
        except:
            print "loads error"
            print tweet.json
            sys.exit()


        print data['user']['screen_name']
        print tweet.created_at
        if 'retweeted_status' in data:
            #print data
            p0 = Tweet_User(
                       screen_name=data['retweeted_status']['user']['screen_name'],
                       name=data['retweeted_status']['user']['name'],
                       description=data['retweeted_status']['user']['description'],
                       profile_image_url=data['retweeted_status']['user']['profile_image_url'],
                       location=data['retweeted_status']['user']['location'],
                        )
            p0.save()
        # add tweet_user
        p1 = Tweet_User(
                       screen_name=data['user']['screen_name'],
                       name=data['user']['name'],
                       description=data['user']['description'],
                       profile_image_url=data['user']['profile_image_url'],
                       location=data['user']['location'],
                        )
        p1.save()

        qa = Broadcast.objects.get(broadcast_id=broadcast_id)

        if 'retweeted_status' in data:
            qb = Tweet_User.objects.get(screen_name=data['retweeted_status']['user']['screen_name'])
            try:
                if data['retweeted_status'].has_key("entities") is False:
                    data['retweeted_status']['entities'] = {}
                if data['retweeted_status'].has_key("extended_entities") is False:
                    data['retweeted_status']['extended_entities'] = {}

                p4 = Tweet_To_Broadcast.objects.get(tweetid=data['retweeted_status']['id'])
                p5 = Broadcast.objects.get(broadcast_id=p4.broadcast_id)
                p2 = Tweet_To_Broadcast(
                                id = p4.id,
                                tweetid = data['retweeted_status']['id'],
                                broadcast = p5,
                                tweet_user = qb,
                                entities = data['retweeted_status']['entities'],
                                extended_entities = data['retweeted_status']['extended_entities'],
                                hashtags = tweet.hashtags,
                                text = data['retweeted_status']['text'],
                                created_at = self.change_to_dt(data['retweeted_status']['created_at']),
                                retweet_count = data['retweeted_status']['retweet_count']
                                )
            except ObjectDoesNotExist:
                p2 = Tweet_To_Broadcast(
                                tweetid = data['retweeted_status']['id'],
                                broadcast = qa,
                                tweet_user = qb,
                                entities = data['retweeted_status']['entities'],
                                extended_entities = data['retweeted_status']['extended_entities'],
                                hashtags = tweet.hashtags,
                                text = data['retweeted_status']['text'],
                                created_at = self.change_to_dt(data['retweeted_status']['created_at']),
                                retweet_count = data['retweeted_status']['retweet_count']
                                )
            p2.save()

        else:
            qb = Tweet_User.objects.get(screen_name=data['user']['screen_name'])
            # insert tweet_to_broadcast
            print tweet
            #print data
            if data.has_key("entities") is False:
                 data['entities'] = {}
            if data.has_key("extended_entities") is False:
                 data['extended_entities'] = {}

            p2 = Tweet_To_Broadcast(
                                tweetid = tweet.tweetid,
                                broadcast = qa,
                                tweet_user = qb,
                                hashtags = tweet.hashtags,
                                text = tweet.text,
                                entities = data['entities'],
                                extended_entities = data['extended_entities'],
                                created_at = tweet.created_at,
                                retweet_count = 0
                                )
            p2.save()

        # count up ranking
        try:
            print qa.broadcast_id
            qc = Tweet_To_Broadcast_Rank.objects.get(broadcast_id=qa.broadcast_id)
            qc.count = (qc.count + 1)
            qc.save()
        except:
            p3 = Tweet_To_Broadcast_Rank(
                                         broadcast_id = qa,
                                         count=1
                                         )
            p3.save()


    def check(self,tweet,blist):

        for b in blist:
            if self.datetime_to_epoch(b['begin_time']) <= self.datetime_to_epoch(tweet.created_at) <= self.datetime_to_epoch(b['end_time']):
                for tweethashtag in jsonDec.decode(tweet.hashtags):
                    tweethashtag = tweethashtag.encode('utf-8')
                    if tweethashtag in b['broadcast_hashtag']:
                        print "match:",b['broadcast_id'],b['begin_time'],b['end_time'],tweethashtag
                        return b['broadcast_id']
        return None

    def checkexist_boolen(self,tweetid):
        try:
            Tweet_To_Broadcast.objects.get(tweetid=tweetid)
            return True
        except ObjectDoesNotExist:
            return False

    def delete_duplicate_tweetid(self,tweetid):
        try:
            tweet = Tweet.objects.filter(tweetid=tweetid)
            tweet.filter(tombstone=0).delete()
            print "delete_duplicate_tweetid:",
            print tweet.queue
            return True
        except:
            return False

    def flag_bulk_hashtag(self,tweet):
        try:
            print tweet.text
        except:
            print ""

        tweet.tombstone = 3
        tweet.update_date = datetime.utcnow()
        tweet.save()
        return

    def flag_spam_tweet(self,tweet):
        try:
            print tweet.text
        except:
            print ""

        tweet.tombstone = 4
        tweet.update_date = datetime.utcnow()
        tweet.save()
        return


    def list_ng_hash(self):
        nghashlist = [u'相互フォロー',
                      u'sougofollow',
                      u'有益なことをつぶやこう',
                      u'戸賀崎智信',
                      u'アプリ',
                      u'bot',
                      ]
        return nghashlist

    def check_ng_hash(self,listhash,listnghash):
        for hash in listhash:
            if hash in listnghash:
                return True
        return None

    def list_ng_word(self):
        ngwordlist = [u'【カトパン大好き】',
                      u'【Chromecast】',
                      u'【残りわずか】',
                      u'【拡散希望】',
                      u'婚活サービス',
                      u'NAVER まとめ',
                      u'【そっくり激似AV】',
                      u'[E★小説]',
                      u'個人専用ですのでクリックしないで下さい',
                      u'管理人専用ですのでクリックしないで下さい',
                      u'PS4プレゼントキャンペーン',
                      u'【激似そっくりAV】',
                      ]
        return ngwordlist

    def check_ng_word(self,str,listngword):
        for ngword in listngword:
            try:
                i = str.index(ngword)  # 引数で与えられた文字列を先頭から探索した場合の出現位置を返す
                return True
            except ValueError:
                i = None
        return i

    def handle(self, *args, **options):
        num_insert = 0
        num_pass = 0
        num_delete = 0
        num_bulk = 0
        num_spam = 0
        num_total = 0

        blist = []
        for broadcast in Broadcast.objects.all().order_by('-broadcast_id'):
            dict = {"broadcast_id":broadcast.broadcast_id,
                    "begin_time":broadcast.begin_time,
                    "end_time":broadcast.end_time,
                    "broadcast_hashtag":json.loads(broadcast.broadcast_hashtag),
                    }
            blist.append(dict)

        print "loading: list_ng_word"
        listngword = self.list_ng_word()
        print "loading: list_ng_word"
        listnghash = self.list_ng_hash()
        print "loading: Broadcast.objects"

        for tweet in Tweet.objects.filter(tombstone=0)[:2000]:
            sys.stdout.write('{0} worker\r'.format(num_total))
            sys.stdout.flush()

            num_total = num_total + 1
            dt = datetime.utcnow()
            # bulk hashtag
            hashlist = []
            hashlist = json.loads(tweet.hashtags)
            if len(hashlist) >= 8:
                print hashlist
                self.flag_bulk_hashtag(tweet)
                num_bulk = num_bulk + 1
                continue

            if self.check_ng_hash(hashlist,listnghash) is True:
                self.flag_spam_tweet(tweet)
                num_spam = num_spam + 1
                #self.flag_bulk_hashtag(tweet)
                continue

            if self.check_ng_word(tweet.text,listngword) is True:
                self.flag_spam_tweet(tweet)
                num_spam = num_spam + 1
                #self.flag_bulk_hashtag(tweet)
                continue

            if self.checkexist_boolen(tweet.tweetid) is False:
                # tweetと番組を紐づける
                broadcast_id = self.check(tweet,blist)

                if broadcast_id:
                    self.build(broadcast_id,tweet)
                    num_insert = num_insert + 1
                    # add flag tombstone
                    tweet.tombstone = 1

                else:
                    num_pass = num_pass + 1
                    # add flag tombstone
                    tweet.tombstone = 2


                tweet.update_date = dt
                tweet.save()

            else:
                self.delete_duplicate_tweetid(tweet.tweetid)
                num_delete = num_delete + 1
                print tweet.tweetid

        print ""
        print "insert:%d" % num_insert
        print "pass:  %d" % num_pass
        print "delete:  %d" % num_delete
        print "bulk:  %d" % num_bulk
        print "spam:  %d" % num_spam
        print "total: %d" % num_total
