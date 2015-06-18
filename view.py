# -*- coding: utf-8 -*-

import json
from collections import OrderedDict
from django.http import HttpResponse
from cms.models import Book
from radio.models import Broadcaster,Broadcast,Tweet_To_Broadcast_Rank,Tweet_To_Broadcast
from django.shortcuts import get_object_or_404, render_to_response

from load_profile_images import *
from load_image import *

def render_json_response(request, data, status=None):
    '''response を JSON で返却'''
    json_str = json.dumps(data, ensure_ascii=False, indent=2)
    callback = request.GET.get('callback')
    if not callback:
        callback = request.REQUEST.get('callback')  # POSTでJSONPの場合
    if callback:
        json_str = "%s(%s)" % (callback, json_str)
        response = HttpResponse(json_str, content_type='application/javascript; charset=UTF-8', status=status)
    else:
        response = HttpResponse(json_str, content_type='application/json; charset=UTF-8', status=status)

    response["Access-Control-Allow-Origin"] = "*"
    response["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
    response["Access-Control-Max-Age"] = "1000"
    response["Access-Control-Allow-Headers"] = "*"
    return response

def book_list(request):
    '''書籍と感想のJSONを返す'''
    books = []
    for book in Book.objects.all().order_by('id'):

        impressions = []
        for impression in book.impressions.order_by('id'):
            impression_dict = OrderedDict([
                ('id', impression.id),
                ('comment', impression.comment),
            ])
            impressions.append(impression_dict)

        book_dict = OrderedDict([
            ('id', book.id),
            ('name', book.name),
            ('publisher', book.publisher),
            ('page', book.page),
            ('impressions', impressions)
        ])
        books.append(book_dict)

    data = OrderedDict([ ('books', books) ])
    return render_json_response(request, data)

# Create your views here.
def broadcaster_list(request,broadcaster_id):
    p = get_object_or_404(Broadcaster, id=broadcaster_id)
    broadcaster = OrderedDict([
            ('id', p.id),
            ('broadcaster_id', p.broadcaster_id),
            ('broadcaster_type', p.broadcaster_type),
            ('broadcaster_name_en', p.broadcaster_name_en),
            ('broadcaster_name_jp', p.broadcaster_name_jp),
        ])
    return render_json_response(request, broadcaster)


def broadcast_list(request,broadcast_id):
    p = get_object_or_404(Broadcast,broadcast_id=broadcast_id)
    #p.begin_time = str(p.begin_time)
    #p.end_time = str(p.end_time)
    p.begin_time = p.begin_time.isoformat()
    p.end_time = p.end_time.isoformat()
    broadcast = OrderedDict([
            ('broadcast_id',p.broadcast_id),
            ('broadcaster_id',p.broadcaster_id),
            ('broadcast_name_en',p.broadcast_name_en),
            ('broadcast_name_jp',p.broadcast_name_jp),
            ('broadcast_hashtag' ,p.broadcast_hashtag),
            ('broadcast_week',p.broadcast_week),
            ('begin_time',p.begin_time),
            ("end_time",p.end_time),
        ])
    return render_json_response(request, broadcast)

def broadcast_rank(request,broadcast_id):
    try:
        p = Tweet_To_Broadcast_Rank.objects.get(broadcast_id_id=broadcast_id)
        create_date = int(p.create_date.strftime('%Y%m%d'))
        update_date = int(p.update_date.strftime('%Y%m%d'))

        broadcast = OrderedDict([
            ('id',p.id),
            ('broadcast_id',p.broadcast_id_id),
            ('count',p.count),
            ('create_date',create_date),
            ('update_date',update_date),
        ])
    except:
        broadcast = OrderedDict([
            ('id',0),
            ('broadcast_id',0),
            ('count',0),
            ('create_date',0),
            ('update_date',0),
        ])
    return render_json_response(request, broadcast)

def broadcast_hot_tweet(request,broadcast_id):
    p = Tweet_To_Broadcast.objects.filter(broadcast_id=broadcast_id)
    p = p.order_by('-retweet_count')[:1]
    print p.query

    if p:
        load_profile_images = DjangoLoadImage()
        load_image = DjangoLoadImage2()
        load_profile_image_url = load_profile_images.load_profile_images(p[0].tweet_user.profile_image_url)
        load_tweet_image_url = load_image.load_image(p[0].entities)
        broadcast = OrderedDict([
            ('broadcast_id',p[0].broadcast_id),
            ('tweetid',p[0].tweetid),
            ('tweet_user_id',p[0].tweet_user_id),
            ('tweet_user_profile_image_url',p[0].tweet_user.profile_image_url),
            ('load_profile_image_url',load_profile_image_url),
            ('load_tweet_image_url',load_tweet_image_url),
            ('text',p[0].text),
        ])
    else:
         broadcast = OrderedDict([
            ('broadcast_id',0),
            ('tweetid',0),
            ('tweet_user_id',0),
            ('tweet_user_profile_image_url','/static/images/gif-load.gif'),
            ('load_profile_image_url','/static/images/gif-load.gif'),
            ('text',""),
        ])
    return render_json_response(request, broadcast)
