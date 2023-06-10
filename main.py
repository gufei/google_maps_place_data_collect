import os.path
import pandas as pd
import urllib.request
from urllib.parse import quote
import string
import json
import numpy
import signal
import multitasking
from tqdm import tqdm
import datetime
from dotenv import load_dotenv

load_dotenv()

signal.signal(signal.SIGINT, multitasking.killall)

pd.set_option('display.unicode.east_asian_width', True)  # 设置输出右对齐

#   Google Key
googleKey = os.getenv('GOOGLE_CLOUD_API_KEY', '')

if not googleKey:
    print("请配置 GOOGLE_CLOUD_API_KEY")
    exit()

TIMEOUT = 30

keywordArr = os.getenv('KEYWORDS', '').split(',')
typeArr = os.getenv('SEARCH_TYPE').split(',')

searchMode = os.getenv('SEARCH_MODE', 'keyword')

count = dict()
countLine = dict()
df = dict()
outfile = dict()
pbar = dict()

# 参数，该参数通过 地理编码服务获取 https://maps.googleapis.com/maps/api/geocode/json?language=zh-CN&address=%E6%96%B0%E5%8A%A0%E5%9D%A1&key={googleKey}
lonRange = [float(os.getenv('COLLECT_BOUNDS_WEST', 103.594)),
            float(os.getenv('COLLECT_BOUNDS_EAST', 104.0945001))]  # the range of longitude 经度的范围
latRange = [float(os.getenv('COLLECT_BOUNDS_SOUTH', 1.1496)),
            float(os.getenv('COLLECT_BOUNDS_NORTH', 1.4784001))]  # the range of latitude 纬度的范围
lonDivision = float(os.getenv('LNG_DIVISION', 0.005))  # 分块查询，每格约0.4km
latDivision = float(os.getenv('LAT_DIVISION', 0.005))  # 分块查询，每格约0.4km
radius = int(os.getenv('RADIUS', 500))  # 查询参数 半径 500m

dirpath = os.getenv('DATA_DIR', 'data')
if not os.path.exists(dirpath):
    os.makedirs(dirpath)


def xjp_cj(keyword=None, search_type=None):
    global count

    if searchMode == "keyword":
        key = keyword
    elif searchMode == "type":
        key = search_type

    count[key] = 0
    countLine[key] = 0

    outfile[key] = dirpath + "/" + key + ".csv"

    if os.path.isfile(outfile[key]):
        df[key] = pd.read_csv(outfile[key], index_col="place_id")
    else:
        df[key] = pd.DataFrame(
            columns=['place_id', '商户名称', '商户评分', '评价数', '商户品类', '商户地址', '营业时间', '运营状态',
                     '官网地址',
                     '电话', '坐标', '最新评论时间'])
        df[key].set_index('place_id', inplace=True)
        df[key].to_csv(outfile[key], index=True)

    lonArr = numpy.arange(lonRange[0], lonRange[1], lonDivision)
    latArr = numpy.arange(latRange[0], latRange[1], latDivision)
    tqdmNum = len(lonArr) * len(latArr)
    print(key + ' 开始爬取')
    print(key + ' 共有' + str(tqdmNum) + '次请求')

    pbar[key] = tqdm(total=tqdmNum)

    for lon in lonArr:
        # print('已进行' + str(count) + '次请求，得到' + str(countLine) + '条有效信息')
        for lat in latArr:
            # print('已进行' + str(count) + '次请求，得到' + str(countLine) + '条有效信息')
            #   发请求
            latlon = str(lat) + ',' + str(lon)
            get_search(latlon, radius, key=key)
            count[key] = count[key] + 1
    multitasking.wait_for_tasks()

    print(key + ' 已进行' + str(count[key]) + '次请求，得到' + str(countLine[key]) + '条有效信息，总计' + str(
        len(df[key])) + "条信息")

    print(key + ' 结束')


def json_request(url: string):
    url = quote(url, safe=string.printable)
    req = urllib.request.urlopen(url, timeout=TIMEOUT)
    response = req.read().decode('utf-8')
    return json.loads(response)


def get_data(key, place_id=None, item=None):
    global countLine
    if not place_id:
        place_id = item['place_id']
        types = item['types']

    if place_id in df[key].index.values:
        return

    basic_url = 'https://maps.googleapis.com/maps/api/place/details/json?key={0}&language=zh-CN&place_id={1}&reviews_sort=newest'
    url = basic_url.format(googleKey, place_id)
    responseJSON = json_request(url)
    if responseJSON['status'] != "OK":
        print(url)
        print(place_id + " 详情返回数据状态错误 状态：" + responseJSON['status'])
        return
    # print(responseJSON)
    result = responseJSON['result']
    data = {
        "place_id": place_id,
        "商户名称": result.get('name'),
        "商户评分": result.get('rating'),
        "评价数": result.get('user_ratings_total'),
        "商户品类": ','.join(result.get('types')),
        "商户地址": result.get('formatted_address'),
        "营业时间": ','.join(result['opening_hours']['weekday_text']) if "opening_hours" in result else "",
        "运营状态": result.get('business_status'),
        "官网地址": result.get('website'),
        "电话": result.get('international_phone_number'),
        "坐标": str(result['geometry']['location']) if "geometry" in result else "",
        "最新评论时间": datetime.datetime.fromtimestamp(result['reviews'][0]["time"]) if "reviews" in result else ""

    }
    # 多线程，再判断一次place_id是否存在
    if place_id in df[key].index.values:
        return
    try:
        df[key].loc[place_id] = data
        data_df = pd.DataFrame([data])
        data_df.to_csv(outfile[key], mode='a', index=False, header=False)
        countLine[key] = countLine[key] + 1
    except:
        pass


@multitasking.task
def get_search(location, radius, key, next_page_token=None):
    if searchMode == "keyword":
        basic_url = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?key={0}&location={1}&radius={2}&keyword={3}&language=zh-CN'
    elif searchMode == "type":
        basic_url = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?key={0}&location={1}&radius={2}&type={3}&language=zh-CN'

    url = basic_url.format(googleKey, location, radius, key)

    if next_page_token:
        url = url + "&pagetoken=" + next_page_token
        print("产生翻页")
        print(url)

    # print(url)
    responseJSON = json_request(url)
    if responseJSON['status'] != "OK":
        if not next_page_token:
            pbar[key].update()
        return
    for item in responseJSON['results']:
        if 'plus_code' in item \
                and (
                "马来西亚" in item['plus_code']['compound_code'] or "印度尼西亚" in item['plus_code']['compound_code']) \
                and "新加坡" not in item['plus_code']['compound_code'] \
                and "Singapore" not in item['plus_code']['compound_code']:
            continue
        get_data(key=key, item=item)

    if "next_page_token" in responseJSON:
        get_search(location, radius, key, next_page_token)
    else:
        pbar[key].update()


if __name__ == "__main__":
    if searchMode == "keyword":
        for keyword in keywordArr:
            if keyword.strip():
                xjp_cj(keyword=keyword.strip())
    elif searchMode == "type":
        for search_type in typeArr:
            if search_type.strip():
                xjp_cj(search_type=search_type.strip())
