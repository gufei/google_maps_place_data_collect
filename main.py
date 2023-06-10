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

count = dict()
countLine = dict()
df = dict()
outfile = dict()
pbar = dict()

# 参数，该参数通过 地理编码服务获取 https://maps.googleapis.com/maps/api/geocode/json?language=zh-CN&address=%E6%96%B0%E5%8A%A0%E5%9D%A1&key={googleKey}
lonRange = [os.getenv('COLLECT_BOUNDS_WEST', 103.594),
            os.getenv('COLLECT_BOUNDS_EAST', 104.0945001)]  # the range of longitude 经度的范围
latRange = [os.getenv('COLLECT_BOUNDS_SOUTH', 1.1496),
            os.getenv('COLLECT_BOUNDS_NORTH', 1.4784001)]  # the range of latitude 纬度的范围
lonDivision = os.getenv('LNG_DIVISION', 0.005)  # 分块查询，每格约0.4km
latDivision = os.getenv('LAT_DIVISION', 0.005)  # 分块查询，每格约0.4km
radius = os.getenv('RADIUS', 500)  # 查询参数 半径 500m

dirpath = os.getenv('DATA_DIR', 'data')
if not os.path.exists(dirpath):
    os.makedirs(dirpath)


def xjp_cj(keyword):
    global count
    # keyword = "beauty"
    outfile[keyword] = dirpath + "/" + keyword + ".csv"
    count[keyword] = 0
    countLine[keyword] = 0

    if os.path.isfile(outfile[keyword]):
        df[keyword] = pd.read_csv(outfile[keyword], index_col="place_id")
    else:
        df[keyword] = pd.DataFrame(
            columns=['place_id', '商户名称', '商户评分', '评价数', '商户品类', '商户地址', '营业时间', '运营状态',
                     '官网地址',
                     '电话', '坐标', '最新评论时间'])
        df[keyword].set_index('place_id', inplace=True)
        df[keyword].to_csv(outfile[keyword], index=True)

    lonArr = numpy.arange(lonRange[0], lonRange[1], lonDivision)
    latArr = numpy.arange(latRange[0], latRange[1], latDivision)
    tqdmNum = len(lonArr) * len(latArr)
    print(keyword + ' 开始爬取')
    print(keyword + ' 共有' + str(tqdmNum) + '次请求')

    pbar[keyword] = tqdm(total=tqdmNum)

    for lon in lonArr:
        # print('已进行' + str(count) + '次请求，得到' + str(countLine) + '条有效信息')
        for lat in latArr:
            # print('已进行' + str(count) + '次请求，得到' + str(countLine) + '条有效信息')
            #   发请求
            latlon = str(lat) + ',' + str(lon)
            get_search(latlon, radius, keyword)
            count[keyword] = count[keyword] + 1
    multitasking.wait_for_tasks()

    print(keyword + ' 已进行' + str(count[keyword]) + '次请求，得到' + str(countLine[keyword]) + '条有效信息')

    print(keyword + ' 结束')


def json_request(url: string):
    url = quote(url, safe=string.printable)
    req = urllib.request.urlopen(url, timeout=TIMEOUT)
    response = req.read().decode('utf-8')
    return json.loads(response)


def get_data(keyword, place_id=None, item=None):
    global countLine
    if not place_id:
        place_id = item['place_id']
        types = item['types']

    if place_id in df[keyword].index.values:
        countLine[keyword] = countLine[keyword] + 1
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
    if place_id in df[keyword].index.values:
        countLine[keyword] = countLine[keyword] + 1
        return
    df[keyword].loc[place_id] = data
    data_df = pd.DataFrame([data])
    data_df.to_csv(outfile[keyword], mode='a', index=False, header=False)

    countLine[keyword] = countLine[keyword] + 1


@multitasking.task
def get_search(location, radius, keyword, next_page_token=None):
    basic_url = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?key={0}&location={1}&radius={2}&keyword={3}&language=zh-CN'
    url = basic_url.format(googleKey, location, radius, keyword)

    if next_page_token:
        url = url + "&pagetoken=" + next_page_token
        print("产生翻页")
        print(url)

    # print(url)
    responseJSON = json_request(url)
    if responseJSON['status'] != "OK":
        if not next_page_token:
            pbar[keyword].update()
        return
    for item in responseJSON['results']:
        if 'plus_code' in item \
                and (
                "马来西亚" in item['plus_code']['compound_code'] or "印度尼西亚" in item['plus_code']['compound_code']) \
                and "新加坡" not in item['plus_code']['compound_code'] \
                and "Singapore" not in item['plus_code']['compound_code']:
            continue
        get_data(keyword=keyword, item=item)

    if "next_page_token" in responseJSON:
        get_search(location, radius, keyword, next_page_token)
    else:
        pbar[keyword].update()


if __name__ == "__main__":
    for keyword in keywordArr:
        if keyword.strip():
            xjp_cj(keyword.strip())
