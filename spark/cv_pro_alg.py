from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql import SparkSession
import zlib
import binascii
import json
import time
from datetime import datetime


def uncompress(s):
    try:
        txt = zlib.decompress(binascii.unhexlify(s))
    except:
        txt = "{}"
    try:
        json_obj = json.loads(txt, strict=False)
        txt = json.dumps(json_obj, ensure_ascii=False)
        if json_obj == None:
            txt = "{}"
    except:
        txt = "{}"
    return txt


def extract_alg(line):
    """algorithms下为明文数据，不需要解压。"""
    try:
        l = line.strip().split('\t')
        cvid = l[0]
        txt = json.loads(l[1])  # json转dict
        if txt.get('cv_tag'):
            return (cvid, txt.get('cv_tag'))
    except:
        return None


def extract_pro_work(line):
    try:
        l = line.strip().split('\t')
        cvid = l[0]
        txt_ = uncompress(l[1])  # 可读的文本
        txt = json.loads(txt_)  # json转dict
        if txt.get('project') and txt.get('work'):
            return (cvid, (txt.get('project'), txt.get('work')))
    except:
        return None


if __name__ == '__main__':
    sc = SparkContext(appName='cv_salary')
    basic = sc.textFile('/basic_data/icdc/resumes_extras/20190921/icdc_1/*')  \
        .map(extract_pro_work) \
        .filter(lambda x: x is not None)
    basic.saveAsTextFile('/user/atom_guoyanan/cv_basic_part')

    alg = sc.textFile('/basic_data/icdc/algorithms/20190921/icdc_1/*') \
        .map(extract_alg) \
        .filter(lambda x: x is not None) 
    alg.saveAsTextFile('/user/atom_guoyanan/cv_alg_part')

    alg.join(basic) \
        .map(lambda x: {"id": x[0], "cv_tag": x[1][0], "pro": x[1][1][0], "work": x[1][1][1]}) \
        .map(json.dumps) \
        .saveAsTextFile('/user/atom_guoyanan/cv_join_part')
    sc.stop()
