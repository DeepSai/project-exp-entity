import re
import json
import datetime
from tqdm import tqdm
from pprint import pprint
import pandas as pd


def get_func(cv_tag, wid):
    """返回4级，3级，2级职能。
    input:
    cv_tag: {"5AB38B45DDD21":{"add_kws":[],
                            "should":["4209242:11.1017"],
                            "must":["3200269:11.1017"],
                            "category":"2200047:11.1017"},
             "5AB38B45DDE51":{"add_kws":[],
                             "should":[],
                             "must":[],
                             "category":""}}
    wid: "5AB38B45DDD21"

    output: 4209242, 3200269, 2200047
    """
    f = cv_tag.get(wid)
    if f:
        should = f['should']
        must = f['must']
        cate = f['category']
    else:
        return None

    if should and must and cate:
        f4 = should[0].split(':')[0]  # 4级职能
        f3 = must[0].split(':')[0]   # 3级职能
        f2 = cate.split(':')[0]  # 2级职能
        return f2, f3, f4
    else:
        return None


def get_work_dates(dic):
    """返回该cv的所有work起始日期和id。
    input: {'cv_tag':"{}",
            'id': '38834409',
            'work': {},
            'pro':{'5AB38B45DDF07': {},
                   '5AB38B45DDF10': {}}}

    output: [['2013年08月', '2015年04月', '5AB38B45DDD21'],
             ['2012年12月', '2013年08月', '5AB38B45DDD9C'],
             ['2012年05月', '2012年12月', '5AB38B45DDDFA'],
             ['2011年07月', '2011年11月', '5AB38B45DDE51']]
    """
    works_date = []
    for k, v in dic.get('work').items():
        s = v.get('start_time')
        e = v.get('end_time')
        if s and e:
            works_date.append([s, e, k])

    return works_date


def str2date(s):
    """
    返回日期数据。
    input: '2014年08月'
    output: datetime.date
    """
    if re.match('\d+年\d+月+', s):
        time = re.findall('\d+', s)
        y = int(time[0])
        m = int(time[1])
        if m >= 1 and m <= 12:
            date = datetime.date(year=y, month=m, day=1)
            return date
        else:
            return None
    else:
        return None


def get_pro_workid(pro_date, works_date):
    """返回该项目的职能id。
    input:
    pro_date: ['2014年08月', '2015年03月']
    works_date: [['2013年08月', '2015年04月', '5AB38B45DDD21'],
                 ['2012年12月', '2013年08月', '5AB38B45DDD9C']]

    output: '5AB38B45DDD21'
    """
    start_p = str2date(pro_date[0])
    end_p = str2date(pro_date[1])
    if start_p and end_p:
        for w in works_date:
            s, e, wid = w[0], w[1], w[2]
            start_w = str2date(s)
            end_w = str2date(e)
            if start_w and end_w:
                if (start_w <= start_p) and (end_p <= end_w):
                    return wid
            else:
                continue
    else:
        return None


def clean(txt):
    """清洗文本。
    tip: 偶尔传入的txt为None，所以使用if判断。
    """
    if txt:
        txt = re.sub(r'[\t\r\n]', '', txt)
        lst = re.findall(r'[\wa-z+.]+', txt)
        return ' '.join(lst)
    else:
        return ''


def light_clean(txt):
    """清洗文本。
    只去除制表符，换行符，换行符。
    """
    if txt:
        txt = re.sub(r'[\t\r\n]', '', txt)
        return txt
    else:
        return ''


def to_funcid():
    f = open('./join')
    f_out = open('./join_pro2fid.tsv', mode='w', encoding='utf8')
    col_name = ['name_r', 'name', 'des_r', 'des', 'res_r',
                'res', 'f2_id', 'f3_id', 'f4_id']
    f_out.write('\t'.join(col_name)+'\n')

    for line in tqdm(f):
        try:
            dic = json.loads(line.strip())
        except:
            continue

        cv_tag = json.loads(dic.get('cv_tag'))
        work_dates = get_work_dates(dic)

        if work_dates:
            # 对每一个project进行处理，判断其workid，进而判断职能
            for pro in dic.get('pro').values():
                name = clean(pro.get('name', ''))
                des = clean(pro.get('describe', ''))
                res = clean(pro.get('responsibilities', ''))
                if len(des) >= 20 and len(des) <= 500 and len(res) >= 10 and len(res) <= 500:
                    s = pro.get('start_time')
                    e = pro.get('end_time')
                    if s and e:
                        pro_date = [s, e]
                        workid = get_pro_workid(pro_date, work_dates)
                    else:
                        continue

                    if workid:
                        funcs = get_func(cv_tag, workid)
                    else:
                        continue

                    if funcs:
                        name_r = light_clean(pro.get('name', ''))
                        des_r = light_clean(pro.get('describe', ''))
                        res_r = light_clean(pro.get('responsibilities', ''))
                        col = [name_r, name, des_r, des, res_r,
                               res, funcs[0], funcs[1], funcs[2]]
                        txt = '\t'.join(col)+'\n'
                        f_out.write(txt)
                    else:
                        continue

    f.close()
    f_out.close()


def to_funcname():
    df = pd.read_csv('./join_pro2fid.tsv', sep='\t')
    func = pd.read_csv('./function_taxonomy.txt', sep='\t')
    id_name_map = {}
    ids = func['id'].tolist()
    names = func['name'].tolist()
    for x, y in zip(ids, names):
        id_name_map[x] = y

    data = df.sort_values(by=['f2_id', 'f3_id', 'f4_id'], ascending=True)
    for c in ['f2_id', 'f3_id', 'f4_id']:
        f = c.split('_')[0]
        data[f+'_name'] = data[c].map(id_name_map)

    # 过滤没有职能的数据
    data = data[data['f2_name'].notnull() & data['f3_name'].notnull()
                & data['f4_name'].notnull()]

    print('f2 category num:', data['f2_name'].nunique())
    print('f3 category num:', data['f3_name'].nunique())
    print('data num:', len(data))

    col_name = ['name_r', 'name', 'des_r', 'des',
                'res_r', 'res', 'f2_name', 'f3_name', 'f4_name']
    data[col_name].to_csv('./join_pro2fname.tsv', sep='\t')
    data.head()


if __name__ == "__main__":
    to_funcid()
    to_funcname()
