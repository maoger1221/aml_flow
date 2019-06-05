# -*- coding: utf-8 -*-

from __future__ import print_function

import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.sensors import HdfsSensor

from hdfs import *
import datetime
import pandas as pd
from sklearn.preprocessing import LabelEncoder
import xgboost as xgb
import os
import json
import shutil
from sklearn import metrics

args = {
    'owner': 'aml',
    'start_date': airflow.utils.dates.days_ago(0, hour=0),
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=5),
    'email': ['maotao@4paradigm.com', 'maopengyu@4paradigm.com'],
    'email_on_failure': True,
    'email_on_retry': True,
}

dag = DAG(
    dag_id='aml_flow',
    catchup=False,
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=datetime.timedelta(minutes=60),
)
#############################################
# 全局变量
# 获取配置文件中的参数
sl_config = Variable.get('sl_config', deserialize_json=True, default_var={"hdfs_url":"http://172.27.128.237:50070","hdfs_user":"hdfs","daily_dir_list":["trx","str"],"static_dir_list":["retail","corporate","account"],"base_local_path":"/root/airflow/aml_data/sl_data/{}/","base_local_metrics_path":"/root/airflow/aml_data/sl_data/{}/for_metrics/","base_local_model_path":"/root/airflow/aml_data/model/{}","base_local_predict_res_path":"/root/airflow/aml_data/bp_data/res/{}","model_prefix":"he_test_xgboost","predict_res_prefix":"pred_full_table","base_remote_daily_path":"/anti-money/daily_data_group/{}/daily/{}","base_remote_static_path":"/anti-money/daily_data_group/{}/all","base_remote_model_path":"/anti-money/he_test/model/{}","base_remote_predict_res_path":"/anti-money/he_test/predict_res/{}","specified_model_path":"","start_time":"2018-05-01","end_time":"2018-05-27","metrics_start_time":"2018-05-28","metrics_end_time":"2018-05-30"})
print('config: {}'.format(sl_config))
hdfs_url = sl_config['hdfs_url']
hdfs_user = sl_config['hdfs_user']
daily_dir_list = sl_config['daily_dir_list']
static_dir_list = sl_config['static_dir_list']
base_local_path = sl_config['base_local_path']
base_remote_daily_path = sl_config['base_remote_daily_path']
base_remote_static_path = sl_config['base_remote_static_path']
start_time = sl_config['start_time']
end_time = sl_config['end_time']
base_local_model_path = sl_config['base_local_model_path']
base_local_predict_res_path = sl_config['base_local_predict_res_path']
model_prefix = sl_config['model_prefix']
base_remote_model_path = sl_config['base_remote_model_path']
metrics_start_time = sl_config['metrics_start_time']
metrics_end_time = sl_config['metrics_end_time']
base_local_metrics_path = sl_config['base_local_metrics_path']
predict_res_prefix = sl_config['predict_res_prefix']
base_remote_predict_res_path = sl_config['base_remote_predict_res_path']
specified_model_path = sl_config['specified_model_path']
today = datetime.date.today().strftime('%Y-%m-%d')
poke_interval = 60
model_id = '{}_{}'.format(model_prefix, today)
uploaded_model_path = base_remote_model_path.format(model_id)+'.model'
hdfs_client = InsecureClient(url=hdfs_url, user=hdfs_user)

#############################################


def clear_local_path(local_path):
    if os.path.exists(local_path):
        if os.path.isfile(local_path):
            os.remove(local_path)
        else:
            # 删除目录及其下子文件
            shutil.rmtree(local_path)
            os.makedirs(local_path)
    else:
        os.makedirs(local_path)


def get_data_from_hdfs(**kwargs):
    date_list = download_data_from_hdfs(start_time, end_time, base_local_path)
    kwargs['ti'].xcom_push(key='date_list', value=date_list)
    # 下载今日的数据
    download_daily_data_from_hdfs(base_local_path, today, today)


def download_data_from_hdfs(start_time, end_time, base_local_path):
    date_list = download_daily_data_from_hdfs(base_local_path, start_time, end_time)
    download_static_data_from_hdfs(base_local_path)
    return date_list


def download_daily_data_from_hdfs(base_local_path, start_time, end_time):
    start_date = datetime.datetime.strptime(start_time, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_time, '%Y-%m-%d')
    date_list = []
    for i in range((end_date - start_date).days + 1):
        range_date = start_date + datetime.timedelta(days=i)
        date_list.append(range_date.strftime('%Y-%m-%d'))
    # 下载起始时间范围内的文件
    for sl_dir in daily_dir_list:
        # clear_local_path(base_local_path.format(sl_dir))
        for date in date_list:
            # 当本地文件不存在时，才下载
            if not os.path.exists(base_local_path.format(sl_dir)+'{}/'.format(date)):
                if not os.path.exists(base_local_path.format(sl_dir)):
                    os.makedirs(base_local_path.format(sl_dir))
                try:
                    print("downloading {} from {}".format(base_local_path.format(sl_dir), base_remote_daily_path.format(sl_dir, date)))
                    # 这个client的download，目标本地路径/a/b/，远端路径/c/d/，当/a/b/存在时，下载后/a/b/d/；当/a/b/不存在时，下载后/a/b/
                    hdfs_client.download(base_remote_daily_path.format(sl_dir, date), base_local_path.format(sl_dir))
                except HdfsError as e:
                    # 这个hdfs库无法判断文件是否存在，只能采用这种粗暴的方式
                    print(base_remote_daily_path.format(sl_dir, date) + "下载文件出错:" + e.message)
            else:
                print(base_local_path.format(sl_dir)+'{}/'.format(date) + "已存在，无需下载")
    return date_list


def download_static_data_from_hdfs(base_local_path):
    for sl_dir in static_dir_list:
        if not os.path.exists(base_local_path.format(sl_dir)):
            # clear_local_path(base_local_path.format(sl_dir))
            if not os.path.exists(base_local_path.format(sl_dir)):
                os.makedirs(base_local_path.format(sl_dir))
            print("downloading {} from {}".format(base_local_path.format(sl_dir), base_remote_static_path.format(sl_dir)))
            hdfs_client.download(base_remote_static_path.format(sl_dir), base_local_path.format(sl_dir))
        else:
            print(base_local_path.format(sl_dir) + "已存在，无需下载")


get_data_operator = PythonOperator(
    task_id='get_data_operator',
    provide_context=True,
    python_callable=get_data_from_hdfs,
    dag=dag,
)


# sensor这玩意怎么从xcom中取数据呀？现在从全局变量里取
def get_daily_file_path(filename):
    return base_remote_daily_path.format(filename, today)


# 循环生成多个sensor
for daily_dir in daily_dir_list:
    daily_file_sensor = HdfsSensor(
        task_id='daily_{}_file_sensor'.format(daily_dir),
        poke_interval=poke_interval,  # (seconds)
        timeout=60 * 60 * 24,  # timeout in 12 hours
        filepath=get_daily_file_path(daily_dir),
        hdfs_conn_id='aml_hdfs',
        dag=dag
    )
    daily_file_sensor >> get_data_operator


def process_data(**kwargs):
    date_list = kwargs['ti'].xcom_pull(key='date_list', task_ids='get_data_operator')
    X, y, str_with_trx_with_retail_with_corporate_with_account = process_data_from_local(date_list, base_local_path)
    kwargs['ti'].xcom_push(key='X', value=X)
    kwargs['ti'].xcom_push(key='y', value=y)


def process_data_from_local(date_list, base_local_path):
    # 读取每日更新的表
    daily_dir_dict = {}
    for sl_dir in daily_dir_list:
        # 初始化
        daily_dir_dict[sl_dir] = pd.DataFrame()
        for date in date_list:
            if os.path.exists(base_local_path.format(sl_dir) + date):
                print("read daily table from path: {}".format(base_local_path.format(sl_dir) + date))
                daily_dir_dict[sl_dir] = daily_dir_dict[sl_dir].append(pd.read_parquet(base_local_path.format(sl_dir) + date))
    # 读取静态表
    static_dir_dict = {}
    for sl_dir in static_dir_list:
        # 初始化
        print("read static table from path: {}".format(base_local_path.format(sl_dir) + 'all'))
        static_dir_dict[sl_dir] = pd.read_parquet(base_local_path.format(sl_dir) + 'all', engine='auto')
    # 拼表
    # 这里还是写死了，讨厌
    str_with_trx = pd.merge(daily_dir_dict['str'], daily_dir_dict['trx'], how='left', on=['trx_id'])
    print("merged str_with_trx")
    str_with_trx_with_retail = pd.merge(str_with_trx, static_dir_dict['retail'], how='left', on=['cust_id'])
    print("merged str_with_trx_with_retail")
    str_with_trx_with_retail_with_corporate = pd.merge(str_with_trx_with_retail, static_dir_dict['corporate'], how='left', on=['cust_id'])
    print("merged str_with_trx_with_retail_with_corporate")
    str_with_trx_with_retail_with_corporate_with_account = pd.merge(str_with_trx_with_retail_with_corporate, static_dir_dict['account'], how='left', on=['account_id'])
    print("merged str_with_trx_with_retail_with_corporate_with_account")
    # 对类别特征做Label Encoder
    le = LabelEncoder()
    # labeled_data = str_with_trx_with_retail_with_corporate_with_account.copy()
    labeled_data = str_with_trx_with_retail_with_corporate_with_account.astype(str).apply(le.fit_transform)
    X = labeled_data.drop(['label', 'trx_id', 'ins_id', 'cust_id', 'account_id', 'target_cust_id', 'target_account_id'], axis=1)
    y = labeled_data['label']
    return X, y, str_with_trx_with_retail_with_corporate_with_account


process_data_operator = PythonOperator(
    task_id='process_data_operator',
    provide_context=True,
    python_callable=process_data,
    dag=dag,
)


def train_model(**kwargs):
    X = kwargs['ti'].xcom_pull(key='X', task_ids='process_data_operator')
    y = kwargs['ti'].xcom_pull(key='y', task_ids='process_data_operator')

    local_model_path = base_local_model_path.format('{}.model'.format(model_prefix))

    # 模型训练
    dtrain = xgb.DMatrix(X, label=y)
    watchlist = [(dtrain, 'train')]

    params = {'booster': 'gbtree',
              'objective': 'binary:logistic',
              'eval_metric': 'auc',
              'max_depth': 4,
              'lambda': 10,
              'subsample': 0.75,
              'colsample_bytree': 0.75,
              'min_child_weight': 2,
              'eta': 0.025,
              'seed': 0,
              'nthread': 8,
              'silent': 1}
    bst = xgb.train(params, dtrain, num_boost_round=1000, evals=watchlist)
    clear_local_path(local_model_path)
    bst.save_model(local_model_path)
    # 第二个参数为False，当文件不存在时return none，文件存在时返回文件信息
    if hdfs_client.status(uploaded_model_path, False):
        hdfs_client.upload(uploaded_model_path, local_model_path)
        print('uploaded local model {} to {} successfully'.format(local_model_path, uploaded_model_path))
    else:
        print('{}已存在'.format(uploaded_model_path))


train_model_operator = PythonOperator(
    task_id='train_model_operator',
    provide_context=True,
    python_callable=train_model,
    dag=dag,
)


def get_metrics(**kwargs):
    # 下载数据
    date_list_for_metrics = download_data_from_hdfs(metrics_start_time, metrics_end_time, base_local_metrics_path)
    # 数据处理
    X, y, str_with_trx_with_retail_with_corporate_with_account = process_data_from_local(date_list_for_metrics, base_local_metrics_path)

    dtrain = xgb.DMatrix(X)
    clear_local_path('/root/airflow/aml_data/model/he_test_xgboost.model')
    modle = xgb.Booster(model_file='/root/airflow/aml_data/model/he_test_xgboost.model')
    y_pred = modle.predict(dtrain)
    y_pred_binary = (y_pred >= 0.5) * 1

    auc = metrics.roc_auc_score(y, y_pred)
    acc = metrics.accuracy_score(y, y_pred_binary)
    recall = metrics.recall_score(y, y_pred_binary)
    F1_score = metrics.f1_score(y, y_pred_binary)
    Precesion = metrics.precision_score(y, y_pred_binary)

    print('AUC: %.4f' % auc)
    print('ACC: %.4f' % acc)
    print('Recall: %.4f' % recall)
    print('F1-score: %.4f' % F1_score)
    print('Precesion: %.4f' % Precesion)
    metrics.confusion_matrix(y, y_pred_binary)

    res_dict = {}
    res_dict['model_id'] = model_id
    res_dict['auc'] = auc
    res_dict['acc'] = acc
    res_dict['recall'] = recall
    res_dict['f1_score'] = F1_score
    res_dict['precesion'] = Precesion
    res_json = json.dumps(res_dict)

    local_model_meta_path = base_local_model_path.format('{}.meta.json'.format(model_prefix))
    clear_local_path(local_model_meta_path)
    with open(local_model_meta_path, "w") as f:
        f.write(res_json)

    # 第二个参数为False，当文件不存在时return none，文件存在时返回文件信息
    if hdfs_client.status(base_remote_model_path.format(model_id) + '.meta.json', False):
        hdfs_client.upload(base_remote_model_path.format(model_id) + '.meta.json', local_model_meta_path)
        print('uploaded local meta {} to {} successfully'.format(local_model_meta_path, base_remote_model_path.format( model_id) + '.meta.json'))
    else:
        print('{}已存在'.format(base_remote_model_path.format(model_id) + '.meta.json'))


get_metrics_operator = PythonOperator(
    task_id='get_metrics_operator',
    provide_context=True,
    python_callable=get_metrics,
    dag=dag,
)


def bp_process_data(**kwargs):
    bp_X, y, str_with_trx_with_retail_with_corporate_with_account = process_data_from_local([today], base_local_path)
    kwargs['ti'].xcom_push(key='bp_X', value=bp_X)
    kwargs['ti'].xcom_push(key='str_with_trx_with_retail_with_corporate_with_account', value=str_with_trx_with_retail_with_corporate_with_account)


bp_process_data_operator = PythonOperator(
    task_id='bp_process_data_operator',
    provide_context=True,
    python_callable=bp_process_data,
    dag=dag,
)

if specified_model_path.strip():
    model_sensor = HdfsSensor(
        task_id='model_sensor',
        poke_interval=poke_interval,  # (seconds)
        timeout=60 * 60 * 24,  # timeout in 24 hours
        filepath=specified_model_path,
        hdfs_conn_id='aml_hdfs',
        dag=dag
    )
else:
    model_sensor = HdfsSensor(
        task_id='model_sensor',
        poke_interval=poke_interval,  # (seconds)
        timeout=60 * 60 * 24,  # timeout in 24 hours
        filepath=uploaded_model_path,
        hdfs_conn_id='aml_hdfs',
        dag=dag
    )


def predict(**kwargs):
    X = kwargs['ti'].xcom_pull(key='bp_X', task_ids='bp_process_data_operator')
    str_with_trx_with_retail_with_corporate_with_account = kwargs['ti'].xcom_pull(key='str_with_trx_with_retail_with_corporate_with_account', task_ids='process_data_operator')
    local_model_path = base_local_model_path.format('{}.model'.format(model_prefix))

    dtrain = xgb.DMatrix(X)
    model = xgb.Booster(model_file=local_model_path)
    y_pred = model.predict(dtrain)
    pred_full_table = str_with_trx_with_retail_with_corporate_with_account.copy()
    pred_full_table['pred_score'] = y_pred

    predict_res_name = predict_res_prefix + today + '.csv'

    clear_local_path(base_local_predict_res_path.format(predict_res_name))
    pred_full_table.to_csv(base_local_predict_res_path.format(predict_res_name))

    # 第二个参数为False，当文件不存在时return none，文件存在时返回文件信息
    if hdfs_client.status(base_remote_predict_res_path.format(predict_res_name), False):
        hdfs_client.upload(base_remote_predict_res_path.format(predict_res_name), base_local_predict_res_path.format(predict_res_name))
        print('uploaded predict res {} to {} successfully'.format(base_local_predict_res_path.format(predict_res_name), base_remote_predict_res_path.format(predict_res_name)))
    else:
        print('{}已存在'.format(base_remote_predict_res_path.format(predict_res_name)))


predict_operator = PythonOperator(
    task_id='predict_operator',
    provide_context=True,
    python_callable=predict,
    dag=dag,
)


get_data_operator >> process_data_operator >> train_model_operator >> get_metrics_operator
get_data_operator >> bp_process_data_operator >> model_sensor >> predict_operator
