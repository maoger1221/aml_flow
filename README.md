相关文档：

[官方文档](http://airflow.apache.org/installation.html)

[github地址](https://github.com/apache/airflow)

Airflow管理和调度各种离线定时 Job ，可以替代 crontab。

# 1. 安装及初始化
```bash
# airflow needs a home, ~/airflow is the default,
# but you can lay foundation somewhere else if you prefer
# (optional)
export AIRFLOW_HOME=~/airflow

# install from pypi using pip
pip install apache-airflow

# initialize the database
airflow initdb

# start the web server, default port is 8080
airflow webserver -p 8080
# nohup airflow webserver -p 8080 > ~/airflow/active.log 2>&1 &

# start the scheduler
airflow scheduler

# 删除dag(需要先去~/airflow/dags中删除py文件)
airflow delete_dag -y {dag_id}
```

安装路径: /root/anaconda3/envs/he_test/lib/python3.7/site-packages/
```bash
# 守护进程运行webserver
airflow webserver -D -p 8080
# 守护进程运行调度器
airflow scheduler -D
```
当在~/airflow/dags文件下添加py文件后，需要等待一会，才会在web中显示
重新运行airflow scheduler可以立刻在web中显示py文件

# 2. XCOM
可以使用xcom在不同的operator间传递变量(好像也可以在不同的dag之间传递参数，需要试一下)
xcom_pull(self, task_ids, dag_id, key, include_prior_dates)

```python
def processing_data(**kwargs):
    kwargs['ti'].xcom_push(key='X', value=X)
    kwargs['ti'].xcom_push(key='str_with_trx_with_retail_with_corporate_with_account', value=str_with_trx_with_retail_with_corporate_with_account)


processing_data_operator = PythonOperator(
    task_id='processing_data_operator',
    provide_context=True,
    python_callable=processing_data,
    dag=dag,
)


def predict(**kwargs):
    ti = kwargs['ti']
    X = ti.xcom_pull(key='X', task_ids='processing_data_operator')
    
predict_operator = PythonOperator(
    task_id='predict_operator',
    provide_context=True,
    python_callable=predict,
    dag=dag,
)
```

# 3. 几个概念
https://www.cnblogs.com/piperck/p/10101423.html

* DAG
* DAG 意为有向无循环图，在 Airflow 中则定义了整个完整的作业。同一个 DAG 中的所有 Task 拥有相同的调度时间。
* Task
* Task 为 DAG 中具体的作业任务，它必须存在于某一个 DAG 之中。Task 在 DAG 中配置依赖关系，跨 DAG 的依赖是可行的，但是并不推荐。跨 DAG 依赖会导致 DAG 图的直观性降低，并给依赖管理带来麻烦。
* DAG Run
* 当一个 DAG 满足它的调度时间，或者被外部触发时，就会产生一个 DAG Run。可以理解为由 DAG 实例化的实例。
* Task Instance
* 当一个 Task 被调度启动时，就会产生一个 Task Instance。可以理解为由 Task 实例化的实例。

# 4. 常用命令
```bash
airflow test　dag_id task_id execution_date 　　测试task
示例:　airflow test example_hello_world_dag hello_task 20180516

airflow run dag_id task_id execution_date 运行task

airflow run -A dag_id task_id execution_date 忽略依赖task运行task

airflow trigger_dag dag_id -r RUN_ID -e EXEC_DATE  运行整个dag文件

airflow webserver -D　 守护进程运行webserver

airflow scheduler -D　 守护进程运行调度

airflow worker -D 守护进程运行celery worker

airflow worker -c 1 -D 守护进程运行celery worker并指定任务并发数为1

airflow pause dag_id　 暂停任务

airflow unpause dag_id 取消暂停，等同于在管理界面打开off按钮

airflow list_tasks dag_id 查看task列表

airflow clear dag_id 清空任务实例
```
# 5. 运行一个dag的流程
* 在~/airflow/dags文件下添加py文件，(需要等待一会，才会在web中显示，如果未开启webserver，也是可以运行的)
* airflow unpause dag_id（取消暂停任务，任务会按照设定时间周期执行）
* airflow trigger_dag dag_id（立刻运行整个dag）
# 6. 重启一个dag的流程
```bash
rm -rf ~/airflow/dags/aml_sl_with_config.py
airflow delete_dag -y aml_sl_with_config
ps -ef |grep "airflow scheduler" |awk '{print $2}'|xargs kill -9
vi ~/airflow/dags/aml_sl_with_config.py
nohup airflow scheduler &
```
# 7. json配置文件格式
```
{
  "hdfs_url": "http://172.27.128.237:50070",
  "hdfs_user": "hdfs",
  "daily_dir_list": [
    "trx",
    "str"
  ],
  "static_dir_list": [
    "retail",
    "corporate",
    "account"
  ],
  "base_local_path": "/root/airflow/aml_data/sl_data/{}/",
  "base_local_metrics_path": "/root/airflow/aml_data/sl_data/{}/for_metrics/",
  "base_local_model_path": "/root/airflow/aml_data/model/{}",
  "base_local_predict_res_path": "/root/airflow/aml_data/bp_data/res/{}",
  "model_prefix": "he_test_xgboost",
  "predict_res_prefix": "pred_full_table",
  "base_remote_daily_path": "/anti-money/daily_data_group/{}/daily/{}",
  "base_remote_static_path": "/anti-money/daily_data_group/{}/all",
  "base_remote_model_path": "/anti-money/he_test/model/{}",
  "base_remote_predict_res_path": "/anti-money/he_test/predict_res/{}",
  "specified_model_path":"",
  "start_time": "2018-05-01",
  "end_time": "2018-05-27",
  "metrics_start_time": "2018-05-28",
  "metrics_end_time": "2018-05-30"
}
```
# 8. 配置参数方式
* Menu -> Admin -> Variables
# 9. 一个可配置参数的自学习demo
```python
# -*- coding: utf-8 -*-

from __future__ import print_function

import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

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
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=2),
    'email': ['maotao@4paradigm.com', 'maopengyu@4paradigm.com'],
    'email_on_failure': True,
    'email_on_retry': True,
}

dag = DAG(
    dag_id='aml_sl_with_config',
    catchup=False,
    default_args=args,
    schedule_interval='0 * * * *',
    dagrun_timeout=datetime.timedelta(minutes=60),
)


def clear_local_path(local_path):
    if os.path.exists(local_path):
        if os.path.isfile(local_path):
            os.remove(local_path)
        else:
            # 删除目录及其下子文件
            shutil.rmtree(local_path)
            os.mkdir(local_path)
    else:
        os.mkdir(local_path)


def get_config_from_variables(**kwargs):
    # 获取配置文件中的参数
    sl_config = Variable.get('sl_config', deserialize_json=True, default_var={"hdfs_url":"http://172.27.128.237:50070","hdfs_user":"hdfs","daily_dir_list":["trx","str"],"static_dir_list":["retail","corporate","account"],"base_local_path":"/root/airflow/aml_data/sl_data/{}/","base_local_metrics_path":"/root/airflow/aml_data/sl_data/{}/for_metrics/","base_local_model_path":"/root/airflow/aml_data/model/{}","model_prefix":"he_test_xgboost","base_remote_daily_path":"/anti-money/daily_data_group/{}/daily/{}","base_remote_static_path":"/anti-money/daily_data_group/{}/all","base_remote_model_path":"/anti-money/he_test/model/{}","start_time":"2018-05-01","end_time":"2018-05-27","metrics_start_time":"2018-05-28","metrics_end_time":"2018-05-30"})
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
    model_prefix = sl_config['model_prefix']
    base_remote_model_path = sl_config['base_remote_model_path']
    metrics_start_time = sl_config['metrics_start_time']
    metrics_end_time = sl_config['metrics_end_time']
    base_local_metrics_path = sl_config['base_local_metrics_path']

    kwargs['ti'].xcom_push(key='hdfs_url', value=hdfs_url)
    kwargs['ti'].xcom_push(key='hdfs_user', value=hdfs_user)
    kwargs['ti'].xcom_push(key='static_dir_list', value=static_dir_list)
    kwargs['ti'].xcom_push(key='daily_dir_list', value=daily_dir_list)
    kwargs['ti'].xcom_push(key='base_local_path', value=base_local_path)

    kwargs['ti'].xcom_push(key='base_remote_daily_path', value=base_remote_daily_path)
    kwargs['ti'].xcom_push(key='base_remote_static_path', value=base_remote_static_path)
    kwargs['ti'].xcom_push(key='start_time', value=start_time)
    kwargs['ti'].xcom_push(key='end_time', value=end_time)
    kwargs['ti'].xcom_push(key='metrics_start_time', value=metrics_start_time)
    kwargs['ti'].xcom_push(key='metrics_end_time', value=metrics_end_time)
    kwargs['ti'].xcom_push(key='base_local_metrics_path', value=base_local_metrics_path)

    kwargs['ti'].xcom_push(key='base_local_model_path', value=base_local_model_path)
    kwargs['ti'].xcom_push(key='model_prefix', value=model_prefix)
    kwargs['ti'].xcom_push(key='base_remote_model_path', value=base_remote_model_path)


get_config_operator = PythonOperator(
    task_id='get_config_operator',
    provide_context=True,
    python_callable=get_config_from_variables,
    dag=dag,
)


def get_data_from_hdfs(**kwargs):
    start_time = kwargs['ti'].xcom_pull(key='start_time', task_ids='get_config_operator')
    end_time = kwargs['ti'].xcom_pull(key='end_time', task_ids='get_config_operator')
    base_local_path = kwargs['ti'].xcom_pull(key='base_local_path', task_ids='get_config_operator')
    download_data_from_hdfs(kwargs, start_time, end_time, base_local_path)


def download_data_from_hdfs(kwargs, start_time, end_time, base_local_path):
    hdfs_url = kwargs['ti'].xcom_pull(key='hdfs_url', task_ids='get_config_operator')
    hdfs_user = kwargs['ti'].xcom_pull(key='hdfs_user', task_ids='get_config_operator')
    daily_dir_list = kwargs['ti'].xcom_pull(key='daily_dir_list', task_ids='get_config_operator')
    static_dir_list = kwargs['ti'].xcom_pull(key='static_dir_list', task_ids='get_config_operator')
    base_remote_daily_path = kwargs['ti'].xcom_pull(key='base_remote_daily_path', task_ids='get_config_operator')
    base_remote_static_path = kwargs['ti'].xcom_pull(key='base_remote_static_path', task_ids='get_config_operator')
    hdfs_client = InsecureClient(url=hdfs_url, user=hdfs_user)
    start_date = datetime.datetime.strptime(start_time, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_time, '%Y-%m-%d')
    date_list = []
    for i in range((end_date - start_date).days + 1):
        range_date = start_date + datetime.timedelta(days=i)
        date_list.append(range_date.strftime('%Y-%m-%d'))

    kwargs['ti'].xcom_push(key='date_list', value=date_list)

    # 下载起始时间范围内的文件
    for sl_dir in daily_dir_list:
        clear_local_path(base_local_path.format(sl_dir))
        for date in date_list:
            try:
                print("downloading {} from {}".format(base_local_path.format(sl_dir), base_remote_daily_path.format(sl_dir, date)))
                hdfs_client.download(base_remote_daily_path.format(sl_dir, date), base_local_path.format(sl_dir))
            except HdfsError:
                # 这个hdfs库无法判断文件是否存在，只能采用这种粗暴的方式
                print(base_remote_daily_path.format(sl_dir, date) + "，hdfs文件不存在")
    for sl_dir in static_dir_list:
        clear_local_path(base_local_path.format(sl_dir))
        print("downloading {} from {}".format(base_local_path.format(sl_dir), base_remote_static_path.format(sl_dir)))
        hdfs_client.download(base_remote_static_path.format(sl_dir), base_local_path.format(sl_dir))

    return date_list, hdfs_client


get_data_operator = PythonOperator(
    task_id='get_data_operator',
    provide_context=True,
    python_callable=get_data_from_hdfs,
    dag=dag,
)


def process_data(**kwargs):
    date_list = kwargs['ti'].xcom_pull(key='date_list', task_ids='get_data_operator')
    base_local_path = kwargs['ti'].xcom_pull(key='base_local_path', task_ids='get_config_operator')
    X, y = process_data_from_local(kwargs, date_list, base_local_path)
    kwargs['ti'].xcom_push(key='X', value=X)
    kwargs['ti'].xcom_push(key='y', value=y)


def process_data_from_local(kwargs, date_list, base_local_path):
    daily_dir_list = kwargs['ti'].xcom_pull(key='daily_dir_list', task_ids='get_config_operator')
    static_dir_list = kwargs['ti'].xcom_pull(key='static_dir_list', task_ids='get_config_operator')
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
    return X, y


process_data_operator = PythonOperator(
    task_id='process_data_operator',
    provide_context=True,
    python_callable=process_data,
    dag=dag,
)


def train_model(**kwargs):
    hdfs_url = kwargs['ti'].xcom_pull(key='hdfs_url', task_ids='get_config_operator')
    hdfs_user = kwargs['ti'].xcom_pull(key='hdfs_user', task_ids='get_config_operator')
    X = kwargs['ti'].xcom_pull(key='X', task_ids='process_data_operator')
    y = kwargs['ti'].xcom_pull(key='y', task_ids='process_data_operator')
    base_local_model_path = kwargs['ti'].xcom_pull(key='base_local_model_path', task_ids='get_config_operator')
    model_prefix = kwargs['ti'].xcom_pull(key='model_prefix', task_ids='get_config_operator')
    base_remote_model_path = kwargs['ti'].xcom_pull(key='base_remote_model_path', task_ids='get_config_operator')

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
    model_id = '{}_{}'.format(model_prefix, datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S'))
    kwargs['ti'].xcom_push(key='model_id', value=model_id)
    hdfs_client = InsecureClient(url=hdfs_url, user=hdfs_user)
    hdfs_client.upload(base_remote_model_path.format(model_id)+'.model', local_model_path)
    print('uploaded local model {} to {} successfully'.format(local_model_path, base_remote_model_path.format(model_id)+'.model'))


train_model_operator = PythonOperator(
    task_id='train_model_operator',
    provide_context=True,
    python_callable=train_model,
    dag=dag,
)


def get_metrics(**kwargs):
    metrics_start_time = kwargs['ti'].xcom_pull(key='metrics_start_time', task_ids='get_config_operator')
    metrics_end_time = kwargs['ti'].xcom_pull(key='metrics_end_time', task_ids='get_config_operator')
    base_local_metrics_path = kwargs['ti'].xcom_pull(key='base_local_metrics_path', task_ids='get_config_operator')
    # 下载数据
    date_list_for_metrics, hdfs_client = download_data_from_hdfs(kwargs, metrics_start_time, metrics_end_time, base_local_metrics_path)
    # 数据处理
    X, y = process_data_from_local(kwargs, date_list_for_metrics, base_local_metrics_path)

    dtrain = xgb.DMatrix(X)
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

    ti = kwargs['ti']
    model_id = ti.xcom_pull(key='model_id', task_ids='train_model_operator')

    res_dict = {}
    res_dict['model_id'] = model_id
    res_dict['auc'] = auc
    res_dict['acc'] = acc
    res_dict['recall'] = recall
    res_dict['f1_score'] = F1_score
    res_dict['precesion'] = Precesion
    res_json = json.dumps(res_dict)

    base_local_model_path = kwargs['ti'].xcom_pull(key='base_local_model_path', task_ids='get_config_operator')
    model_prefix = kwargs['ti'].xcom_pull(key='model_prefix', task_ids='get_config_operator')
    base_remote_model_path = kwargs['ti'].xcom_pull(key='base_remote_model_path', task_ids='get_config_operator')
    local_model_meta_path = base_local_model_path.format('{}.meta.json'.format(model_prefix))
    clear_local_path(local_model_meta_path)
    with open(local_model_meta_path, "w") as f:
        f.write(res_json)

    hdfs_client.upload(base_remote_model_path.format(model_id)+'.meta.json', local_model_meta_path)
    print('uploaded local meta {} to {} successfully'.format(local_model_meta_path, base_remote_model_path.format(model_id)+'.meta.json'))


get_metrics_operator = PythonOperator(
    task_id='get_metrics_operator',
    provide_context=True,
    python_callable=get_metrics,
    dag=dag,
)


get_data_operator.set_upstream(get_config_operator)
process_data_operator.set_upstream(get_data_operator)
train_model_operator.set_upstream(process_data_operator)
get_metrics_operator.set_upstream(train_model_operator)

```

# 10. sensor
就是根据poke_interval这个时间间隔去轮询。不同的sensor（eg. hdfs_sensor, http_sensor）去实现不同的poke方法

*看来自带的hdfs_sensor不支持python3*
```
*** Reading local file: /root/airflow/logs/aml_sensor/hdfs_sensor/2019-06-03T13:00:00+00:00/1.log
[2019-06-03 22:38:03,089] {__init__.py:1139} INFO - Dependencies all met for <TaskInstance: aml_sensor.hdfs_sensor 2019-06-03T13:00:00+00:00 [queued]>
[2019-06-03 22:38:03,096] {__init__.py:1139} INFO - Dependencies all met for <TaskInstance: aml_sensor.hdfs_sensor 2019-06-03T13:00:00+00:00 [queued]>
[2019-06-03 22:38:03,096] {__init__.py:1353} INFO - 
--------------------------------------------------------------------------------
[2019-06-03 22:38:03,096] {__init__.py:1354} INFO - Starting attempt 1 of 2
[2019-06-03 22:38:03,096] {__init__.py:1355} INFO - 
--------------------------------------------------------------------------------
[2019-06-03 22:38:03,105] {__init__.py:1374} INFO - Executing <Task(HdfsSensor): hdfs_sensor> on 2019-06-03T13:00:00+00:00
[2019-06-03 22:38:03,106] {base_task_runner.py:119} INFO - Running: ['airflow', 'run', 'aml_sensor', 'hdfs_sensor', '2019-06-03T13:00:00+00:00', '--job_id', '484', '--raw', '-sd', 'DAGS_FOLDER/aml_senor.py', '--cfg_path', '/tmp/tmp3rq2m6ky']
[2019-06-03 22:38:03,995] {base_task_runner.py:101} INFO - Job 484: Subtask hdfs_sensor [2019-06-03 22:38:03,994] {__init__.py:51} INFO - Using executor SequentialExecutor
[2019-06-03 22:38:04,243] {base_task_runner.py:101} INFO - Job 484: Subtask hdfs_sensor [2019-06-03 22:38:04,242] {__init__.py:305} INFO - Filling up the DagBag from /root/airflow/dags/aml_senor.py
[2019-06-03 22:38:04,350] {base_task_runner.py:101} INFO - Job 484: Subtask hdfs_sensor [2019-06-03 22:38:04,349] {cli.py:517} INFO - Running <TaskInstance: aml_sensor.hdfs_sensor 2019-06-03T13:00:00+00:00 [running]> on host m7-notebook-gpu01
[2019-06-03 22:38:04,442] {__init__.py:1580} ERROR - This HDFSHook implementation requires snakebite, but snakebite is not compatible with Python 3 (as of August 2015). Please use Python 2 if you require this hook  -- or help by submitting a PR!
Traceback (most recent call last):
  File "/root/anaconda3/envs/he_test/lib/python3.7/site-packages/airflow/models/__init__.py", line 1441, in _run_raw_task
    result = task_copy.execute(context=context)
  File "/root/anaconda3/envs/he_test/lib/python3.7/site-packages/airflow/sensors/base_sensor_operator.py", line 108, in execute
    while not self.poke(context):
  File "/root/anaconda3/envs/he_test/lib/python3.7/site-packages/airflow/sensors/hdfs_sensor.py", line 105, in poke
    sb = self.hook(self.hdfs_conn_id).get_conn()
  File "/root/anaconda3/envs/he_test/lib/python3.7/site-packages/airflow/hooks/hdfs_hook.py", line 49, in __init__
    'This HDFSHook implementation requires snakebite, but '
ImportError: This HDFSHook implementation requires snakebite, but snakebite is not compatible with Python 3 (as of August 2015). Please use Python 2 if you require this hook  -- or help by submitting a PR!
[2019-06-03 22:38:04,496] {__init__.py:1603} INFO - Marking task as UP_FOR_RETRY
[2019-06-03 22:38:04,512] {base_task_runner.py:101} INFO - Job 484: Subtask hdfs_sensor Traceback (most recent call last):
[2019-06-03 22:38:04,512] {base_task_runner.py:101} INFO - Job 484: Subtask hdfs_sensor   File "/root/anaconda3/envs/he_test/bin/airflow", line 32, in <module>
[2019-06-03 22:38:04,512] {base_task_runner.py:101} INFO - Job 484: Subtask hdfs_sensor     args.func(args)
[2019-06-03 22:38:04,512] {base_task_runner.py:101} INFO - Job 484: Subtask hdfs_sensor   File "/root/anaconda3/envs/he_test/lib/python3.7/site-packages/airflow/utils/cli.py", line 74, in wrapper
[2019-06-03 22:38:04,512] {base_task_runner.py:101} INFO - Job 484: Subtask hdfs_sensor     return f(*args, **kwargs)
[2019-06-03 22:38:04,512] {base_task_runner.py:101} INFO - Job 484: Subtask hdfs_sensor   File "/root/anaconda3/envs/he_test/lib/python3.7/site-packages/airflow/bin/cli.py", line 523, in run
[2019-06-03 22:38:04,512] {base_task_runner.py:101} INFO - Job 484: Subtask hdfs_sensor     _run(args, dag, ti)
[2019-06-03 22:38:04,512] {base_task_runner.py:101} INFO - Job 484: Subtask hdfs_sensor   File "/root/anaconda3/envs/he_test/lib/python3.7/site-packages/airflow/bin/cli.py", line 442, in _run
[2019-06-03 22:38:04,513] {base_task_runner.py:101} INFO - Job 484: Subtask hdfs_sensor     pool=args.pool,
[2019-06-03 22:38:04,513] {base_task_runner.py:101} INFO - Job 484: Subtask hdfs_sensor   File "/root/anaconda3/envs/he_test/lib/python3.7/site-packages/airflow/utils/db.py", line 73, in wrapper
[2019-06-03 22:38:04,513] {base_task_runner.py:101} INFO - Job 484: Subtask hdfs_sensor     return func(*args, **kwargs)
[2019-06-03 22:38:04,513] {base_task_runner.py:101} INFO - Job 484: Subtask hdfs_sensor   File "/root/anaconda3/envs/he_test/lib/python3.7/site-packages/airflow/models/__init__.py", line 1441, in _run_raw_task
[2019-06-03 22:38:04,513] {base_task_runner.py:101} INFO - Job 484: Subtask hdfs_sensor     result = task_copy.execute(context=context)
[2019-06-03 22:38:04,513] {base_task_runner.py:101} INFO - Job 484: Subtask hdfs_sensor   File "/root/anaconda3/envs/he_test/lib/python3.7/site-packages/airflow/sensors/base_sensor_operator.py", line 108, in execute
[2019-06-03 22:38:04,513] {base_task_runner.py:101} INFO - Job 484: Subtask hdfs_sensor     while not self.poke(context):
[2019-06-03 22:38:04,513] {base_task_runner.py:101} INFO - Job 484: Subtask hdfs_sensor   File "/root/anaconda3/envs/he_test/lib/python3.7/site-packages/airflow/sensors/hdfs_sensor.py", line 105, in poke
[2019-06-03 22:38:04,513] {base_task_runner.py:101} INFO - Job 484: Subtask hdfs_sensor     sb = self.hook(self.hdfs_conn_id).get_conn()
[2019-06-03 22:38:04,513] {base_task_runner.py:101} INFO - Job 484: Subtask hdfs_sensor   File "/root/anaconda3/envs/he_test/lib/python3.7/site-packages/airflow/hooks/hdfs_hook.py", line 49, in __init__
[2019-06-03 22:38:04,513] {base_task_runner.py:101} INFO - Job 484: Subtask hdfs_sensor     'This HDFSHook implementation requires snakebite, but '
[2019-06-03 22:38:04,514] {base_task_runner.py:101} INFO - Job 484: Subtask hdfs_sensor ImportError: This HDFSHook implementation requires snakebite, but snakebite is not compatible with Python 3 (as of August 2015). Please use Python 2 if you require this hook  -- or help by submitting a PR!
[2019-06-03 22:38:08,084] {logging_mixin.py:95} INFO - [2019-06-03 22:38:08,084] {jobs.py:2562} INFO - Task exited with return code 1
```

暂时的解决方案

pip install snakebite-py3

不是官方的，这玩意只有一颗星星，不知道靠不靠谱。

https://pypi.org/project/snakebite-py3/

https://github.com/kirklg/snakebite/tree/feature/python3

* 没有找到hdfs_sensor的demo，只能看源码来猜他怎么用。
```
hdfs_conn_id='AML_HDFS'
```
* 这里传入的字符串，有两种使用方式，一种是从环境变量里取，另一种是从数据库中取
```python
@classmethod
@provide_session
def _get_connections_from_db(cls, conn_id, session=None):
    db = (
        session.query(Connection)
        .filter(Connection.conn_id == conn_id)
        .all()
    )
    session.expunge_all()
    if not db:
        raise AirflowException(
            "The conn_id `{0}` isn't defined".format(conn_id))
    return db

@classmethod
def _get_connection_from_env(cls, conn_id):
    environment_uri = os.environ.get(CONN_ENV_PREFIX + conn_id.upper())
    conn = None
    if environment_uri:
        conn = Connection(conn_id=conn_id, uri=environment_uri)
    return conn

```
* 这里手动在环境变量里塞入了"AIRFLOW_CONN_AML_HDFS"
```bash
export AIRFLOW_CONN_AML_HDFS="m7-solution-cpu01:8020"
```
* Connection构造方法中会取出url，然后解析它的host和port的信息，最后传到hdfs客户端中
* 可以在声明DAG时的default_args中添加env的声明，但需要注意，如果设置了env，airflow就不再访问系统的环境变量，所以这里设置的env一定要包含程序运行所需的所有环境变量，否则会出错
```python
import os
local_env = os.environ
local_env['PATH'] = os.environ['PATH'] + ":" + Variable.get('PATH')
local_env['JAVA_HOME'] = Variable.get('JAVA_HOME')
 
# 在dag的default_args中添加'env':dict(local_env)
```
* 使用自定义环境变量的方式好像并没有成功！
* 使用下面这个方法，在web中配置参数后可以生效。

`Menu -> Admin -> Connections可以在这里配置连接所需参数。`

# 11. 自学习、批量预估设计

# 12. 发现
* 上游所有依赖节点都跑完，下个节点才会运行。
* 定义在operator之外的代码，每个operator运行时，都会重新执行一遍。
