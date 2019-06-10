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
  "hdfs_url": "localhost:50070",
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
![](https://upload-images.jianshu.io/upload_images/13252021-93c7fafe7430c398.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

![](https://upload-images.jianshu.io/upload_images/13252021-c161fcbc065fe77e.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

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
    sl_config = Variable.get('sl_config', deserialize_json=True, default_var={"hdfs_url":"localhost:50070","hdfs_user":"hdfs","daily_dir_list":["trx","str"],"static_dir_list":["retail","corporate","account"],"base_local_path":"/root/airflow/aml_data/sl_data/{}/","base_local_metrics_path":"/root/airflow/aml_data/sl_data/{}/for_metrics/","base_local_model_path":"/root/airflow/aml_data/model/{}","model_prefix":"he_test_xgboost","base_remote_daily_path":"/anti-money/daily_data_group/{}/daily/{}","base_remote_static_path":"/anti-money/daily_data_group/{}/all","base_remote_model_path":"/anti-money/he_test/model/{}","start_time":"2018-05-01","end_time":"2018-05-27","metrics_start_time":"2018-05-28","metrics_end_time":"2018-05-30"})
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
![base_sensor源码](https://upload-images.jianshu.io/upload_images/13252021-edfd84acd3a9c41d.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

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
![](https://upload-images.jianshu.io/upload_images/13252021-d5ea87d9d4338e18.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

`Menu -> Admin -> Connections可以在这里配置连接所需参数。`
![](https://upload-images.jianshu.io/upload_images/13252021-91046da3d8e06b7f.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


# 11. 自学习、批量预估设计
![airflow_sl&bp.jpg](https://upload-images.jianshu.io/upload_images/13252021-c85b756756410650.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

# 12. 发现
* 上游所有依赖节点都跑完，下个节点才会运行。
* 定义在operator之外的代码，每个operator运行时，都会重新执行一遍。

# 13. 使用k8s管理
```
docker build -t aml_env .
# 创建新的namespace
kubectl create namespace aml
kubectl create -f ./aml.yaml -n aml
```

```
[root@m7-notebook-gpu01] dockerfile # pwd
/root/dockerfile
[root@m7-notebook-gpu01] dockerfile # ll
total 32
-rw-r--r-- 1 root root 16488 Jun 10 17:59 aml_flow.py
-rw-r--r-- 1 root root   716 Jun 10 22:37 aml.yaml
-rw-r--r-- 1 root root   907 Jun 10 22:41 Dockerfile
-rw-r--r-- 1 root root   178 Jun 10 22:19 start.sh
```
`dockerfile`
```
FROM conda/miniconda3-centos7
MAINTAINER Mao Pengyu <maopengyu@4paradigm.com>
 
ENV CONDA_AML he_test
RUN conda create -n $CONDA_AML python=3.7
RUN echo "source activate $CONDA_AML" > ~/.bashrc
ENV PATH /opt/conda/envs/env/bin:$PATH
# 切换到he_test环境
RUN yum -y install \
    curl \
    build-essential \
    gcc \
    graphviz
RUN source activate he_test && \
    conda install -y \
        jupyter \
        requests==2.19.1 \
        ipykernel \
        matplotlib \
        numpy \
        pandas \
        scipy \
        scikit-learn \
        fastparquet \
        snappy \
        pyarrow &&\
    conda install -y -c conda-forge \
        python-hdfs \
        xgboost \
        lightgbm && \
    export AIRFLOW_HOME=/root/airflow && \
    pip --no-cache-dir install apache-airflow snakebite-py3 && \
    # 在notebook中创建he_test的kernel
    python -m ipykernel install --user --name he_test --display-name "he_test"
ADD ./aml_flow.py /root/airflow/dags/
ADD ./start.sh /root/
 
ENTRYPOINT ["bash", "/root/start.sh"]
```
`start.sh`
```
#!/bin/bash

source activate he_test
airflow initdb
nohup airflow webserver -p 8080 &
nohup airflow scheduler &
# 配置connections
airflow connections --add --conn_id aml_hdfs --conn_type 'hdfs' --conn_host m7-solution-cpu01 --conn_login hdfs --conn_port 8020
# 配置variables
airflow variables -s sl_config '{"hdfs_url":"http://172.27.128.237:50070","hdfs_user":"hdfs","daily_dir_list":["trx","str"],"static_dir_list":["retail","corporate","account"],"base_local_path":"/root/airflow/aml_data/sl_data/{}/","base_local_metrics_path":"/root/airflow/aml_data/sl_data/{}/for_metrics/","base_local_model_path":"/root/airflow/aml_data/model/{}","base_local_model_meta_path":"/root/airflow/aml_data/model_meta/{}","base_local_predict_res_path":"/root/airflow/aml_data/bp_data/res/{}","model_prefix":"he_test_xgboost","predict_res_prefix":"pred_full_table","base_remote_daily_path":"/anti-money/daily_data_group/{}/daily/{}","base_remote_static_path":"/anti-money/daily_data_group/{}/all","base_remote_model_path":"/anti-money/he_test/model/{}","base_remote_model_meta_path":"/anti-money/he_test/model_meta/{}","base_remote_predict_res_path":"/anti-money/he_test/predict_res/{}","specified_model_path":"","start_time":"2018-05-01","end_time":"2018-05-27","metrics_start_time":"2018-05-28","metrics_end_time":"2018-05-30"}'
# 取消访问notebook需要token或密码的配置
jupyter notebook --generate-config
echo "c.NotebookApp.token = ''" >> /root/.jupyter/jupyter_notebook_config.py
mkdir /root/notebook
cd /root/notebook
# 脚本中最后一个进程一定要用前台运行方式即在进程最后不加&(&表示后台运行)，否则容器会退出
nohup jupyter notebook --allow-root --ip=0.0.0.0 --port=8888
```
`aml_flow.py`
```python
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
import logging
 
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
sl_config = Variable.get('sl_config', deserialize_json=True, default_var={"hdfs_url":"localhost","hdfs_user":"hdfs","daily_dir_list":["trx","str"],"static_dir_list":["retail","corporate","account"],"base_local_path":"/root/airflow/aml_data/sl_data/{}/","base_local_metrics_path":"/root/airflow/aml_data/sl_data/{}/for_metrics/","base_local_model_path":"/root/airflow/aml_data/model/{}","base_local_model_meta_path":"/root/airflow/aml_data/model_meta/{}","base_local_predict_res_path":"/root/airflow/aml_data/bp_data/res/{}","model_prefix":"he_test_xgboost","predict_res_prefix":"pred_full_table","base_remote_daily_path":"/anti-money/daily_data_group/{}/daily/{}","base_remote_static_path":"/anti-money/daily_data_group/{}/all","base_remote_model_path":"/anti-money/he_test/model/{}","base_remote_model_meta_path":"/anti-money/he_test/model_meta/{}","base_remote_predict_res_path":"/anti-money/he_test/predict_res/{}","specified_model_path":"","start_time":"2018-05-01","end_time":"2018-05-27","metrics_start_time":"2018-05-28","metrics_end_time":"2018-05-30"})
logging.info('config: {}'.format(sl_config))
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
base_local_model_meta_path = sl_config['base_local_model_meta_path']
base_local_predict_res_path = sl_config['base_local_predict_res_path']
model_prefix = sl_config['model_prefix']
base_remote_model_path = sl_config['base_remote_model_path']
metrics_start_time = sl_config['metrics_start_time']
metrics_end_time = sl_config['metrics_end_time']
base_local_metrics_path = sl_config['base_local_metrics_path']
predict_res_prefix = sl_config['predict_res_prefix']
base_remote_predict_res_path = sl_config['base_remote_predict_res_path']
base_remote_model_meta_path = sl_config['base_remote_model_meta_path']
specified_model_path = sl_config['specified_model_path']
today = datetime.date.today().strftime('%Y-%m-%d')
poke_interval = 60
model_id = '{}_{}'.format(model_prefix, today)
uploaded_model_path = base_remote_model_path.format(model_id)+'.model'
local_model_path = base_local_model_path.format('{}.model'.format(model_prefix))
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
                    logging.info("downloading {} from {}".format(base_local_path.format(sl_dir), base_remote_daily_path.format(sl_dir, date)))
                    # 这个client的download，目标本地路径/a/b/，远端路径/c/d/，当/a/b/存在时，下载后/a/b/d/；当/a/b/不存在时，下载后/a/b/
                    hdfs_client.download(base_remote_daily_path.format(sl_dir, date), base_local_path.format(sl_dir))
                except HdfsError as e:
                    # 这个hdfs库无法判断文件是否存在，只能采用这种粗暴的方式
                    logging.info(base_remote_daily_path.format(sl_dir, date) + "下载文件出错:" + e.message)
            else:
                logging.info(base_local_path.format(sl_dir)+'{}/'.format(date) + "已存在，无需下载")
    return date_list
 
 
def download_static_data_from_hdfs(base_local_path):
    for sl_dir in static_dir_list:
        if not os.path.exists(base_local_path.format(sl_dir)):
            # clear_local_path(base_local_path.format(sl_dir))
            if not os.path.exists(base_local_path.format(sl_dir)):
                os.makedirs(base_local_path.format(sl_dir))
            logging.info("downloading {} from {}".format(base_local_path.format(sl_dir), base_remote_static_path.format(sl_dir)))
            hdfs_client.download(base_remote_static_path.format(sl_dir), base_local_path.format(sl_dir))
        else:
            logging.info(base_local_path.format(sl_dir) + "已存在，无需下载")
 
 
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
                logging.info("read daily table from path: {}".format(base_local_path.format(sl_dir) + date))
                daily_dir_dict[sl_dir] = daily_dir_dict[sl_dir].append(pd.read_parquet(base_local_path.format(sl_dir) + date))
    # 读取静态表
    static_dir_dict = {}
    for sl_dir in static_dir_list:
        # 初始化
        logging.info("read static table from path: {}".format(base_local_path.format(sl_dir) + 'all'))
        static_dir_dict[sl_dir] = pd.read_parquet(base_local_path.format(sl_dir) + 'all', engine='auto')
    # 拼表
    # 这里还是写死了，讨厌
    str_with_trx = pd.merge(daily_dir_dict['str'], daily_dir_dict['trx'], how='left', on=['trx_id'])
    logging.info("merged str_with_trx")
    str_with_trx_with_retail = pd.merge(str_with_trx, static_dir_dict['retail'], how='left', on=['cust_id'])
    logging.info("merged str_with_trx_with_retail")
    str_with_trx_with_retail_with_corporate = pd.merge(str_with_trx_with_retail, static_dir_dict['corporate'], how='left', on=['cust_id'])
    logging.info("merged str_with_trx_with_retail_with_corporate")
    str_with_trx_with_retail_with_corporate_with_account = pd.merge(str_with_trx_with_retail_with_corporate, static_dir_dict['account'], how='left', on=['account_id'])
    logging.info("merged str_with_trx_with_retail_with_corporate_with_account")
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
    clear_local_path(base_local_model_path.format(''))
    bst.save_model(local_model_path)
    # 第二个参数为False，当文件不存在时return none，文件存在时返回文件信息
    if not hdfs_client.status(uploaded_model_path, False):
        hdfs_client.upload(uploaded_model_path, local_model_path)
        logging.info('uploaded local model {} to {} successfully'.format(local_model_path, uploaded_model_path))
    else:
        logging.info('{}已存在'.format(uploaded_model_path))
 
 
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
    model = xgb.Booster(model_file=local_model_path)
    y_pred = model.predict(dtrain)
    y_pred_binary = (y_pred >= 0.5) * 1
 
    auc = metrics.roc_auc_score(y, y_pred)
    acc = metrics.accuracy_score(y, y_pred_binary)
    recall = metrics.recall_score(y, y_pred_binary)
    F1_score = metrics.f1_score(y, y_pred_binary)
    Precesion = metrics.precision_score(y, y_pred_binary)
 
    logging.info('AUC: %.4f' % auc)
    logging.info('ACC: %.4f' % acc)
    logging.info('Recall: %.4f' % recall)
    logging.info('F1-score: %.4f' % F1_score)
    logging.info('Precesion: %.4f' % Precesion)
    metrics.confusion_matrix(y, y_pred_binary)
 
    res_dict = {}
    res_dict['model_id'] = model_id
    res_dict['auc'] = auc
    res_dict['acc'] = acc
    res_dict['recall'] = recall
    res_dict['f1_score'] = F1_score
    res_dict['precesion'] = Precesion
    res_json = json.dumps(res_dict)
 
    local_model_meta_path = base_local_model_meta_path.format('{}.meta.json'.format(model_prefix))
    clear_local_path(base_local_model_meta_path.format(''))
    with open(local_model_meta_path, "w") as f:
        f.write(res_json)
 
    # 第二个参数为False，当文件不存在时return none，文件存在时返回文件信息
    if not hdfs_client.status(base_remote_model_meta_path.format(model_id) + '.meta.json', False):
        hdfs_client.upload(base_remote_model_meta_path.format(model_id) + '.meta.json', local_model_meta_path)
        logging.info('uploaded local meta {} to {} successfully'.format(local_model_meta_path, base_remote_model_meta_path.format( model_id) + '.meta.json'))
    else:
        logging.info('{}已存在'.format(base_remote_model_meta_path.format(model_id) + '.meta.json'))
 
 
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
    str_with_trx_with_retail_with_corporate_with_account = kwargs['ti'].xcom_pull(key='str_with_trx_with_retail_with_corporate_with_account', task_ids='bp_process_data_operator')
 
    dtrain = xgb.DMatrix(X)
    model = xgb.Booster(model_file=local_model_path)
    y_pred = model.predict(dtrain)
    pred_full_table = str_with_trx_with_retail_with_corporate_with_account.copy()
    pred_full_table['pred_score'] = y_pred
 
    predict_res_name = predict_res_prefix + today + '.csv'
 
    clear_local_path(base_local_predict_res_path.format(''))
    pred_full_table.to_csv(base_local_predict_res_path.format(predict_res_name))
 
    # 第二个参数为False，当文件不存在时return none，文件存在时返回文件信息
    if not hdfs_client.status(base_remote_predict_res_path.format(predict_res_name), False):
        hdfs_client.upload(base_remote_predict_res_path.format(predict_res_name), base_local_predict_res_path.format(predict_res_name))
        logging.info('uploaded predict res {} to {} successfully'.format(base_local_predict_res_path.format(predict_res_name), base_remote_predict_res_path.format(predict_res_name)))
    else:
        logging.info('{}已存在'.format(base_remote_predict_res_path.format(predict_res_name)))
 
 
predict_operator = PythonOperator(
    task_id='predict_operator',
    provide_context=True,
    python_callable=predict,
    dag=dag,
)
 
get_data_operator >> process_data_operator >> train_model_operator >> get_metrics_operator
get_data_operator >> bp_process_data_operator >> model_sensor >> predict_operator
```
`aml.yaml`
```
apiVersion: v1
kind: Service
metadata:
  name: aml
  labels:
    app: aml
spec:
  type: NodePort
  ports:
  - nodePort: 31234
    port: 8080
    targetPort: 8080
    protocol: TCP
    name: airflow-port
  - nodePort: 31235
    port: 8888
    targetPort: 8888
    protocol: TCP
    name: notebook-port
  selector:
    app: aml
---
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  name: aml
spec:
  replicas: 1
  template:
    metadata:
      labels:
        app: aml
    spec:
      containers:
      - name: aml
        image: aml_env:0.0.6
        resources:
          requests:
            memory: "0"
            cpu: "0"
          limits:
            memory: "128Gi"
            cpu: "20"
        ports:
        - containerPort: 80
```