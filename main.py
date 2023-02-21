import hcl
import pandas as pd
from datetime import datetime, timedelta, timezone

pd.set_option('display.max_columns', None, 'display.max_rows', None)

def log_this(text):
    dt_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open('log.txt', 'a') as f:
        f.write(f'[{dt_now}] {text}\n')

def get_robots():
    iterations = 0
    while True:
        if iterations == 50:
            raise Exception('Custom Timeout due to too many request attempts.')
        rst = hcl.api_get("robots?fields[robots]=ALL")
        if rst.status_code == 200:
            log_this('robot list successfully extracted from API.')
            df_rst = pd.json_normalize(rst.json()['data'])
            return df_rst
        else:
            log_this(f'/!\ robot list extraction failed. ERROR_CODE: {rst.status_code}.')
            log_this(rst)
        iterations = iterations + 1
        
def get_tasks(robot_id):
    url = f"robots/{robot_id}/robot_tasks?fields[robot_tasks]=ALL"
    iterations = 0
    while True:
        if iterations == 50:
            raise Exception('Custom Timeout due to too many request attempts.')
        rst = hcl.api_get(url)
        if rst.status_code == 200:
            log_this('taks list successfully extracted from API.')
            df_rst = pd.json_normalize(rst.json()['data'])
            return df_rst
        else:
            log_this(f'/!\  tasks list extraction failed. ERROR_CODE: {rst.status_code}.')
            log_this(rst)
        iterations = iterations + 1

def get_jobs_prod(robot_id):
    url = f"robots/{robot_id}/jobs?fields[jobs]=ALL&env=production"
    iterations = 0
    while True:
        if iterations == 50:
            raise Exception('Custom Timeout due to too many request attempts.')
        rst = hcl.api_get(url)
        if rst.status_code == 200:
            log_this('jobs list successfully extracted from API.')
            df_rst = pd.json_normalize(rst.json()['data'])
            return df_rst
        else:
            log_this(f'/!\  jobs list extraction failed. ERROR_CODE: {rst.status_code}.')
            log_this(rst)
        iterations = iterations + 1
        
def get_jobs_dev(robot_id):
    url = f"robots/{robot_id}/jobs?fields[jobs]=ALL&env=development"
    iterations = 0
    while True:
        if iterations == 50:
            raise Exception('Custom Timeout due to too many request attempts.')
        rst = hcl.api_get(url)
        if rst.status_code == 200:
            log_this('jobs list successfully extracted from API.')
            df_rst = pd.json_normalize(rst.json()['data'])
            return df_rst
        else:
            log_this(f'/!\  jobs list extraction failed. ERROR_CODE: {rst.status_code}.')
            log_this(rst)
        iterations = iterations + 1
        

src_df_robot = get_robots()

#src_df_robot_tasks = pd.DataFrame()
#for robot_id in src_df_robot['id']:
#    df_tmp = get_tasks(robot_id)
#    if len(df_tmp):
#        df_tmp['robot_id'] = robot_id
#        src_df_robot_tasks = src_df_robot_tasks.append(df_tmp, ignore_index = True)
        
src_df_robot_jobs = pd.DataFrame()
for robot_id in src_df_robot['id']:
    df_tmp = get_jobs_prod(robot_id)
    df_tmp2 = get_jobs_dev(robot_id)
    if len(df_tmp):
        df_tmp['robot_id'] = robot_id
        df_tmp2['robot_id'] = robot_id
        src_df_robot_jobs = src_df_robot_jobs.append(df_tmp, ignore_index = True)
        src_df_robot_jobs = src_df_robot_jobs.append(df_tmp2, ignore_index = True)

# check last success date per task
# delete every failed jobs ran before it
#
def purge_task_fail_succ(DF_jobs):
    for task_id in DF_jobs['relationships.task.data.id'].unique():
        DF_jobs_robot_task =  DF_jobs[DF_jobs['relationships.task.data.id']==task_id]
        DF_jobs_robot_task.sort_values(['attributes.status','attributes.completed_at'],ascending=False,inplace=True,ignore_index=True)
        if DF_jobs_robot_task['attributes.status'][0] =='success':
            last_success_date = datetime.strptime(DF_jobs_robot_task['attributes.completed_at'][0][0:10] +' '+ DF_jobs_robot_task['attributes.completed_at'][0][12:19] , '%Y-%m-%d %H:%M:%S')
            for i in range(1,DF_jobs_robot_task.shape[0]):
                date_job = datetime.strptime(DF_jobs_robot_task['attributes.completed_at'][i][0:10] +' '+DF_jobs_robot_task['attributes.completed_at'][i][12:19],  '%Y-%m-%d %H:%M:%S')
                if last_success_date>date_job and DF_jobs_robot_task['attributes.status'][i] !='success':
                    purge_robot_task_job(DF_jobs_robot_task['robot_id'][i],DF_jobs_robot_task['id'][i])

#purge month-old failed tasks
def purge_month_old_failed_jobs(jobs):
    today = datetime.strptime(datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d')
    for i in range(jobs.shape[0]):
        date_job = datetime.strptime(src_df_robot_jobs['attributes.completed_at'][i][0:10], "%Y-%m-%d")
        delta = today - date_job
        if delta.days>30 and  jobs['attributes.status'][i] =='failed':
            purge_robot_task_job(jobs['robot_id'][i],jobs['id'][i])
    return None

def purge_robot_task_job(robot_id,job_id):
    url = f'robots/jobs/{job_id}'
    rst = hcl.api_delete(url)
    if rst == 200:
        log_this('job successfully deleted from API.')
    else:
        log_this(f'/!\ job deletion failed. ERROR_CODE: {rst.status_code}.')
        log_this(rst)
    return None
#{robot_id}

##Month Purge
#purge_month_old_failed_jobs(src_df_robot_jobs)

## Purge Failed before Sucess
#purge_task_fail_succ(src_df_robot_jobs)