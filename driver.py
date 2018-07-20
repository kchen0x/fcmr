"""
Driver for FCMR to start MapReduce computation.
"""

import json

import fc2
import oss2
import subprocess
import glob
import time
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from aliyunsdkcore import client
from aliyunsdkram.request.v20150501 import CreateRoleRequest
from aliyunsdkram.request.v20150501 import AttachPolicyToRoleRequest
from aliyunsdkram.request.v20150501 import ListRolesRequest

import fcutils

# prepare constant
JOB_INFO = 'jobinfo.json'


## UTILS ##
def write_job_config(job_id, job_bucket, n_mappers, service_name, r_func, r_handler):
    fname = "jobinfo.json"
    with open(fname, 'w') as f:
        req_data = json.dumps({
            "jobId": job_id,
            "jobBucket": job_bucket,
            "mapCount": n_mappers,
            "serviceName": service_name,
            "reducerFunction": r_func,
            "reducerHandler": r_handler
        }, indent=4)
        f.write(req_data)


def zip_func(fname, zipname):
    # faster to zip with shell exec
    subprocess.call(['zip', zipname] + glob.glob(fname) + glob.glob(JOB_INFO) +
                    glob.glob('fcutils.py'))


def write_to_oss(bucket, key, obj_data):
    """

    :type bucket: oss2.Bucket
    """
    bucket.put_object(key, obj_data)


######## MAIN ########

# load config
print('Loading driver config ...')
config = json.loads(open('driverconfig.json', 'r').read())

# get job ID
service_name = config['serviceName']
job_id = config['jobId']

## GET ALL KEYS TO BE PROCESSED
# init
source_bucket_name = config['sourceBucket']
job_bucket_name = config['jobBucket']
user_id = config['userId']
region = config['region']
role_name = config['roleName']
func_memory = config['funcMemory']
concurrent_funcs = config['concurrentFuncs']
access_key_id = config['accessKeyId']
access_key_secret = config['accessKeySecret']

# create oss session
oss_auth = oss2.Auth(access_key_id, access_key_secret)
oss_endpoint = 'http://oss-%s.aliyuncs.com' % region
source_bucket = oss2.Bucket(oss_auth, oss_endpoint, source_bucket_name)
job_bucket = oss2.Bucket(oss_auth, oss_endpoint, job_bucket_name)

job_bucket.create_bucket()

# create role
print('Creating role & policy ...')
acs_client = client.AcsClient(access_key_id, access_key_secret, region)
list_role_request = ListRolesRequest.ListRolesRequest()
role_list = []
for role in json.loads(acs_client.do_action_with_exception(list_role_request))['Roles']['Role']:
    role_list.append(role['RoleName'])
if role_name not in role_list:
    print('Creating Role: fcmr ...')
    role_request = CreateRoleRequest.CreateRoleRequest()
    role_request.set_RoleName(role_name)
    role_request.set_AssumeRolePolicyDocument(open('rolePolicy.json', 'r').read())
    response = acs_client.do_action_with_exception(role_request)

    # attach policy
    print('Attaching Policy to the role ...')
    policy_request = AttachPolicyToRoleRequest.AttachPolicyToRoleRequest()
    policy_request.set_RoleName(role_name)
    policy_request.set_PolicyType('System')
    policy_request.set_PolicyName('AliyunOSSFullAccess')
    response = acs_client.do_action_with_exception(policy_request)
    policy_request.set_PolicyName('AliyunFCFullAccess')
    response = acs_client.do_action_with_exception(policy_request)
    policy_request.set_PolicyName('AliyunLogFullAccess')
    response = acs_client.do_action_with_exception(policy_request)

# create fc client
print('Creating FC client ...')
fc_endpoint = 'http://%s.%s.fc.aliyuncs.com' % (user_id, region)
fc_client = fc2.Client(
    endpoint=fc_endpoint,
    accessKeyID=access_key_id,
    accessKeySecret=access_key_secret
)
fc_role = 'acs:ram::%s:role/%s' % (user_id, role_name)
service_list = []
print('Checking service ...')
for service in fc_client.list_services().data['services']:
    service_list.append(service['serviceName'])
if service_name not in service_list:
    print('Creating service ...')
    # TODO: add logService settings
    fc_client.create_service(service_name, role=fc_role)

# fetch all the keys that match prefix
all_keys = []
for obj in oss2.ObjectIterator(source_bucket, prefix=config['prefix']):
    if obj.size > 0:
        all_keys.append(obj)

bsize = fcutils.compute_batch_size(all_keys, func_memory, concurrent_funcs)
batches = fcutils.batch_creator(all_keys, bsize)
n_mappers = len(batches)

## Create the Functions
mapper_name = 'mapper-' + job_id
reducer_name = 'reducer-' + job_id
coordinator_name = 'coordinator-' + job_id

# write job config
write_job_config(job_id, job_bucket_name, n_mappers, service_name, reducer_name, config['reducer']['handler'])

zip_func(config['mapper']['name'], config['mapper']['zip'])
zip_func(config['reducer']['name'], config['reducer']['zip'])
zip_func(config['coordinator']['name'], config['coordinator']['zip'])

# mapper
mapper = fcutils.FuncManager(fc_client, service_name, region, config['mapper']['zip'],
                             mapper_name, config['mapper']['handler'], fc_role)
mapper.update_code_or_create_on_noexist()

# reducer
reducer = fcutils.FuncManager(fc_client, service_name, region, config['reducer']['zip'],
                              reducer_name, config['reducer']['handler'], fc_role)
reducer.update_code_or_create_on_noexist()

# coordinator
coordinator = fcutils.FuncManager(fc_client, service_name, region, config['coordinator']['zip'],
                                  coordinator_name, config['coordinator']['handler'], fc_role)
coordinator.update_code_or_create_on_noexist()

# add permission
# TODO: make sure if we need this or not?
# coordinator.add_permission(random.randint(1, 1000), job_bucket_name)

# create event source for coordinator
coordinator.create_oss_eventsource_trigger('job_bucket', job_bucket_name, user_id)

# write job data to OSS
j_key = job_id + '/jobdata'
data = json.dumps({
    "mapCount": n_mappers,
    "totalOSSFiles": len(all_keys),
    "startTime": time.time()
})
write_to_oss(job_bucket, j_key, data)

### Execute ###

mapper_outputs = []


## INVOKE MAPPERS
def invoke_function(batches, m_id):
    """

    :param batches:
    :param m_id:
    :return:
    """

    batch = [k.key for k in batches[m_id - 1]]
    resp = mapper.client.invoke_function(mapper.service_name, mapper.func_name, payload=json.dumps({
        "sourceBucket": source_bucket_name,
        "keys": batch,
        "jobBucket": job_bucket_name,
        "jobId": job_id,
        "mapperId": m_id
    }))

    out = resp.data
    mapper_outputs.append(out)
    print('\tmapper {0} output: {1}'.format(m_id, out))


# execute parallel
print('{0} Mappers in total'.format(n_mappers))
pool = ThreadPool(n_mappers)
ids = [i + 1 for i in range(n_mappers)]
invoke_func_partial = partial(invoke_function, batches)

# burst request handling
mappers_executed = 0
while mappers_executed < n_mappers:
    nm = min(concurrent_funcs, n_mappers)
    results = pool.map(invoke_func_partial,
                       ids[mappers_executed:mappers_executed + nm])
    mappers_executed += nm

pool.close()
pool.join()

print('All the mappers finished.')

# delete mapper function
mapper.delete_function()

# calculate costs - approx (since we are using exec time reported by out function
# and not billed ms)
total_func_secs = 0
total_oss_get_ops = 0
total_oss_put_ops = 0
oss_storage_hours = 0
total_lines = 0

for output in mapper_outputs:
    output = json.loads(output)
    total_oss_get_ops += int(output[0])
    total_lines += int(output[1])
    total_func_secs += float(output[2])

# NOTE: Wait for the job to complete so that we can compute total cost;
# create poll every 10 secs

# get all reducer keys
reducer_keys = []

# total execution time for reducers
reducer_func_time = 0

while True:
    job_keys = job_bucket.list_objects(prefix=job_id).object_list
    keys = [jk.key for jk in job_keys]
    total_oss_size = sum([jk.size for jk in job_keys])

    print('Check to see if the job is done ...')

    # check job done
    if job_id + '/result' in keys:
        print('Job done.')
        # TODO do real calculation
        print job_bucket.get_object_meta(job_id + '/result').resp
        for key in keys:
            if 'task/reducer' in key:
                # TODO do real calculation
                reducer_keys.append(key)
        break
    time.sleep(5)

# OSS Storage cost - Account for mappers only;
oss_storage_hours_cost = 1 * 0.0000521 * (total_oss_size/1024.0/1024.0/1024.0)
oss_put_cost = len(job_keys) * 0.005 / 1000

# OSS GET # $0.004/10000
total_oss_get_ops += len(job_keys)
oss_get_cost = total_oss_get_ops * 0.004/10000

# Total FC function costs
total_func_secs += reducer_func_time
func_cost = total_func_secs * 0.00001667 * func_memory / 1024.0
oss_cost = oss_get_cost + oss_put_cost + oss_storage_hours_cost

# Print costs
print "Reducer FC", reducer_func_time * 0.00001667 * func_memory / 1024.0
print "Lambda Cost", func_cost
print "OSS Storage Cost", oss_storage_hours_cost
print "OSS Request Cost", oss_get_cost + oss_put_cost
print "OSS Cost", oss_cost
print "Total Cost: ", func_cost + oss_cost
print "Total Lines:", total_lines

# Delete Reducer function
reducer.delete_function()
coordinator.delete_function()
# Todo: delete service
