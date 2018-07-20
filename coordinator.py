import json
import time

import fc2
import oss2

import fcutils

DEFAULT_REGION = 'cn-hangzhou'

### STATES
MAPPERS_DONE = 0
REDUCER_STEP = 1


# create oss session
def write_to_oss(bucket, key, obj_data):
    """

    :type bucket: oss2.Bucket
    """
    bucket.put_object(key, obj_data)


def write_reducer_state(n_reducers, n_oss, bucket, fname):
    ts = time.time()
    data = json.dumps({
        "reducerCount": n_reducers,
        "totalOSSFile": n_oss,
        "startTime": ts
    })
    write_to_oss(bucket, fname, data)


def get_mapper_files(files):
    ret = []
    for mf in files:
        if 'task/mapper' in mf.key:
            ret.append(mf)
    return ret


def get_reducer_batch_size(keys):
    # TODO parameterize memory size and concurrency
    batch_size = fcutils.compute_batch_size(keys, 3072, 100)
    return max(batch_size, 2)  # at least 2 in a batch


def check_job_done(files):
    for f in files:
        if 'result' in f.key:
            return True
    return False


def get_reducer_state_info(files, job_id, job_bucket):
    reducers = []
    # max_index = 0
    reducer_step = False
    r_index = 0

    # check if step is complete

    # check for the reducer state
    # determine the latest reducer step
    for f in files:
        if 'reducerstate.' in f.key:
            idx = int(f.key.split('.')[1])
            if idx > r_index:
                r_index = idx
            reducer_step = True

    # find with reducer state is complete
    if reducer_step is False:
        return [MAPPERS_DONE, get_mapper_files(files)]
    else:
        # check the current step is done
        key = '{0}/reducerstate.{1}'.format(job_id, r_index)
        object_stream = job_bucket.get_object(key)
        contents = object_stream.read()

        # get reducer outputs
        for f in files:
            fname = f.key
            parts = fname.split('/')
            if len(parts) < 3:
                continue
            rFname = 'reducer/' + str(r_index)
            if rFname in fname:
                reducers.append(f)

        if int(json.loads(contents)['reducerCount']) == len(reducers):
            return r_index, reducers
        else:
            return r_index, []


def handler(event, context):
    print('Received event: ' + json.dumps(event, indent=2))

    start_time = time.time()
    evt = json.loads(event)
    creds = context.credentials
    region = context.region

    # job bucket, we just got a notification from this bucket
    bucket_name = evt['events'][0]['oss']['bucket']['name']
    oss_auth = oss2.StsAuth(creds.access_key_id, creds.access_key_secret, creds.security_token)
    oss_endpoint = 'http://oss-%s.aliyuncs.com' % region
    job_bucket = oss2.Bucket(oss_auth, oss_endpoint, bucket_name)

    config = json.loads(open('jobinfo.json', 'r').read())

    job_id = config['jobId']
    map_count = config['mapCount']
    service_name = config['serviceName']
    r_function_name = config['reducerFunction']
    r_handler = config['reducerHandler']

    # fc session
    fc_endpoint = 'https://%s.%s.fc.aliyuncs.com' % (context.account_id, region)
    fc_client = fc2.Client(
        endpoint=fc_endpoint,
        accessKeyID=creds.access_key_id,
        accessKeySecret=creds.access_key_secret,
        securityToken=creds.security_token
    )

    # get job files
    # files: <class>SimplifiedObjectInfo
    files = []
    for obj in oss2.ObjectIterator(job_bucket, prefix=job_id):
        if obj.size > 0:
            files.append(obj)

    if check_job_done(files):
        print('Job done!!! Check the result file.')
        return
    else:
        ### stateless coordinator logic
        mapper_keys = get_mapper_files(files)
        print('Mappers done so far {0}'.format(len(mapper_keys)))

        if map_count == len(mapper_keys):
            # all the mapper have finished , time to schedule the reducers
            step_info = get_reducer_state_info(files, job_id, job_bucket)

            print('Step info: {0}'.format(step_info))

            step_number = step_info[0]
            reducer_keys = step_info[1]

            if len(reducer_keys) == 0:
                print('Still waiting for finishing Reducer step{0}'.format(step_number))
                return

            # compute this based on meradata of files
            r_batch_size = get_reducer_batch_size(reducer_keys)

            print('Starting the Reducer step{0}\nBatch Size {1}'.format(step_number, r_batch_size))

            # create batch params for the FC function
            r_batch_params = fcutils.batch_creator(reducer_keys, r_batch_size)

            # build
            n_reducers = len(r_batch_params)
            n_oss = n_reducers * len(r_batch_params[0])
            step_id = step_number + 1

            for i in range(len(r_batch_params)):
                batch = [b.key for b in r_batch_params[i]]

                resp = fc_client.invoke_function(service_name,
                                                 r_function_name,
                                                 payload=json.dumps({
                                                     "bucket": bucket_name,
                                                     "keys": batch,
                                                     "jobBucket": bucket_name,
                                                     "jobId": job_id,
                                                     "nReducers": n_reducers,
                                                     "stepId": step_id,
                                                     "reducerId": i
                                                 }))
                print(resp.data)

            # now write the reducer state
            fname = '{0}/reducerstate.{1}'.format(job_id, step_id)
            write_reducer_state(n_reducers, n_oss, job_bucket, fname)
        else:
            print('Still waiting for all the mappers to finish ...')
