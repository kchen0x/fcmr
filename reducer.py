import json
import time

import oss2

# constants
TASK_MAPPER_PREFIX = 'task/mapper/'
TASK_REDUCER_PREFIX = 'task/reducer/'

def handler(event, context):
    start_time = time.time()

    evt = json.loads(event)
    job_bucket_name = evt['jobBucket']
    bucket_name = evt['bucket']
    reducer_keys = evt['keys']
    job_id = evt['jobId']
    r_id = evt['reducerId']
    step_id = evt['stepId']
    n_reducers = evt['nReducers']

    creds = context.credentials
    region = context.region

    oss_auth = oss2.StsAuth(creds.access_key_id, creds.access_key_secret, creds.security_token)
    oss_endpoint = 'http://oss-%s.aliyuncs.com' % region
    job_bucket = oss2.Bucket(oss_auth, oss_endpoint, job_bucket_name)
    bucket = oss2.Bucket(oss_auth, oss_endpoint, bucket_name)

    # aggr
    results ={}
    line_count = 0

    # download and process all keys
    for key in reducer_keys:
        object_stream = job_bucket.get_object(key)
        contents = object_stream.read()

        try:
            for srcIP, val in json.loads(contents).iteritems():
                line_count += 1
                if srcIP not in results:
                    results[srcIP] = 0
                results[srcIP] += float(val)
        except Exception, e:
            print(e)

    time_in_secs = time.time() - start_time

    pret = [len(reducer_keys), line_count, time_in_secs]
    print('Reducer output {0}'.format(pret))

    if n_reducers == 1:
        # last reducer file , final result
        fname = '{0}/result'.format(job_id)
    else:
        fname = '{0}/{1}{2}/{3}'.format(job_id, TASK_REDUCER_PREFIX, step_id, r_id)

    metadata = {
        "lineCount": str(line_count),
        "processingTime": str(time_in_secs)
    }
    write_to_oss(job_bucket, fname, json.dumps(results), metadata)
    return pret


def write_to_oss(bucket, key, obj_data, meta):
    """

    :type bucket: oss2.Bucket
    """
    bucket.put_object(key, obj_data, meta)