import time
import json
import oss2

# constants
TASK_MAPPER_PREFIX = 'task/mapper/'


def handler(event, context):
    start_time = time.time()
    evt = json.loads(event)

    job_bucket_name = evt['jobBucket']
    src_bucket_name = evt['sourceBucket']
    src_keys = evt['keys']
    job_id = evt['jobId']
    mapper_id = evt['mapperId']
    creds = context.credentials
    region = context.region

    oss_auth = oss2.StsAuth(creds.access_key_id, creds.access_key_secret, creds.security_token)
    oss_endpoint = 'http://oss-%s.aliyuncs.com' % region
    source_bucket = oss2.Bucket(oss_auth, oss_endpoint, src_bucket_name)
    job_bucket = oss2.Bucket(oss_auth, oss_endpoint, job_bucket_name)

    # aggr
    output = {}
    line_count = 0
    err = ''

    # INPUT CSV => OUTPUT JSON

    # Download and process all keys
    for key in src_keys:
        object_stream = source_bucket.get_object(key)
        contents = object_stream.read()

        for line in contents.split('\n')[:-1]:
            line_count += 1
            try:
                data = line.split(',')
                srcIp = data[0][:8]
                if srcIp not in output:
                    output[srcIp] = 0
                output[srcIp] += float(data[3])
            except Exception, e:
                print e
                # err += '%s' % e

    time_in_secs = (time.time() - start_time)
    # timeTaken = time_in_secs * 1000000000 # in 10^9
    # s3DownloadTime = 0
    # totalProcessingTime = 0
    pret = [len(src_keys), line_count, time_in_secs, err]
    metadata = {
        "lineCount": str(line_count),
        "processingTime": str(time_in_secs)
    }
    mapper_fname = "%s/%s%s" % (job_id, TASK_MAPPER_PREFIX, mapper_id)

    write_to_oss(job_bucket, mapper_fname, json.dumps(output), metadata)
    return pret


def write_to_oss(bucket, key, obj_data, meta):
    """

    :type bucket: oss2.Bucket
    """
    bucket.put_object(key, obj_data, meta)
