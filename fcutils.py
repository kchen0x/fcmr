import os


def compute_batch_size(keys, memory, concurrency):
    max_mem_for_data = 0.6 * memory * 1000 * 1000
    size = 1.0
    for key in keys:
        if isinstance(key, dict):
            size += key['Size']
        else:
            key += key.size
    avg_object_size = size / len(keys)
    print "Dataset size: %s, nKeys: %s, avg: %s" % (size, len(keys), avg_object_size)
    if avg_object_size < max_mem_for_data and len(keys) < concurrency:
        bsize = 1
    else:
        bsize = int(round(max_mem_for_data / avg_object_size))
    return bsize


def batch_creator(keys, batch_size):
    batches = []
    batch = []
    for i in range(len(keys)):
        batch.append(keys[i])
        if len(batch) >= batch_size:
            batches.append(batch)
            batch = []
    if len(batch):
        batches.append(batch)
    return batches


class FuncManager(object):
    def __init__(self, func, oss, region, codepath, job_id, fname, handler, funcmem=3072):
        self.func = func
        self.region = 'cn-hangzhou' if region is None else region
        self.oss = oss
        self.codepath = codepath
        self.job_id = job_id
        self.func_name = fname
        self.handler = handler
        self.role = os.environ.get('MRTEST_ROLE')
        self.memory = funcmem
        self.timeout = 300
        self.func_arn = None  # set after creation

    def create_function(self):
        pass

    def update_function(self):
        pass

    def update_code_or_create_on_noexist(self):
        pass

    def add_permission(self, sId, bucket):
        pass

    def create_oss_eventsource_trigger(self, bucket, prefix=None):
        pass

    def delete_function(self):
        pass

    @classmethod
    def cleanup_logs(cls, func_name):
        pass
