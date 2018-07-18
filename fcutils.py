import fc2


def compute_batch_size(keys, memory, concurrency):
    max_mem_for_data = 0.6 * memory * 1000 * 1000
    size = 1.0
    for key in keys:
        if isinstance(key, dict):
            size += key['Size']
        else:
            size += key.size
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
    def __init__(self, client, service_name, region, codepath, fname, handler, fc_role, funcmem=3072):
        """

        :type funcmem: int
        :type client: fc2.Client
        """
        self.client = client
        self.service_name = service_name
        self.region = 'cn-hangzhou' if region is None else region
        self.codepath = codepath
        self.func_name = fname
        self.handler = handler
        self.role = fc_role
        self.runtime = 'python2.7'
        self.memory = funcmem
        self.timeout = 300

        # self.func_arn = None  # set after creation

    def create_function(self):
        self.client.create_function(self.service_name,
                                    self.func_name,
                                    self.runtime,
                                    self.handler,
                                    codeZipFile=self.codepath,
                                    memorySize=self.memory,
                                    timeout=self.timeout)

    def update_function(self):
        self.client.update_function(self.service_name,
                                    self.func_name,
                                    codeZipFile=self.codepath,
                                    handler=self.handler,
                                    memorySize=self.memory,
                                    runtime=self.runtime,
                                    timeout=self.timeout)

    def update_code_or_create_on_noexist(self):
        response = self.client.list_functions(self.service_name, prefix=self.func_name)
        if len(response.data['functions']) > 0:
            self.update_function()
        else:
            self.create_function()

    def add_permission(self, sId, bucket):
        pass

    def create_oss_eventsource_trigger(self, trigger_name, bucket, user_id, prefix=None):
        self.client.create_trigger(self.service_name,
                                   self.func_name,
                                   trigger_name,
                                   'oss',
                                   # TODO make sure of trigger config
                                   '',
                                   'acs:oss::%s:%s:%s' % (self.region, user_id, bucket),
                                   self.role)

    def delete_function(self):
        self.client.delete_function(self.service_name, self.func_name)

    @classmethod
    def cleanup_logs(cls, func_name):
        pass
