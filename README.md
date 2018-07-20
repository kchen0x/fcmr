# FCMR

## how to run:

Custom your personal information in `driverconfig.json`, expecially:

```json
{
    "userId": "<your_user_id>",
    "accessKeyId": "<your_ak_id>",
    "accessKeySecret": "<your_ak_secret>",
    
    // if you have your own test data
    "sourceBucket": "mrtest-data",
    "prefix": "test-data"
}

```

then, you can just run the `driver.py` to start map-reduce computation.

```
$ python driver.py
```

The example comes from [AMPLab bigdata benchmark](https://amplab.cs.berkeley.edu/benchmark/) test, the 2a aggregation query on tiny dataset. You can use the public OSS bucket we provided for source data or upload them from local as your wish.

## todo

- split driver into driver and resources management
- simplify function handler
    - allow user just implement `map()` and `reduce()` method
- provide more data and examples
- complete requirements.txt