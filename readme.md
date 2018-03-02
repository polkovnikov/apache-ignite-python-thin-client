# Apache Ignite Python Thin Client 0.1.0

##  What is Apache Ignite Python Thin Client?

The client designed for the new client binary protocol introdiced in Apache Ignite 2.0
https://cwiki.apache.org/confluence/display/IGNITE/IEP-9+Thin+Client+Protocol

## Is the Apache Ignite Python Thin Client fully compatible with client protocol

No. The python client supports only limited set of cache operations, namely:
`cache_put`,`cache_put_all`,`cache_get`,`cache_get_all`,`cache_remove_key`,
`cache_clear_key`,`cache_contains_key`,`cache_contains_keys`,
`cache_remove_all`,`cache_clear`,`cache_create_with_name`,`cache_destroy`

## How to use it?
```python
ignite import ThinClient, ThinClientException

thin_client = ThinClient()
try:
  thin_client.connect()
  thin_client.cache_put(1, 'value 1')
  value = thin_client.get(1)
  print(value)
except ThinClientException as e:
  print(str(e))
  raise e
```

## How to run tests?
Noise test framewrok used for Apache Ignite Python Thin Client
`noise -v tests`