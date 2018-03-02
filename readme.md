# Apache Ignite Python Thin Client 0.1.0

##  What is Apache Ignite Python Thin Client?

The client designed for the new client binary protocol introdiced in Apache Ignite 2.4
https://cwiki.apache.org/confluence/display/IGNITE/IEP-9+Thin+Client+Protocol

## Is Apache Ignite Python Thin Client fully compatible with client protocol?

No. The python client supports only limited set of cache operations, namely:
`cache_put`,`cache_put_all`,`cache_get`,`cache_get_all`,`cache_remove_key`,
`cache_clear_key`,`cache_contains_key`,`cache_contains_keys`,
`cache_remove_all`,`cache_clear`,`cache_create_with_name`,`cache_destroy`

## How to use it?
```python
from ignite import ThinClient, ThinClientException

thin_client = ThinClient()
try:
  thin_client.connect()
  thin_client.cache_put('mycache', 1, 'value 1')
  value = thin_client.get('mycache', 1)
  print(value)
except ThinClientException as e:
  print(str(e))
  raise e
```

## Where could I find the API documentation?

There's no documentation yet due to the implementation as a prototype.   
The tests in `tests` directory can be used as the code samples.

## What data types supported by Apache Ignite Python Thin Client?

The client supports all python types except `set` but including `list` and `dictionary`.

## Does Apache Ignite Python Thin Client support interoperability for Java?

Partially yes with following limitations: 

* A complex java class will be converted into a python dictionary for `cache_get` operations.

* A python dictionary will be converted into Java `HashMap` for `cache_put` operations.

* A python class not supported

* Using some primitive java data types like `int`, `short` requires `key_type` and `value_type` in `**kwargs` 
for `cache_put` operations   


## How to run tests?

Requirements: 
* Nose test framework
* Apache Ignite 2.4+ installation.

Run:
* Start Apache Ignite node(s) with test configuration file:

`$IGNITE_HOME/bin/ignite.sh tests/test.xml`

* Start tests 

`nosetests -v tests`

## License

Apache Ignite Python Thin Client distributed under Apache License 2.0