#!/usr/bin/env python3

from ignite import ThinClient, ThinClientException
from time import time

thin = ThinClient()


def setup_module():
    global thin
    try:
        thin.connect()
    except ConnectionRefusedError as e:
        print('The thin client tests required started Apache Ignite cluster.\n'
              'Please read readme.md for instruction how to start Apache Ignite cluster\n%s' % str(e))
        raise e


def test_put_get():
    thin.cache_clear('atomic')
    thin.cache_put('atomic', 1, 'value 1')
    value = thin.cache_get('atomic', 1)
    assert value == 'value 1', "Received value is 'value 1'"


def test_put_all_get_all():
    thin.cache_clear('atomic')
    send_entries = {
        2: 'value 2',
        3: 'value 3'
    }
    thin.cache_put_all(
        'atomic',
        {
            2: 'value 2',
            3: 'value 3'
        }
    )
    rcvd_values = thin.cache_get_all(
        'atomic',
        [2, 3]
    )
    assert rcvd_values == send_entries, 'Received entries %s' % str(send_entries)


def test_contains_key():
    thin.cache_clear('atomic')
    thin.cache_put('atomic', 4, 'value 4')
    exist_entry = thin.cache_contains_key('atomic', 4)
    assert exist_entry, "Entry for key 4 found"


def test_contains_keys():
    thin.cache_clear('atomic')
    send_entries = {
        5: 'value 5',
        6: 'value 6'
    }
    thin.cache_put_all('atomic', send_entries)
    exist_entries = thin.cache_contains_keys('atomic', [5, 6])
    assert exist_entries, "Entries for keys 5,6 found"


def test_clear():
    thin.cache_clear('atomic')
    send_entries = {
        7: 'value 7',
        8: 'value 8'
    }
    thin.cache_put_all('atomic', send_entries)
    size = thin.cache_get_size('atomic')
    assert size == 2, 'Cache size is 2 before clear (%s)' % size
    thin.cache_clear('atomic')
    size = thin.cache_get_size('atomic')
    assert size == 0, 'Cache size is 2 after clear (%s)' % size


def test_clear_key():
    thin.cache_clear('atomic')
    thin.cache_put('atomic', 9, 'value 9')
    size = thin.cache_get_size('atomic')
    assert size == 1, 'Cache size is 1 before clear key (%s)' % size
    thin.cache_clear_key('atomic', 9)
    size = thin.cache_get_size('atomic')
    assert size == 0, 'Cache size is 0 after clear key (%s)' % size


def test_remove_key():
    thin.cache_clear('atomic')
    thin.cache_put('atomic', 10, 'value 10')
    size = thin.cache_get_size('atomic')
    assert size == 1, 'Cache size is 1 before remove key (%s)' % size
    thin.cache_remove_key('atomic', 10)
    size = thin.cache_get_size('atomic')
    assert size == 0, 'Cache size is 0 after remove key (%s)' % size


def test_remove_all():
    thin.cache_clear('atomic')
    send_entries = {
        11: 'value 11',
        12: 'value 12'
    }
    thin.cache_put_all('atomic', send_entries)
    size = thin.cache_get_size('atomic')
    assert size == 2, 'Cache size is 2 before remove all (%s)' % size
    thin.cache_remove_all('atomic')
    size = thin.cache_get_size('atomic')
    assert size == 0, 'Cache size is 2 after remove all (%s)' % size


def test_create_destroy_cache():
    cache = 'my_cache_%s' % time()
    thin.cache_create_with_name(cache)
    thin.cache_put(cache, 'key 1', 1)
    value = thin.cache_get(cache, 'key 1')
    assert value == 1, "Received value for key 'key 1' is 1 (%s)" % value
    thin.cache_destroy(cache)
    recent_exception = ''
    try:
        thin.cache_get(cache, 'key 1')
    except ThinClientException as e:
        recent_exception = str(e)
    finally:
        assert 'Cache does not exist' in recent_exception, "Cache '%s' does not exists" % cache