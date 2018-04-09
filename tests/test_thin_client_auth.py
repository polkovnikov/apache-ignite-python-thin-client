#!/usr/bin/env python3

from ignite import ThinClient, ThinClientException
from time import time

thin = ThinClient(username='ignite', password='ignite', version='1.1.0')

bad_thin = ThinClient(username='ignite', password='wrong', version='1.1.0')


def test_put_get():
    global thin
    try:
        thin.connect()
    except ConnectionRefusedError as e:
        print('The thin client tests required started and activated Apache Ignite cluster.\n'
              'Please read readme.md for instruction how to start Apache Ignite cluster\n%s' % str(e))
        raise e
    thin.cache_clear('atomic')
    thin.cache_put('atomic', 1, 'value 1')
    value = thin.cache_get('atomic', 1)
    assert value == 'value 1', "Received value is 'value 1'"


def test_wrong_password():
    global bad_thin
    exception_text = '[no exceptions]'
    try:
        bad_thin.connect()
    except ThinClientException as e:
        exception_text = str(e)
    finally:
        assert 'The user name or password is incorrect [userName=ignite]' in exception_text, \
            'The connection must fail due to wrong password only: %s' % exception_text