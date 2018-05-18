#!/usr/bin/env python3

from ignite.binary import BinaryObject
from multiprocessing.dummy import Process, Pool as ThreadPool
from socket import socket, AF_INET, SOCK_STREAM, error
from struct import pack
from threading import get_ident, Thread, active_count
from queue import Queue


def java_string_hashcode(s):
    h = 0
    for c in s:
        h = (31 * h + ord(c)) & 0xFFFFFFFF
    return ((h + 0x80000000) & 0xFFFFFFFF) - 0x80000000


class ThinClientException(Exception):
    pass


class ThinClientPoolException(Exception):
    pass


class ThinClient:

    sock = None

    packet_formats = {
        'handshake.1.0.0': {
            'code': -1,
            'request': ['version', b'\x01', 'version_number_1', 'version_number_2', 'version_number_3', b'\x02'],
            'response': ['success', 'routes'],
            'response_routes': {
                'success': {
                    0: ['version_number_1', 'version_number_2', 'version_number_3', 'binary_object'],
                    1: []
                }
            }
        },
        'handshake': {
            'code': -1,
            'request': ['version', b'\x01', 'version_number_1', 'version_number_2', 'version_number_3', b'\x02'],
            'response': ['success', 'routes'],
            'response_routes': {
                'success': {
                    0: ['version_number_1', 'version_number_2', 'version_number_3', 'binary_object'],
                    1: []
                }
            },
            'request.auth': ['version', b'\x01', 'version_number_1', 'version_number_2', 'version_number_3', b'\x02',
                             'binary_object_username', 'binary_object_password'],
            'response.auth': ['success', 'routes'],
            'response_routes.auth': {
                'success': {
                    0: ['version_number_1', 'version_number_2', 'version_number_3', 'binary_object'],
                    1: []
                }
            }
        },
        'OP_CACHE_GET': {
            'code': 1000,
            'request': ['op_code', 'request_id', 'cache_id', 'flags', 'binary_object_key'],
            'response': ['request_id', 'status', 'binary_object']
        },
        'OP_CACHE_PUT': {
            'code': 1001,
            'request': ['op_code', 'request_id', 'cache_id', 'flags', 'binary_object_key', 'binary_object_value'],
            'response': ['request_id', 'status', 'routes'],
            'response_routes': {
                'status': {
                    0: [],
                    -1: ['binary_object']
                }
            }
        },
        'OP_CACHE_GET_ALL': {
            'code': 1003,
            'request': ['op_code', 'request_id', 'cache_id', 'flags', 'binary_object_count', 'binary_objects'],
            'response': ['request_id', 'status', 'routes'],
            'response_routes': {
                'status': {
                    0: ['binary_object_count', 'binary_object'],
                    -1: ['binary_object']
                }
            }
        },
        'OP_CACHE_PUT_ALL': {
            'code': 1004,
            'request': ['op_code', 'request_id', 'cache_id', 'flags', 'binary_object_count', 'binary_objects'],
            'response': ['request_id', 'status', 'routes'],
            'response_routes': {
                'status': {
                    0: ['binary_object_count', 'binary_object'],
                    -1: ['binary_object']
                }
            }
        },
        'OP_CACHE_CONTAINS_KEY': {
            'code': 1011,
            'request': ['op_code', 'request_id', 'cache_id', 'flags', 'binary_object_key'],
            'response': ['request_id', 'status', 'routes'],
            'response_routes': {
                'status': {
                    0: ['bool'],
                    -1: ['binary_object']
                }
            }
        },
        'OP_CACHE_CONTAINS_KEYS': {
            'code': 1012,
            'request': ['op_code', 'request_id', 'cache_id', 'flags', 'binary_object_count', 'binary_objects'],
            'response': ['request_id', 'status', 'routes'],
            'response_routes': {
                'status': {
                    0: ['bool'],
                    -1: ['binary_object']
                }
            }
        },
        'OP_CACHE_CLEAR': {
            'code': 1013,
            'request': ['op_code', 'request_id', 'cache_id', 'flags'],
            'response': ['request_id', 'status', 'routes'],
            'response_routes': {
                'status': {
                    0: [],
                    -1: ['binary_object']
                }
            }
        },
        'OP_CACHE_CLEAR_KEY': {
            'code': 1014,
            'request': ['op_code', 'request_id', 'cache_id', 'flags', 'binary_object_key'],
            'response': ['request_id', 'status', 'routes'],
            'response_routes': {
                'status': {
                    0: [],
                    -1: ['binary_object']
                }
            }
        },
        'OP_CACHE_REMOVE_KEY': {
            'code': 1016,
            'request': ['op_code', 'request_id', 'cache_id', 'flags', 'binary_object_key'],
            'response': ['request_id', 'status', 'routes'],
            'response_routes': {
                'status': {
                    0: ['bool'],
                    -1: ['binary_object']
                }
            }
        },
        'OP_CACHE_REMOVE_ALL': {
            'code': 1019,
            'request': ['op_code', 'request_id', 'cache_id', 'flags'],
            'response': ['request_id', 'status', 'routes'],
            'response_routes': {
                'status': {
                    0: [],
                    -1: ['binary_object']
                }
            }
        },
        'OP_CACHE_GET_SIZE': {
            'code': 1020,
            'request': ['op_code', 'request_id', 'cache_id', 'flags', b'\x00\x00\x00\x00'],
            'response': ['request_id', 'status', 'routes'],
            'response_routes': {
                'status': {
                    0: ['long'],
                    -1: ['binary_object']
                }
            }
        },
        'OP_CACHE_GET_NAMES': {
            'code': 1050,
            'request': ['op_code', 'request_id'],
            'response': ['request_id', 'status', 'binary_object'],
        },
        'OP_CACHE_CREATE_WITH_NAME': {
            'code': 1051,
            'request': ['op_code', 'request_id', 'binary_object'],
            'response': ['request_id', 'status', 'routes'],
            'response_routes': {
                'status': {
                    0: [],
                    -1: ['binary_object']
                }
            }
        },
        'OP_CACHE_DESTROY': {
            'code': 1056,
            'request': ['op_code', 'request_id', 'cache_id'],
            'response': ['request_id', 'status', 'routes'],
            'response_routes': {
                'status': {
                    0: [],
                    -1: ['binary_object']
                }
            }
        },
        'OP_SCAN_QUERY': {
            'code': 2000,
            'request': ['op_code', 'request_id', 'cache_id', 'binary_object', 'filter_platform', 'cursor_page_size', 'partition', 'flag'],
            'response': ['request_id', 'status', 'routes'],
            'response_routes': {
                'status': {
                    0: ['row_count', 'binary_objects', 'bool'],
                    -1: ['binary_object']
                }
            }
        }
    }

    def __communicate(self, *args, **kwargs):
        try:
            self.__encode_request(*args)
            self.raw_response = None
            self.sock.send(self.raw_request)
            self.raw_response = self.sock.recv(4096)
            self.__decode_request(*args)
            if self.response.get('status') is not None:
                if self.response['status'] != 0:
                    err_msg = BinaryObject().load_bytes(self.response['binary_object']).deserialize()
                    raise ThinClientException("Operation %s failed: %s" % (self.operation, err_msg))
        finally:
            self.request_id += 1
            if kwargs.get('debug') is True:
                print("Encoded:      %s" % self.request)
                print("Raw request:  %s" % list(self.raw_request))
                print("Raw response: %s" % list(self.raw_response))
                print("Decoded:      %s" % self.response)

    def __encode_request(self, operation, mode=None):
        if not mode:
            mode = ''
        data = self.request
        encoded = b''
        op_code = self.packet_formats[operation]['code']
        # Find the field attributes
        attrs = {}
        for field in data.keys():
            if '.' in field:
                field_name, field_attr_name = field.split('.')
                if attrs.get(field_name) is None:
                    attrs[field_name] = {}
                attrs[field_name][field_attr_name] = data[field]
        fields = self.packet_formats[operation]['request%s' % mode]
        for field in fields:
            if isinstance(field, bytes):
                encoded += field
            elif not '.' in field:
                if field == 'binary_object_count':
                    encoded += int(data['binary_object_count']).to_bytes(4, byteorder='little')
                elif field == 'binary_objects':
                    if isinstance(data[field], list):
                        for obj in data[field]:
                            encoded += BinaryObject().load_value(obj).serialize()
                    elif isinstance(data[field], dict):
                        for obj_key in data[field].keys():
                            encoded += BinaryObject().load_value(obj_key).serialize() \
                                       + BinaryObject().load_value(data[field][obj_key]).serialize()
                elif field.startswith('binary_object'):
                    encoded += BinaryObject().load_value(data[field]).serialize(type=attrs[field].get('type'))
                elif field == 'cache_id':
                    encoded += int(java_string_hashcode(data['cache'])).to_bytes(4, byteorder='little', signed=True)
                elif field == 'flags':
                    encoded += b'\x00'
                elif field == 'op_code':
                    encoded += int(op_code).to_bytes(2, byteorder='little')
                elif field == 'request_id':
                    encoded += self.request_id.to_bytes(8, byteorder='little')
                elif field.startswith('version_number'):
                    encoded += data[field].to_bytes(2, byteorder='little')
        encoded = len(encoded).to_bytes(4, byteorder='little') + encoded
        self.raw_request = encoded

    def __decode_request(self, operation, mode=None):
        if not mode:
            mode = ''
        data = self.raw_response
        decoded = {}
        pos = 0
        msg_len = int.from_bytes(data[pos:pos+4], byteorder='little')
        pos += 4
        fields = list(self.packet_formats[operation]['response%s' % mode])
        routes = self.packet_formats[operation].get('response_routes%s' % mode)
        if routes is None:
            routes = {}
        field_idx = 0
        while field_idx < len(fields):
            field = fields[field_idx]
            val = None
            if field == 'binary_object_count':
                val = int.from_bytes(data[pos:pos+4], byteorder='little')
                pos += 4
            elif field == 'binary_object':
                val = data[pos:]
                pos = msg_len-1
            elif field == 'bool':
                val = int.from_bytes(data[pos:pos+1], byteorder='little')
                pos += 1
            elif field == 'cache_id':
                val = int.from_bytes(data[pos:pos+4], byteorder='little')
                pos += 4
            elif field == 'flags':
                val = int.from_bytes(data[pos:pos+1], byteorder='little')
                pos += 1
            elif field == 'long':
                val = int.from_bytes(data[pos:pos+8], byteorder='little')
                pos += 8
            elif field == 'request_id':
                val = int.from_bytes(data[pos:pos+8], byteorder='little')
                pos += 8
            elif field == 'status':
                val = int.from_bytes(data[pos:pos+4], byteorder='little')
                pos += 4
            elif field == 'success':
                val = int.from_bytes(data[pos:pos+1], byteorder='little')
                pos += 1
            elif field.startswith('version_number'):
                val = int.from_bytes(data[pos:pos+2], byteorder='little')
                pos += 2
            if val is not None:
                decoded[field] = val
            if routes.get(field) is not None:
                if routes[field].get(decoded[field]) is not None:
                    fields.extend(routes[field][decoded[field]])
                else:
                    fields.extend(routes[field][-1])
            field_idx += 1
        self.response = decoded

    def __init__(self, **kwargs):
        # Set protocol version
        self.host = '127.0.0.1'
        if kwargs.get('host'):
            self.host = kwargs.get('host')
        self.port = 10800
        if kwargs.get('port'):
            self.port = kwargs.get('port')
        self.version = [1, 0, 0]
        if kwargs.get('version'):
            self.version = []
            for num in str(kwargs.get('version')).split('.'):
                self.version.append(int(num))
        self.auth = False
        self.username = kwargs.get('username')
        self.password = kwargs.get('password')
        self.request = {}
        self.response = {}
        self.raw_request = None
        self.raw_response = None
        self.operation = None
        self.request_id = get_ident()
        if self.request_id > 2**32:
            self.request_id -= 2**32
        self.request_id += 2**32
        self.bin_obj = None

    def __del__(self):
        if self.sock is not None:
            self.sock.close()

    def connect(self, addr_port=None):
        self.sock = socket(AF_INET, SOCK_STREAM)
        try:
            if addr_port is None:
                addr_port = (self.host, self.port)
            self.sock.connect(addr_port)
            operation = 'handshake'
            version_text = '%s.%s.%s' % (self.version[0], self.version[1], self.version[2])
            if self.packet_formats.get('handshake.%s' % version_text):
                self.operation = 'handshake.%s' % version_text
            self.request = {
                'version_number_1': self.version[0],
                'version_number_2': self.version[1],
                'version_number_3': self.version[2],
                'binary_object_username': self.username,
                'binary_object_username.type': 'python.str',
                'binary_object_password': self.password,
                'binary_object_password.type': 'python.str',
            }
            mode = None
            if self.username is not None and self.password is not None:
                mode = '.auth'
            self.__communicate(operation, mode)
            if self.response['success'] != 1:
                err_msg = BinaryObject().load_bytes(self.response['binary_object']).deserialize()
                raise ThinClientException("Connection failed: %s" % err_msg)
        except error as e:
            print("something went wrong %s" % str(e))
            raise e

    def disconnect(self):
        self.sock.close()

    def cache_get(self, cache, key, **kwargs):
        self.request = {
            'cache': cache,
            'binary_object_key': key,
            'binary_object_key.type': kwargs.get('key_type')
        }
        self.__communicate('OP_CACHE_GET')
        return BinaryObject().load_bytes(self.response['binary_object']).deserialize()

    def cache_get_binary_object(self, cache, key, **kwargs):
        self.request = {
            'cache': cache,
            'binary_object_key': key,
            'binary_object_key.type': kwargs.get('key_type')
        }
        self.__communicate('OP_CACHE_GET')
        return BinaryObject().load_bytes(self.response['binary_object'])

    def cache_put(self, cache, key, val, **kwargs):
        self.request = {
            'cache': cache,
            'binary_object_key': key,
            'binary_object_key.type': kwargs.get('key_type'),
            'binary_object_value': val,
            'binary_object_value.type': kwargs.get('value_type'),
        }
        self.__communicate('OP_CACHE_PUT')
        return self.response['status'] == 0

    def cache_get_all(self, cache, keys, **kwargs):
        self.request = {
            'cache': cache,
            'binary_objects': keys,
            'binary_object_count': len(keys),
        }
        self.__communicate('OP_CACHE_GET_ALL')
        doubled_len = int(2*self.response['binary_object_count']).to_bytes(4, byteorder='little')
        list_values = BinaryObject().load_bytes(
            b'\x17\x00\x00\x00\x00' + doubled_len + self.response['binary_object']
        ).deserialize()
        value = {}
        for item_idx in range(0, len(list_values), 2):
            value[list_values[item_idx]] = list_values[item_idx+1]
        return value

    def cache_put_all(self, cache, data, **kwargs):
        self.request = {
            'cache': cache,
            'binary_objects': data,
            'binary_object_count': len(data),
        }
        self.__communicate('OP_CACHE_PUT_ALL')
        return self.response['status'] == 0

    def cache_contains_key(self, cache, key, **kwargs):
        self.request = {
            'cache': cache,
            'binary_object_key': key,
            'binary_object_key.type': kwargs.get('key_type'),
        }
        self.__communicate('OP_CACHE_CONTAINS_KEY')
        return self.response['bool'] == 1

    def cache_contains_keys(self, cache, keys, **kwargs):
        self.request = {
            'cache': cache,
            'binary_objects': keys,
            'binary_object_count': len(keys),
        }
        self.__communicate('OP_CACHE_CONTAINS_KEYS')
        return self.response['bool'] == 1

    def cache_clear(self, cache):
        self.request = {
            'cache': cache
        }
        self.__communicate('OP_CACHE_CLEAR')
        return self.response['status'] == 0

    def cache_clear_key(self, cache, key, **kwargs):
        self.request = {
            'cache': cache,
            'binary_object_key': key,
            'binary_object_key.type': kwargs.get('key_type'),
        }
        self.__communicate('OP_CACHE_CLEAR_KEY')

    def cache_remove_key(self, cache, key, **kwargs):
        self.request = {
            'cache': cache,
            'binary_object_key': key,
            'binary_object_key.type': kwargs.get('key_type'),
        }
        self.__communicate('OP_CACHE_REMOVE_KEY')
        return self.response['bool'] == 1

    def cache_remove_all(self, cache):
        self.request = {
            'cache': cache
        }
        self.__communicate('OP_CACHE_REMOVE_ALL')

    def cache_get_size(self, cache):
        self.request = {
            'cache': cache,
            'flags': 0
        }
        self.__communicate('OP_CACHE_GET_SIZE')
        return self.response['long']

    def cache_destroy(self, cache):
        self.request = {
            'cache': cache
        }
        self.__communicate('OP_CACHE_DESTROY')

    def cache_create_with_name(self, cache):
        self.request = {
            'binary_object': cache,
            'binary_object.type': 'python.str',
        }
        self.__communicate('OP_CACHE_CREATE_WITH_NAME')

    def cache_get_names(self):
        self.request = {}
        self.__communicate('OP_CACHE_GET_NAMES')
        return sorted(BinaryObject().load_bytes(b'\x14'+self.response['binary_object']).deserialize())


class ThinClientPool:

    def __init__(self, threads, addr_port=None):
        self.threads = threads
        self.kwargs = {}
        self.addr_port = addr_port

    def __exec_thin_client(self, *operations_args):
        if len(operations_args) == 0:
            return {}
        thin = ThinClient()
        thin.connect(self.addr_port)
        results = {}
        for arg in operations_args:
            operation_id = arg[0]
            method_name = arg[1]
            if len(arg) == 2:
                val = getattr(thin, method_name)()
            elif len(arg) == 3:
                val = getattr(thin, method_name)(*arg[2])
            else:
                val = getattr(thin, method_name)(*arg[2], **arg[3])
            results[operation_id] = {
                'result': val,
                'method': method_name
            }
            if len(arg) == 3:
                results[operation_id]['args'] = arg[2]
            if len(arg) == 4:
                results[operation_id]['kwargs'] = arg[3]
        thin.disconnect()
        return results

    def execute(self, args, **kwargs):
        """
        Execute operations in parallel threads.
        Note: the parallel execution will not guarantee the order of operations!
        :param      args:   The dictionary of arguments for operations in format:
                            {
                                operation_id_1: [method_name, args[], kwargs{}],
                                operation_id_2: [method_name, args[], kwargs{}],
                                ...
                            }
                    kwargs: Various options for result formatting
        :return:
        """
        pool = ThreadPool(self.threads)
        grouped_args = []
        for idx in range(0, self.threads):
            grouped_args.append([])
        idx = 0
        if isinstance(args, dict):
            for oper_id in args.keys():
                if idx == self.threads:
                    idx = 0
                oper_args = [oper_id]
                for oper_arg in args[oper_id]:
                    oper_args.append(oper_arg)
                if len(oper_args) == 1:
                    raise ThinClientPoolException("Arguments number for operation %s is 0: %s" % (oper_id, oper_args))
                # if len(oper_args) == 2:
                #     oper_args.append([])
                # if len(oper_args) == 3:
                #     oper_args.append([])
                grouped_args[idx].append(oper_args)
                idx += 1
        else:
            raise ThinClientPoolException(
                'Wrong argument type: expected dictionary, found %s' % type(args).__name__
            )
        grouped_results = pool.starmap(self.__exec_thin_client, grouped_args)
        pool.close()
        pool.join()
        result_type = kwargs.get('result_type')
        if result_type is not None:
            if result_type == 'result_to_list':
                joined_list = []
                for grouped_result in grouped_results:
                    for oper_id in grouped_result.keys():
                        if isinstance(grouped_result[oper_id]['result'], list):
                            joined_list.extend(grouped_result[oper_id]['result'])
                        else:
                            joined_list.append(grouped_result[oper_id]['result'])
                return joined_list
            elif result_type == 'list':
                joined_list = []
                for grouped_result in grouped_results:
                    for oper_id in grouped_result.keys():
                        joined_list.append(grouped_result[oper_id])
                return joined_list
        results = {}
        for grouped_result in grouped_results:
            results.update(grouped_result)
        return grouped_results
