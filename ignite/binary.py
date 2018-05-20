#!/usr/bin/env python3

from struct import pack, unpack
from uuid import UUID


class BinaryException(Exception):
    pass


class BinaryObject:

    # Supported types
    types = {
        # Python types (default conversion)
        'python.int':       {'code': 4, 'size': 8, 'parsing': 'to_bytes-from_bytes'},
        'python.float':     {'code': 6, 'size': 8, 'parsing': 'pack-unpack'},
        'python.bool': {
            'code': 8,
            'size': 1,
            'parsing': 'to_bytes-from_bytes',
            'from_values': {True: 1, False: 0},
            'from_bytes': {1: True, 0: False}
        },
        'python.str':       {'code': 9, 'parsing': 'encode-decode'},
        'python.UUID':      {'code': 10, 'size': 16, 'parsing': 'bytes-property'},
        'python.bytes':     {'code': 12, 'parsing': 'as-is'},
        'python.list':      {'code': 23, 'add_item_code': True, 'type_id': True},
        'python.dict':      {'code': 25, 'add_item_code': True},
        'python.NoneType':  {'code': 101, 'size': 0},
        'python.class':     {'code': 103},

        # Binary object types (for implicit serialization and interoperability)
        'byte':     {'code': 1, 'size': 1, 'parsing': 'to_bytes-from_bytes'},
        'short':    {'code': 2, 'size': 2, 'parsing': 'to_bytes-from_bytes'},
        'int':      {'code': 3, 'size': 4, 'parsing': 'to_bytes-from_bytes'},
        'long':     {'code': 4, 'size': 8, 'parsing': 'to_bytes-from_bytes'},
        'float':    {'code': 5, 'size': 4, 'parsing': 'pack-unpack'},
        'double':   {'code': 6, 'size': 8, 'parsing': 'pack-unpack'},
        'char':     {'code': 7, 'parsing': 'encode-decode', 'skip_length_header': True, 'zerofill': 2},
        'bool':     {
            'code': 8,
            'size': 1,
            'parsing': 'to_bytes-from_bytes',
            'from_values': {True: 1, False: 0},
            'from_bytes': {1: True, 0: False}
        },
        'string':           {'code': 9, 'parsing': 'encode-decode'},
        'uuid':             {'code': 10, 'size': 16, 'parsing': 'bytes-property'},
        'date':             {'code': 11, 'size': 8, 'parsing': 'pack-unpack'},
        'array.byte':       {'code': 12, 'parsing': 'as-is'},
        'array.short':      {'code': 13, 'item_code': 2},
        'array.int':        {'code': 14, 'item_code': 3},
        'array.long':       {'code': 15, 'item_code': 4},
        'array.float':      {'code': 16, 'item_code': 5},
        'array.double':     {'code': 17, 'item_code': 6},
        'array.char':       {'code': 18, 'item_code': 7},
        'array.bool':       {'code': 19, 'item_code': 8},
        'array.string':     {'code': 20, 'add_item_code': True},
        'array.uuid':       {'code': 21, 'add_item_code': True},
        'array.date':       {'code': 22, 'add_item_code': True},
        'array.Object':     {'code': 23, 'add_item_code': True, 'type_id': True},
        'map':              {'code': 25, 'add_item_code': True},
    }

    pack_float_formats = {4: 'f', 8: 'd'}

    @classmethod
    def type_by_code(cls, code):
        name = None
        # First we're trying to get a python type
        for type_name in cls.types.keys():
            if code == cls.types[type_name]['code'] and type_name.startswith('python.'):
                name = type_name
                break
        # If python type is not found try native binary type
        if name is None:
            for type_name in cls.types.keys():
                if code == cls.types[type_name]['code'] and not type_name.startswith('python.'):
                    name = type_name
                    break
        return name

    @classmethod
    def binary_types(cls):
        return cls.types

    def __init__(self, **kwargs):
        self.raw_bytes = None
        self.value = None
        self.preferred_type = None
        self.debug_data = {
            'type_code': None
        }

    def load_bytes(self, raw_bytes):
        self.raw_bytes = raw_bytes
        return self

    def load_value(self, value):
        self.value = value
        return self

    def deserialize_entry(self, binary, pos, **kwargs):
        is_single_type = kwargs.get('is_single_type')
        value = None
        completed = False
        while (pos < len(binary)) and not completed:
            code = kwargs.get('code')
            if code is None:
                code = int.from_bytes(binary[pos:pos+1], byteorder='little')
                pos += 1
            if self.debug_data.get('type_code') is None:
                self.debug_data['type_code'] = code
            type_name = self.type_by_code(code)
            if type_name == 'python.class':
                raise BinaryException("Complex object (class) not supported yet, use dict")
            if type_name is None:
                raise BinaryException("Unknown type code %s in position %s, %s" % (code, pos, list(binary)))
            type_data = self.types[type_name]
            # Process primitive types and byte arrays
            is_primitive_type = 'list' not in type_name \
                                and 'array' not in type_name \
                                and 'map' not in type_name \
                                and 'dict' not in type_name
            #print(pos, code, is_primitive_type, type_name)
            if is_primitive_type or type_name == 'python.bytes':
                size = type_data.get('size')
                parsing = type_data.get('parsing')
                if parsing == 'encode-decode':
                    size = 2
                    if type_data.get('skip_length_header') is not True:
                        size = int.from_bytes(binary[pos:pos+4], byteorder='little')
                        pos += 4
                    if type_data.get('zerofill') is not None:
                        sz = size
                        for idx in range(0, sz):
                            if binary[pos] == 0:
                                pos += 1
                                size -= 1
                            else:
                                break
                    value = binary[pos:pos+size].decode()
                    pos += size
                elif parsing == 'to_bytes-from_bytes':
                    from_bytes = type_data.get('from_bytes')
                    if from_bytes is not None:
                        value = from_bytes[int.from_bytes(binary[pos:pos+size], byteorder='little')]
                    else:
                        value = int.from_bytes(binary[pos:pos+size], byteorder='little', signed=True)
                    pos += size
                elif parsing == 'pack-unpack':
                    value, = unpack(self.pack_float_formats[size], binary[pos:pos+size])
                    pos += size
                elif parsing == 'as-is':
                    size = int.from_bytes(binary[pos:pos+4], byteorder='little')
                    pos += 4
                    value = binary[pos:pos+size]
                elif parsing == 'bytes-property':
                    value = UUID(bytes=binary[pos:pos+size])
                    pos += size
                completed = True
            else:
                if type_name.startswith('array.') or type_name == 'python.list':
                    # skip 4 bytes with ignite type_id, N/A for python
                    if type_data.get('type_id') is True:
                        pos += 4
                    value = []
                    # Number array elements
                    item_cnt = int.from_bytes(binary[pos:pos+4], byteorder='little')
                    pos += 4
                    # Item code
                    code = type_data.get('item_code')
                    #print(item_cnt)
                    for item_idx in range(0, item_cnt):
                        #print(">> at %s, started for code %s" % (pos, code))
                        item_value, pos = self.deserialize_entry(
                            binary, pos,
                            code=code,
                            is_single_type=True
                        )
                        #print(">> item '%s -> %s/%s'" % (item_value, len(value), item_cnt))
                        value.append(item_value)
                    # It's recursive call, stop processing
                    if is_single_type:
                        completed = True
                elif type_name == 'map' or type_name == 'python.dict':
                    value = {}
                    # Number of keys and values pair
                    item_cnt = int.from_bytes(binary[pos:pos+4], byteorder='little')
                    #print("elements: %s" % item_cnt)
                    pos += 4
                    # skip 1 byte where type of map is defined
                    pos += 1
                    cur_key = None
                    item_idx = 0
                    while item_idx < 2*item_cnt:
                        #print(">> at %s" % pos)
                        item, pos = self.deserialize_entry(
                            binary, pos, is_single_type=True
                        )
                        #print(">> item '%s'" % item)
                        if cur_key is None:
                            cur_key = item
                        else:
                            value[cur_key] = item
                            cur_key = None
                        item_idx += 1
                    # It's recursive call, stop processing
                    if is_single_type:
                        completed = True
        return value, pos

    def deserialize(self):
        binary = self.raw_bytes
        #print("read: %s" % list(binary))
        value, pos = self.deserialize_entry(binary, 0)
        #print("read: %s" % value)
        return value

    def skip_entries(self, entry_num, pos):
        binary = self.raw_bytes
        for idx in range(0, entry_num):
            value, pos = self.deserialize_entry(binary, pos)
        return pos

    def serialize_entry(self, value, binary, **kwargs):
        type_name = kwargs.get('type')
        force_use_type = True
        if type_name is None:
            type_name = "python.%s" % type(value).__name__
            force_use_type = False
        if type(value).__name__ == 'NoneType':
            type_name = 'python.NoneType'
        is_list_type = (type_name.startswith('array.') or type_name == 'python.list') and type_name != 'array.byte'
        is_dict_type = type_name == 'python.dict' or type_name == 'map'
        if self.types.get(type_name) == 'python.class':
            raise BinaryException("Complex object (class) not supported yet, use dict")
        if self.types.get(type_name) is not None:
            type_data = self.types[type_name]
            binary += type_data['code'].to_bytes(1, byteorder='little')
            if self.debug_data.get('type_codes') is None:
                self.debug_data['type_codes'] = type_data['code']
            parsing = type_data.get('parsing')
            size = type_data.get('size')
            #print(type_name)
            if is_list_type:
                if type_data.get('type_id') is True:
                    binary += b'\x01\x02\x03\x04'
                binary += len(value).to_bytes(4, byteorder='little')
            elif is_dict_type:
                binary += (1*len(value.keys())).to_bytes(4, byteorder='little') + b'\x01'
            if parsing == 'to_bytes-from_bytes':
                if type_data.get('from_values') is None:
                    binary += value.to_bytes(type_data['size'], byteorder='little', signed=True)
                else:
                    binary += (type_data['from_values'][value]).to_bytes(type_data['size'], byteorder='little', signed=True)
            elif parsing == 'encode-decode':
                encoded_bytes = value.encode()
                if type_data.get('skip_length_header') is not True:
                    binary += len(encoded_bytes).to_bytes(4, byteorder='little')
                if type_data.get('zerofill') is not None:
                    for idx in range(len(encoded_bytes), type_data['zerofill']):
                        encoded_bytes = b'\x00' + encoded_bytes
                binary += encoded_bytes
            elif parsing == 'pack-unpack':
                binary += pack(self.pack_float_formats[size], value)
            elif parsing == 'as-is':
                binary += len(value).to_bytes(4, byteorder='little') + value
            elif parsing == 'bytes-property':
                binary += value.bytes
                #print(list(len(value.bytes).to_bytes(4, byteorder='little') + value.bytes))
            elif parsing is None:
                pass
            if is_list_type:
                item_type_name = None
                if force_use_type:
                    item_type_name = type_name.split('.')[1]
                if item_type_name == 'Object':
                    item_type_name = None
                for item in value:
                    cur_binary = self.serialize_entry(item, b'', type=item_type_name)
                    if type_data.get('add_item_code') is True:
                        binary += cur_binary[0:1]
                    binary += cur_binary[1:]
            elif is_dict_type:
                for item_key in sorted(value.keys()):
                    cur_binary = self.serialize_entry(item_key, b'')
                    if type_data.get('add_item_code') is True:
                        binary += cur_binary[0:1]
                    binary += cur_binary[1:]
                    item_value = value[item_key]
                    cur_binary = self.serialize_entry(item_value, b'')
                    if type_data.get('add_item_code') is True:
                        binary += cur_binary[0:1]
                    binary += cur_binary[1:]
        else:
            raise BinaryException("Unknown type %s" % type_name)
        return binary

    def serialize(self, **kwargs):
        entries = self.value
        #print("serialize before '%s'" % entries)
        bytes_array = self.serialize_entry(entries, b'', type=kwargs.get('type'))
        #print("serialize after: %s" % list(bytes_array))
        return bytes_array

    def debug(self, key):
        return self.debug_data.get(key)
