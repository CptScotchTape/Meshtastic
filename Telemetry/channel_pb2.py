# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: channel.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\rchannel.proto\"\x83\x01\n\x0f\x43hannelSettings\x12\x17\n\x0b\x63hannel_num\x18\x01 \x01(\rB\x02\x18\x01\x12\x0b\n\x03psk\x18\x02 \x01(\x0c\x12\x0c\n\x04name\x18\x03 \x01(\t\x12\n\n\x02id\x18\x04 \x01(\x07\x12\x16\n\x0euplink_enabled\x18\x05 \x01(\x08\x12\x18\n\x10\x64ownlink_enabled\x18\x06 \x01(\x08\"\x8b\x01\n\x07\x43hannel\x12\r\n\x05index\x18\x01 \x01(\x05\x12\"\n\x08settings\x18\x02 \x01(\x0b\x32\x10.ChannelSettings\x12\x1b\n\x04role\x18\x03 \x01(\x0e\x32\r.Channel.Role\"0\n\x04Role\x12\x0c\n\x08\x44ISABLED\x10\x00\x12\x0b\n\x07PRIMARY\x10\x01\x12\r\n\tSECONDARY\x10\x02\x42J\n\x13\x63om.geeksville.meshB\rChannelProtosH\x03Z\"github.com/meshtastic/go/generatedb\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'channel_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'\n\023com.geeksville.meshB\rChannelProtosH\003Z\"github.com/meshtastic/go/generated'
  _CHANNELSETTINGS.fields_by_name['channel_num']._options = None
  _CHANNELSETTINGS.fields_by_name['channel_num']._serialized_options = b'\030\001'
  _CHANNELSETTINGS._serialized_start=18
  _CHANNELSETTINGS._serialized_end=149
  _CHANNEL._serialized_start=152
  _CHANNEL._serialized_end=291
  _CHANNEL_ROLE._serialized_start=243
  _CHANNEL_ROLE._serialized_end=291
# @@protoc_insertion_point(module_scope)