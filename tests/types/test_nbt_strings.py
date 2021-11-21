# -*- coding: utf-8 -*-
import gzip
import os.path
from quarry.types.nbt import *
TagCompound.preserve_order = True # for testing purposes.


stringtest_path = os.path.join(os.path.dirname(__file__), "stringtest.nbt")


stringtest_alt_repr = u"""
TAG_Compound(""): 4 entries
{
  TAG_String("empty"): ""
  TAG_String("hello_world"): "Hello world!"
  TAG_String("null"): "\x00"
  TAG_String("surrogate_pair"): "\U0001f30a"
}
"""


def test_empty_encode():
    assert TagString(u'').to_bytes() == b'\x00\x00'


def test_hello_world_encode():
    assert TagString(u'Hello world!').to_bytes() == b'\x00\x0cHello world!'


def test_null_encode():
    assert TagString(u'\0').to_bytes() == b'\x00\x02\xc0\x80'


def test_surrogate_pair_encode():
    assert TagString(u'\U0001f30a').to_bytes() == b'\x00\x06\xed\xa1\xbc\xed\xbc\x8a'

def test_stringtest_alt_repr():
    stringtest = NBTFile.load(stringtest_path).root_tag
    assert alt_repr(stringtest) == stringtest_alt_repr.strip()
