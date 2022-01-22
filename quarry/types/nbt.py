import collections
import functools
import gzip
import os
import re
import sys
import time
import zlib

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../brigadier.py"))

from brigadier.string_reader import StringReader

from mutf8.mutf8 import encode_modified_utf8, decode_modified_utf8

from quarry.types.buffer import Buffer
from quarry.types.text_format import ansify_text, get_format, unformat_text
from quarry.types.chunk import PackedArray

_kinds = {}
_ids = {}

def nbt_path_join(*args):
    """Join two NBT paths into a longer path, similar to os.path.join()."""
    if len(args) == 0:
        return '{}'
    if len(args) == 1:
        return args[0]
    if args[-1] == '':
        return nbt_path_join(*args[:-1])
    if args[1].startswith('['):
        return nbt_path_join(f'{args[0]}{args[1]}', *args[2:])
    return nbt_path_join(f'{args[0]}.{args[1]}', *args[2:])


# Base types ------------------------------------------------------------------

@functools.total_ordering
class _Tag(object):
    __slots__ = ('value',)

    def __init__(self, value):
        self.value = value

    prefix = ('', get_format('gold').ansi_code)
    postfix = ('', get_format('reset').ansi_code)

    @classmethod
    def from_bytes(cls, bytes):
        return cls.from_buff(Buffer(bytes))

    @classmethod
    def from_buff(cls, buff):
        raise NotImplementedError

    def to_bytes(self):
        raise NotImplementedError

    def deep_copy(self):
        return type(self).from_bytes(self.to_bytes())

    def to_obj(self):
        return self.value

    def __hash__(self):
        return hash(self.to_bytes())

    def __repr__(self):
        return "%s(%r)" % (type(self).__name__, self.value)

    def __eq__(self, other):
        return self.to_obj() == other.to_obj()

    def equals_exact(self, other):
        return self.to_bytes() == other.to_bytes()

    def __lt__(self, other):
        return self.to_obj() < other.to_obj()

    def to_mojangson(self, sort=None, highlight=False):
        prefix = self.prefix[highlight]
        postfix = self.postfix[highlight]

        return f'{prefix}{self.value!s}{postfix}'

    def tree(self, sort=None, indent='    ', level=0):
        result = self.to_mojangson(sort=sort, highlight=True)
        if level == 0:
            print(result)
        else:
            return result

    @staticmethod
    def _nbt_path_node_prefix_check(path):
        """Returns path as a StringReader, raising SyntaxError for universally invalid first characters."""
        if not isinstance(path, StringReader):
            path = StringReader(path)
        if path.can_read() and path.peek() == '.':
            raise SyntaxError(f"Invalid NBT path element at position {path.get_cursor()}: {'...' if path.get_cursor() > 10 else ''}{path.get_read()[-10:]}<--[HERE]")
        return path

    @staticmethod
    def _nbt_path_node_suffix_check(path):
        """Verifies end of path or node separator, and raises SyntaxError on failure."""
        if path.can_read():
            if path.peek() == '.':
                path.skip()
            elif path.peek() != '[':
                raise SyntaxError(f"Invalid NBT path element at position {path.get_cursor()}: {'...' if path.get_cursor() > 10 else ''}{path.get_read()[-10:]}<--[HERE]")

    def is_subset(self, other):
        return self.value == other.value

    def diff(self, other, order_matters=True, show_values=False, path=''):
        raise NotImplementedError

    def has_path(self, path):
        raise NotImplementedError

    def at_path(self, path):
        raise NotImplementedError

    def count_multipath(self, path):
        raise NotImplementedError

    def iter_multipath_pair(self, path):
        raise NotImplementedError

    def iter_multipath(self, path):
        for _, tag in self.iter_multipath_pair(path):
            yield tag

class _DataTag(_Tag):
    __slots__ = ()
    fmt = None

    @classmethod
    def from_buff(cls, buff):
        return cls(buff.unpack(cls.fmt))

    def to_bytes(self):
        return Buffer.pack(self.fmt, self.value)

    def diff(self, other, order_matters=True, show_values=False, path=''):
        if type(self) != type(other):
            print(f'Diff at path {path!r}: type')
            if show_values:
                print(f'  -  self is type: {type( self)}')
                print(f'  - other is type: {type(other)}')
            return True

        if self.to_obj() != other.to_obj():
            print(f'Diff at path {path!r}: value')
            if show_values:
                print(f'  -  self is: { self.to_obj()}')
                print(f'  - other is: {other.to_obj()}')
            return True

        return False

    def has_path(self, path):
        path = self._nbt_path_node_prefix_check(path)
        self._nbt_path_node_suffix_check(path)
        return not path.can_read()

    def at_path(self, path):
        path = self._nbt_path_node_prefix_check(path)
        self._nbt_path_node_suffix_check(path)
        if not path.can_read():
            return self
        else:
            raise KeyError(f'{type(self)!s} cannot contain other tags, and is the end of path {path!r}')

    def count_multipath(self, path):
        path = self._nbt_path_node_prefix_check(path)
        self._nbt_path_node_suffix_check(path)
        if not path.can_read():
            return 1
        else:
            return 0

    def iter_multipath_pair(self, path):
        path = self._nbt_path_node_prefix_check(path)
        self._nbt_path_node_suffix_check(path)
        if not path.can_read():
            yield ('', self)


class _ArrayTag(_Tag):
    __slots__ = ()
    width = None
    separator = (',', f'{get_format("white").ansi_code}, ')

    def __len__(self):
        return len(self.value)

    @classmethod
    def from_buff(cls, buff):
        length = buff.unpack('i')
        data = buff.read(length * (cls.width // 8))
        return cls(PackedArray.from_bytes(data, length, cls.width, cls.width))

    def to_bytes(self):
        data = self.value.to_bytes()
        data = Buffer.pack('i', len(data) // (self.width // 8)) + data
        return data

    def to_obj(self):
        return list(self.value)

    def diff(self, other, order_matters=True, show_values=False, path=''):
        if type(self) != type(other):
            print(f'Diff at path {path!r}: type')
            if show_values:
                print(f'  -  self is type: {type( self)}')
                print(f'  - other is type: {type(other)}')
            return True

        if len(self.value) != len(other.value):
            print(f'Diff at path {path!r}: length')
            if show_values:
                print(f'  -  self is length: {len( self)}')
                print(f'  - other is length: {len(other)}')
            return True

        different = False
        for i in range(len(self.value)):
            subpath = f'{path}[{i}]'
            if self.value[i] != other.value[i]:
                print(f'Diff at path {subpath!r}: value')
                if show_values:
                    print(f'  -  self is: { self.value[i]}')
                    print(f'  - other is: {other.value[i]}')
                different = True

        return different

    def is_subset(self, other):
        if (
            type(other) != type(self) or
            len(other) != len(self)
        ):
            return False
        for i in range(len(self.value)):
            if self.value[i] != other.value[i]:
                return False
        return True

    def to_mojangson(self, sort=None, highlight=False):
        prefix = self.prefix[highlight]
        separator = self.separator[highlight]
        type_postfix = self.type_postfix[highlight]
        postfix = self.postfix[highlight]

        inner_mojangson = []
        for content in self.value:
            # Converted packed unsigned values to signed values
            if content >= (1<<(self.width - 1)):
                content -= (1<<(self.width))
            inner_mojangson.append(f'{content!s}{type_postfix}')
        return f'{prefix}{separator.join(inner_mojangson)}{postfix}'

    def tree(self, sort=None, indent='    ', level=0):
        prefix = self.prefix[True]
        separator = self.separator[True]
        type_postfix = self.type_postfix[True]
        postfix = self.postfix[True]

        inner_mojangson = []
        for content in self.value:
            inner_mojangson.append(f'{content!s}{type_postfix}')

        if len(inner_mojangson) <= 8:
            result = f'{prefix}{separator.join(inner_mojangson)}{postfix}'
        else:
            result = f'{prefix}{separator.join(inner_mojangson[:8])}{separator}...({len(inner_mojangson)!s} entries total){postfix}'

        if level == 0:
            print(result)
        else:
            return result

    def has_path(self, path):
        path = self._nbt_path_node_prefix_check(path)
        if not path.can_read():
            return True
        if path.peek() != '[':
            return False
        path.skip()

        # Index case - list[#]
        index = path.read_int()
        if not path.can_read() or path.peek() != ']':
            return False
        path.skip()
        self._nbt_path_node_suffix_check(path)
        if path.can_read():
            raise KeyError(f'{type(self)!s} cannot contain other tags, and is the end of path {path!r}')
        if -len(self.value) > index or index >= len(self.value):
            return False
        return self.value[index].has_path(path)

    def at_path(self, path):
        path = self._nbt_path_node_prefix_check(path)
        if not path.can_read():
            return self
        if path.peek() != '[':
            raise SyntaxError("Cannot index numeric array without [].")
        path.skip()

        # Index case - list[#]
        index = path.read_int()
        if not path.can_read() or path.peek() != ']':
            raise SyntaxError("Unterminated NBT path: Missing ']'")
        path.skip()
        self._nbt_path_node_suffix_check(path)
        if path.can_read():
            raise KeyError(f'{type(self)!s} cannot contain other tags, and is the end of path {path!r}')
        if -len(self.value) > index or index >= len(self.value):
            raise IndexError(f'Index {index!s} not in range ({len(self.value)!s} entries)')
        return self.value[index].at_path(path)

    def count_multipath(self, path):
        path = self._nbt_path_node_prefix_check(path)
        if not path.can_read():
            return 1
        if path.peek() != '[':
            # Wrong type here, but could be the right type for a sibling tag
            return 0
        path.skip()
        if not path.can_read():
            raise SyntaxError("Unterminated NBT path: Missing ']'")

        count = 0
        if path.peek() == ']':
            # All children case - list[]
            path.skip()
            self._nbt_path_node_suffix_check(path)
            if path.can_read():
                # Last node here, but a sibling tag could have children
                return 0
            for child in self.value:
                count += 1
            return count

        # Index case - list[#]
        index = path.read_int()
        if not path.can_read() or path.peek() != ']':
            raise SyntaxError("Unterminated NBT path: Missing ']'")
        path.skip()
        self._nbt_path_node_suffix_check(path)
        if path.can_read():
            # Last node here, but a sibling tag could have children
            return 0
        if -len(self.value) > index or index >= len(self.value):
            return 0
        return 1

    def iter_multipath_pair(self, path):
        path = self._nbt_path_node_prefix_check(path)
        if not path.can_read():
            yield ('', self)
            return
        if path.peek() != '[':
            # Wrong type here, but could be the right type for a sibling tag
            return
        path.skip()
        if not path.can_read():
            raise SyntaxError("Unterminated NBT path: Missing ']'")

        count = 0
        if path.peek() == ']':
            # All children case - list[]
            path.skip()
            self._nbt_path_node_suffix_check(path)
            if path.can_read():
                raise KeyError(f'{type(self)!s} cannot contain other tags, and is the end of path {path!r}')
            for index, child in enumerate(self.value):
                yield (f'[{index}]', child)
            return

        # Index case - list[#]
        index = path.read_int()
        if not path.can_read() or path.peek() != ']':
            raise SyntaxError("Unterminated NBT path: Missing ']'")
        path.skip()
        self._nbt_path_node_suffix_check(path)
        if path.can_read():
            raise KeyError(f'{type(self)!s} cannot contain other tags, and is the end of path {path!r}')
        if -len(self.value) > index or index >= len(self.value):
            return
        for subpath, tag in self.value[index].iter_multipath_pair(path):
            yield (nbt_path_join(f'[{index}]', subpath), tag)


# NBT tags --------------------------------------------------------------------

class TagByte(_DataTag):
    __slots__ = ()
    fmt = 'b'
    postfix = ('b', f'{get_format("red").ansi_code}b{get_format("reset").ansi_code}')


class TagShort(_DataTag):
    __slots__ = ()
    fmt = 'h'
    postfix = ('s', f'{get_format("red").ansi_code}s{get_format("reset").ansi_code}')


class TagInt(_DataTag):
    __slots__ = ()
    fmt = 'i'
    postfix = ('', get_format('reset').ansi_code)


class TagLong(_DataTag):
    __slots__ = ()
    fmt = 'q'
    postfix = ('L', f'{get_format("red").ansi_code}L{get_format("reset").ansi_code}')


class TagFloat(_DataTag):
    __slots__ = ()
    fmt = 'f'
    postfix = ('f', f'{get_format("red").ansi_code}f{get_format("reset").ansi_code}')


class TagDouble(_DataTag):
    __slots__ = ()
    fmt = 'd'
    postfix = ('d', f'{get_format("red").ansi_code}d{get_format("reset").ansi_code}')


class TagString(_Tag):
    __slots__ = ()

    @staticmethod
    def use_single_quotes(text):
        single_quote_count = text.count("'")
        double_quote_count = text.count('"')
        if single_quote_count == double_quote_count:
            if single_quote_count == 0:
                return False
            return text.find("'") > text.find('"')
        return single_quote_count < double_quote_count

    @property
    def prefix(self):
        q = "'" if self.use_single_quotes(self.value) else '"'
        return (q, f'{get_format("white").ansi_code}{q}{get_format("green").ansi_code}')

    @property
    def postfix(self):
        q = "'" if self.use_single_quotes(self.value) else '"'
        return (q, f'{get_format("white").ansi_code}{q}{get_format("reset").ansi_code}')

    @classmethod
    def from_buff(cls, buff):
        string_length = buff.unpack('H')
        return cls(decode_modified_utf8(buff.read(string_length)))

    def to_bytes(self):
        data = encode_modified_utf8(self.value)
        return Buffer.pack('H', len(data)) + data

    def diff(self, other, order_matters=True, show_values=False, path=''):
        if type(self) != type(other):
            print(f'Diff at path {path!r}: type')
            if show_values:
                print(f'  -  self is type: {type( self)}')
                print(f'  - other is type: {type(other)}')
            return True

        if self.to_obj() != other.to_obj():
            print(f'Diff at path {path!r}: value')
            if show_values:
                print(f'  -  self is: { self.to_obj()}')
                print(f'  - other is: {other.to_obj()}')
            return True

        return False

    def has_path(self, path):
        path = self._nbt_path_node_prefix_check(path)
        return not path.can_read()

    def at_path(self, path):
        path = self._nbt_path_node_prefix_check(path)
        self._nbt_path_node_suffix_check(path)
        if not path.can_read():
            return self
        else:
            raise KeyError(f'{type(self)!s} cannot contain other tags, and is the end of path {path!r}')

    def count_multipath(self, path):
        path = self._nbt_path_node_prefix_check(path)
        self._nbt_path_node_suffix_check(path)
        if not path.can_read():
            return 1
        else:
            return 0

    def iter_multipath_pair(self, path):
        path = self._nbt_path_node_prefix_check(path)
        self._nbt_path_node_suffix_check(path)
        if not path.can_read():
            yield ('', self)

    @staticmethod
    def escape_value(text):
        text = text.replace('\\', '\\\\').replace('\n', '\\n"')
        if self.use_single_quotes(text):
            text = text.replace("'", "\\'")
        else:
            text = text.replace('"', '\\"')
        return text

    def to_mojangson(self, sort=None, highlight=False):
        prefix = self.prefix[highlight]
        postfix = self.postfix[highlight]

        text = self.escape_value(self.value)
        if highlight:
            text = ansify_text(text, show_section=True)
        return f'{prefix}{text}{postfix}'

    def tree(self, sort=None, indent='    ', level=0):
        result = self.to_mojangson(highlight=True)
        if level == 0:
            print(result)
        else:
            return result


class TagByteArray(_ArrayTag):
    __slots__ = ()
    width = 8
    prefix = ('[B;', f'{get_format("white").ansi_code}[{get_format("red").ansi_code}B{get_format("white").ansi_code}; {get_format("gold").ansi_code}')
    postfix = (']', f'{get_format("white").ansi_code}]{get_format("reset").ansi_code}')
    separator = (',', f'{get_format("white").ansi_code}, {get_format("gold").ansi_code}')
    type_postfix = ('b', f'{get_format("red").ansi_code}b')


class TagIntArray(_ArrayTag):
    __slots__ = ()
    width = 32
    prefix = ('[I;', f'{get_format("white").ansi_code}[{get_format("red").ansi_code}I{get_format("white").ansi_code}; {get_format("gold").ansi_code}')
    postfix = (']', f'{get_format("white").ansi_code}]{get_format("reset").ansi_code}')
    separator = (',', f'{get_format("white").ansi_code}, {get_format("gold").ansi_code}')
    type_postfix = ('', '')


class TagLongArray(_ArrayTag):
    __slots__ = ()
    width = 64
    prefix = ('[L;', f'{get_format("white").ansi_code}[{get_format("red").ansi_code}L{get_format("white").ansi_code}; {get_format("gold").ansi_code}')
    postfix = (']', f'{get_format("white").ansi_code}]{get_format("reset").ansi_code}')
    separator = (',', f'{get_format("white").ansi_code}, {get_format("gold").ansi_code}')
    type_postfix = ('l', f'{get_format("red").ansi_code}l')


class TagList(_Tag):
    __slots__ = ()
    prefix = ('[', f'{get_format("white").ansi_code}[{get_format("gold").ansi_code}')
    postfix = (']', f'{get_format("white").ansi_code}]{get_format("reset").ansi_code}')
    separator = (',', f'{get_format("white").ansi_code}, {get_format("gold").ansi_code}')

    def __len__(self):
        return len(self.value)

    @classmethod
    def from_buff(cls, buff):
        inner_kind_id, array_length = buff.unpack('bi')
        inner_kind = _kinds[inner_kind_id]
        return cls([inner_kind.from_buff(buff) for _ in range(array_length)])

    def to_bytes(self):
        if len(self.value) > 0:
            head = self.value[0]
        else:
            head = TagByte(0)

        return Buffer.pack('bi', _ids[type(head)], len(self.value)) + \
               b"".join(tag.to_bytes() for tag in self.value)

    def to_obj(self):
        return [tag.to_obj() for tag in self.value]

    def diff(self, other, order_matters=True, show_values=False, path=''):
        if type(self) != type(other):
            print(f'Diff at path {path!r}: type')
            if show_values:
                print(f'  -  self is type: {type( self)}')
                print(f'  - other is type: {type(other)}')
            return True

        if len(self.value) != len(other.value):
            print(f'Diff at path {path!r}: length')
            if show_values:
                print(f'  -  self is length: {len( self)}')
                print(f'  - other is length: {len(other)}')
            return True

        different = False
        for i in range(len(self.value)):
            different |= self.value[i].diff(other.value[i], order_matters, show_values, '{}[{}]'.format(path, i))
        return different

    def is_subset(self, other):
        if type(other) != TagList:
            return False
        for self_value in self.value:
            if not any(self_value.is_subset(other_value) for other_value in other.value):
                return False
        return True

    def to_mojangson(self, sort=None, highlight=False):
        prefix = self.prefix[highlight]
        separator = self.separator[highlight]
        postfix = self.postfix[highlight]

        inner_mojangson = []
        for content in self.value:
            inner_mojangson.append(content.to_mojangson(sort, highlight))
        return f'{prefix}{separator.join(inner_mojangson)}{postfix}'

    def tree(self, sort=None, indent='    ', level=0):
        prefix = f'{self.prefix[True]}\n'
        separator = f'{self.separator[True]}\n'
        postfix = f'{indent*level}{self.postfix[True]}'

        inner_mojangson = []
        for content in self.value:
            inner_mojangson.append(indent*(level+1) + content.tree(sort, indent, level+1))

        if len(inner_mojangson) == 0:
            result = f'{prefix}{postfix}'
        else:
            result = f'{prefix}{separator.join(inner_mojangson)}\n{postfix}'

        if level == 0:
            print(result)
        else:
            return result

    def has_path(self, path):
        path = self._nbt_path_node_prefix_check(path)
        if not path.can_read():
            return True
        if path.peek() != '[':
            # Wrong type here, but could be the right type for a sibling tag
            return False
        path.skip()
        if not path.can_read():
            raise SyntaxError("Unterminated NBT path: Missing ']'")

        count = 0
        if path.peek() == ']':
            # All children case - list[]
            path.skip()
            self._nbt_path_node_suffix_check(path)
            cursor = path.get_cursor()
            for child in self.value:
                path.set_cursor(cursor)
                if child.has_path(path):
                    return True
            return False

        # Index case - list[#]
        index = path.read_int()
        if not path.can_read() or path.peek() != ']':
            raise SyntaxError("Unterminated NBT path: Missing ']'")
        path.skip()
        self._nbt_path_node_suffix_check(path)
        if -len(self.value) > index or index >= len(self.value):
            return False
        return self.value[index].has_path(path)

    def at_path(self, path):
        path = self._nbt_path_node_prefix_check(path)
        if not path.can_read():
            return self
        if path.peek() != '[':
            # Wrong type here, but could be the right type for a sibling tag
            raise SyntaxError(f"Unexpected character in NBT path of TagList: {path.peek()!r}")
        path.skip()
        if not path.can_read():
            raise SyntaxError("Unterminated NBT path: Missing ']'")

        # Index case - list[#]
        index = path.read_int()
        if not path.can_read() or path.peek() != ']':
            raise SyntaxError("Unterminated NBT path: Missing ']'")
        path.skip()
        self._nbt_path_node_suffix_check(path)
        if -len(self.value) > index or index >= len(self.value):
            return False
        return self.value[index].at_path(path)

    def count_multipath(self, path):
        path = self._nbt_path_node_prefix_check(path)
        if not path.can_read():
            return 1
        if path.peek() != '[':
            # Wrong type here, but could be the right type for a sibling tag
            return 0
        path.skip()
        if not path.can_read():
            raise SyntaxError("Unterminated NBT path: Missing ']'")

        count = 0
        if path.peek() == ']':
            # All children case - list[]
            path.skip()
            self._nbt_path_node_suffix_check(path)
            cursor = path.get_cursor()
            for child in self.value:
                path.set_cursor(cursor)
                count += child.count_multipath(path)
            return count

        if path.peek() == '{':
            # List of matching compounds case - list[{}]
            child_must_match = TagCompound.from_mojangson(path) # advances cursor; raises SyntaxError
            if not path.can_read() or path.peek() != ']':
                raise SyntaxError("Unterminated NBT path: Missing ']'")
            self._nbt_path_node_suffix_check(path)
            cursor = path.get_cursor()
            for child in self.value:
                path.set_cursor(cursor)
                if not child_must_match.is_subset(child):
                    continue
                count += child.count_multipath(path)
            return count

        # Index case - list[#]
        index = path.read_int()
        if not path.can_read() or path.peek() != ']':
            raise SyntaxError("Unterminated NBT path: Missing ']'")
        path.skip()
        self._nbt_path_node_suffix_check(path)
        if -len(self.value) > index or index >= len(self.value):
            return 0
        return self.value[index].count_multipath(path)

    def iter_multipath_pair(self, path):
        path = self._nbt_path_node_prefix_check(path)
        if not path.can_read():
            yield ('', self)
            return
        if path.peek() != '[':
            # Wrong type here, but could be the right type for a sibling tag
            return
        path.skip()
        if not path.can_read():
            raise SyntaxError("Unterminated NBT path: Missing ']'")

        count = 0
        if path.peek() == ']':
            # All children case - list[]
            path.skip()
            self._nbt_path_node_suffix_check(path)
            cursor = path.get_cursor()
            for index, child in enumerate(self.value):
                path.set_cursor(cursor)
                for subpath, tag in child.iter_multipath_pair(path):
                    yield (nbt_path_join(f'[{index}]', subpath), tag)
            return

        if path.peek() == '{':
            # List of matching compounds case - list[{}]
            child_must_match = TagCompound.from_mojangson(path) # advances cursor; raises SyntaxError
            if not path.can_read() or path.peek() != ']':
                raise SyntaxError("Unterminated NBT path: Missing ']'")
            self._nbt_path_node_suffix_check(path)
            cursor = path.get_cursor()
            for index, child in enumerate(self.value):
                path.set_cursor(cursor)
                if not child_must_match.is_subset(child):
                    continue
                for subpath, tag in child.iter_multipath_pair(path):
                    yield (nbt_path_join(f'[{index}]', subpath), tag)
            return

        # Index case - list[#]
        index = path.read_int()
        if not path.can_read() or path.peek() != ']':
            raise SyntaxError("Unterminated NBT path: Missing ']'")
        path.skip()
        self._nbt_path_node_suffix_check(path)
        if -len(self.value) > index or index >= len(self.value):
            return
        for subpath, tag in self.value[index].iter_multipath_pair(path):
            yield (nbt_path_join(f'[{index}]', subpath), tag)


class TagCompound(_Tag):
    __slots__ = ()

    root = False
    preserve_order = True
    prefix = ('{', f'{get_format("white").ansi_code}{{{get_format("gold").ansi_code}')
    postfix = ('}', f'{get_format("white").ansi_code}}}{get_format("reset").ansi_code}')
    separator = (',', f'{get_format("white").ansi_code}, {get_format("gold").ansi_code}')
    key_value_separator = (':', f'{get_format("white").ansi_code}: ')
    regexUnquotedString = re.compile(r'''[A-Za-z0-9._+-]+''')

    @classmethod
    def from_buff(cls, buff):
        if cls.preserve_order:
            value = collections.OrderedDict()
        else:
            value = {}

        while True:
            kind_id = buff.unpack('b')
            if kind_id == 0:
                return cls(value)
            kind = _kinds[kind_id]
            name = TagString.from_buff(buff).value
            tag = kind.from_buff(buff)
            value[name] = tag
            if cls.root:
                return cls(value)

    def to_bytes(self):
        string = b""
        for name, tag in self.value.items():
            string += Buffer.pack('b', _ids[type(tag)])
            string += TagString(name).to_bytes()
            string += tag.to_bytes()

        if len(self.value) == 0 or not self.root:
            string += Buffer.pack('b', 0)

        return string

    def to_obj(self):
        return dict((name, tag.to_obj()) for name, tag in self.value.items())

    def update(self, other_tag):
        for name, new_tag in other_tag.value.items():
            old_tag = self.value.get(name)

            if old_tag and not new_tag:
                del self.value[name]
            elif isinstance(old_tag, TagCompound) \
                    and isinstance(new_tag, TagCompound):
                self.value[name].update(new_tag)
            else:
                self.value[name] = new_tag

    def diff(self, other, order_matters=True, show_values=True, path=''):
        if type(self) != type(other):
            print(f'Diff at path {path!r}: type')
            if show_values:
                print(f'  -  self is type: {type( self)}')
                print(f'  - other is type: {type(other)}')
            return True

        own_keys = self.value.keys()
        other_keys = other.value.keys()
        # Order insensitive
        if own_keys != other_keys:
            print(f'Diff at path {path!r}: keys')
            if show_values:
                print(f'  - both have keys: {list(own_keys & other_keys)}')
                print(f'  -  self has keys: {list(own_keys - other_keys)}')
                print(f'  - other has keys: {list(other_keys - own_keys)}')
            return True

        different = False
        # Order sensitive
        if order_matters and list(own_keys) != list(other_keys):
            print(f'Diff at path {path!r}: key order')
            if show_values:
                print(f'  -  self key order: {own_keys}')
                print(f'  - other key order: {other_keys}')
            different = True

        # Order insensitive
        conditional_dot = '' if len(path) == 0 else '.'
        for key in self.value.keys():
            subpath = f'{path}{conditional_dot}{key}'
            if type(self.value[key]) != type(other.value[key]):
                print(f'Diff at path {subpath!r}: type')
                if show_values:
                    print(f'  -  self is type: {type( self.value[key])}')
                    print(f'  - other is type: {type(other.value[key])}')
                different = True

            different |= self.value[key].diff(other.value[key], order_matters, show_values, subpath)
        return different

    def is_subset(self, other):
        if type(other) != TagCompound:
            return False
        for aKey in self.value.keys():
            if (
                aKey not in other.value.keys() or
                not self.value[aKey].is_subset(other.value[aKey])
            ):
                return False
        return True

    def to_mojangson(self, sort=None, highlight=False):
        prefix = self.prefix[highlight]
        key_value_separator = self.key_value_separator[highlight]
        separator = self.separator[highlight]
        postfix = self.postfix[highlight]

        if isinstance(sort, list):
            keys = []
            for key in sort:
                if key in self.value.keys():
                    keys.append(key)
            for key in sorted(self.value.keys()):
                if key not in sort:
                    keys.append(key)
        else:
            keys = self.value.keys()

        inner_mojangson = []
        for key in keys:
            content = self.value[key]
            if regexUnquotedString.fullmatch(key):
                key_str = key
            else:
                key_str = TagString.escape_value(key)
            inner_mojangson.append(f'{key_str}{key_value_separator}{content.to_mojangson(sort, highlight)}')
        return f'{prefix}{separator.join(inner_mojangson)}{postfix}'

    def tree(self, sort=None, indent='    ', level=0):
        prefix = f'{self.prefix[True]}\n'
        key_value_separator = self.key_value_separator[True]
        separator = f'{self.separator[True]}\n'
        postfix = f'{indent*level}{self.postfix[True]}'

        if isinstance(sort, list):
            keys = []
            for key in sort:
                if key in self.value.keys():
                    keys.append(key)
            for key in sorted(self.value.keys()):
                if key not in sort:
                    keys.append(key)
        else:
            keys = self.value.keys()

        inner_mojangson = []
        for key in keys:
            content = self.value[key]
            inner_mojangson.append(f'{indent*(level+1)}{key}{key_value_separator}{content.tree(sort, indent, level+1)}')

        if len(inner_mojangson) == 0:
            result = f'{prefix}{postfix}'
        else:
            result = f'{prefix}{separator.join(inner_mojangson)}\n{postfix}'

        if level == 0:
            print(result)
        else:
            return result

    def has_path(self, path):
        path = self._nbt_path_node_prefix_check(path)
        if not path.can_read():
            return True

        target = self
        if path.peek() != '{':
            # Assume tag name
            child_tag_name = None
            if path.peek() == '[':
                # Wrong type, but could be correct for a sibling tag.
                return False
            if path.peek() in ' .]}\\':
                raise SyntaxError("Unexpected character - expected { or valid tag name")

            if path.peek() == '"':
                # Quoted tag name (allows unusual characters; can't quote with ')
                path.skip()
                child_tag_name = path.read_string_until('"')
            else:
                # Assume unquoted tag name (different rules than normal unquoted string!)
                start = path.get_cursor()
                while path.can_read() and path.peek() not in ' .[]{}"':
                    path.skip()
                if path.can_read() and path.peek() not in '.{[':
                    raise SyntaxError(f'Unexpected character {path.peek()!r} at char {path.get_cursor()} - expected [, {{, or .')
                end = path.get_cursor()
                if start == end:
                    raise SyntaxError(f"Invalid unquoted tag name; can't start with {path.peek()}")
                child_tag_name = path.get_string()[start:end]

            if child_tag_name not in self.value:
                return False
            target = self.value[child_tag_name]

            # Check if this is the end of the path node
            if not path.can_read():
                return True
            if path.peek() in '.[':
                target._nbt_path_node_suffix_check(path)
                return target.has_path(path)

        # Assume qualifying {} is required.
        target_must_match = TagCompound.from_mojangson(path) # advances cursor; raises SyntaxError
        target._nbt_path_node_suffix_check(path)
        if target_must_match.is_subset(target):
            # Target node matches requirements
            return target.has_path(path)
        else:
            # Target node does not match requirements
            return False

    def at_path(self, path):
        path = self._nbt_path_node_prefix_check(path)
        if not path.can_read():
            return self

        target = self
        if path.peek() != '{':
            # Assume tag name
            child_tag_name = None
            if path.peek() == '[':
                # Wrong type, but could be correct for a sibling tag.
                raise LookupError("Cannot index into compound tag as list/array tag.")
            if path.peek() in ' .]}\\':
                raise SyntaxError("Unexpected character - expected { or valid tag name")

            if path.peek() == '"':
                # Quoted tag name (allows unusual characters; can't quote with ')
                path.skip()
                child_tag_name = path.read_string_until('"')
            else:
                # Assume unquoted tag name (different rules than normal unquoted string!)
                start = path.get_cursor()
                while path.can_read() and path.peek() not in ' .[]{}"':
                    path.skip()
                if path.can_read() and path.peek() not in '.[{':
                    raise SyntaxError(f'Unexpected character {path.peek()!r} at char {path.get_cursor()} - expected [, {{, or .')
                end = path.get_cursor()
                if start == end:
                    raise SyntaxError(f"Invalid unquoted tag name; can't start with {path.peek()}")
                child_tag_name = path.get_string()[start:end]

            if child_tag_name not in self.value:
                raise KeyError(f"{child_tag_name!r} not in {self.value.keys()!r}")
            target = self.value[child_tag_name]

            # Check if this is the end of the path node
            if not path.can_read():
                return target
            if path.peek() in '.[':
                target._nbt_path_node_suffix_check(path)
                return target.at_path(path)

        # Assume qualifying {} is required.
        target_must_match = TagCompound.from_mojangson(path) # advances cursor; raises SyntaxError
        target._nbt_path_node_suffix_check(path)
        if target_must_match.is_subset(target):
            # Target node matches requirements
            return target.at_path(path)
        else:
            # Target node does not match requirements
            raise KeyError(f"{target.to_mojangson(highlight=True)} does not match {target_must_match.to_mojangson(highlight=True)}")

    def count_multipath(self, path):
        path = self._nbt_path_node_prefix_check(path)
        if not path.can_read():
            return 1

        target = self
        if path.peek() != '{':
            # Assume tag name
            child_tag_name = None
            if path.peek() == '[':
                # Wrong type, but could be correct for a sibling tag.
                return 0
            if path.peek() in ' .]}\\':
                raise SyntaxError("Unexpected character - expected { or valid tag name")

            if path.peek() == '"':
                # Quoted tag name (allows unusual characters; can't quote with ')
                path.skip()
                child_tag_name = path.read_string_until('"')
            else:
                # Assume unquoted tag name (different rules than normal unquoted string!)
                start = path.get_cursor()
                while path.can_read() and path.peek() not in ' .[]{}"':
                    path.skip()
                if path.can_read() and path.peek() not in '.[{':
                    raise SyntaxError(f'Unexpected character {path.peek()!r} at char {path.get_cursor()} - expected [, {{, or .')
                end = path.get_cursor()
                if start == end:
                    raise SyntaxError(f"Invalid unquoted tag name; can't start with {path.peek()}")
                child_tag_name = path.get_string()[start:end]

            if child_tag_name not in self.value:
                return 0
            target = self.value[child_tag_name]

            # Check if this is the end of the path node
            if not path.can_read():
                return 1
            if path.peek() in '.[':
                target._nbt_path_node_suffix_check(path)
                return target.count_multipath(path)

        # Assume qualifying {} is required.
        target_must_match = TagCompound.from_mojangson(path) # advances cursor; raises SyntaxError
        target._nbt_path_node_suffix_check(path)
        if target_must_match.is_subset(target):
            # Target node matches requirements
            return target.count_multipath(path)
        else:
            # Target node does not match requirements
            return 0

    def iter_multipath_pair(self, path):
        path = self._nbt_path_node_prefix_check(path)
        if not path.can_read():
            yield ('', self)
            return

        child_tag_name_raw = ''
        target = self
        if path.peek() != '{':
            # Assume tag name
            child_tag_name = None
            if path.peek() == '[':
                # Wrong type, but could be correct for a sibling tag.
                return
            if path.peek() in ' .]}\\':
                raise SyntaxError("Unexpected character - expected { or valid tag name")

            if path.peek() == '"':
                # Quoted tag name (allows unusual characters; can't quote with ')
                start = path.get_cursor()
                path.skip()
                child_tag_name = path.read_string_until('"')
                end = path.get_cursor()
                child_tag_name_raw = path.get_string()[start:end]
            else:
                # Assume unquoted tag name (different rules than normal unquoted string!)
                start = path.get_cursor()
                while path.can_read() and path.peek() not in ' .[]{}"':
                    path.skip()
                if path.can_read() and path.peek() not in '.[{':
                    raise SyntaxError(f'Unexpected character {path.peek()!r} at char {path.get_cursor()} - expected [, {{, or .')
                end = path.get_cursor()
                if start == end:
                    raise SyntaxError(f"Invalid unquoted tag name; can't start with {path.peek()}")
                child_tag_name = path.get_string()[start:end]
                child_tag_name_raw = child_tag_name

            if child_tag_name not in self.value:
                return
            target = self.value[child_tag_name]

            # Check if this is the end of the path node
            if not path.can_read():
                yield (child_tag_name_raw, target)
                return
            if path.peek() in '.[':
                target._nbt_path_node_suffix_check(path)
                for subpath, tag in target.iter_multipath_pair(path):
                    yield (nbt_path_join(child_tag_name_raw, subpath), tag)
                return

        # Assume qualifying {} is required.
        target_must_match = TagCompound.from_mojangson(path) # advances cursor; raises SyntaxError
        target._nbt_path_node_suffix_check(path)
        if target_must_match.is_subset(target):
            # Target node matches requirements
            for subpath, tag in target.iter_multipath_pair(path):
                yield (nbt_path_join(child_tag_name_raw, subpath), tag)
            return
        else:
            # Target node does not match requirements
            return

    @classmethod
    def from_mojangson(cls, json):
        """Convert a Mojangson string into NBT"""
        return MojangsonParser(json).parse_compound()

class MojangsonParser(object):
    """Convert MojangSON into Quarry's NBT format.

    Example: {display:{Name:"{\"text\":\"Excaliber\"}"}}
    """

    regexDoubleNoSuffix = re.compile(r'''^[-+]?([0-9]+[.]|[0-9]*[.][0-9]+)(e[-+]?[0-9]+)?$''', re.IGNORECASE)
    regexDouble         = re.compile(r'''^[-+]?([0-9]+[.]?|[0-9]*[.][0-9]+)(e[-+]?[0-9]+)?d$''', re.IGNORECASE)
    regexFloat          = re.compile(r'''^[-+]?([0-9]+[.]?|[0-9]*[.][0-9]+)(e[-+]?[0-9]+)?f$''', re.IGNORECASE)
    regexByte           = re.compile(r'''^[-+]?(0|[1-9][0-9]*)b$''', re.IGNORECASE)
    regexLong           = re.compile(r'''^[-+]?(0|[1-9][0-9]*)l$''', re.IGNORECASE)
    regexShort          = re.compile(r'''^[-+]?(0|[1-9][0-9]*)s$''', re.IGNORECASE)
    regexInt            = re.compile(r'''^[-+]?(0|[1-9][0-9]*)$''', re.IGNORECASE)

    def __init__(self, json):
        if isinstance(json, StringReader):
            self.reader = json
        else:
            self.reader = StringReader(json)

        #########################################################
        # Set to True for verbose parsing - useful to find errors
        self.debug = False

    def parse_key_string(self):
        if self.debug:
            print("parse_key_string")

        self.reader.skip_whitespace()
        if not self.reader.can_read():
            self.raise_error("Failed to parse TagCompound key")
        else:
            return self.reader.read_string()

    def parse_literal(self, literal_str):
        if self.debug:
            print("parse_literal")

        if self.regexFloat.match(literal_str):
            return TagFloat(float(literal_str[:-1]))

        if self.regexByte.match(literal_str):
            return TagByte(int(literal_str[:-1]))

        if self.regexLong.match(literal_str):
            return TagLong(int(literal_str[:-1]))

        if self.regexShort.match(literal_str):
            return TagShort(int(literal_str[:-1]))

        if self.regexInt.match(literal_str):
            return TagInt(int(literal_str))

        if self.regexDouble.match(literal_str):
            return TagDouble(float(literal_str[:-1]))

        if self.regexDoubleNoSuffix.match(literal_str):
            return TagDouble(float(literal_str))

        if "true" == literal_str.lower():
            return TagByte(1)

        if "false" == literal_str.lower():
            return TagByte(0)

        return TagString(literal_str)

    def parse_literal_or_string(self):
        if self.debug:
            print("parse_literal_or_string")

        self.reader.skip_whitespace()
        orig_pos = self.reader.get_cursor()

        if StringReader.is_quoted_string_start(self.reader.peek()):
            return TagString(self.reader.read_quoted_string())
        else:
            val = self.reader.read_unquoted_string()

            if not val:
                self.reader.set_cursor(orig_pos)
                self.raise_error("Failed to parse literal or string value")
            else:
                return self.parse_literal(val)

    def parse_any_tag(self):
        if self.debug:
            print("parse_any_tag")

        self.reader.skip_whitespace()
        if not self.reader.can_read():
            self.raise_error("Failed while parsing value")
        else:
            nextChar = self.reader.peek()
            if nextChar == '{':
                return self.parse_compound()
            elif nextChar == '[':
                return self.parse_array()
            else:
                return self.parse_literal_or_string()

    def parse_array(self):
        if self.debug:
            print("parse_array")

        if self.reader.can_read(3) and (not StringReader.is_quoted_string_start(self.reader.peek(1))) and self.reader.peek(2) == ';':
            return self.parse_typed_numeric_array()
        else:
            return self.parse_non_numeric_array()

    def parse_non_numeric_array(self):
        if self.debug:
            print("parse_non_numeric_array")

        self.advance_and_fail_if_next_is_not('[')
        self.reader.skip_whitespace()
        if not self.reader.can_read():
            self.raise_error("Failed to parse non-numeric array")
        else:
            nbt_list = []
            item_type = None

            while self.reader.peek() != ']':
                orig_pos = self.reader.get_cursor()
                new_value = self.parse_any_tag()
                new_type = type(new_value)

                if not item_type:
                    item_type = new_type
                elif item_type != new_type:
                    self.reader.set_cursor(orig_pos)
                    self.raise_error(f"Mixed types in list! {item_type!s} != {new_type!s}")

                nbt_list.append(new_value)
                if not self.seek_to_next_comma_delim_element():
                    break

                if not self.reader.can_read():
                    self.raise_error("Unexpected end of array")

            self.advance_and_fail_if_next_is_not(']')
            return TagList(nbt_list)

    def parse_typed_numeric_array(self):
        if self.debug:
            print("parse_typed_numeric_array")

        self.advance_and_fail_if_next_is_not('[')
        orig_pos = self.reader.get_cursor()
        first_char = self.reader.read()

        # Read ;
        self.reader.read()
        self.reader.skip_whitespace()
        if not self.reader.can_read():
            self.raise_error("Unexpected end of numeric array")
        elif first_char == 'B':
            return TagByteArray(PackedArray.from_int_list(self.parse_numeric_array_as_type(TagByte), 8))
        elif first_char == 'L':
            return TagLongArray(PackedArray.from_int_list(self.parse_numeric_array_as_type(TagLong), 64))
        elif first_char == 'I':
            return TagIntArray(PackedArray.from_int_list(self.parse_numeric_array_as_type(TagInt), 32))
        else:
            self.reader.set_cursor(orig_pos)
            self.raise_error(f"Unexpected type character {first_char!r} in numeric array")

    def parse_numeric_array_as_type(self, item_type):
        if self.debug:
            print("parse_numeric_array_as_type")

        array = []

        while True:
            if self.reader.peek() != ']':
                orig_pos = self.reader.get_cursor()
                new_value = self.parse_any_tag()
                new_type = type(new_value)

                if new_type != item_type:
                    self.reader.set_cursor(orig_pos)
                    self.raise_error(f"Mixed types in list! {item_type!s} != {new_type!s}")

                # Important! Numeric arrays just contain numbers! (i.e. not an array of TagInt)
                array.append(new_value.value)

                if self.seek_to_next_comma_delim_element():
                    if not self.reader.can_read():
                        self.raise_error("Unexpected end of numeric array elements")
                    continue

            self.advance_and_fail_if_next_is_not(']')
            return array

    def parse_compound(self):
        if self.debug:
            print("parse_compound")

        self.advance_and_fail_if_next_is_not('{')
        compound = collections.OrderedDict()

        self.reader.skip_whitespace()

        while self.reader.can_read() and self.reader.peek() != '}':
            orig_pos = self.reader.get_cursor()
            key = self.parse_key_string()

            if not key:
                self.reader.set_cursor(orig_pos)
                self.raise_error("Failed to parse TagCompound key")

            self.advance_and_fail_if_next_is_not(':')

            if self.debug:
                print(f"Parsing value of {key!r} - value: {self.reader.string[self.reader.cursor:]!r}")

            compound[key] = self.parse_any_tag()
            if not self.seek_to_next_comma_delim_element():
                break

            if not self.reader.can_read():
                self.raise_error("Failed to parse TagCompound element")

        self.advance_and_fail_if_next_is_not('}')
        return TagCompound(compound)

    def seek_to_next_comma_delim_element(self):
        if self.debug:
            print("seek_to_next_comma_delim_element")

        self.reader.skip_whitespace()
        if self.reader.can_read() and self.reader.peek() == ',':
            self.reader.skip()
            self.reader.skip_whitespace()
            return True
        else:
            return False

    def advance_and_fail_if_next_is_not(self, char):
        if self.debug:
            print(f"advance_and_fail_if_next_is_not: {char!r}")

        self.reader.skip_whitespace()
        self.reader.expect(char)

    def raise_error(self, msg):
        raise SyntaxError(f"msg at ->{self.reader.string[self.reader.cursor:]}")

class TagRoot(TagCompound):
    __slots__ = ()
    root = True

    @classmethod
    def from_body(cls, body):
        return cls({u"": body})

    @property
    def body(self):
        return self.value[u""]


# Register tags ---------------------------------------------------------------

_kinds[0] = type(None)
_kinds[1] = TagByte
_kinds[2] = TagShort
_kinds[3] = TagInt
_kinds[4] = TagLong
_kinds[5] = TagFloat
_kinds[6] = TagDouble
_kinds[7] = TagByteArray
_kinds[8] = TagString
_kinds[9] = TagList
_kinds[10] = TagCompound
_kinds[11] = TagIntArray
_kinds[12] = TagLongArray
_ids.update({v: k for k, v in _kinds.items()})


# Files -----------------------------------------------------------------------

class NBTFile(object):
    root_tag = None

    def __init__(self, root_tag):
        self.root_tag = root_tag

    @classmethod
    def load(cls, path):
        with gzip.open(path, 'rb') as fd:
            return cls(TagRoot.from_bytes(fd.read()))

    def save(self, path):
        with gzip.open(path, 'wb') as fd:
            fd.write(self.root_tag.to_bytes())


class RegionFile(object):
    """
    Experimental support for the Minecraft world storage format (``.mca``).
    """
    def __init__(self, path, read_only=False):
        self.fd = open(path, "rb" if read_only else "r+b")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.fd.close()

    def close(self):
        """
        Closes the region file.
        """
        self.fd.close()

    def save_chunk(self, chunk):
        """
        Saves the given chunk, which should be a ``TagRoot``, to the region
        file.
        """

        # Compress chunk
        chunk_x = chunk.body.value["Level"].value["xPos"].value & 0x1f
        chunk_z = chunk.body.value["Level"].value["zPos"].value & 0x1f
        chunk = zlib.compress(chunk.to_bytes())
        chunk = Buffer.pack('IB', len(chunk), 2) + chunk
        chunk_length = 1 + (len(chunk) - 1) // 4096

        # Load extents
        extents = [(0, 2)]
        self.fd.seek(0)
        buff = Buffer(self.fd.read(4096))
        for idx in range(1024):
            z, x = divmod(idx, 32)
            entry = buff.unpack('I')
            offset, length = entry >> 8, entry & 0xFF
            if offset > 0 and not (x == chunk_x and z == chunk_z):
                extents.append((offset, length))
        extents.sort()
        extents.append((extents[-1][0] + extents[-1][1] + chunk_length, 0))

        # Compute new extent
        for idx in range(len(extents) - 1):
            start = extents[idx][0] + extents[idx][1]
            end = extents[idx+1][0]
            if (end - start) >= chunk_length:
                chunk_offset = start
                extents.insert(idx+1, (chunk_offset, chunk_length))
                break

        # Write extent header
        self.fd.seek(4 * (32 * chunk_z + chunk_x))
        self.fd.write(Buffer.pack(
            'I', (chunk_offset << 8) | (chunk_length & 0xFF)))

        # Write timestamp header
        self.fd.seek(4096 + 4 * (32 * chunk_z + chunk_x))
        self.fd.write(Buffer.pack('I', int(time.time())))

        # Write chunk
        self.fd.seek(4096 * chunk_offset)
        self.fd.write(chunk)

        # Truncate file
        self.fd.seek(4096 * extents[-1][0])
        self.fd.truncate()

    def list_chunks(self):
        """
        Returns a list of (cx, cz) tuples for all existing chunks.
        """

        result = []

        for chunk_z in range(32):
            for chunk_x in range(32):
                # Read extent header
                self.fd.seek(4 * (32 * chunk_z + chunk_x))
                entry = Buffer(self.fd.read(4)).unpack('I')
                chunk_offset = entry >> 8

                if chunk_offset:
                    result.append((chunk_x,chunk_z))

        return result


    def load_chunk(self, chunk_x, chunk_z):
        """
        Loads the chunk at the given co-ordinates from the region file.
        The co-ordinates should range from 0 to 31. Returns a ``TagRoot``.
        If no chunk is found, returns None.
        """

        buff = Buffer()

        # Read extent header
        self.fd.seek(4 * (32 * chunk_z + chunk_x))
        buff.add(self.fd.read(4))
        entry = buff.unpack('I')
        chunk_offset, chunk_length = entry >> 8, entry & 0xFF
        if chunk_offset == 0:
            #raise ValueError((chunk_x, chunk_z))
            return None

        if entry:
            # Read chunk
            self.fd.seek(4096 * chunk_offset)
            buff.add(self.fd.read(4096 * chunk_length))
            compressed_size, compression_format = buff.unpack('IB')
            # Fix off-by-one when reading
            compressed_size = min(compressed_size, len(buff))

            chunk = buff.read(compressed_size)
            chunk = zlib.decompress(chunk)
            chunk = TagRoot.from_bytes(chunk)
            return chunk
        else:
            # No chunk at that location
            return None


    def restore_chunk(self, old_region, chunk_x, chunk_z):
        """
        Restore the same chunk from an older region file as fast as possible.

        Returns True if successful, otherwise False.
        """
        buff = Buffer()

        # Read extent header
        old_region.fd.seek(4 * (32 * chunk_z + chunk_x))
        buff.add(old_region.fd.read(4))
        entry = buff.unpack('I')
        chunk_offset, chunk_length = entry >> 8, entry & 0xFF

        if not entry:
            # TODO Delete the chunk in the new region file.
            return False

        # Read chunk
        old_region.fd.seek(4096 * chunk_offset)
        buff.add(old_region.fd.read(4096 * chunk_length))
        old_chunk = buff.read(buff.unpack('IB')[0])

        # Skip decompression/unpacking/packing/compression

        # Delete any variables that shouldn't carry forward.
        del buff
        del entry
        del chunk_offset

        # Save the region file back.
        chunk = Buffer.pack('IB', len(old_chunk), 2) + old_chunk

        # Load extents (The header and chunk offsets and lengths, ignoring the chunk being saved.)
        extents = [(0, 2)]
        self.fd.seek(0)
        buff = Buffer(self.fd.read(4096))
        for idx in range(1024):
            z, x = divmod(idx, 32)
            entry = buff.unpack('I')
            offset, length = entry >> 8, entry & 0xFF
            if offset > 0 and not (x == chunk_x and z == chunk_z):
                extents.append((offset, length))
        extents.sort()
        extents.append((extents[-1][0] + extents[-1][1] + chunk_length, 0))

        # Compute new extent
        for idx in range(len(extents) - 1):
            start = extents[idx][0] + extents[idx][1]
            end = extents[idx+1][0]
            if (end - start) >= chunk_length:
                chunk_offset = start
                extents.insert(idx+1, (chunk_offset, chunk_length))
                break

        # Write extent header
        self.fd.seek(4 * (32 * chunk_z + chunk_x))
        self.fd.write(Buffer.pack(
            'I', (chunk_offset << 8) | (chunk_length & 0xFF)))

        # Write timestamp header
        self.fd.seek(4096 + 4 * (32 * chunk_z + chunk_x))
        self.fd.write(Buffer.pack('I', int(time.time())))

        # Write chunk
        self.fd.seek(4096 * chunk_offset)
        self.fd.write(chunk)

        # Truncate file
        self.fd.seek(4096 * extents[-1][0])
        self.fd.truncate()

        return True

    def load_chunk_section(self, chunk_x, chunk_y, chunk_z):
        """
        Loads the chunk section at the given co-ordinates from the region file.
        The co-ordinates should range from 0 to 31. Returns a ``TagRoot``.
        """

        chunk = self.load_chunk(chunk_x, chunk_z)
        sections = chunk.body.value["Level"].value["Sections"].value
        for section in sections:
            if section.value["Y"].value == chunk_y:
                return chunk, section

        raise ValueError((chunk_x, chunk_y, chunk_z))


# Debug -----------------------------------------------------------------------

def alt_repr(tag, level=0):
    """
    Returns a human-readable representation of a tag using the same format as
    used the NBT specification.
    """
    name = lambda kind: type(kind).__name__.replace("Tag", "TAG_")

    if isinstance(tag, _ArrayTag):
        return "%s%s: %d entries" % (
            "  " * level,
            name(tag),
            len(tag.value))

    elif isinstance(tag, TagList):
        return "%s%s: %d entries\n%s{\n%s\n%s}" % (
            "  " * level,
            name(tag),
            len(tag.value),
            "  " * level,
            u"\n".join(alt_repr(tag, level+1) for tag in tag.value),
            "  " * level)

    elif isinstance(tag, TagRoot):
        return u"\n".join(
                alt_repr(tag, level).replace(': ', '("%s"): ' % name, 1)
                for name, tag in tag.value.items())

    elif isinstance(tag, TagCompound):
        return "%s%s: %d entries\n%s{\n%s\n%s}" % (
            "  " * level,
            name(tag),
            len(tag.value),
            "  " * level,
            u"\n".join(
                alt_repr(tag, level+1).replace(': ', '("%s"): ' % name, 1)
                for name, tag in tag.value.items()),
            "  " * level)

    elif isinstance(tag, TagString):
        return '%s%s: "%s"' % (
            "  " * level,
            name(tag),
            tag.value)

    else:
        return "%s%s: %r" % (
            "  " * level,
            name(tag),
            tag.value)
