import collections
import functools
import gzip
import re
import time
import zlib

from brigadier.string_reader import StringReader
from quarry.types.buffer import Buffer
from quarry.types.text_format import ansify_text, get_format, unformat_text

_kinds = {}
_ids = {}

# Base types ------------------------------------------------------------------

@functools.total_ordering
class _Tag(object):
    def __init__(self, value):
        self.value = value

    prefix = ('',get_format('gold').ansi_code)
    postfix = ('',get_format('reset').ansi_code)

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

    def to_mojangson(self,sort=None,highlight=False):
        prefix = self.prefix[highlight]
        postfix = self.postfix[highlight]

        return prefix + str(self.value) + postfix

    def tree(self,sort=None,indent='    ',level=0):
        prefix = self.prefix[True]
        postfix = self.postfix[True]

        result = prefix + str(self.value) + postfix
        if level == 0:
            print(result)
        else:
            return result


class _DataTag(_Tag):
    fmt = None

    @classmethod
    def from_buff(cls, buff):
        return cls(buff.unpack(cls.fmt))

    def to_bytes(self):
        return Buffer.pack(self.fmt, self.value)

    def is_subset(self,other):
        return self.value == other.value

    def has_path(self,path):
        if len(path) == 0:
            return True
        else:
            return False

    def at_path(self,path):
        if len(path) == 0:
            return self
        else:
            raise KeyError( str(type(self)) + ' cannot contain other tags, and is the end of path "' + path + '"' )


class _ArrayTag(_Tag):
    fmt = None
    separator = (',',get_format('white').ansi_code+', ')

    def __len__(self):
        return len(self.value)

    @classmethod
    def from_buff(cls, buff):
        array_length = buff.unpack('i')
        return cls(list(buff.unpack_array(cls.fmt, array_length)))

    def to_bytes(self):
        return (
            Buffer.pack('i', len(self.value)) +
            Buffer.pack_array(self.fmt, self.value))

    def is_subset(self,other):
        if (
            type(other) != type(self) or
            len(other) != len(self)
        ):
            return False
        for i in range(len(self.value)):
            if self.value[i] != other.value[i]:
                return False
        return True

    def to_mojangson(self,sort=None,highlight=False):
        prefix = self.prefix[highlight]
        separator = self.separator[highlight]
        type_postfix = self.type_postfix[highlight]
        postfix = self.postfix[highlight]

        inner_mojangson = []
        for content in self.value:
            inner_mojangson.append( str(content) + type_postfix )
        return prefix + separator.join(inner_mojangson) + postfix

    def tree(self,sort=None,indent='    ',level=0):
        prefix = self.prefix[True]
        separator = self.separator[True]
        type_postfix = self.type_postfix[True]
        postfix = self.postfix[True]

        inner_mojangson = []
        for content in self.value:
            inner_mojangson.append( str(content) + type_postfix )

        if len(inner_mojangson) <= 8:
            result = prefix + separator.join(inner_mojangson) + postfix
        else:
            result = prefix + separator.join(inner_mojangson[:8]) + separator + '...(' + str(len(inner_mojangson)) + ' entries total)' + postfix

        if level == 0:
            print(result)
        else:
            return result

    def has_path(self,path):
        if not isinstance(path,StringReader):
            path = StringReader(path)
        if path.peek() == '[':
            path = path[1:]
        if not ']' in path:
            return False
        if path.find(']') + 1 != len(path):
            return False
        path = path[:-1]

        index = -1
        try:
            index = int(path)
        except:
            return False

        if index < 0 or index >= len(self.value):
            return False

        return True

    def at_path(self,path):
        if path.startswith('['):
            path = path[1:]
        if not ']' in path:
            raise IndexError( '] not in path "' + path + '"' )
        if path.find(']') + 1 != len(path):
            raise IndexError( str(type(self)) + ' cannot contain other tags, and is the end of path "' + path + '"' )
        path = path[:-1]

        index = -1
        try:
            index = int(path)
        except:
            raise IndexError( 'Path index is not an integer: ' + path )

        if index < 0 or index >= len(self.value):
            raise IndexError( 'Index ' + str(index) + ' not in range (' + str(len(self.value)) + ' entries)' )

        return self.value[index]


# NBT tags --------------------------------------------------------------------

class TagByte(_DataTag):
    fmt = 'b'
    postfix = ('b',get_format('red').ansi_code+'b'+get_format('reset').ansi_code)


class TagShort(_DataTag):
    fmt = 'h'
    postfix = ('s',get_format('red').ansi_code+'s'+get_format('reset').ansi_code)


class TagInt(_DataTag):
    fmt = 'i'
    postfix = ('',get_format('reset').ansi_code)


class TagLong(_DataTag):
    fmt = 'q'
    postfix = ('L',get_format('red').ansi_code+'L'+get_format('reset').ansi_code)


class TagFloat(_DataTag):
    fmt = 'f'
    postfix = ('f',get_format('red').ansi_code+'f'+get_format('reset').ansi_code)


class TagDouble(_DataTag):
    fmt = 'd'
    postfix = ('d',get_format('red').ansi_code+'d'+get_format('reset').ansi_code)


class TagString(_Tag):
    prefix = ('"',get_format('white').ansi_code+'"'+get_format('green').ansi_code)
    postfix = ('"',get_format('white').ansi_code+'"'+get_format('reset').ansi_code)

    @classmethod
    def from_buff(cls, buff):
        string_length = buff.unpack('H')
        return cls(buff.read(string_length).decode('utf8'))

    def to_bytes(self):
        data = self.value.encode('utf8')
        return Buffer.pack('H', len(data)) + data

    def is_subset(self,other):
        return self.value == other.value

    def to_mojangson(self,sort=None,highlight=False):
        prefix = self.prefix[highlight]
        postfix = self.postfix[highlight]

        text = self.value.replace('\\','\\\\').replace('\n','\\n"').replace('"','\\"')
        if highlight:
            text = ansify_text(text,show_section=True)
        return prefix + text + postfix

    def tree(self,sort=None,indent='    ',level=0):
        prefix = self.prefix[True]
        postfix = self.postfix[True]

        text = self.value.replace('\\','\\\\').replace('\n','\\n"').replace('"','\\"')
        text = ansify_text(text,show_section=True)
        result = prefix + text + postfix

        if level == 0:
            print(result)
        else:
            return result


class TagByteArray(_ArrayTag):
    fmt = 'b'
    prefix = ('[B;',get_format('white').ansi_code+'['+get_format('red').ansi_code+'B'+get_format('white').ansi_code+'; '+get_format('gold').ansi_code)
    postfix = (']',get_format('white').ansi_code+']'+get_format('reset').ansi_code)
    separator = (',',get_format('white').ansi_code+', '+get_format('gold').ansi_code)
    type_postfix = ('b',get_format('red').ansi_code+'b')


class TagIntArray(_ArrayTag):
    fmt = 'i'
    prefix = ('[I;',get_format('white').ansi_code+'['+get_format('red').ansi_code+'I'+get_format('white').ansi_code+'; '+get_format('gold').ansi_code)
    postfix = (']',get_format('white').ansi_code+']'+get_format('reset').ansi_code)
    separator = (',',get_format('white').ansi_code+', '+get_format('gold').ansi_code)
    type_postfix = ('','')


class TagLongArray(_ArrayTag):
    fmt = 'q'
    prefix = ('[L;',get_format('white').ansi_code+'['+get_format('red').ansi_code+'L'+get_format('white').ansi_code+'; '+get_format('gold').ansi_code)
    postfix = (']',get_format('white').ansi_code+']'+get_format('reset').ansi_code)
    separator = (',',get_format('white').ansi_code+', '+get_format('gold').ansi_code)
    type_postfix = ('l',get_format('red').ansi_code+'l')


class TagUnsignedLongArray(_ArrayTag):
    fmt = 'Q'
    prefix = ('[L;',get_format('white').ansi_code+'['+get_format('red').ansi_code+'L'+get_format('white').ansi_code+'; '+get_format('gold').ansi_code)
    postfix = (']',get_format('white').ansi_code+']'+get_format('reset').ansi_code)
    separator = (',',get_format('white').ansi_code+', '+get_format('gold').ansi_code)
    type_postfix = ('l',get_format('red').ansi_code+'l')


class TagList(_Tag):
    prefix = ('[',get_format('white').ansi_code+'['+get_format('gold').ansi_code)
    postfix = (']',get_format('white').ansi_code+']'+get_format('reset').ansi_code)
    separator = (',',get_format('white').ansi_code+', '+get_format('gold').ansi_code)

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

    def is_subset(self,other):
        if type(other) != TagList:
            return False
        for self_value in self.value:
            if not any(self_value.is_subset(other_value) for other_value in other.value):
                return False
        return True

    def to_mojangson(self,sort=None,highlight=False):
        prefix = self.prefix[highlight]
        separator = self.separator[highlight]
        postfix = self.postfix[highlight]

        inner_mojangson = []
        for content in self.value:
            inner_mojangson.append(content.to_mojangson(sort,highlight))
        return prefix + separator.join(inner_mojangson) + postfix

    def tree(self,sort=None,indent='    ',level=0):
        prefix = self.prefix[True] + '\n'
        separator = self.separator[True] + '\n'
        postfix = indent*level + self.postfix[True]

        inner_mojangson = []
        for content in self.value:
            inner_mojangson.append( indent*(level+1) + content.tree(sort,indent,level+1) )

        if len(inner_mojangson) == 0:
            result = prefix + postfix
        else:
            result = prefix + separator.join(inner_mojangson) + '\n' + postfix

        if level == 0:
            print(result)
        else:
            return result

    def has_path(self,path):
        if path.startswith('['):
            path = path[1:]
        if not ']' in path:
            return False

        split_index = path.find(']')
        array_index = path[:split_index]
        path = path[split_index+1:]

        try:
            array_index = int(array_index)
        except:
            return False

        if array_index < 0 or array_index >= len(self.value):
            return False

        if path == '':
            return True
        else:
            return self.value[array_index].has_path(path)

    def at_path(self,path):
        if path.startswith('['):
            path = path[1:]
        if not ']' in path:
            raise IndexError( '] not in path "' + path + '"' )

        split_index = path.find(']')
        array_index = path[:split_index]
        path = path[split_index+1:]

        try:
            array_index = int(array_index)
        except:
            raise IndexError( 'Array index is not an integer: ' + array_index )

        if array_index < 0 or array_index >= len(self.value):
            raise IndexError( 'Index ' + str(array_index) + ' not in range (' + str(len(self.value)) + ' entries)' )

        if path == '':
            return self.value[array_index]
        else:
            return self.value[array_index].at_path(path)


class TagCompound(_Tag):
    root = False
    preserve_order = True
    prefix = ('{',get_format('white').ansi_code+'{'+get_format('gold').ansi_code)
    postfix = ('}',get_format('white').ansi_code+'}'+get_format('reset').ansi_code)
    separator = (',',get_format('white').ansi_code+', '+get_format('gold').ansi_code)
    key_value_separator = (':',get_format('white').ansi_code+': ')

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

            # ~~ Evil Hack Alert ~~
            # Signed bitwise arithmetic in Python is simultaneously elegant and
            # baffling. Special-case the BlockStates array to use /unsigned/
            # integers, in contravention of spec, but to the great relief of
            # this programmer.
            if kind is TagLongArray and name == "BlockStates":
                kind = TagUnsignedLongArray

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

    def is_subset(self,other):
        if type(other) != TagCompound:
            return False
        for aKey in self.value.keys():
            if (
                aKey not in other.value.keys() or
                not self.value[aKey].is_subset(other.value[aKey])
            ):
                return False
        return True

    def to_mojangson(self,sort=None,highlight=False):
        prefix = self.prefix[highlight]
        key_value_separator = self.key_value_separator[highlight]
        separator = self.separator[highlight]
        postfix = self.postfix[highlight]

        if type(sort) in (list,tuple):
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
            inner_mojangson.append( key + key_value_separator + content.to_mojangson(sort,highlight) )
        return prefix + separator.join(inner_mojangson) + postfix

    def tree(self,sort=None,indent='    ',level=0):
        prefix = self.prefix[True] + '\n'
        key_value_separator = self.key_value_separator[True]
        separator = self.separator[True] + '\n'
        postfix = indent*level + self.postfix[True]

        if type(sort) in (list,tuple):
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
            inner_mojangson.append( indent*(level+1) + key + key_value_separator + content.tree(sort,indent,level+1) )

        if len(inner_mojangson) == 0:
            result = prefix + postfix
        else:
            result = prefix + separator.join(inner_mojangson) + '\n' + postfix

        if level == 0:
            print(result)
        else:
            return result

    def has_path(self,path):
        if path.startswith('.') or path.startswith('['):
            return False
        split_index = len(path)
        bracket_index = path.find('[')
        dot_index = path.find('.')

        if dot_index >= 0:
            split_index = min(split_index,dot_index)
        if bracket_index >= 0:
            split_index = min(split_index,bracket_index)

        key = path[:split_index]
        path = path[split_index+1:]
        if key not in self.value:
            return False

        if path == '':
            return True
        else:
            return self.value[key].has_path(path)

    def at_path(self,path):
        if path.startswith('.') or path.startswith('['):
            raise KeyError( path + 'not in ' + str(self.value.keys()) )
        split_index = len(path)
        bracket_index = path.find('[')
        dot_index = path.find('.')

        if dot_index >= 0:
            split_index = min(split_index,dot_index)
        if bracket_index >= 0:
            split_index = min(split_index,bracket_index)

        key = path[:split_index]
        path = path[split_index+1:]
        if key not in self.value:
            raise KeyError( 'key "' + key + '" not in ' + str(self.value.keys()) )

        if path == '':
            return self.value[key]
        else:
            return self.value[key].at_path(path)

    @classmethod
    def from_mojangson(cls, json):
        """
        Convert a Mojangson string into NBT
        """
        class MojangsonParser(object):
            """
            Convert MojangSON such as {display:{Name:"{\"text\":\"Excaliber\"}"}}
            into Quarry's NBT format
            """

            regexFloat          = re.compile(r'''[-+]?(?:[0-9]*[.][0-9]+|[0-9]+[.]?)(?:e[-+]?[0-9]+)?f$''', re.IGNORECASE)
            regexDouble         = re.compile(r'''[-+]?(?:[0-9]*[.][0-9]+|[0-9]+[.]?)(?:e[-+]?[0-9]+)?d$''', re.IGNORECASE)
            regexDoubleNoSuffix = re.compile(r'''[-+]?(?:[0-9]*[.][0-9]+|[0-9]+[.])(?:e[-+]?[0-9]+)?$''', re.IGNORECASE)
            regexByte           = re.compile(r'''[-+]?(?:0|[1-9][0-9]*)b$''', re.IGNORECASE)
            regexShort          = re.compile(r'''[-+]?(?:0|[1-9][0-9]*)s$''', re.IGNORECASE)
            regexInt            = re.compile(r'''[-+]?(?:0|[1-9][0-9]*)$''', re.IGNORECASE)
            regexLong           = re.compile(r'''[-+]?(?:0|[1-9][0-9]*)l$''', re.IGNORECASE)

            def __init__(self, json):
                self.reader = StringReader(json)

                #########################################################
                # Set to True for verbose parsing - useful to find errors
                self.debug = False

            def parse_key_string(self):
                if self.debug:
                    print("parse_key_string")

                self.reader.skipWhitespace()
                if not self.reader.canRead():
                    self.raise_error("Failed to parse TagCompound key")
                else:
                    return self.reader.readString()

            # TODO: Need to check these sizes correctly!
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

                self.reader.skipWhitespace()
                orig_pos = self.reader.getCursor()

                if self.reader.peek() == '"':
                    return TagString(self.reader.readQuotedString())
                else:
                    val = self.reader.readUnquotedString()

                    if not val:
                        self.reader.setCursor(i)
                        self.raise_error("Failed to parse literal or string value")
                    else:
                        return self.parse_literal(val)

            def parse_any_tag(self):
                if self.debug:
                    print("parse_any_tag")

                self.reader.skipWhitespace()
                if not self.reader.canRead():
                    self.raise_error("Failed while parsing value")
                else:
                    nextChar = self.reader.peek()
                    if nextChar == '{':
                        return self.parse_compound()
                    else:
                        if nextChar == '[':
                            return self.parse_array()
                        else:
                            return self.parse_literal_or_string()

            def parse_array(self):
                if self.debug:
                    print("parse_array")

                if self.reader.canRead(3) and self.reader.peek(1) != '"' and self.reader.peek(2) == ';':
                    return self.parse_typed_numeric_array()
                else:
                    return self.parse_non_numeric_array()

            def parse_non_numeric_array(self):
                if self.debug:
                    print("parse_non_numeric_array")

                self.advance_and_fail_if_next_is_not('[')
                self.reader.skipWhitespace()
                if not self.reader.canRead():
                    self.raise_error("Failed to parse non-numeric array")
                else:
                    nbt_list = []
                    item_type = None

                    while self.reader.peek() != ']':
                        orig_pos = self.reader.getCursor()
                        new_value = self.parse_any_tag()
                        new_type = type(new_value)

                        if not item_type:
                            item_type = new_type
                        elif item_type != new_type:
                            self.reader.setCursor(orig_pos)
                            self.raise_error("Mixed types in list! " + str(item_type) + " != " + str(new_type))

                        nbt_list.append(new_value)
                        if not self.seek_to_next_comma_delim_element():
                            break

                        if not self.reader.canRead():
                            self.raise_error("Unexpected end of array")

                    self.advance_and_fail_if_next_is_not(']')
                    return TagList(nbt_list)

            def parse_typed_numeric_array(self):
                if self.debug:
                    print("parse_typed_numeric_array")

                self.advance_and_fail_if_next_is_not('[')
                orig_pos = self.reader.getCursor()
                first_char = self.reader.read()

                # Read ;
                self.reader.read()
                self.reader.skipWhitespace()
                if not self.reader.canRead():
                    self.raise_error("Unexpected end of numeric array")
                elif first_char == 'B':
                    return TagByteArray(self.parse_numeric_array_as_type(TagByte))
                elif first_char == 'L':
                    return TagLongArray(self.parse_numeric_array_as_type(TagLong))
                elif first_char == 'I':
                    return TagIntArray(self.parse_numeric_array_as_type(TagInt))
                else:
                    self.reader.setCursor(orig_pos)
                    self.raise_error("Unexpected type character '" + first_char + "' in numeric array")

            def parse_numeric_array_as_type(self, item_type):
                if self.debug:
                    print("parse_numeric_array_as_type")

                array = []

                while True:
                    if self.reader.peek() != ']':
                        orig_pos = self.reader.getCursor()
                        new_value = self.parse_any_tag()
                        new_type = type(new_value)

                        if new_type != item_type:
                            self.reader.setCursor(orig_pos)
                            self.raise_error("Mixed types in list! " + str(item_type) + " != " + str(new_type))

                        # Important! Numeric arrays just contain numbers! (i.e. not an array of TagInt)
                        array.append(new_value.value)

                        if self.seek_to_next_comma_delim_element():
                            if not self.reader.canRead():
                                self.raise_error("Unexpected end of numeric array elements")
                            continue

                    self.advance_and_fail_if_next_is_not(']')
                    return array

            def parse_compound(self):
                if self.debug:
                    print("parse_compound")

                self.advance_and_fail_if_next_is_not('{')
                compound = collections.OrderedDict()

                self.reader.skipWhitespace()

                while self.reader.canRead() and self.reader.peek() != '}':
                    orig_pos = self.reader.getCursor()
                    key = self.parse_key_string()

                    if not key:
                        self.reader.setCursor(orig_pos)
                        self.raise_error("Failed to parse TagCompound key")

                    self.advance_and_fail_if_next_is_not(':')

                    if self.debug:
                        print("Parsing value of '" + key + "' - value: '" + self.reader.string[self.reader.cursor:] + "'")

                    compound[key] = self.parse_any_tag()
                    if not self.seek_to_next_comma_delim_element():
                        break

                    if not self.reader.canRead():
                        self.raise_error("Failed to parse TagCompound element")

                self.advance_and_fail_if_next_is_not('}')
                return TagCompound(compound)


            def seek_to_next_comma_delim_element(self):
                if self.debug:
                    print("seek_to_next_comma_delim_element")

                self.reader.skipWhitespace()
                if self.reader.canRead() and self.reader.peek() == ',':
                    self.reader.skip()
                    self.reader.skipWhitespace()
                    return True
                else:
                    return False

            def advance_and_fail_if_next_is_not(self, char):
                if self.debug:
                    print("advance_and_fail_if_next_is_not: '" + char + "'")

                self.reader.skipWhitespace()
                self.reader.expect(char)

            def raise_error(self, msg):
                raise SyntaxError(msg + " at ->" + self.reader.string[self.reader.cursor:])

        return MojangsonParser(json).parse_compound()

class TagRoot(TagCompound):
    root = True

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
_ids[TagUnsignedLongArray] = 12


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
    def __init__(self, path):
        self.fd = open(path, "r+b")

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
        Returns a list of (cx,cz) tuples for all existing chunks.
        """

        result = []

        for chunk_z in range(32):
            for chunk_x in range(32):
                # Read extent header
                self.fd.seek(4 * (32 * chunk_z + chunk_x))
                entry = Buffer(self.fd.read(4)).unpack('I')

                if entry:
                    result.append( ( chunk_x, chunk_z ) )

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

        if entry:
            # Read chunk
            self.fd.seek(4096 * chunk_offset)
            buff.add(self.fd.read(4096 * chunk_length))
            chunk = buff.read(buff.unpack('IB')[0])
            chunk = zlib.decompress(chunk)
            chunk = TagRoot.from_bytes(chunk)
            return chunk
        else:
            # No chunk at that location
            return None


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
