import collections
import functools
import gzip
import time
import zlib

from quarry.types.buffer import Buffer

_kinds = {}
_ids = {}


# Format codes
formatCodes = {
    "colors":'0123456789abcdef',
    "styles":'klmnor',
    "list":[
        {"id":"0", "display":"Black",         "technical":"black",         "ansi":"\x1b[30m",   "foreground":0x000000, "background":0x000000},
        {"id":"1", "display":"Dark Blue",     "technical":"dark_blue",     "ansi":"\x1b[34m",   "foreground":0x0000AA, "background":0x00002A},
        {"id":"2", "display":"Dark Green",    "technical":"dark_green",    "ansi":"\x1b[32m",   "foreground":0x00AA00, "background":0x002A00},
        {"id":"3", "display":"Dark Aqua",     "technical":"dark_aqua",     "ansi":"\x1b[36m",   "foreground":0x00AAAA, "background":0x002A2A},
        {"id":"4", "display":"Dark Red",      "technical":"dark_red",      "ansi":"\x1b[31m",   "foreground":0xAA0000, "background":0x2A0000},
        {"id":"5", "display":"Dark Purple",   "technical":"dark_purple",   "ansi":"\x1b[35m",   "foreground":0xAA00AA, "background":0x2A002A},
        {"id":"6", "display":"Gold",          "technical":"gold",          "ansi":"\x1b[33m",   "foreground":0xFFAA00, "background":0x2A2A00},
        {"id":"7", "display":"Gray",          "technical":"gray",          "ansi":"\x1b[37m",   "foreground":0xAAAAAA, "background":0x2A2A2A},
        {"id":"8", "display":"Dark Gray",     "technical":"dark_gray",     "ansi":"\x1b[30;1m", "foreground":0x555555, "background":0x151515},
        {"id":"9", "display":"Blue",          "technical":"blue",          "ansi":"\x1b[34;1m", "foreground":0x5555FF, "background":0x15153F},
        {"id":"a", "display":"Green",         "technical":"green",         "ansi":"\x1b[32;1m", "foreground":0x55FF55, "background":0x153F15},
        {"id":"b", "display":"Aqua",          "technical":"aqua",          "ansi":"\x1b[36;1m", "foreground":0x55FFFF, "background":0x153F3F},
        {"id":"c", "display":"Red",           "technical":"red",           "ansi":"\x1b[31;1m", "foreground":0xFF5555, "background":0x3F1515},
        {"id":"d", "display":"Light Purple",  "technical":"light_purple",  "ansi":"\x1b[35;1m", "foreground":0xFF55FF, "background":0x3F153F},
        {"id":"e", "display":"Yellow",        "technical":"yellow",        "ansi":"\x1b[33;1m", "foreground":0xFFFF55, "background":0x3F3F15},
        {"id":"f", "display":"White",         "technical":"white",         "ansi":"\x1b[37;1m", "foreground":0xFFFFFF, "background":0x3F3F3F},

        {"id":"k", "display":"Obfuscated",    "technical":"obfuscated",    "ansi":"\x1b[7m",    },
        {"id":"l", "display":"Bold",          "technical":"bold",          "ansi":"\x1b[1m",    },
        {"id":"m", "display":"Strikethrough", "technical":"strikethrough", "ansi":"\x1b[9m",    },
        {"id":"n", "display":"Underline",     "technical":"underlined",    "ansi":"\x1b[4m",    },
        {"id":"o", "display":"Italic",        "technical":"italic",        "ansi":"\x1b[3m",    },
        {"id":"r", "display":"Reset",         "technical":"reset",         "ansi":"\x1b[0m",    },
    ],
}

def getColor(match,key=None):
    """
    Return one piece of color information by an ID or name
    """
    for format in formatCodes["list"]:
        if (
            str(match).lower() == format["id"] or
            str(match).lower() == format["display"].lower() or
            str(match).lower() == format["technical"]
        ):
            # Match found; return the specified part of the format,
            # otherwise the whole format
            return format.get(key,format)

def unformatText(text):
    """
    Return the provided text without §-style format codes
    """
    while '§' in text:
        i = text.find('§')
        text = text[:i]+text[i+2:]
    return text

def ansifyText(text):
    """
    Return the provided text with §-style format codes converted to ansi format codes (compatible with most terminals)
    """
    while '§' in text:
        i = text.find('§')
        ansiCode = getColor(text[i+1],'ansi')
        text = text[:i]+ansiCode+text[i+2:]
    return text

# Base types ------------------------------------------------------------------

@functools.total_ordering
class _Tag(object):
    def __init__(self, value):
        self.value = value

    prefix = ('',getColor('gold','ansi'))
    postfix = ('',getColor('reset','ansi'))

    @classmethod
    def from_bytes(cls, bytes):
        return cls.from_buff(Buffer(bytes))

    @classmethod
    def from_buff(cls, buff):
        raise NotImplementedError

    def to_bytes(self):
        raise NotImplementedError

    def to_obj(self):
        return self.value

    def __repr__(self):
        return "%s(%r)" % (type(self).__name__, self.value)

    def __eq__(self, other):
        return self.to_obj() == other.to_obj()

    def __lt__(self, other):
        return self.to_obj() < other.to_obj()

    def to_json(self,sort=None,highlight=False):
        return self.prefix[highlight] + str(self.value) + self.postfix[highlight]


class _DataTag(_Tag):
    fmt = None

    @classmethod
    def from_buff(cls, buff):
        return cls(buff.unpack(cls.fmt))

    def to_bytes(self):
        return Buffer.pack(self.fmt, self.value)

    def is_subset(self,other):
        return self.value == other.value

    def at_path(self,path):
        return self


class _ArrayTag(_Tag):
    fmt = None
    separator = (',',getColor('white','ansi')+', ')

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

    def to_json(self,sort=None,highlight=False):
        content_json = []
        for content in self.value:
            content_json.append(content.to_json(sort,highlight))
        return self.prefix[highlight] + self.separator[highlight].join(content_json) + self.postfix[highlight]

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
    postfix = ('b',getColor('red','ansi')+'b'+getColor('reset','ansi'))


class TagShort(_DataTag):
    fmt = 'h'
    postfix = ('s',getColor('red','ansi')+'s'+getColor('reset','ansi'))


class TagInt(_DataTag):
    fmt = 'i'
    postfix = ('',getColor('reset','ansi'))


class TagLong(_DataTag):
    fmt = 'q'
    postfix = ('l',getColor('red','ansi')+'l'+getColor('reset','ansi'))


class TagFloat(_DataTag):
    fmt = 'f'
    postfix = ('f',getColor('red','ansi')+'f'+getColor('reset','ansi'))


class TagDouble(_DataTag):
    fmt = 'd'
    postfix = ('d',getColor('red','ansi')+'d'+getColor('reset','ansi'))


class TagString(_Tag):
    prefix = ('"',getColor('white','ansi')+'"'+getColor('green','ansi'))
    postfix = ('"',getColor('reset','ansi')+getColor('white','ansi')+'"'+getColor('reset','ansi'))

    @classmethod
    def from_buff(cls, buff):
        string_length = buff.unpack('H')
        return cls(buff.read(string_length).decode('utf8'))

    def to_bytes(self):
        data = self.value.encode('utf8')
        return Buffer.pack('H', len(data)) + data

    def is_subset(self,other):
        return self.value == other.value

    def to_json(self,sort=None,highlight=False):
        text = self.value.replace('\\','\\\\').replace('\n','\\n"').replace('"','\\"')
        if highlight:
            text = ansifyText(text)
        return self.prefix[highlight] + text + self.postfix[highlight]


class TagByteArray(_ArrayTag):
    fmt = 'b'
    prefix = ('[B;',getColor('white','ansi')+'['+getColor('red','ansi')+'B'+getColor('white','ansi')+'; '+getColor('gold','ansi'))
    postfix = ('b]',getColor('red','ansi')+'b'+getColor('white','ansi')+']'+getColor('reset','ansi'))
    separator = ('b,',getColor('red','ansi')+'b'+getColor('white','ansi')+', '+getColor('gold','ansi'))


class TagIntArray(_ArrayTag):
    fmt = 'i'
    prefix = ('[I;',getColor('white','ansi')+'['+getColor('red','ansi')+'I'+getColor('white','ansi')+'; '+getColor('gold','ansi'))
    postfix = (']',getColor('white','ansi')+']'+getColor('reset','ansi'))
    separator = (',',getColor('white','ansi')+', '+getColor('gold','ansi'))


class TagLongArray(_ArrayTag):
    fmt = 'q'
    prefix = ('[L;',getColor('white','ansi')+'['+getColor('red','ansi')+'L'+getColor('white','ansi')+'; '+getColor('gold','ansi'))
    postfix = ('l]',getColor('red','ansi')+'l'+getColor('white','ansi')+']'+getColor('reset','ansi'))
    separator = ('l,',getColor('red','ansi')+'l'+getColor('white','ansi')+', '+getColor('gold','ansi'))


class TagUnsignedLongArray(_ArrayTag):
    fmt = 'Q'
    prefix = ('[L;',getColor('white','ansi')+'['+getColor('red','ansi')+'L'+getColor('white','ansi')+'; '+getColor('gold','ansi'))
    postfix = ('l]',getColor('red','ansi')+'l'+getColor('white','ansi')+']'+getColor('reset','ansi'))
    separator = ('l,',getColor('red','ansi')+'l'+getColor('white','ansi')+', '+getColor('gold','ansi'))


class TagList(_Tag):
    prefix = ('[',getColor('white','ansi')+'['+getColor('gold','ansi'))
    postfix = (']',getColor('white','ansi')+']'+getColor('reset','ansi'))
    separator = (',',getColor('white','ansi')+', '+getColor('gold','ansi'))

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

    def to_json(self,sort=None,highlight=False):
        content_json = []
        for content in self.value:
            content_json.append(content.to_json(sort,highlight))
        return self.prefix[highlight] + self.separator[highlight].join(content_json) + self.postfix[highlight]

    def at_path(self,path):
        if path.startswith('['):
            path = path[1:]
        if not ']' in path:
            raise IndexError( '] not in path "' + path + '"' )
        if ']' not in path:
            raise IndexError( 'Could not find "]" in "' + path + '"' )

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
    preserve_order = False
    prefix = ('{',getColor('white','ansi')+'{'+getColor('gold','ansi'))
    postfix = ('}',getColor('white','ansi')+'}'+getColor('reset','ansi'))
    separator = (',',getColor('white','ansi')+', '+getColor('gold','ansi'))
    key_value_separator = (':',getColor('white','ansi')+': ')

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

    def to_json(self,sort=None,highlight=False):
        if type(sort) in (list,tuple):
            keys = []
            for key in sort:
                if key in self.value.keys():
                    keys.append(key)
            for key in self.value.keys():
                if key not in sort:
                    keys.append(key)
        else:
            keys = self.value.keys()

        content_json = []
        for key in keys:
            content = self.value[key]
            content_json.append( key + self.key_value_separator[highlight] + content.to_json(sort,highlight) )
        return self.prefix[highlight] + self.separator[highlight].join(content_json) + self.postfix[highlight]

    def at_path(self,path):
        if path.startswith('.') or path.startswith('['):
            raise KeyError( path + 'not in ' + str(self) )
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
            raise KeyError( 'key "' + key + '" not in ' + str(self) )

        if path == '':
            return self.value[key]
        else:
            return self.value[key].at_path(path)

'''
    @classmethod
    def from_json(cls,json):
        """
        Convert a json-style NBT string into NBT
        """
        debug = ''
        startQuote = None
        charsToIgnore = 0

        def next_type(json):
            best = None
            best_pre = ''
            for kind in _kinds.values():
                if (
                    'prefix' not in dir(kind) or
                    len(kind.prefix[0]) == 0 or
                    not json.startswith(kind.prefix[0]) or
                    len(kind.prefix[0]) < len(best_pre)
                ):
                    continue
                best = kind
                best_pre = kind.prefix[0]
            return(best)

        class parse_state(object):
            """
            The state of the current value being parsed;
            used to simplify adding values to tags, or
            to finish adding values to a tag.
            """
            def __init__(self):
                self.next_name = ''
                self.next_value = None
                self.result = TagCompound({})
                self.stack = [self.result]

            def store_tag(self):
                # Add current tag to the parent tag
                parent = self.stack[-1]
                if type(parent) is TagCompound:
                    # parent is TagCompound
                    parent[self.next_name] = self.next_value

                elif type(parent) is TagList:
                    # parent is TagList
                    parent.append(self.next_value)

                elif isinstance(tag, _ArrayTag):
                    # parent is a numeric array
                    parent.append(self.next_value.value)

                else:
                    raise TypeError("Unexpected parent data type while parsing NBT json")

                # Add new tag to stack if it's a container
                child = self.next_value
                if (
                    type(child) is TagCompound or
                    type(child) is TagList or
                    isinstance(tag, _ArrayTag)
                ):
                    self.stack.append(child)

                # Reset current tag state
                self.next_name = ''
                self.next_value = None

        state = parse_state()

        # Begin parse
        for i,c in enumerate(json):
            # i is the index of character c in json
            if charsToIgnore > 0:
                debug += c
                charsToIgnore -= 1
                continue
            elif c == '\\':
                # This charcter is a \, ignore next character
                debug += '\\'
                charsToIgnore = 1
                continue
            elif c == '"':
                # Quote found, is it start or end?
                debug += '"'
                if startQuote is not None:
                    # It is an end quote, accept the value
                    # Include the quote marks to identify type

                    # Note that this might in fact be the tag NAME,
                    # not the tag VALUE. This will be updated when a
                    # colon signifies the value starts next, or when
                    # it is clear the end of the tag has arrived.
                    cls.currentValue = json[startQuote: i + 1]
                    startQuote = None
                    continue
                else:
                    # It is a start quote, record the location
                    debug += '"'
                    startQuote = i
                    continue
            elif startQuote is not None:
                # We're inside quotes; other cases should be ignored.
                debug += '~'
                continue
            elif c == '{':
                # New compound tag
                if i == 0:
                    # This is the starting tag, and accounted for.
                    debug += '{'
                    continue
                else:
                    # This tag is not accounted for.
                    debug += '{'
                    #
'''

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

    def load_chunk(self, chunk_x, chunk_z):
        """
        Loads the chunk at the given co-ordinates from the region file.
        The co-ordinates should range from 0 to 31. Returns a ``TagRoot``.
        """

        buff = Buffer()

        # Read extent header
        self.fd.seek(4 * (32 * chunk_z + chunk_x))
        buff.add(self.fd.read(4))
        entry = buff.unpack('I')
        chunk_offset, chunk_length = entry >> 8, entry & 0xFF

        # Read chunk
        self.fd.seek(4096 * chunk_offset)
        buff.add(self.fd.read(4096 * chunk_length))
        chunk = buff.read(buff.unpack('IB')[0])
        chunk = zlib.decompress(chunk)
        chunk = TagRoot.from_bytes(chunk)
        return chunk


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
