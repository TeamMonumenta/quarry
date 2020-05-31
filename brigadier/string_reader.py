#!/usr/bin/env python3
import re

class StringReader(object):
    """
    Python implementation of:
    https://github.com/Mojang/brigadier/blob/master/src/main/java/com/mojang/brigadier/StringReader.java
    """
    regexUnquotedString = re.compile(r'''[-+._0-9A-Za-z]''')

    def __init__(self, stringIn):
        if isinstance(stringIn, type(self)):
            self.string = stringIn.string
            self.cursor = stringIn.cursor
        elif isinstance(stringIn, str):
            self.string = stringIn
            self.cursor = 0
        else:
            raise TypeError('Cannot parse type ' + str(type(stringIn)))

    def getString(self):
        return self.string

    def setCursor(self, cursor):
        self.cursor = cursor

    def getRemainingLength(self):
        return len(self.string) - self.cursor

    def getTotalLength(self):
        return len(self.string)

    def __len__(self):
        return len(self.string)

    def getCursor(self):
        return self.cursor

    def getRead(self):
        """
        Return the part of the source string that's already been read
        """
        return self.string[:self.cursor]

    def getRemaining(self):
        return self.string[self.cursor:]

    def canRead(self, length=1):
        return self.cursor + length <= len(self.string)

    def peek(self, offset=0):
        return self.string[self.cursor+offset]

    def read(self):
        self.cursor += 1
        return self.string[self.cursor-1]

    def skip(self):
        self.cursor += 1

    @classmethod
    def isAllowedNumber(cls, c):
        return c in '0123456789-.'

    @classmethod
    def isQuotedStringStart(cls, c):
        return c == "'" or c == '"'

    def skipWhitespace(self):
        while self.canRead() and self.peek().isspace():
            self.skip()

    def readInt(self):
        start = self.cursor
        while self.canRead() and self.isAllowedNumber(self.peek()):
            self.skip()
        number = self.string[start:self.cursor]
        if len(number) == 0:
            raise SyntaxError("could not find an integer: end of string, or first character not in [-.0-9]")
        try:
            if -1*(2**31) > int(number) or int(number) >= 2**31:
                raise ValueError()
            return int(number)
        except ValueError:
            cursor = start
            raise ValueError("could not parse '" + number + "' as a 32-bit integer expressed in base 10")

    def readLong(self):
        start = self.cursor
        while self.canRead() and self.isAllowedNumber(self.peek()):
            self.skip()
        number = self.string[start:self.cursor]
        if len(number) == 0:
            raise SyntaxError("could not find a long: end of string, or first character not in [-.0-9]")
        try:
            if -1*(2**63) > int(number) or int(number) >= 2**63:
                raise ValueError()
            return int(number)
        except ValueError:
            cursor = start
            raise ValueError("could not parse '" + number + "' as a 64-bit integer expressed in base 10")

    def readDouble(self):
        start = self.cursor
        while self.canRead() and self.isAllowedNumber(self.peek()):
            self.skip()
        number = self.string[start:self.cursor]
        if len(number) == 0:
            raise SyntaxError("could not find a double: end of string, or first character not in [-.0-9]")
        try:
            return float(number)
        except ValueError:
            cursor = start
            raise ValueError("could not parse '" + number + "' as a 64-bit IEEE double-precision float expresssed in base 10")

    def readFloat(self):
        start = self.cursor
        while self.canRead() and self.isAllowedNumber(self.peek()):
            self.skip()
        number = self.string[start:self.cursor]
        if len(number) == 0:
            raise SyntaxError("could not find a float: end of string, or first character not in [-.0-9]")
        try:
            if abs(float(number)) >= 2.0**128:
                # This number exceeds the range of a 32-bit IEEE float
                raise ValueError()
            return float(number)
        except ValueError:
            cursor = start
            raise ValueError("could not parse '" + number + "' as a 32-bit IEEE float expresssed in base 10")

    def isAllowedInUnquotedString(self, c):
        return bool(self.regexUnquotedString.match(c))

    def readUnquotedString(self):
        start = self.cursor
        while self.canRead() and self.isAllowedInUnquotedString(self.peek()):
            self.skip()
        return self.string[start:self.cursor]

    def readQuotedString(self):
        if not self.canRead():
            return ""
        nxt = self.peek()
        if not self.isQuotedStringStart(nxt):
            raise SyntaxError("Expected quotes to begin string, got '" + c + "'")
        self.skip()
        return self.readStringUntil(nxt)

    def readStringUntil(self, terminator):
        result = ''
        escaped = False
        while self.canRead():
            c = self.read()
            if escaped:
                if c == terminator or c == '\\':
                    result += c
                    escaped = False
                else:
                    self.cursor -= 1
                    raise SyntaxError("Unexpected escaped character '" + c + "'")
            elif c == '\\':
                escaped = True
            elif c == terminator:
                return result
            else:
                result += c

        raise SyntaxError("expected end quote")

    def readString(self):
        if not self.canRead():
            return ""
        nxt = self.peek()
        if self.isQuotedStringStart(nxt):
            self.skip()
            return self.readStringUntil(nxt)
        return self.readUnquotedString()

    def readBoolean(self):
        start = self.cursor
        value = self.readString()
        if len(value) == 0:
            raise SyntaxError("expected boolean")
        if value == "true":
            return True
        elif value == "false":
            return False
        else:
            self.cursor = start
            raise SyntaxError("invalid boolean")

    def expect(self, c):
        if not self.canRead() or self.peek() != c:
            raise SyntaxError("Expected character '" + str(c) + "' at ->'" + self.string[self.cursor:] + "'")
        self.skip()

