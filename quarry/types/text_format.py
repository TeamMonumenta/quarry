import enum

class TextFormatBase(object):
    """
    A single color or formating option
    """
    def __init__(self, section_code, display_name, technical_name, ansi_code, foreground_color=None, background_color=None):
        self.section_code = section_code
        self.display_name = display_name
        self.technical_name = technical_name
        self.ansi_code = ansi_code
        self.foreground_color = foreground_color
        self.background_color = background_color

    def __eq__(self, other):
        if type(self) == type(other):
            return self.section_code == other.section_code

        if type(other) == int:
            try:
                if int(self.id,16) == other:
                    return True
            except:
                pass

            if self.foreground_color == other:
                return True

            if self.background_color == other:
                return True

            return False

        if type(other) == str:
            if self.section_code == other:
                return True

            if self.display_name == other:
                return True

            if self.technical_name == other:
                return True

            if self.ansi_code == other:
                return True

            return False

        return False

class TextColors(enum.Enum):
    """
    Minecraft text color codes
    """
    black         = TextFormatBase("§0", "Black",         "black",         "\x1b[0m\x1b[30m",   0x000000, 0x000000)
    dark_blue     = TextFormatBase("§1", "Dark Blue",     "dark_blue",     "\x1b[0m\x1b[34m",   0x0000AA, 0x00002A)
    dark_green    = TextFormatBase("§2", "Dark Green",    "dark_green",    "\x1b[0m\x1b[32m",   0x00AA00, 0x002A00)
    dark_aqua     = TextFormatBase("§3", "Dark Aqua",     "dark_aqua",     "\x1b[0m\x1b[36m",   0x00AAAA, 0x002A2A)
    dark_red      = TextFormatBase("§4", "Dark Red",      "dark_red",      "\x1b[0m\x1b[31m",   0xAA0000, 0x2A0000)
    dark_purple   = TextFormatBase("§5", "Dark Purple",   "dark_purple",   "\x1b[0m\x1b[35m",   0xAA00AA, 0x2A002A)
    gold          = TextFormatBase("§6", "Gold",          "gold",          "\x1b[0m\x1b[33m",   0xFFAA00, 0x2A2A00)
    gray          = TextFormatBase("§7", "Gray",          "gray",          "\x1b[0m\x1b[37m",   0xAAAAAA, 0x2A2A2A)
    dark_gray     = TextFormatBase("§8", "Dark Gray",     "dark_gray",     "\x1b[0m\x1b[30;1m", 0x555555, 0x151515)
    blue          = TextFormatBase("§9", "Blue",          "blue",          "\x1b[0m\x1b[34;1m", 0x5555FF, 0x15153F)
    green         = TextFormatBase("§a", "Green",         "green",         "\x1b[0m\x1b[32;1m", 0x55FF55, 0x153F15)
    aqua          = TextFormatBase("§b", "Aqua",          "aqua",          "\x1b[0m\x1b[36;1m", 0x55FFFF, 0x153F3F)
    red           = TextFormatBase("§c", "Red",           "red",           "\x1b[0m\x1b[31;1m", 0xFF5555, 0x3F1515)
    light_purple  = TextFormatBase("§d", "Light Purple",  "light_purple",  "\x1b[0m\x1b[35;1m", 0xFF55FF, 0x3F153F)
    yellow        = TextFormatBase("§e", "Yellow",        "yellow",        "\x1b[0m\x1b[33;1m", 0xFFFF55, 0x3F3F15)
    white         = TextFormatBase("§f", "White",         "white",         "\x1b[0m\x1b[37;1m", 0xFFFFFF, 0x3F3F3F)

class TextStyles(enum.Enum):
    """
    Minecraft text color codes
    """
    obfuscated    = TextFormatBase("§k", "Obfuscated",    "obfuscated",    "\x1b[7m", )
    bold          = TextFormatBase("§l", "Bold",          "bold",          "\x1b[1m", )
    strikethrough = TextFormatBase("§m", "Strikethrough", "strikethrough", "\x1b[9m", )
    underlined    = TextFormatBase("§n", "Underline",     "underlined",    "\x1b[4m", )
    italic        = TextFormatBase("§o", "Italic",        "italic",        "\x1b[3m", )
    reset         = TextFormatBase("§r", "Reset",         "reset",         "\x1b[0m", )

class TextFormats(enum.Enum):
    black = TextColors.black.value
    dark_blue = TextColors.dark_blue.value
    dark_green = TextColors.dark_green.value
    dark_aqua = TextColors.dark_aqua.value
    dark_red = TextColors.dark_red.value
    dark_purple = TextColors.dark_purple.value
    gold = TextColors.gold.value
    gray = TextColors.gray.value
    dark_gray = TextColors.dark_gray.value
    blue = TextColors.blue.value
    green = TextColors.green.value
    aqua = TextColors.aqua.value
    red = TextColors.red.value
    light_purple = TextColors.light_purple.value
    yellow = TextColors.yellow.value
    white = TextColors.white.value

    obfuscated = TextStyles.obfuscated.value
    bold = TextStyles.bold.value
    strikethrough = TextStyles.strikethrough.value
    underlined = TextStyles.underlined.value
    italic = TextStyles.italic.value
    reset = TextStyles.reset.value

def get_format(match):
    """
    Return one piece of color information by an ID or name
    """
    for format in TextFormats:
        if format.value == match:
            return format.value
    else:
        raise KeyError("No such format code: {}".format(match))

def ansify_text(text,show_section=False):
    """
    Return the provided text with §-style format codes converted to ansi format codes (compatible with most terminals)
    """
    result = text
    for format in TextFormats:
        result = result.replace(
            format.value.section_code,
            format.value.section_code + format.value.ansi_code
        )
    return result

def unformat_text(text):
    """
    Return the provided text without §-style format codes
    """
    result = text
    for format in TextFormats:
        result = result.replace(format.value.section_code, '')
    return result

