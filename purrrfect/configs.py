import enum


class OutputEnum(str, enum.Enum):
    PLAIN = "plain"
    JSON = "json"
    RICH = "rich"
    QUIET = "quiet"
    STDOUT = "stdout"
