class StatcanError(Exception):
    pass

class MetadataError(StatcanError):
    pass

class InvalidDimensionError(StatcanError):
    pass

class InvalidMemberError(StatcanError):
    pass

class InvalidCoordinateError(StatcanError):
    pass
