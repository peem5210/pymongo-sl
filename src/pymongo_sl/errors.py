class PyMongoSLError(Exception):
    """
    """
class ConfigError(PyMongoSLError):
    """Raised when something is incorrectly configured.
    """
class MissingArgsError(PyMongoSLError):
    """Raised when something is missing from the required arguments.
    """