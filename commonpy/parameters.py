"""
application-wide parameters
"""
import os

from commonpy.singleton import Singleton
try:
    import constants
except ImportError:
    class constants:
        OVERRIDABLE_ENV = {}
        REQUIRED_ENV = []


class SysParams(dict, metaclass=Singleton):
    """
    A utility class intended to hold all system-wide and configurable
    parameters.

    This is a dictionary class, so that the user can use it as follows:
        import SysParams

        myparams = SysParams()
        myparams['foo'] = 'bar'
    """
    def __init__(self, required_env=None, overrideable_env=None):
        #  Get required environment variables, fail if any are missing.
        super().__init__()
        required = set((required_env or []) + constants.REQUIRED_ENV)
        missing = required - set(os.environ.keys())
        if missing:
            print("ERROR: missing environment variable(s): {}".format(', '.join(missing)))
            exit(1)

        #  Get the required environment variables
        self.update({key: os.getenv(key) for key in required})

        #  Get overridable environment variables
        overrideable = {**(overrideable_env or {}), **constants.OVERRIDABLE_ENV}
        self.update({key: os.getenv(key, val) for
                     key, val in overrideable.items()})

    @property
    def params(self):
        """
        Make the (internal) params object a read-only property (primarily to
        facilite test/mock).
        """
        return self
