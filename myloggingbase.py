"""
Elias Wood (owns13927@yahoo.com)
2015-04-06
a base class for all my classes to help with logging and timestamps
"""
import logging
import datetime

class MyLoggingBase(object):
    """
    this 'abstract' base class is only
    helpful in simplifying logging.
    Initialize, and use:
        .logger.debug # for the small things
        .logger.info # for general flow - shouldn't have vars
        .logger.warning # something's wrong, but skippable
        .logger.error # something's wrong and may break something later
        .logger.critical # something really bad happened! nuclear!
    It also has some static timestamp things...
    """
    __version__ = '0.1.0'

    logger = None

    def __init__(self,name=None):
        """name -- a str/unicode name of the logger (default: <class name>)"""
        object.__init__(self)
        self.logger = logging.getLogger(name if isinstance(name,(unicode,str))
                                        else self.__class__.__name__)

    def set_logger_level(self,lvl):
        """Set the logging level for the class.  Returns True if set correctly."""
        if self.logger_set():
            if lvl in (logging.DEBUG,logging.INFO,logging.WARNING,
                       logging.ERROR,logging.CRITICAL):
                self.logger.setLevel(lvl)
                return True
            else: return False

    def logger_enabled(self):
        """duplicate function for convenience"""
        return self.log_set()

    def logger_set(self):
        """Returns True if the .logger is set and there are handlers
        (somewhere to output to) for logging; False otherwise."""
        return (self.logger is not None and len(logging._handlers) != 0)

    #========================================================================#
    # Timestamp things
    #========================================================================#
    @staticmethod
    def get_current_timestamp(for_file=False):
        """Formats the current time (using datetime.datetime.now) to ISO 8601.
        ForFile -- if True, replaces colons with periods."""
        if for_file: s = '%Y-%m-%dT%H.%M.%S,%f'
        else: s = '%Y-%m-%dT%H:%M:%S,%f'
        # only accurate to 3 for win8.1, but 6 given (for after sec)
        return MyLoggingBase.get_current_datetime().strftime(s)
    
    @staticmethod
    def get_current_datetime():
        """Helper function for getting the current datetime (datetime.now())"""
        return datetime.datetime.now()

#===============================================================================
# Main
#===============================================================================
if __name__ == '__main__':
    try: from tests import test_myloggingbase
    except ImportError: print 'no test for myloggingbase'
    else: test_myloggingbase.run_test()
