"""
Elias Wood (owns13927@yahoo.com)
2015-04-06
a base class for all my classes to help with logging and timestamps
"""
import logging
import os
import datetime

class MyLoggingBase(object):
    """
    this 'abstract' base class is only
    helpful in simplifying logging and some general things.
    Initialize, and use:
        .logger.debug # for the small things
        .logger.info # for general flow - shouldn't have vars
        .logger.warning # something's wrong, but skippable
        .logger.error # something's wrong and may break something later
        .logger.critical # something really bad happened! nuclear!
    It also has _get_summary_info to override to supply summary info (by calling
        self._log_summary_info()
    It also has some static timestamp things...
    NOTE: get_output_fd & get_resource_fd use cwd!
    """
    __version__ = '0.1.1'
    _TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S,%f'
    logger = None
    
    
    
    def __init__(self,name=None):
        """name -- a str/unicode name of the logger (default: <class name>)"""
        object.__init__(self)
        self.logger = logging.getLogger(name if isinstance(name,(unicode,str))
                                        else self.__class__.__name__)
    
    #===========================================================================
    # Logging 
    #===========================================================================
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
    
    @staticmethod
    def init_logging(**keys):
        """
        file_log_name=None  # name of log file (defaults to output dir)
        file_log_lvl='DEBUG'  # level to log in file (None to not log to file)
        console_log_lvl='DEBUG'  # level to log to console
        show_warning=True # show warning for not writing to the file or console.
        # valid log_lvls: None,DEBUG,INFO,WARNING,ERROR,CRITICAL
        """
        # set values
        file_log_name = keys.get('file_log_name',None)
        file_log_lvl = keys.get('file_log_lvl','DEBUG')
        console_log_lvl = keys.get('console_log_lvl','DEBUG')
        show_warning = keys.get('show_warning',True)
            
        # raise error if bad value passed
        valid_log_lvls = (None,'DEBUG','INFO','WARNING','ERROR','CRITICAL')
        if file_log_lvl not in valid_log_lvls:
            raise ValueError('bad param passed for file_log_lvl {0!r}'.format(file_log_lvl))
        if console_log_lvl not in valid_log_lvls:
            raise ValueError('bad param passed for console_log_lvl {0!r}'.format(console_log_lvl))
               
        # set logging level
        logging.getLogger().setLevel(logging.DEBUG)
            
        # create logging formatter
        f = '%(asctime)s,%(msecs)-3d:%(threadName)-10s:%(levelname)-7s:%(name)s.%(funcName)s:%(message)s'
        log_formatter = logging.Formatter(f)
            
        # create handlers based on request
        if file_log_lvl:
            # add file handler
            if file_log_name==None: file_log_name = MyLoggingBase.get_output_fd('log{}.log'.format(MyLoggingBase.get_current_timestamp(True)))
            h = logging.FileHandler(file_log_name)#,mode='w') #to not append for the day
            h.setLevel(logging.__getattribute__(file_log_lvl)) # @UndefinedVariable
            h.setFormatter(log_formatter)
            logging.getLogger().addHandler(h)
                
        if console_log_lvl:
            # add console handler
            import sys
            h2 = logging.StreamHandler(sys.stdout) # to change the coloring! from stderr to stdout
            h2.setFormatter(log_formatter)
            h2.setLevel(logging.__getattribute__(console_log_lvl)) # @UndefinedVariable
            logging.getLogger().addHandler(h2)
        elif show_warning:
            print '======================================='
            print 'not showing log in console per request!'
            print file_log_name
            print '======================================='
            logging.warning('=======================================')
            logging.warning('not showing log in console per request!')
            logging.warning('=======================================')
            
        if not file_log_lvl and show_warning:
            logging.warning('=======================================')
            logging.warning('= not saving log to file per request! =')
            logging.warning('=======================================')
        '''
        # necessary import for exception hooking
        # log all myexceptions!
        def exception_hook(exctype, exc, tb):
            #this method just handles the output, not raising the error... 
            logging.critical('\n'+''.join(traceback.format_tb(tb)))
            logging.critical('%s: %s',exctype,exc.message)
        sys.excepthook = exception_hook
        '''
    
    #===============================================================================
    # get/join with project folders
    #===============================================================================
    # NOTE: __file__ will still reference this loc..., assuming working dir...
    @staticmethod
    def get_resource_fd(filename=None):
        """pass a filename to join with the resource folder"""
        dir_name = os.path.join(os.path.realpath(''),'resources') #__file__
        return MyLoggingBase.join_folder_and_file(dir_name,filename)
    
    @staticmethod
    def get_output_fd(filename=None):
        """pass a filename to join with the output folder"""
        dir_name = os.path.join(os.path.realpath(''),'output') #__file__
        return MyLoggingBase.join_folder_and_file(dir_name,filename)
    
    @staticmethod
    def join_folder_and_file(fd,filename=None):
        """tests if folder exists, returns None if if doesn't, the filepath if successful!"""
        if isinstance(filename,(str,unicode)):
            return os.path.join(fd,filename)
        else: return fd
        
    #===========================================================================
    # Summary
    #===========================================================================
    def _get_summary_info(self):
        """override to add useful summary info."""
        return []
    
    def _log_summary_info(self,prepend=''):
        """call to log all important summary information."""
        if prepend:
            for i in self._get_summary_info():
                self.logger.info('{!s}: {!s}'.format(prepend,i))
        else:
            for i in self._get_summary_info():
                self.logger.info('{!s}'.format(i))
    
    #===========================================================================
    # # Timestamp
    #===========================================================================
    @staticmethod
    def get_current_datetime(utc=False):
        """Helper function for getting the current datetime.
        param utc: If False, returns the time based on the current timezone.
                   If True, returns the UTC time."""
        if utc: datetime.datetime.utcnow()
        else: return datetime.datetime.now()
    
    @staticmethod
    def datetime_to_timestamp(dt,for_file=False):
        """Formats the datetime passed to ISO 8601 string.
        for_file -- if True, replaces colons with periods."""
        s = (MyLoggingBase._TIMESTAMP_FORMAT.replace(':','.') if for_file
             else MyLoggingBase._TIMESTAMP_FORMAT)
        
        # only accurate to 3 for win8.1, but 6 given (for after sec)
        return dt.strftime(s)
    
    @staticmethod
    def get_current_timestamp(for_file=False,utc=False):
        """Formats the current time (using datetime.datetime.now) to ISO 8601.
        for_file -- if True, replaces colons with periods."""
        return MyLoggingBase.datetime_to_timestamp(MyLoggingBase.get_current_datetime(utc),for_file)
    
#===============================================================================
# Main
#===============================================================================
if __name__ == '__main__':
    try: import tests.test_myloggingbase
    except ImportError: print 'no test for myloggingbase'
    else: tests.test_myloggingbase.run_test()
