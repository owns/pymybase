# -*- coding: utf-8 -*-
"""
@author: Elias Wood (owns13927@yahoo.com)
2015-04-06
a base class for all my classes to help with logging and timestamps
"""
import logging
import os
import sys
import datetime

# python2 compatible
try: s = basestring # @UndefinedVariable pylint: disable=invalid-name
except NameError: basestring = str #@ReservedAssignment pylint: disable=invalid-name
else: del s

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
    It also has get_summary_info to override to supply summary info (by calling
        self.log_summary_info())
    It also has some static timestamp things...
    NOTE: get_output_fd & get_resource_fd use cwd!
    CHANGELOG:
    2018-06-07 v0.5.0: Elias Wood
        change __init__(name) var to logger_name
        removed unnecessary logging functions (logger_enabled,logger_set,init_logging)
            use http://docs.python-guide.org/en/latest/writing/logging/
        _TIMESTAMP_FORMAT to TIMESTAMP_FORMAT
        added config_file to init_logging
        replaced join_folder_and_file with path_exists and returns abs path
    2018-02-06 v0.4.0: Elias Wood
        logging uses rotating file handler
        several methods to @classmethod
        small changes
    2016-05-27 v0.3.0: Elias Wood
        updated get/join project folders to handle if frozen (cx_frozen)
    """
    __version__ = '0.5.0'
    TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'
    logger = None


    def __init__(self,logger_name=None,**keys):  #@UnusedVariable pylint: disable=unused-argument
        """name -- a str/unicode name of the logger (default: <class name>)"""
        object.__init__(self)
        # init logger
        self.logger = logging.getLogger(logger_name if isinstance(logger_name,basestring)
                                        else self.__class__.__name__)
        # add null handler if none exists to avoid "No handler found" warnings.
        if not self.logger.handlers:
            try:  # Python 2.7+
                from logging import NullHandler
            except ImportError:
                class NullHandler(logging.Handler):
                    def emit(self, record):
                        pass
            self.logger.addHandler(NullHandler())
    
    #===========================================================================
    # Logging
    #===========================================================================
    @classmethod
    def initLogging(cls,**keys): cls.init_logging(**keys);  #pylint: disable=unnecessary-semicolon
    
    @classmethod
    def init_logging(cls,**keys):
        """
        file_log_name=None  # name of log file (defaults to output dir)
        file_log_lvl='DEBUG'  # level to log in file (None to not log to file)
        console_log_lvl='DEBUG'  # level to log to console
        show_warning=True # show warning for not writing to the file or console.
        config_file='ABS/FILE/PATH.[ini|json]' # load config details from config file
        # valid log_lvls: None,DEBUG,INFO,WARNING,ERROR,CRITICAL
        """
        # check config file first!
        config_file = keys.get('config_file',None)
        if config_file:
            # extension?
            config_ext = os.path.splitext(config_file)[1].lower()
            if config_ext == '.json':
                # imports
                import json
                from logging.config import dictConfig
                # open and parse json
                with open(config_file, 'r') as f:
                    config = json.load(f)
                dictConfig(config) # load!
            elif config_ext == '.ini':
                from logging.config import fileConfig
                fileConfig(config_file)
            else:
                raise ValueError('unable to load extension for config file, "{0}"'.format(config_file))
            return #exit
        
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

        # create logging formatter .%(msecs)-3d
        fmt = '%(asctime)-23s:%(threadName)-10s:%(levelname)-7s:%(name)s.%(funcName)s:%(message)s'
        log_formatter = logging.Formatter(fmt)

        # create handlers based on request
        if file_log_lvl:
            # --- add file handler ---
            # set default name
            if file_log_name is None: file_log_name = 'log.log'
            # set default directory
            directory = os.path.dirname(file_log_name)
            if directory == '':
                # get_output_fd inside this will return this classes path output dir
                file_log_name = cls.get_fd(filename=file_log_name,fd='output',call_depth=2)
                directory = os.path.dirname(file_log_name)
            # create parent directory(s) if needed
            if not os.path.exists(directory): os.makedirs(directory)
            #fhndl = logging.FileHandler(file_log_name)#,mode='w') #to not append for the day
            from logging.handlers import RotatingFileHandler
            fhndl = RotatingFileHandler(
                filename = file_log_name,
                mode='a',
                maxBytes=10485760, #10MB
                backupCount=3)
            fhndl.setLevel(logging.__getattribute__(file_log_lvl))  #@UndefinedVariable pylint: disable=no-member
            fhndl.setFormatter(log_formatter)
            logging.getLogger().addHandler(fhndl)

        if console_log_lvl:
            # add console handler
            #import sys
            shndl = logging.StreamHandler(sys.stdout) # change the coloring! from stderr to stdout
            shndl.setFormatter(log_formatter)
            shndl.setLevel(logging.__getattribute__(console_log_lvl)) #@UndefinedVariable pylint: disable=no-member
            logging.getLogger().addHandler(shndl)
        elif show_warning:
            print('=======================================')
            print('not showing log in console per request!')
            print(file_log_name)
            print('=======================================')
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

    #===========================================================================
    # static method for filling a dict with Nones where needed...
    #===========================================================================
    @classmethod
    def fillDict(cls,d,keys,val=None,**updates):  #pylint: disable=invalid-name
        cls.fill_dict(d,keys,val,**updates)
        
    @classmethod
    def fill_dict(cls,d,keys,val=None,**updates):  #pylint: disable=invalid-name
        """
        fills the dictionary (d) with None for each key in keys.
        val: what to fill the each key with.
        After filling, d is updated by updates (d.update(updates))
        returns the filled dict (not a copy).
        NOTE: for sub-dict values needed, key.skey.... --> d[key][skey]...
        NOTE: this means a key of 'key.name' is invalid!!!
        """
        for key in keys: cls._fill_dict(d,key,val)
        d.update(updates)
        return d

    @classmethod
    def _fill_dict(cls,d,key,val):  #pylint: disable=invalid-name
        """
        helper function to populate individual key.
        d: a dictionary
        key: a string
        val: the default value
        """
        ind = key.find('.')
        if ind == -1:
            d.setdefault(key,val)
        else:
            if d.get(key[:ind]) is None: d[key[:ind]] = dict()

            if not isinstance(d[key[:ind]],dict):
                raise ValueError(
                    'the key {!r} is invalid. top value is type {}, not a dict!'.format(
                        key,type(d[key[:ind]])))

            cls._fill_dict(d[key[:ind]],key[ind+1:],val)

    #===============================================================================
    # get/join with project folders
    #===============================================================================
    # NOTE: __file__ will still reference this loc..., assuming working dir...
    @classmethod
    def get_resource_fd(cls,filename=''):
        """pass a filename to join with the resource folder (handles frozen)"""
        #dir_name = os.path.join(os.path.realpath(''),) #__file__
        #return cls.join_folder_and_file(dir_name,filename)
        return cls.get_fd(filename=filename,fd='resources',call_depth=2)
    @classmethod
    def get_output_fd(cls,filename=''):
        """pass a filename to join with the output folder (handles frozen)"""
        return cls.get_fd(filename=filename,fd='output',call_depth=2)
    @classmethod
    def get_fd(cls,filename='', fd='', call_depth=2):
        """get the output folder, depth > 1"""
        try: filepath =  sys._getframe(call_depth).f_code.co_filename  #pylint: disable=protected-access
        except: filepath = __file__  #pylint: disable=bare-except
        return os.path.join(os.path.dirname(
                sys.executable if getattr(sys, 'frozen', False) else filepath
            ),fd,filename)

    @classmethod
    def path_exists(cls,folder,filename=''):
        """
        tests if folder/file exists.
        returns None if if doesn't, the folder/file if successful!
        """
        path = os.path.abspath(os.path.join(folder, filename) if filename else folder)
        return path if os.path.exists(path) else None

    #===========================================================================
    # Summary
    #===========================================================================
    def get_summary_info(self):  #pylint: disable=no-self-use
        """override to add useful summary info."""
        return []

    def log_summary_info(self,prepend=''):
        """call to log all important summary information."""
        if prepend:
            for i in self.get_summary_info():
                self.logger.info('%s: %s',prepend,i)
        else:
            for i in self.get_summary_info():
                self.logger.info('%s',i)

    #===========================================================================
    # Timestamp
    #===========================================================================
    @staticmethod
    def get_current_datetime(utc=False,add_sec=0):
        """Helper function for getting the current datetime.
        param utc: If False, returns the time based on the current timezone.
                   If True, returns the UTC time."""
        current_time = datetime.datetime.utcnow() if utc else datetime.datetime.now()
        if add_sec: current_time += datetime.timedelta(seconds=add_sec)
        return current_time

    @classmethod
    def datetime_to_timestamp(cls,dt,for_file=False,dt_format=None,add_sec=0):  #pylint: disable=invalid-name
        """Formats the datetime passed to ISO 8601 string.
        for_file -- if True, replaces colons with periods."""
        if dt_format is None: dt_format = cls.TIMESTAMP_FORMAT
        if for_file: dt_format = dt_format.replace(':','').replace('-','').replace('T','')

        if add_sec: dt += datetime.timedelta(seconds=add_sec)
        # only accurate to 3 for win8.1, but 6 given (for after sec)
        return dt.strftime(dt_format)

    @classmethod
    def get_current_timestamp(cls,utc=False,add_sec=0,
                              for_file=False,dt_format=None):
        """Formats the current time (using datetime.datetime.now) to ISO 8601.
        for_file -- if True, replaces colons with periods."""
        return cls.datetime_to_timestamp(
                    cls.get_current_datetime(utc=utc,add_sec=add_sec),
                    for_file=for_file,dt_format=dt_format)

    #===========================================================================
    # Reading/Writing key-value pairing files
    #===========================================================================
    @staticmethod
    def read_file_key_values(filename):
        """reads the file (filename) and returns the key-
        value pairing (a dictionary)"""
        ret = dict() #{i:None for i in keys}

        try:
            with open(filename,'r') as reader:
                # get all key-value pairs from the file, line-by-line
                for key,value in (l.rstrip().split('=',1) for l in reader if '=' in l):
                    # if no arguments are passed or the key is one we're looking for
                    #if not keys or key in ret:
                    ret[key] = value
        except IOError: pass
        return ret

    @staticmethod
    def write_file_key_values(filename,overwrite_file=True,**keys):
        """writes the key-value pairing passed (**keys) to the
        file (filename).
        If overwrite_file is fail, the operation failed.
        is the operation is successful, True is returned.
        """
        # overwrite?
        if not overwrite_file and os.path.exists(filename): return False

        # open the file
        try:
            with open(filename,'w') as writer:
                writer.writelines(('{}={}\n'.format(k,v) for k,v in keys.items()
                              if v is not None))
        except IOError: return False
        else: return True

#===============================================================================
# Main
#===============================================================================
if __name__ == '__main__':
    #try: import tests.test_myloggingbase
    #except ImportError: print 'no test for myloggingbase'
    #else: tests.test_myloggingbase.run_test()
    MyLoggingBase.init_logging(
        file_log_lvl=None#'DEBUG'
        ,show_warnings=False
    )
    #file_log_name=os.path.basename(__file__)+'.log')
    base = MyLoggingBase()

    base.logger.info('hello world')
    out = base.get_output_fd()
    base.logger.info('output - %s',out)
    