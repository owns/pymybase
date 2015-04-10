"""
Elias Wood (owns13927@yahoo.com)
2015-04-06
"""

import os
from myloggingbase import MyLoggingBase
import traceback
#IDEA: move to myloggingbase???
#===============================================================================
# # Init Logging
#===============================================================================
def init_logging(**keys):
    """
    file_log_name=None  # name of log file
    file_log_lvl='DEBUG'  # level to log in file (None to not log to file)
    console_log_lvl='DEBUG'  # level to log to console
    show_warning=True # show warning for not writing to the file or console.
    # valid log_lvls: None,DEBUG,INFO,WARNING,ERROR,CRITICAL
    """
    import logging
    # necessary import for exception hooking
    import sys
    
    # set values
    file_log_name = keys.get('file_log_name')
    file_log_lvl = keys.get('file_log_lvl')
    console_log_lvl = keys.get('console_log_lvl')
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
        if file_log_name==None: file_log_name = get_output_fd('log{}.log'.format(MyLoggingBase.get_current_timestamp(True)))
        h = logging.FileHandler(file_log_name)#,mode='w') #to not append for the day
        h.setLevel(logging.__getattribute__(file_log_lvl)) # @UndefinedVariable
        h.setFormatter(log_formatter)
        logging.getLogger().addHandler(h)
            
    if console_log_lvl:
        # add console handler
        h2 = logging.StreamHandler(sys.stdout) # to change the coloring!
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
        
    # log all exceptions!
    def exception_hook(exctype, exc, tb):
        #this method just handles the output, not raising the error... 
        logging.critical('\n'+''.join(traceback.format_tb(tb)))
        logging.critical('%s: %s',exctype,exc.message)
            
    sys.excepthook = exception_hook

#===============================================================================
# # get/join with project folders
#===============================================================================
def get_resource_fd(filename=None):
    """pass a filename to join with the resource folder"""
    dir_name = os.path.join(os.path.dirname(__file__),'resources')
    return join_folder_and_file(dir_name,filename)

def get_output_fd(filename=None):
    """pass a filename to join with the output folder"""
    dir_name = os.path.join(os.path.dirname(__file__),'output')
    return join_folder_and_file(dir_name,filename)

def join_folder_and_file(fd,filename=None):
    """tests if folder exists, returns None if if doesn't, the filepath if successful!"""
    if isinstance(filename,(str,unicode)):
        return os.path.join(fd,filename)
    else: return fd

#===============================================================================
# MAIN
#===============================================================================
if __name__ == '__main__':
    try: from tests import test_main
    except ImportError: print 'no test for main'
    else: test_main.run_test()
    