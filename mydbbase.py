# -*- coding: utf-8 -*-
"""
Elias Wood (owns13927@yahoo.com)
2015-04-07
a basic class for simplifying sqlite3

CHANGELOG:
2016-09-13: Elias Wood
    renamed to mydbbase!
2016-09-04: Elias Wood
    re-worked to 
2016-02-04: Elias Wood
    unique index on item_id to optimize adding jobs
"""
import os
import sqlite3 as sql
from threading import Lock as Thread_Lock
from myloggingbase import MyLoggingBase

class MyDbBase(MyLoggingBase):
    """
    db base class to simplify sqlite3
    """
    
    #===========================================================================
    # Class Constants
    #===========================================================================
    
    #===========================================================================
    # Variables
    #===========================================================================
    _db = None # the database connection object
    _filename = None # the name of the db file
    _lock = None # a thread lock to control read/writes
    
    #===========================================================================
    # Tracked Metrics
    #===========================================================================
    __total_changes = None
    
    def __init__(self,filename=None,*args,**keys):
        """filename if the file name for the db file. If no
        filename is passed, no db will be created; you can
        then use either .new(...) or .open(...)."""
        
        super(MyDb,self).__init__(*args,**keys)
        self.__total_changes = 0
        
        self._lock = Thread_Lock()
        
        # open db if filename passed
        if filename != None: self.open(filename)
            
    #===========================================================================
    # Creating a new Db
    #===========================================================================
    def new(self,filename):
        """create a new db.  Will NOT overwrite an existing file! returns """
        
        # shouldn't happen...
        if os.path.exists(filename):
            raise FileExists('the file "{}" already exists'.format(filename))
        
        # open the connections
        db = self._try_open(filename)
        
        self._filename = filename     
        self._db = db
        
        self.logger.info('database created %s',filename)
        return self._init_db_structure()
        
    #===========================================================================
    #=============================== DB FILE IO ================================
    #===========================================================================
    def _try_open(self,filename):
        """close prev db and try to open the new one. NOTE: will create."""
        db = None
        with self._lock:
            if self._is_open(): self._close()
            
            #                             ,isolation_level=self._isolation_level
            try: db = sql.connect(filename,check_same_thread=False) #auto-commit
            except sql.Error as e:
                # log and return failure!
                self.logger.warning('failed. %s: %r',e.__class__.__name__,e)
                raise e
            
        return db
        
    #===========================================================================
    # Open Db
    #===========================================================================
    def open(self,filename,create=True):
        """If create is True, a new db will be created if the specified one DNE.
        # NOTE: pass ':memory:' for filename to get an 'in memory/RAM' db"""
        self.logger.debug('filename=%s create=%s',filename,create)
        
        # create an in memory db
        if filename==':memory:': return self.new(filename)
        
        # open db if file exists...
        elif os.path.exists(filename):
            # try open
            self._db = self._try_open(filename)
                        
            # everything went well
            self.logger.info('database opened')
            self._filename = filename
            # do whenever is needed when first opened
            self._on_open()
            return True
            
        # create db only if it should be created!
        elif create:
            # make sure the file can be created
            try: open(filename,'w').close()
            except IOError as e:
                # log and return failure!
                self.logger.warning('IOError with filename %s. %s: %r',
                                    filename,e.__class__.__name__,e)
                raise e
            else:
                os.remove(filename) # delete the file!
                return self.new(filename)
        
        # file DNE and shouldn't be created!
        else:
            self.logger.warning('file DNE and you requested not to create one - no action taken!')
            return False
        
    #===========================================================================
    # Close Db
    #===========================================================================
    def close(self,commit=True):
        """return true if db closed successfully.  The db commits every action;
        the 'commit' argument is there for overriding and has no function."""
        with self._lock: success = self._close(commit)
        return success
    
    def _close(self,commit=True):
        if self._db!=None:
            try:
                # save changes if needed (never because using autocommit)
                if commit: self._db.commit() # not necessary...
                self.__total_changes = self._db.total_changes
                # close db
                self._db.close()
                self._db = None
                
            except sql.Error as e:
                # log and return failure!
                self.logger.critical('failed. %s: %r',e.__class__.__name__,e)
                return False
            # close success!
            else: return True
        else: return True # already closed / not opened - success!
            
            
    #===========================================================================
    #============================ GETTERS / SETTERS ============================
    #===========================================================================
    def is_open(self):
        # is there a db connection open?
        with self._lock: is_open = self._is_open()
        return is_open 

    def _is_open(self):
        return self._db != None # assumes you have the lock

    def get_db(self):
        # not thread safe!!!
        if self.is_open(): return self._db
        else: return None
    
    def get_filename(self):
        with self._lock:
            filename = self._filename if self._is_open() else None
        return filename
    
    def get_tables(self):
        """returns the list of tables or None if something failed..."""
        tables = None
        with self._lock:
            if self._is_open():
                try:
                    with self._db as conn:
                        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                except sql.Error as e:
                    self.logger.warning('failed!?!? %r',e)
                    raise e
        return tables
    
    #===========================================================================
    # Initialize DB Structure (tables etc)
    #===========================================================================
    def _init_db_structure(self):
        """run when new db created."""
        #with self._db as conn: conn.execute('')
        pass
    
    def _on_open(self):
        """run when the db is first opened."""
        #with self._db as conn: conn.execute('')
        pass
    
    #===========================================================================
    # Get Summary info
    #===========================================================================
    def _get_summary_info(self):
        """useful for printing summary information.  Override (remember to call
        the parent!) to add info to the summary (by calling _log_summary_info().
        """
        a = super(MyDb,self)._get_summary_info()
        a.extend(('total db changes: {:,}'.format(self._db.total_changes
                                      if self._db else self.__total_changes),
            'filename: "{}"'.format(self._filename)))
        return a



#===============================================================================
# Exception classes
#===============================================================================
class FileExists(Exception):
    """ db not created b/c file exists"""


#===============================================================================
# Main
#===============================================================================
if __name__ == '__main__':
    MyLoggingBase.init_logging(file_log_lvl=None)
    
    a = MyDb(':memory:')
    
    a.log_summary_info()
    a.close()