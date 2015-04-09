"""
Elias Wood (owns13927@yahoo.com)
2015-04-07
a basic class for handling jobs (to work with mythread class).
Can also override a few methods to get a nice db class.
"""
import os
import sqlite3 as sql
from threading import Lock as Thread_Lock
import Queue
from myloggingbase import MyLoggingBase
from tests import test_mydb

class MyDb(MyLoggingBase):
    """
    db with jobs table
    NOTES:
        if you want to put a limit on the queues:
            just run populate_queues.
            It'll go through all the remaining jobs and add them
            can add option in mythread to do this whenever a job completes?
    """
    
    _db = None # the database connection object
    _filename = None # the name of the db file
    _lock = None # a thread lock to control read/updates
    
    _jobs_added_count = None
    _jobs_updated_count = None
    #_isolation_level = None # the iso level (https://docs.python.org/2/library/sqlite3.html#connection-objects)
    
    _JOBS_ID,_JOBS_ITEM_ID,_JOBS_JOB_TYPE,_JOBS_INIT_DATA,_JOBS_START_VALUE,_JOBS_END_VALUE = 0,1,2,3,4,5
    
    JOB_ID,ITEM_ID,INIT_DATA,START_VALUE,END_VALUE = 0,1,2,3,4
    
    def __init__(self,filename=None):
        MyLoggingBase.__init__(self)
        
        self._jobs_added_count = 0
        self._jobs_updated_count = 0
        
        self._lock = Thread_Lock()
        
        if filename != None:
            self.open(filename)
            
    #===========================================================================
    # Creating a new Db
    #===========================================================================
    def new(self,filename):
        # create a new db
        # will NOT overwrite an existing file!
        
        # shouldn't happen...
        if os.path.exists(filename): return False
        
        # open the connections
        db = self._try_open(filename)
        if db == None: return False

        self._filename = filename     
        self._db = db
        
        self.logger.info('database created %s',filename)
        self._init_db_structure()
        
    #===========================================================================
    #=============================== DB FILE IO ================================
    #===========================================================================
    def _try_open(self,filename):
        # close db if one is open
        if self.is_open(): self.close()
        
        db = None
        try: db = sql.connect(filename,check_same_thread=False)#,isolation_level=self._isolation_level) # auto-commit
        except sql.Error, e:
            # log and return failure!
            self.logger.critical('failed. %s: %r',e.__class__.__name__,e)
            return None
        
        # everything went well
        else: return db
        
    #===========================================================================
    # Open Db
    #===========================================================================
    def open(self,filename,create=True):
        # if create is true, a new db will be created if the specified one DNE.
        # NOTE: pass ':memory:' for filename to get a 'in RAM' db
        if self.__debug: self.logger.debug('filename=%s create=%s',filename,create)
        
        # open db if file exists...
        if os.path.exists(filename) or filename==':memory:':
            # init db - try
            try: self._db = sql.connect(filename,check_same_thread=False)
            except sql.Error, e:
                # log and return failure!
                self.logger.critical('failed. %s: %r',e.__class__.__name__,e)
                return False
            
            # everything went well
            else:
                self.logger.info('database opened')
                self._filename = filename
                return True
        
        # create db only if it should be created!
        elif create:
            # make sure the file can be created
            try: open(filename,'w').close()
            except IOError as e:
                # log and return failure!
                self.logger.warning('IOError with filename %s. %s: %r',
                                    filename,e.__class__.__name__,e)
                return False
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
        # return true if db closed successfully
        # if there are changes (check _isChanged()) they are saved (commit=True)
        retV = False
        with self._lock:
            if self._db!=None:
                try:
                    # save changes if needed - never because using autocommit
                    if commit: self._db.commit() # not necessary...
                    # close db
                    self._db.close()
                    self._db = None
                    
                except sql.Error, e:
                    # log and return failure!
                    self.logger('failed. %s: %r',e.__class__.__name__,e)
                
                # close success!
                else: retV = True
                
        return retV
            
    #===========================================================================
    #============================ GETTERS / SETTERS ============================
    #===========================================================================
    def is_open(self):
        # is there a db connection open?
        with self._lock:
            is_open = self._db != None
        return is_open 

    def _is_open(self):
        # assumes you have the lock
        return self._db != None

    def get_db(self):
        # not thread safe!!!
        if self.is_open(): return self._db
        else: return None
    
    def get_filename(self):
        with self._lock:
            filename = self._filename if self._is_open() else None
            
        return filename
    
    #===========================================================================
    # Initialize DB Structure (tables etc)
    #===========================================================================
    def _init_db_structure(self):
        # create job table
        with self._db as conn:
            conn.execute('''
                CREATE TABLE jobs(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id TEXT,
                job_type TEXT,
                init_data INTEGER,
                start_value TEXT,
                end_value TEXT)''')
    
    def get_tables(self):
        retV = False
        with self._lock:
            if self._is_open():
                try:
                    with self._db as conn:
                        retV = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                except sql.Error as e:
                    self.logger.warning('failed!?!? %r',e)
        return retV
    #===========================================================================
    #=============================== JOB THINGS ================================
    #===========================================================================
    def iter_jobs(self):
        # NOTE: this locks the db!!!
        with self._lock:
            if self._is_open():
                try:
                    with self._db as conn:
                        for i in conn.execute('SELECT * FROM jobs'):
                            yield i
                except sql.Error as e:
                    self.logger.warning('failed!?!? %r',e)
    
    def get_job_count(self):
        retV = False
        with self._lock:
            if self._is_open():
                try:
                    with self._db as conn:
                        retV = conn.execute('SELECT COUNT(id) FROM jobs').fetchone()[0]
                except sql.Error as e:
                    self.logger.warning('counting jobs failed!?!? %r',e)
        return retV
    
    #===========================================================================
    # Populate Queues
    #===========================================================================
    def populate_queues(self,queues):
        """
        populates queues with jobs but won't wait!
        returns False if there was a problem querying the jobs table, True otherwise.
        """
        retV = True
        #c = 0
        with self._lock:
            if self._is_open():
                for k in queues:
                    # note: k is the job type
                    # get a db connection to a list of the jobs
                    try:
                        with self._db as conn:
                            job_list = conn.execute('SELECT id,item_id,init_data,start_value,end_value FROM jobs WHERE job_type=?',(k,))
                    except sql.Error as e:
                        self.logger.warning('populating queue=%s failed!?!? %r',k,e)
                        retV = False
                        continue
                     
                    # okay, now let's see if we can add them (the queues aren't full...
                    for job in job_list: 
                        try: queues[k].put(job,False)
                        except Queue.Full:
                            if self.__debug: self.logger.debug('queue is full, and I am not waiting!')
                            break
                        #else: c += 1
        
        return retV
    
    
    #===========================================================================
    # Add Job
    #===========================================================================
    def add_job(self,item_id,job_type,init_data=None):#,start_value=None,end_value=None):
        """
        adds job to list and returns job_id.  Returns False if failed to add!
        """
        retV = False
        with self._lock:
            if self._is_open():
                retV = self._add_job(item_id, job_type, init_data)
        return retV
    
    def _add_job(self,item_id,job_type,init_data=None):
        '''
        the non thread-safe way of adding a job...
        '''
        retV = False
        try:
            with self._db as conn:
                # see if the jobs already there
                add_to_db = True
                #old_job_id = None # if we need to update the init_data
                max_init_data = None
                for row in conn.execute('SELECT * from jobs where item_id=? and job_type=?',(item_id,job_type)):
                    # keep track of max init_data. if >= init_data ==> no need to add
                    max_init_data = max(max_init_data,row[self._JOBS_INIT_DATA])
                    # job hasn't started so no need to add
                    if row[self._JOBS_START_VALUE] is None:
                        # this means there is a similar job but it hasn't been started, so there's no need to add one
                        add_to_db = False
                        #if row[self._JOBS_INIT_DATA] != init_data:
                        #    # but we should update the initial value
                        #    old_job_id = row[self._JOBS_ID]
                        if init_data is None:
                            break
                        #else: pass # keep tracking jobs init_data to see if necessary to add
                
                # to update or not to update?
                # if we should and adding there are some new values to pull...
                if add_to_db:
                    if init_data==None or init_data > max_init_data:
                        conn.execute('INSERT INTO jobs (item_id,job_type,init_data) values (?,?,?)',
                                     (item_id,job_type,init_data))
                        retV = conn.execute('SELECT * FROM jobs WHERE item_id=? and job_type=? and start_value is null',
                                            (item_id,job_type)).fetchone()[self._JOBS_ID]
                    else: self.logger.info('no need to add the job. jobs in the db will already get all of them...') 
                else:
                    self.logger.warning("trying to add a job but it's already been added and not started/updated!")
                    # no need to update - cannot update queues...
                    #if old_job_id != None:
                    #    # update job with newer init_data
                    #    conn.execute('UPDATE jobs SET init_data=? WHERE job_id=?',
                    #             (init_data,old_job_id))
                    #    self.logger.info('updated job with newer info... won\'t update queue though....')
                    #else:
        except sql.Error as e:
            self.logger.warning('added job failed! %r',e)
            retV = False
        #else: retV = t!=self._db.total_changes
        return retV
    
    #===========================================================================
    # Update Job 
    #===========================================================================
    def update_job(self,job_id,start_value,end_value=None):
        """
        updates job with new data, start_value and end_value
        Returns True for success
        """
        retV = False
        with self._lock:
            if self._is_open():
                t = self._db.total_changes
                try:
                    with self._db as conn:
                        if end_value is None:
                            conn.execute('UPDATE jobs set start_value=? WHERE id=?',
                                         (start_value,job_id))
                        else:
                            # they provided an end_value
                            conn.execute('UPDATE jobs set start_value=?,end_value=? WHERE id=?',
                                         (start_value,end_value,job_id))
                except sql.Error as e:
                    self.logger.warning('updating job failed! %r',e)
                    retV = False
                else: retV = t!=self._db.total_changes
                
        return retV
          
    #===========================================================================
    # Remove Job
    #===========================================================================
    def remove_job(self,job_id):
        """
        removed job from the table.  Assuming b/c the job is done!
        Returns True for success
        """
        retV = False
        with self._lock:
            if self._is_open():
                t = self._db.total_changes
                try:
                    with self._db as conn:
                        conn.execute('DELETE FROM jobs WHERE id=?',(job_id,))
                except sql.Error as e:
                    self.logger.warning('removing job failed! %r',e)
                    retV = False
                else: retV = t!=self._db.total_changes
                
        return retV
    
    #===========================================================================
    # Get Summary info
    #===========================================================================
    def _get_summary_info(self):
        return ['total db changes: {:,}'.format(self._db.total_changes),
                'jobs in jobs table: {:,}'.format(self.get_job_count())]

#===============================================================================
# Main
#===============================================================================
if __name__ == '__main__':
    try: from tests import test_mydb
    except ImportError: print 'no test for mydb'
    else: test_mydb.run_test()
    