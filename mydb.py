"""
Elias Wood (owns13927@yahoo.com)
2015-04-07
a basic class for handling jobs (to work with mythread class).
Can also override a few methods to get a nice db class.

CHANGELOG:
2016-02-04: Elias Wood
    unique index on item_id to optimize adding jobs
"""
import os
import sqlite3 as sql
import threading
#from threading import Lock as Thread_Lock
#from threading import Event as Thread_Event
from collections import deque
from myloggingbase import MyLoggingBase

QUEUE_FIFO = True
QUEUE_LIFO = False

class MyDb(MyLoggingBase):
    """
    db with jobs table.  Works closely with MyThread class which masks the using
    of this class to the end-user (the developer) for handling 'jobs' and
    smart recovery.  Read the details in the MyThread class description.
    NOTES:
        Methods that start with _* are usually not thread safe and don't check
            if the db is open - use with caution.
        if you want to put a limit on the queues:
            just run populate_queues.
            It'll go through all the remaining jobs and add them
            can add option in mythread to do this whenever the queue is empty.
    """
    #TODO: thread event per queue
    
    #===========================================================================
    # Class Constants
    #===========================================================================
    _JOBS_ID,_JOBS_IN_QUEUE,_JOBS_ITEM_ID,_JOBS_JOB_TYPE, = 0,1,2,3
    _JOBS_INIT_DATA,_JOBS_START_VALUE,_JOBS_END_VALUE = 4,5,6
    
    JOB_ID,ITEM_ID,INIT_DATA,START_VALUE,END_VALUE = 0,1,2,3,4
    
    #===========================================================================
    # Variables
    #===========================================================================
    _queues = None
    queue_max = None
    queue_type = None
    _db = None # the database connection object
    _filename = None # the name of the db file
    _lock = None # a thread lock to control read/writes
    _queue_lock = None
    event = None
    
    #===========================================================================
    # Tracked Metrics
    #===========================================================================
    _jobs_added_count = None
    _jobs_updated_count = None
    _jobs_removed_count = None
    _populate_count = None
    __total_changes = 0
    
    def __init__(self,filename=None,queue_max=None,queue_type=QUEUE_FIFO):
        """filename if the file name for the db file. If no filename is passed,
        no db will be created; you can then use either .new(...) or .open(...)."""
        
        super(MyDb,self).__init__()
        
        self._jobs_added_count = 0
        self._jobs_updated_count = 0
        self._jobs_removed_count = 0
        self._populate_count = 0
        
        self._lock = threading.Lock()
        self._queue_lock = threading.Lock()
        self.event = threading.Event()
        self.event.clear()
        self._queues = {}
        self.queue_max = queue_max if queue_max else None
        self.queue_type = queue_type
        
        if filename != None:
            self.open(filename)
            
    #===========================================================================
    # Creating a new Db
    #===========================================================================
    def new(self,filename):
        """create a new db.  Will NOT overwrite an existing file! returns """
        
        # shouldn't happen...
        if os.path.exists(filename): return False
        
        # open the connections
        db = self._try_open(filename)
        if db == None: return False

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
            except sql.Error, e:
                # log and return failure!
                self.logger.critical('failed. %s: %r',e.__class__.__name__,e)
                db = None
            
        return db
        
    #===========================================================================
    # Open Db
    #===========================================================================
    def open(self,filename,create=True):
        """If create is True, a new db will be created if the specified one DNE.
        # NOTE: pass ':memory:' for filename to get an 'in memory/RAM' db"""
        self.logger.debug('filename=%s create=%s',filename,create)
        
        # create an in memory db
        if filename==':memory:':
            return self.new(filename)
        
        # open db if file exists...
        elif os.path.exists(filename):
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
                self.populate_queues(first_call=True)
                return True
            
        # create db only if it should be created!
        elif create or filename==':memory:':
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
                
            except sql.Error, e:
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
        return tables
    
    def get_job_count(self):
        """returns the number of jobs in the db."""
        job_cnt = False
        with self._lock:
            if self._is_open():
                try:
                    with self._db as conn:
                        job_cnt = conn.execute('SELECT COUNT(id) FROM jobs').fetchone()[0]
                except sql.Error as e:
                    self.logger.warning('failed!?!? %r',e)
        return job_cnt
    
    def iter_jobs(self):
        """iterate through all the jobs.  NOTE: this locks the db!!!"""
        with self._lock:
            if self._is_open():
                try:
                    with self._db as conn:
                        for i in conn.execute('SELECT * FROM jobs'):
                            yield i
                except sql.Error as e:
                    self.logger.warning('failed!?!? %r',e)
    
    def get_event(self):
        return self.event
    
    #===========================================================================
    # Initialize DB Structure (tables etc)
    #===========================================================================
    def _init_db_structure(self):
        """creates job table. override to make the db more interesting!
        This method is called only when a new db is created."""
        with self._db as conn:
            conn.execute('''
                CREATE TABLE jobs(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                in_queue INTEGER DEFAULT 1,
                item_id TEXT NOT NULL,
                job_type TEXT NOT NULL,
                init_data INTEGER,
                start_value TEXT,
                end_value TEXT,
                CONSTRAINT jobs_itemId_jobType_initData_uni UNIQUE
                    (item_id,job_type,init_data))''')
            conn.execute('CREATE INDEX jobs_item_id_idx ON jobs (item_id)')
        return True
    #===========================================================================
    #============================= 'Queue' Things ==============================
    #===========================================================================
    def put_in_queue(self,job_type,item,**keys):
        return self.put_many_in_queue(job_type,(item,),**keys)
        
    def put_many_in_queue(self,job_type,items,**keys):
        """ returns tuple of job ids that
        failed to be added / kicked out of the queue
        """
        with self._queue_lock:
            idsAddedRemoved = self._put_many_in_queue(job_type, items,**keys)
        return idsAddedRemoved
    
    def _put_many_in_queue(self,job_type,items,**keys):
        """ items is iterable. returns ids not added / removed from the queue """
        if job_type in self._queues:
            q = self._queues[job_type]
        else:
            q = self._queues[job_type] = deque(maxlen=self.queue_max)
        
        idsAdded = dict()
        idsRemoved = tuple() # ids not in the queue
        items = (i for i in items) # make generator
        # if there's a limit
        if q.maxlen:
            
            if self.queue_type == QUEUE_FIFO:
                # if FIFO stop appending till full
                #q.extend(items.next() for i in xrange(q.maxlen-len(q)))
                try:
                    for i in xrange(q.maxlen-len(q)):
                        q.append(items.next())
                        idsAdded[q[-1][0]] = None # job id
                except StopIteration: pass
                else:
                    # we still might have some records
                    idsRemoved = tuple(i[0] for i in items)
            else:
                # if LIFO, remove needed from queue
                d = dict()
                for item in items:
                    if q.maxlen == len(q): d[q.pop()[0]] = None
                    q.appendleft(item)
                
                idsRemoved = tuple(d.keys())
                
        # if queue is limitless, append everything!
        else:
            for i in items:
                q.append(i)
                idsAdded[i[0]] = None # job id
        
        # wake up threads or let them sleep
        if q: self.event.set()
        #else: self.event.clear()
        
        return tuple(idsAdded.keys()),idsRemoved
    
    def get_next_job(self,job_type):
        return self.get_next_jobs(job_type,count=1)[0]
    
    def get_next_jobs(self,job_type,count=1):
        """get the next job that is job_type that hasn't been started by
        another thread yet.  Returns the standard job tuple.  Raises
        StopIteration if:
            1. the queue was never created (no job_type added)
            2. wait is false and the queue is empty
        Params:
            job_type: the type of the job
        NOTES:
            queues are thread-safe, so this method is too!
        """
        # try to get the next item
        items = list()
        cont = True
        
        self._queue_lock.acquire()
        
        while cont and count > 0:
            try: item = self._queues[job_type].popleft()
            except (KeyError,IndexError), e:
                if self.event.is_set():
                    self.event.clear() # no data in queue
                    # log something
                    self.logger.warning('no "%s" jobs in queue yet...'
                                        if type(e) == IndexError else
                                        '"%s" queue not initialized...',job_type)
                    
                    self._queue_lock.release()
                    if not self.populate_queues(job_type):
                        # if returns false -> no more jobs
                        cont = False
                    
                    self._queue_lock.acquire()
                    
                else: cont = False # stop
            else:
                
                items.append(item) # fastest item - dict
                count -= 1 # we got one - keep going
        
        self._queue_lock.release()
         
        if cont or items: return items
        else: raise StopIteration
    
    #===========================================================================
    # Populate Queues
    #===========================================================================
    def populate_queues(self,job_type=None,first_call=False,job_types=None):
        """populates queues with jobs but won't wait!
        returns False if there was a problem querying the jobs table, True otherwise.
        """
        self._populate_count += 1
        success = True
        
        try:
            with self._lock,self._db as conn:
                # clear "in_queue" flag if first run
                if first_call: conn.execute('UPDATE jobs SET in_queue=0')
                
                # get how many we're able to add
                out_queue,total = conn.execute("""SELECT
                    ifnull(-1*SUM(in_queue-1),0) "not_in_queue",
                    COUNT(in_queue) "total" FROM jobs""").fetchone()
                
                self.logger.debug('not in_queue=%s / total=%s',out_queue,total)
                
                # are there items not in queues?
                if out_queue==0:
                    # there are no jobs to add...
                    success = False
                    raise RuntimeError('stopped b/c no data to get')
                
                # get all job types (or use the one given)
                job_types = tuple(i[0] for i in conn.execute(
                        'SELECT distinct job_type FROM jobs')) \
                    if job_type is None else ((job_type,) \
                    if job_types is None else job_types)
                
                # if there's no jobs, we failed to populate the queues
                if not job_types: success = False
                
                for job_type in job_types:
                    with self._queue_lock:
                    
                        # find how many records the queue needs
                        if self.queue_max: # if there's a limit for queues,
                            if job_type in self._queues: # if this queue's been created
                                # find out how much is left to add
                                limit = self._queues[job_type].maxlen-len(self._queues[job_type])
                                # NOTE: if it's full, limit == 0 --> nothing returned!
                            # if queues not created, it's max will be queue_max 
                            else: limit = self.queue_max
                        # if no limits, get everything!
                        else: limit = -1
                        
                        # insert into queues
                        added,removed = self._put_many_in_queue(job_type,conn.execute(
                            """ SELECT * FROM (
                                SELECT id,item_id,init_data,start_value,end_value
                                FROM jobs WHERE in_queue=0 AND job_type=? """+
                                'ORDER BY id '+
                                ('' if self.queue_type==QUEUE_FIFO else 'DESC ')+
                                'LIMIT ?) a ORDER BY id ASC',(job_type,limit)))
                        
                        if removed:
                            self.logger.warning(
                                    'we are adding too much! %d',len(removed))
                        
                        # update db
                        def g(idsIn,idsOut):
                            for i in idsIn: yield (1,i)
                            for i in idsOut: yield (0,i)     
                        conn.executemany('UPDATE jobs SET in_queue=? WHERE id=?',g(added,removed))
                        
                        # is there anything?
                        if len(self._queues[job_type]) == 0: success = False
                
        except RuntimeError, e: pass # we stopped on purpose
        except sql.Error, e:
            self.logger.warning('populating queues failed!?!? %r',e)
            success = False
        
        except Exception, e:
            raise e
            
        return success
    #'''
    #===========================================================================
    #=============================== JOB THINGS ================================
    #===========================================================================
    #===========================================================================
    # Add Job
    #===========================================================================
    def add_job(self,item_id,job_type,init_data=None):#,start_value=None,end_value=None):
        """Adds job and returns job_id.  Returns None if failed to add!"""
        job_ids = self.add_jobs(((item_id,job_type,init_data),))
        return job_ids[0] if job_ids else False
    
    def add_jobs(self,items):
        """The non thread-safe way of adding a job... used internally."""
        job_ids = {}
        attempt_cnt = 0
        with self._lock,self._db as conn:
            
            '''
            t = conn.total_changes
            conn.executemany("""INSERT OR IGNORE INTO jobs (item_id,job_type,init_data)
                VALUES (:iId,:jType,:data)""",
                (dict(iId=i[0],jType=i[1],data=i[2]) for i in items))
            if conn.total_changes != t:
                self.populate_queues()
            '''
            try_to_add_again = dict()
            for item in items:
                attempt_cnt += 1
                item_id = item[0]
                job_type = item[1]
                init_data = item[2] if len(item)>2 else None
                try:
                    #------ see if the jobs already there ------
                    add_to_db = True
                    max_init_data = None
                    for row in conn.execute('''SELECT init_data,start_value
                            FROM jobs WHERE item_id=? AND job_type=?''',(item_id,job_type)):
                        # keep track of max init_data. if >= init_data ==> no need to add
                        max_init_data = max(max_init_data,row[0])
                        # job hasn't started so no need to add
                        if row[1] is None:
                            # this means there is a similar job but it hasn't been started,
                            # so there's no need to add one
                            add_to_db = False
                            # if init_data isn't used, there's no reason to add a new one
                            break
                            #if init_data is None: break
                            #else: pass # keep tracking jobs init_data to see if necessary to add
                    
                    #------ to insert or not to insert? ------
                    if add_to_db:
                        if init_data==None or init_data > max_init_data:
                            #insert job into db
                            job_id = conn.execute('INSERT INTO jobs (item_id,job_type,init_data) values (?,?,?)',
                                         (item_id,job_type,init_data)).lastrowid
                            job_ids[job_id] = None # track the jobs we've added
                            self._jobs_added_count += 1
                                  
                            # no FIFO if don't add asap...     
                            # REMOVED: will let waiting threads populate in batch  
                            # add job to queue (will also wake up any waiting threads)
                            added,removed = self.put_in_queue(job_type,(job_id,item_id,init_data,None,None)) # @UnusedVariable
                            # update the db to sync with what's in use / in the queue.
                            if removed: conn.execute('UPDATE jobs SET in_queue=0 WHERE id=?',removed) # only 1 at most
                            #conn.executemany('UPDATE jobs SET in_queue=0 WHERE id=?',((i,) for i in removed))
                            
                        else: self.logger.debug('no need to add %s. init_data is less',item_id) 
                    else:
                        try_to_add_again[item_id] = None
                    
                except sql.Error, e: self.logger.warning('added job %s failed! %r',item,e)
            
            if try_to_add_again:
                self.logger.warning("tried to add %d jobs again, but original "+
                                    "hasn't started yet!",len(try_to_add_again))
                
        self.logger.debug('%d / %d jobs added',len(job_ids),attempt_cnt)
        return job_ids.keys()
    
    #===========================================================================
    # Update Job 
    #===========================================================================
    def update_job(self,job_id,start_value,end_value=None):
        """updates job with new data, start_value and end_value
        Returns True for success."""
        success = False
        with self._lock,self._db as conn:
            t = self._db.total_changes
            try:
                conn.execute('''UPDATE jobs SET start_value=?,
                        end_value=ifnull(?,end_value) 
                    WHERE id=?''',(start_value,end_value,job_id))
                    
                self._jobs_updated_count += 1
            except sql.Error as e: self.logger.warning('updating job failed! %r',e)
            else: success = t!=self._db.total_changes
                
        return success
          
    #===========================================================================
    # Remove Job
    #===========================================================================
    def remove_job(self,job_id):
        """Removed job from the table.  Assuming b/c the job is done!
        Returns True for success."""
        return self.remove_jobs((job_id,))
    
    def remove_jobs(self,job_ids):
        success = False
        with self._lock, self._db as conn:
            t = self._db.total_changes
            try: conn.executemany('DELETE FROM jobs WHERE id=?',
                                  ((job_id,) for job_id in job_ids))
            except sql.Error as e:
                self.logger.warning('removing jobs failed! %r',e)
            else:
                self.logger.debug('%d / %d jobs removed',
                    self._db.total_changes-t,-1 if 'generator' in 
                    job_ids.__class__.__name__ else len(job_ids))
                self._jobs_removed_count += self._db.total_changes-t
                success = t!=self._db.total_changes # success if changes
                
        return success
        
    #===========================================================================
    # Failed Job
    #===========================================================================
    def failed_job(self,job_id,start_value=None,end_value=None):
        """Removed job from the table.  Assuming b/c the job is done!
        Returns True for success."""
        success = False
        with self._lock,self._db as conn:
            t = self._db.total_changes
            try: conn.execute('''UPDATE jobs SET in_queue=0,
                    start_value = ifnull(?,start_value),
                    end_value = ifnull(?,end_value)
                WHERE id=?''',(start_value,end_value,job_id))
            except sql.Error as e:
                self.logger.warning('failing job failed! %r',e)
            else:
                success = t!=self._db.total_changes # success if changes
                
        return success
    
    
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
            'jobs in jobs table: {:,}'.format(self.get_job_count()),
            'jobs added: {:,}'.format(self._jobs_added_count),
            'jobs updated: {:,}'.format(self._jobs_updated_count),
            'jobs removed: {:,}'.format(self._jobs_removed_count),
            'populate_queues call count: {:,}'.format(self._populate_count)))
        return a

#===============================================================================
# Main
#===============================================================================
def main():
    #try: from tests import test_mydb
    #except ImportError: print 'no test for mydb'
    #else: test_mydb.run_test()
    MyLoggingBase.init_logging(file_log_lvl=None)
    
    a = MyDb(':memory:',queue_max=500,queue_type=QUEUE_LIFO)
    
    class Worker(threading.Thread):
        db = None
        size = 100
        continue_looping = True
        continue_waiting = True
        def run(self):
            super(Worker,self).run()
            
            while self.continue_looping:
                try:
                    items = self.db.get_next_jobs('main',count=self.size)
                except StopIteration:
                    if self.continue_waiting: self.db.event.wait()
                    else: break
                else:
                    print self.name+str(len(items))#tuple(i[0] for i in items))
                    self.db.remove_jobs(tuple(i[0] for i in items))
                    
            print self.name + ' done'
            
        def stop(self):
            self.continue_waiting = self.continue_looping = False
            self.db.event.set()
            
        def stop_waiting(self):
            self.continue_waiting = False
            self.db.event.set()
    
    
    workers = [Worker(name='worker'+str(i)) for i in xrange(50)]
    for w in workers:
        w.db = a
        w.start()
    
    a.add_jobs(((i,'main') for i in xrange(10000)))
    
    a.add_jobs(((i,'main') for i in xrange(10000,11000)))
    
    for k,q in a._queues.iteritems():
        print k,[i[0] for i in q]
    
    #from pprint import pprint
    #pprint(a._db.execute('select id,in_queue from jobs').fetchall())
    
    a.log_summary_info()
    
    print 'joinging worker'
    for w in workers: w.stop_waiting()
    for w in workers: w.join()
    
    a.log_summary_info()
    a.close()
    
    print 'done'
    
    
    
    
if __name__ == '__main__':
    MyLoggingBase.init_logging(file_log_lvl=None)
    
    a = MyDb(':memory:',queue_max=3,queue_type=QUEUE_FIFO)
    
    a.add_jobs(((i,'main') for i in xrange(12)))
    print [i[0] for i in a._db.execute('select id from jobs')]
    print [i[0] for i in a._queues['main']]
    
    print [i[0] for i in a.get_next_jobs('main', count=3)]
    print [i[0] for i in a._db.execute('select id from jobs')]
    print [i[0] for i in a._queues['main']]
    
    a.populate_queues('main')
    print [i[0] for i in a._db.execute('select id from jobs')]
    print [i[0] for i in a._queues['main']]
    
    a.log_summary_info()
    a.close()