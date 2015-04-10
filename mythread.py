'''
Created on Mar 20, 2015

@author: owns
'''
from myloggingbase import MyLoggingBase
import threading
import time

class MyThread(MyLoggingBase,threading.Thread):
    """
    Abstract threading class, to be inherited and override:
        _find_job_end_value (optional)
        _process_item (required!)
        _before_looping  (optional)
        _after_looping (optional)
        _flush_files (optional) 
    Read the docs for each method for more details.    
    """
    
    _total_rows = None
    _jobs_completed = None
    
    _db = None
    
    _my_id = None
    _job = None
    
    _CTRL_WAKE_UP,_CTRL_QUEUE,_CTRL_JOB,_CTRL_INTERRUPT = 0,1,2,3
    
    _interupt_process = None
    _continue_waiting = None
    _continue_looping = None
    
    __current_job_id = None
    __current_job_start_v = None
    __current_job_end_v = None
    _DUMMY_END_VALUE = '<<DUMMY_END_VALUE>>' # return this means no lookup is needed when the job is resumed
    _END_JOB = '<<END_JOB_COMPLETE_DO_NOT_PROCESS>>' # return this means the job shouldn't be processed - complete and move on
    __job_updated_time = None
    _update_interval = 30 # seconds

    def __init__(self,job,db,**keys):
        """
        params (required):
            job: the job type the thread should process; the same job_type for MyDb.add_job(...)
            db: the db object to interface with (similar to MyDb)
        """
        # init logging base
        MyLoggingBase.__init__(self)
        
        # set my_id - unique identifier - and thread name (also unique)
        self._my_id = id(self)
        self._thread_name = '{}{}'.format(job[:2],self.get_my_id())
        
        # init threading things...
        threading.Thread.__init__(self,name=self.get_thread_name())
        
        # set vars
        self._db = db
        self._job = job
        self._total_rows = 0
        self._jobs_completed = 0
        
        # make sure our run loop will loop... !
        self._interupt_process = False
        self._continue_waiting = True
        self._continue_looping = True
    
    #===========================================================================
    #============================ GETTERS / SETTERS ============================
    #===========================================================================
    def get_thread_name(self):
        """returns the unique thread name (first two letters of the
        job_type followed by the id - id(self)."""
        return self._thread_name
    
    def get_my_id(self):
        """returns the id - id(self) - for the thread"""
        return self._my_id
    
    def get_job_type(self):
        """returns the job type this thread does; find __init__ for more."""
        return self._job
    
    #===========================================================================
    #========================= Thread Getters / Setters ========================
    #===========================================================================
    def __issue_stop(self,i):
        """add something to the queue to wake up the thread"""
        #if i not in (self._CTRL_QUEUE,self._CTRL_JOB,self._CTRL_INTERRUPT)
        self._db.put_in_queue(self.get_job_type(),i,True)
    
    def stop(self,wait_for_empty_queue=True):
        """stops the thread, either when the queue is empty or right away
        depending on the value passed.
        params:
            wait_for_empty_queue=True: if False, it will stop ASAP, depending
                on how fequently ._check_for_interrupt() is executed in the 
                ._process_item(...) method. If True, it stops when there are no
                more pending jobs."""
        if wait_for_empty_queue:
            self._continue_waiting = False
            self.__issue_stop(self._CTRL_QUEUE)
        else: self.interupt()
    
    def stop_after_job(self):
        """Stop the thread after the current job has ended."""
        self._continue_looping = False
        self.__issue_stop(self._CTRL_JOB)
        
    def interupt(self):
        """Kills the thread ASAP!"""
        self._continue_looping = False
        self._interupt_process = True
        self.__issue_stop(self._CTRL_INTERRUPT)
    
    #===========================================================================
    #============================= Thread runner! ==============================
    #===========================================================================
    def run(self):
        """started when the thread does.  See threading.Thread for more."""
        #TODO: finishe going through the class
        # see we can  initialize things...
        self._before_looping()
        
        # start looping
        while self._continue_looping:
            
            # is there another job to do?
            try: item = self._db.get_next_job(self.get_job_type(),
                                              self._continue_waiting)
            except StopIteration:
                self.logger.debug('queue is empty, and I am not waiting!')
                break
            else:
                # check for interupt
                if self._interupt_process:
                    self.logger.warning("we've been interrupted!")
                elif isinstance(item,int):
                    # someone is telling us to stop!
                    #======================== INTERRUPT ========================
                    if item == self._CTRL_INTERRUPT:
                        if self.__debug: self.logger.debug("I've been told to stop now... interrupted... okay")
                        self._continue_looping = False # don't get more things..
                        self._db.put_in_queue(self.get_job_type(),item,True) # let others know
                    #=========================== JOB ===========================
                    elif item == self._CTRL_JOB:
                        if self.__debug: self.logger.debug("I'm been told to stop when I'm done with my job... that's now. Okay")
                        self._continue_looping = False # don't get more things..
                        self._db.put_in_queue(self.get_job_type(),item,True) # let others know
                    #========================== QUEUE ==========================
                    elif item == self._CTRL_QUEUE:
                        if self.__debug: self.logger.debug("I've been told to stop when the queue is empty... okay")
                        self._continue_waiting = False
                    #========================= WAKE UP =========================
                    elif item == self._CTRL_WAKE_UP:
                        if self.__debug: self.logger.debug("I've been told to wake up... okay.")
                    #========================= IDK... ==========================
                    else:
                        if self.__debug: self.logger.debug("I've been told to stop with %d... i don't know what that means.... I'm ignoring it",item)
                        #self._continue_looping = False
                else:
                    # start the job
                    self.__job_updated_time = job_start_time = time.clock()
                    self.__current_job_id = item[self._db.JOB_ID]
                    
                    item = list(item)
                    
                    # some logging
                    self.logger.debug('starting job %s with id=%s',
                                      self.__current_job_id,
                                      item[self._db.ITEM_ID])
                    
                    # look in db to see what the end value should be (only if end_value isn't set)
                    if item[self._db.END_VALUE] == None:
                        # add it item to pass to process_item
                        item[self._db.END_VALUE] = self.__current_job_end_v = self._find_job_end_value(*item[1:])
                    
                    if item[self._db.END_VALUE] == self._DUMMY_END_VALUE:
                        item[self._db.END_VALUE] = None
                    
                    if item[self._db.END_VALUE] == self._END_JOB:
                        job_completed,rows = True,0
                    else:
                        # process item
                        job_completed,rows = self._process_item(*item[1:]) #item_id,init_data,start_value,end_value
                    
                    # track total rows
                    self._total_rows += rows
                    
                    job_id = self.__current_job_id
                    job_end_time = time.clock() # get time of completed job
                    # handle if job was successful
                    if job_completed:
                        # done processing task
                        self._complete_job() # flush files and update db
                        self._jobs_completed += 1 # track number of jobs
                        # log stats
                        if self.__debug: self.logger.debug('job %s completed, %03d rows, %f seconds',
                                                           job_id,rows,job_end_time-job_start_time)
                    else:
                        self._update_current_job()
                        self.__current_job_id = None
                        # log stats
                        if self.__debug: self.logger.debug('job %s failed, %03d rows, %f seconds',
                                                           job_id,rows,job_end_time-job_start_time)
        
        # to stop when the queue is empty if that message was passed...!
        if not self._continue_waiting:
            self._db.put_in_queue(self.get_job_type(),self._CTRL_QUEUE,True)
                             
        # give us a change to finish a few things if needed
        self._after_looping()
    
    #===========================================================================
    #========== Things that should/can be Overwritten When Inherited ===========
    #===========================================================================
    def _find_job_end_value(self,item_id,init_data,start_value,end_value):
        """find what's the beginning value is.
        * if None is returned and the job later breaks, this method will run
            again.
        * if self._DUMMY_END_VALUE is returned, the job wont run this every time
            it breaks and resumes.
        * if self._END_NOW is returned, process_item isn't called for this job
            and it is considered complete (removed from the queue)
        """
        return self._DUMMY_END_VALUE
        
    def _process_item(self,item_id,init_data=None,
                      start_value=None,end_value=None):
        """
        This method is called for every job in the queue the thread gets.
        return (<whether the job completed> , <number of records processed>)
        make sure you use the functions below while processing! Usually,
        you can just use:
                    if self._thread_block(new_start_value):
                        job_completed = False # we've been interrupted...
                        
        Or you can use the functions separately how you like:
            * self._update_start_value(new_start_value) # update start_value for recovering 
            * self._check_for_interrupt() # returns True if you've been
                interrupted (and updates db if interrupted), but doesn't set a
                flag that the job has failed! there is no flag!
                REMEMBER to return False for job_completed! 
            * self._update_if_needed() # updates based on time - handled internally.
        """
        return True,0
    
    def _before_looping(self):
        """the first method called by .run(), this method is the first
        to be run by the thread when it starts.  You should override and
        initial any files used, etc."""
        pass
    
    def _after_looping(self):
        """the last method called by .run(), this method is the first
        to be run by the thread when it starts.  You should override and
        close any files opened, etc."""
        pass
    
    def _flush_files(self):
        """override if you have using files.  This method is called after a 
        job is updated or completed so files will stay accurate with the db."""
        pass
    
    #===========================================================================
    # Override Get Summary info
    #===========================================================================
    def _get_summary_info(self):
        """override to add useful summary info."""
        return ['jobs completed: {:,}'.format(self._jobs_completed),
                'last data: {}'.format(self.__current_job_start_v),
                'total rows: {:,}'.format(self._total_rows)]
    
    #===========================================================================
    #=========== Things that should be used when class is Inherited ============
    #===========================================================================
    def _thread_block(self,start_value):
        """saves the start_value and updates if needed.  You just need to calls
        this method whenever the start_vlaue has changed to update if needed
        and check if you've been told to stop - interrupted.  returns True if
        you've been interrupted, False otherwise."""
        self._update_start_value(start_value)
        if self._check_for_interrupt(): return False
        else: self._update_if_needed()
        return True
    
    def _update_start_value(self,value):
        self.__current_job_start_v = value
    
    def _check_for_interrupt(self):    
        # see if we've been interrupted.
        # saves and returns False If we've been interrupted
        if self._interupt_process:
            self.logger.warning("we've been interrupted!")
            self._update_current_job()
            return True
        else: return False
        
    def _update_if_needed(self):
        # update periodically
        if time.clock() - self.__job_updated_time > self._update_interval:
            self._update_current_job()
               
    #===========================================================================
    # Before Raising Error
    #===========================================================================
    def _before_raising_error(self):
        """this method should be called before any errors are raised to save
        the current job's progress."""
        self.logger.warning("oops... well, I'll save my progress i guess...")
        # save progress!
        self._update_current_job()

    #===========================================================================
    # Waiting Wrap
    #===========================================================================
    def _before_waiting(self,first_time_waiting=True):
        """call before doing a wait. updates job status (in the db) if needed.""" 
        if first_time_waiting: self._update_current_job() 
    
    def _after_waiting(self):
        """call method after doing a wait."""
        self.__job_updated_time = time.clock() # updated job_updated time so we don't update more than needed
                 
    #===========================================================================
    #=============================== Job Things ================================
    #===========================================================================
    def _complete_job(self):
        """Tell the db the job is finished/complete and flush files"""
        #if there is a job being worked on...
        if self.__current_job_id is not None:
            # updated db
            self._db.remove_job(self.__current_job_id,self.get_job_type())
            self.__current_job_id = None
            # flush files
            self._flush_files()
          
    #===========================================================================
    # Update Job    
    #===========================================================================
    def _update_current_job(self):
        """updates the current job with start_value and end_value if not None."""
        self.logger.info('updating job %s with data %r',self.__current_job_id,
                         self.__current_job_start_v)
        
        if self.__current_job_id is not None:
            #update db (update end_time if able)
            if self.__current_job_end_v is None:
                self._db.update_job(self.__current_job_id,
                                    self.__current_job_start_v)
            else:
                self._db.update_job(self.__current_job_id,
                                    self.__current_job_start_v,
                                    self.__current_job_end_v)
                self.__current_job_end_v = None
            
            # flush files
            self._flush_files()
            
            # save time
            self.__job_updated_time = time.clock()

    #===========================================================================
    # Add Job
    #===========================================================================
    def _add_job(self,item_id,job_type,init_data=None,*args, **keys):
        """
        params:
            item_id: the item to get the job needs to be done for
                NOTE: item_id,job_type is the unique composite key.
            job_type: the job that needs to be done
                NOTE: item_id,job_type is the unique composite key.
            init_data=None: a number, compared if a similar job is started to
                determine if adding the job is necessary.  None means it's
                necessary.  
        """
        # add job to db and get job_id...
        job_id = self._db.add_job(item_id,job_type,init_data)
        
        # log if the job failed to be added
        if job_id is None: self.logger.error('failed to add job %s,%s,%s',
                                             item_id,job_type,init_data)


#===============================================================================
# Main
#===============================================================================
if __name__ == '__main__':
    try: from tests import test_mythread
    except ImportError: print 'no test for mythread'
    else: test_mythread.run_test()
    