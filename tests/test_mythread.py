"""
Elias Wood (owns13927@yahoo.com)
2015-04-09
unit test for mythread
"""

import sys
if '..' not in sys.path: sys.path.append('..')
from mythread import MyThread
from testbase import TestBase

#TODO: complete me

#===============================================================================
# Test MyThread
#===============================================================================
class Test_MyThread(TestBase):
    pass
    '''
    from myloggingbase import MyTesting
    # init testing
    a = MyTesting(file_log_lvl='DEBUG',
                  console_log_lvl='INFO')
    
    # set limits per job
    global job_process_times
    job_process_times = {'a':3,'b':4,'c':5}
    
    threads_per_job = {'a':1,'b':1,'c':1}
    
    # init queues
    queues = dict()
    for job in ('a','b','c'):
        queues[job] = Queue.Queue()
    
    # init db object (a dummy db
    class IDGenerator(MyLoggingBase):
        _last_id_returned = _lock = None
        def __init__(self,*args,**keys):
            MyLoggingBase.__init__(self)
            self._last_id_returned = 0
            self._lock = threading.Lock()
        def add_job(self,*args,**keys):
            with self._lock:
                self._last_id_returned += 1
                n = self._last_id_returned
            return n
        def remove_job(self,job_id):
            return True
        def update_job(self,job_id,start_value,end_value=None):
            return True
        def populate_queues(self,q):
            return True
    
    db = IDGenerator()
    
    # populate queues with broken jobs from before!
    db.populate_queues(queues)
    
    # init threads
    threads = dict()
    threads_count = 0
    for job in ('a','b','c'):
        threads[job] = list()
        for i in xrange(threads_per_job[job]):  # @UnusedVariable
            threads[job].append(MyThread(job,queues,db))
            threads[job][-1].start()
            threads_count += 1
    
    # add to a queue to kick things off
    
    # wait for the threads to finish
    for job in ('a','b','c'):
        for thread in threads[job]:
            thread.stop(wait_for_empty_queue=True) # will tell thread to stop when queue is empty
            
        for thread in threads[job]:
            thread.join() # will wait for thread to stop
    
    # print summary info about each thread
    for threads in threads.itervalues():
        for thread in threads:
            thread._log_summary_info(thread.get_thread_name()) 
            
    # print summary info about db
    db._log_summary_info()
    
    # print what's in the queue
    something_broke = False
    a.logger.info('{0:=^30}'.format('start of queue'))
    for key in queues:
        a.logger.info('{0:=^30}'.format(key))
        # cycle through the queue till it's empty!
        c = 0
        while True:
            try: item = queues[key].get_nowait()
            except Queue.Empty: break
            else:
                c += 1
                if c <= 20:
                    a.logger.info('item=%s',item)
                    if c == 20: a.logger.info('...')
                
                queues[key].task_done()
                if not isinstance(item,int): something_broke = True
        a.logger.info('{0:^30}'.format(str(c)+' items'))
        
    a.logger.info('{0:=^30}'.format('end of queue'))
    a.logger.info('Did something break??? %s',something_broke)
    
    a.close()
    '''
        
#===============================================================================
# Run Test
#===============================================================================
def run_test():
    import unittest
    unittest.main()

#===============================================================================
# Main
#===============================================================================
if __name__ == '__main__':
    run_test()