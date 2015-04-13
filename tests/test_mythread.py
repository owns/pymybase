"""
Elias Wood (owns13927@yahoo.com)
2015-04-09
unit test for mythread
"""

import sys
if '..' not in sys.path: sys.path.append('..')
from mythread import MyThread
from testbase import TestBase

#TODO: test MyThread more/better!

#===============================================================================
# Test MyThread
#===============================================================================
class Test_MyThread(TestBase):
    def test_run(self):
        """make sure inheriting MyThread works correctly!
        Still need to test:
            interrupting threads
            stopping threads after the job is completed
            waiting (self._before_waiting(...))
            before & after looping
            ...
        """
        # init testing thread class
                
        class MyTestThread(MyThread):
            _jobs_w_end_value = 0
            
            def _find_job_end_value(self, item_id, init_data, start_value, end_value):
                # test not processing job
                if item_id == '0':
                    self.logger.warning('oh no!!!!!')
                    return self._END_JOB
                if item_id == '1': return 'hello_world'
                
            def _process_item(self, item_id, init_data=None,start_value=None, end_value=None):
                self.logger.debug('processing item %02d with init data %s',int(item_id),init_data)
                if end_value is not None: self._jobs_w_end_value += 1
                
                job_completed = True
                
                if item_id == '1':
                    # test adding a job
                    self._add_job(id(self),'main')
                    
                    # test updating!
                    a = self._update_interval
                    self._update_interval = 1
                    import time
                    time.sleep(1.1)
                    if self._thread_block(10):
                        # we've been iterupted!
                        job_completed = False
                    self._update_interval = a; del a
                
                return job_completed,1
            
            def _get_summary_info(self):
                a = MyThread._get_summary_info(self)
                a.extend(['jobs not needed to process: {:,}'.format(self._jobs_w_end_value)])
                return a
            
            
        # get temp db
        from mydb import MyDb
        fname = self.get_new_file_name('aaa.db')
        db = MyDb(fname)
        self.addCleanup(db.close) # clean up!
        
        # add jobs
        for i in xrange(10):
            db.add_job(i,'main',10-i)
        
        # init threads & start them
        a = MyTestThread('main',db)
        a.start() # start the thread
        a.stop(wait_for_empty_queue=True) # don't continue to wait on the queue
        a.join() # wait for thread to finish
        
        # db summary
        db._log_summary_info()
        a._log_summary_info()
        
        # --- check things...
        # thread
        self.assertEqual(a._total_rows,10,'the wrong number of rows where processed...')
        self.assertEqual(a._jobs_completed,11,'a job failed somehow???')
        self.assertEqual(a._jobs_w_end_value,1,'_find_job_end_value failed...')
        # db
        self.assertEqual(db._jobs_added_count,11,'failed to add a job...')
        self.assertEqual(db._jobs_updated_count,1,"only one job should've updated...")
        self.assertEqual(db.get_job_count(),0,'not all jobs were processed/removed...')
        
        
#===============================================================================
# Run Test
#===============================================================================
def run_test():
    import os; os.chdir('..')
    import unittest
    suite = unittest.TestLoader().loadTestsFromTestCase(Test_MyThread)
    unittest.TextTestRunner().run(suite)

#===============================================================================
# Main
#===============================================================================
if __name__ == '__main__':
    run_test()