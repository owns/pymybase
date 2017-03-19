"""
Elias Wood (owns13927@yahoo.com)
2015-04-07
unit test for mydbbase
"""

from testbase import TestBase
import sys
if '..' not in sys.path: sys.path.append('..') 
from mydbbase import MyDb

#===============================================================================
# Test MyDb
#===============================================================================
class Test_MyDb(TestBase):
    def test_io_new(self):
        """test the .new functions"""
        fname = self.get_new_file_name('aaa.db')
        
        # test new (memory)
        a = MyDb()
        self.assertTrue(a.new(':memory:'),msg='failed to create a in-memory db...')
        a.close(); del a
        
        # test new
        a = MyDb()
        self.assertTrue(a.new(fname),msg='failed to create a new db...')
        a.close(); del a
        
        # test new (with file already exists)
        a = MyDb()
        self.assertFalse(a.new(fname),msg="db was just created... shouldn't be able to create it again...")
        a.close(); del a
        
    def test_io_open(self):
        """test the .open function"""
        fname = self.get_new_file_name('aaa.db')
        
        # test memory db
        a = MyDb()
        self.assertTrue(a.open(':memory:'),msg="db should've failed to be created...")
        a.close(); del a
        
        # test open (create=False) == FASLE
        a = MyDb()
        self.assertFalse(a.open(fname,create=False),msg="db should've failed to be created...")
        a.close(); del a
        
        # test open (create=True) & creates
        a = MyDb()
        self.assertTrue(a.open(fname,create=True),msg="db should've been created without errors...")
        a.close(); del a
        
        # test open (create=False) == TRUE
        a = MyDb()
        self.assertTrue(a.open(fname,create=False),msg="db was created earlier and shouldn't of failed to open...")
        a.close(); del a
    
    #===========================================================================
    # Job Tests
    #===========================================================================
    def test_job_item(self):
        """test that a job has the correct items (length/count)"""
        a = MyDb(':memory:')
        
        job_id = a.add_job('1','main',3)
        a.get_next_job('main') # emtpy queue
        a.update_job(job_id,'start','end')
        a.populate_queues(first_call=True) # re-populate
        item = a.get_next_job('main') # now it'll be updated!
        
        self.assertEqual(item[a.JOB_ID],job_id,msg="job id isn't the same")
        self.assertEqual(item[a.ITEM_ID],'1',msg="item id isn't the same")
        self.assertEqual(item[a.INIT_DATA],3,msg="init_data isn't the same")
        self.assertEqual(item[a.START_VALUE],'start',msg="start value isn't the same")
        self.assertEqual(item[a.END_VALUE],'end',msg="end value isn't the same")
        
        a.close()
    
    def test_adding_jobs(self):
        """test the db comments right away!"""
        fname = self.get_new_file_name('aaa.db')
        
        a = MyDb(fname) # create db object
        job_id = a.add_job('1','main',3) # add job
        self.assertFalse(job_id is None, msg="Job should've been created w/o issue...")
    
    def test_update_jobs(self):
        """test that a job can be updated"""
        a = MyDb(':memory:')
        job_id = a.add_job('1','main',3)
        self.assertTrue(a.update_job(job_id,'start','end'),msg="why didn't the job update???")

    def test_removing_jobs(self):
        """test that a job can be removed"""
        a = MyDb(':memory:')
        job_id = a.add_job('1','main',3)
        a.get_next_job('main')
        self.assertTrue(a.remove_job(job_id),msg="why wasn't it removed???")

    def test_get_next_job(self):
        """test that a job has the correct items (length/count)"""
        a = MyDb(':memory:')
        
        job_id = a.add_job('1','main',3)
        item = a.get_next_job('main') # now it'll be populated again!
        self.assertTrue(isinstance(item,(list,tuple)),msg="item isn't a sequence...")
        
        self.assertEqual(item[a.JOB_ID],job_id,msg="job id isn't the same")
        self.assertEqual(item[a.ITEM_ID],'1',msg="item id isn't the same")
        self.assertEqual(item[a.INIT_DATA],3,msg="init_data isn't the same")
        self.assertEqual(item[a.START_VALUE],None,msg="start value isn't the same")
        self.assertEqual(item[a.END_VALUE],None,msg="end value isn't the same")
        
    #===========================================================================
    # Recover Test
    #===========================================================================
    def test_db_recover(self):
        """test the db comments right away 100 times!"""
        for i in xrange(30): # @UnusedVariable
            fname = self.get_new_file_name('aaa.db')
            
            a = MyDb(fname) # create db object
            job_id = a.add_job('1','main',3) # add job
            a.get_next_job('main') # empty queue
            a.update_job(job_id, "123","123456789") # update
            #del a
            
            # was the job updated with that value???
            a = MyDb(fname)
            try: item = a.get_next_job('main') # empty queue
            except StopIteration: self.fail(str(i)+'. no job found!?!!?!?!?!')
            
            self.assertEqual(item[a.JOB_ID],job_id,msg="job id isn't the same")
            self.assertEqual(item[a.ITEM_ID],'1',msg="item id isn't the same")
            self.assertEqual(item[a.INIT_DATA],3,msg="init_data isn't the same")
            self.assertEqual(item[a.START_VALUE],'123',msg="start value isn't the same")
            self.assertEqual(item[a.END_VALUE],'123456789',msg="end value isn't the same")
            
            a.close(); del a
            
    #===========================================================================
    # Multi-threaded
    #===========================================================================
    def test_multithreading(self):
        """tests db with multiple threads..."""
        import threading
        import time
        import logging
        # create thead class that uses the db
        class MyThread(threading.Thread):
            _job_type = _db = None
            def __init__(self,db,job_type='main'):
                self._db, self._job_type = db, job_type
                threading.Thread.__init__(self)
            def run(self):
                while True:
                    try: item = self._db.get_next_job(self._job_type)
                    except StopIteration: break
                    logging.debug('processing item %02d of type %-4s',int(item[0]),self._job_type)
                    time.sleep(1) # do something that takes some time...
                    self._db.remove_job(item[0])
        # init and fill db
        db = MyDb(':memory:')
        for i in xrange(30):
            self.assertIsNot(db.add_job(str(i),'main' if i<11 else 'sub'),None,msg="job wasn't added..")
        # create threads
        threads = []
        for i in xrange(10):
            threads.append(MyThread(db,'main' if i<6 else 'sub'))
            threads[-1].start()
        # join threads
        for thread in threads:
            thread.join(10)
            self.assertFalse(thread.isAlive(),'thread of type={} timed-out? took more than 10 sec???'.format(thread._job_type)) 
        # check no jobs
        job_count = db.get_job_count()
        self.assertEqual(job_count,0,'there are {} jobs when there should be 0!'.format(job_count))
        
        
#===============================================================================
# Run Test
#===============================================================================
def run_test():
    import os; os.chdir('..')
    import unittest
    suite = unittest.TestLoader().loadTestsFromTestCase(Test_MyDb)
    unittest.TextTestRunner().run(suite)

#===============================================================================
# Main
#===============================================================================
if __name__ == '__main__':
    run_test()
    