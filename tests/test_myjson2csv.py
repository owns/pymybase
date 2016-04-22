"""
Elias Wood (owns13927@yahoo.com)
2015-04-06
unit test for myjson2csv
"""
import sys
import os
if '..' not in sys.path: sys.path.append('..')
from myjson2csv import MyJSON2CSV
from testbase import TestBase

#===============================================================================
# Test MyJSON2CSV
#===============================================================================
class Test_MyJSON2CSV(TestBase):
    def test_file_io(self):
        """test open,close and overwrite."""
        fname = self.get_new_file_name('aaa.csv')
        # test open/close
        a = MyJSON2CSV(fname)
        a.begin_writing() # only open when this method is called
        self.assertTrue(a.is_open(), 'failed to open...')
        a.close(); del a
        
        # test overwrite
        a = MyJSON2CSV(fname)
        a.begin_writing() # only open when this method is called
        self.assertNotEqual(fname, a.get_filename(),'file overwritten!')
        a.close(); del a
    
    def test_setting_headers(self):
        """test if setting the headers manually works correctly"""
        fname = self.get_new_file_name('aaa.csv')
        a = MyJSON2CSV(fname)
        a.set_headers('k','a','b','c','d.a','d.b','e')
        for i in xrange(100):
            d = dict(a=1,b='2',c=u'3',d=dict(a=4,b=5),e=[1,2,3],k=i)
            a.write_json_object(d)
        self.assertEqual(len(a._missed_headers),0,'the wrong number of fields were determined to be missed {}'.format(a._missed_headers))
        a.close(); del a
    
    def test_setting_headers_missed(self):
        """test setting the headers manually and missing a key (top level)"""
        fname = self.get_new_file_name('aaa.csv')
        a = MyJSON2CSV(fname)
        a.set_headers('k','b','c','d.a','d.b','e')
        for i in xrange(100):
            d = dict(a=1,b='2',c=u'3',d=dict(a=4,b=5),e=[1,2,3],k=i)
            a.write_json_object(d)
        
        self.assertIn('a',a._missed_headers,'MyJSON2CSV failed to discover there was a key not being exported!')
        self.assertEqual(len(a._missed_headers),1,'the wrong number of fields were determined to be missed {}'.format(a._missed_headers))
        a.close(); del a
        
    def test_assuming_headers(self):
        """test the class determining what the heads are (with all the same keys)."""
        fname = self.get_new_file_name('aaa.csv')
        a = MyJSON2CSV(fname)
        for i in xrange(100):
            d = dict(a=1,b='2',c=u'3',d=dict(a=4,b=5),e=[1,2,3],k=i)
            a.write_json_object(d)
        self.assertEqual(len(a._missed_headers),0,'no fields should be missed')
        a.close(); del a
    
    def test_assuming_headers_missed(self):
        """test the class determining what the heads are, but the first record
        doesn't have all the keys (top level)"""
        fname = self.get_new_file_name('aaa.csv')
        a = MyJSON2CSV(fname)
        d = dict(a=1,b='2',c=u'3',d=dict(a=4,b=5),e=[1,2,3])
        for i in xrange(100):
            a.write_json_object(d)
            d = dict(a=1,b='2',c=u'3',d=dict(a=4,b=5),e=[1,2,3],k=i)
        
        self.assertIn('k',a._missed_headers,'MyJSON2CSV failed to discover key k is missing from the csv')
        self.assertEqual(len(a._missed_headers),1,'only one field should be missed')
        a.close(); del a
    
    def test_auto_numbering(self):
        """test to make sure auto numbering works"""
        fname = self.get_new_file_name('aaa.csv')
        a = MyJSON2CSV(fname)
        a.add_row_number(self)
        for i in xrange(100):  # @UnusedVariable
            d = dict(a=1,b='2',c=u'3',d=dict(a=4,b=5),e=[1,2,3],k=i)
            a.write_json_object(d)
            
        self.assertEqual(a._cur_row_num,100,'there should have only been 100 rows written...')
        a.close(); del a
    
    def test_bare_writing(self):
        """test the writerow directly feature"""
        fname = self.get_new_file_name('aaa.csv')
        a = MyJSON2CSV(fname)
        a.add_row_number(self)
        a.set_headers('a','b','c')
        for i in xrange(100):  # @UnusedVariable
            a.writerow((i,i+1,i+2))
        
        self.assertEqual(a._cur_row_num,100,'there should have only been 100 rows written...')
        self.assertIn(MyJSON2CSV._row_num_header_name,a._headers,'auto gen number failed to be added')
        a.close(); del a
    
    def test_custom_header_object(self):
        """test using custom header objects instead of just keys
           and test for keys w/ dots in their name, using a..b!"""
        fname = self.get_new_file_name('aaa.csv')
        a = MyJSON2CSV(fname)
        
        def key_fn(key,value,default):
            return '{} - {}'.format(value,3)
        
        def dict_fn(d,default): return 'hello'
        
        a.set_headers(dict(key='b',name='b+',default='hello'),
                      dict(key='b',name='key_fn',key_fn=key_fn)
                      ,'a',dict(name='dict_fn',dict_fn=dict_fn),
                      'a..b')
        
        for i in xrange(0,10):
            a.write_json_object({'a':i,'b':i+1,'a.b':123})
        
        a.close(); del a
        
        # test the file was written correctly
        with open(fname,'r') as r:
            # headers
            self.assertEqual(r.next().rstrip(),'b+,key_fn,a,dict_fn,a..b',
                             'column headers where not set correctly')
            # rows
            c = 0
            for line in r:
                self.assertEqual(line.rstrip(),
                                 '{0},{0} - 3,{1},hello,123'.format(c+1,c),
                                 'incorrect row - '+str(c+1))
                c += 1
        
        #nfname =  os.path.join(os.environ['USERPROFILE'],"Desktop",
        #                       os.path.basename(fname))
        #try: os.remove(nfname)
        #except IOError: pass
        #os.rename(fname,nfname)
        
        
#===============================================================================
# Run Test
#===============================================================================
def run_test():
    os.chdir('..')
    import unittest
    suite = unittest.TestLoader().loadTestsFromTestCase(Test_MyJSON2CSV)
    unittest.TextTestRunner().run(suite)

#===============================================================================
# Main
#===============================================================================
if __name__ == '__main__':
    run_test()
    