"""
Elias Wood (owns13927@yahoo.com)
2015-04-06
unit test for myloggingbase
"""
import sys
if '..' not in sys.path: sys.path.append('..')
from myloggingbase import MyLoggingBase
from testbase import TestBase

#TODO: add test for fill_dict
'''
MyLoggingBase.init_logging(file_log_lvl = None)
from pprint import pprint
d = dict(id=123)
ad_group_keys = ('account_id','id',
                 'creative.object_story_id',
                 'creative.object_type',
                 'adgroup_review_feedback',
                 'adgroup_status','bid_type',
                 'bid_info.IMPRESSIONS',
                 'bid_info.CLICKS',
                 'bid_info.REACH',
                 'bid_info.SOCIAL',
                 'bid_info.ACTIONS',
                 'campaign_id',
                 'conversion_specs','created_time',
                 'failed_delivery_checks',
                 'name',
                 'targeting',
                 'targeting.genders',
                 'targeting.age_min',
                 'targeting.age_max',
                 'targeting.geo_locations.countries',
                 'targeting.geo_locations.regions',
                 'targeting.geo_locations.cities',
                 'targeting.geo_locations.zips',
                 'targeting.geo_locations.custom_locations',
                 'targeting.geo_locations.geo_markets',
                 'targeting.geo_locations.location_types',
                 'targeting.custom_audiences',
                 'targeting.flexible_spec',
                 'targeting.page_types',
                 'targeting.user_adclusters',
                 'tracking_specs','updated_time','insight_objective',
                 'insight_performance_indicator','extracted_dt')


print 'd',len(d),d
print 'desire',len(ad_group_keys),ad_group_keys

MyLoggingBase.fill_dict(d,ad_group_keys)

print 'after'
print 'd',len(d)
pprint(d)

print 'desire',len(ad_group_keys)
pprint(ad_group_keys)
'''

#===============================================================================
# Test MyLoggingBase
#===============================================================================
class Test_MyLoggingBase(TestBase):
    def test_logger_name(self):
        """test default name"""
        self.assertEqual(MyLoggingBase().logger.name,'MyLoggingBase')
        # test setting own name
        self.assertEqual(MyLoggingBase('test name').logger.name,'test name')
    
    def test_get_current_timestamp(self):
        """for file working???"""
        a = MyLoggingBase()
        s = a.get_current_timestamp(for_file=True)
        self.assertTrue(':' not in s,msg='this method must make the str appropriate for a filename')

        # match standard?
        import re
        s = a.get_current_timestamp(for_file=False) # default
        re_s = '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{1,6}'
        # e.g. '2015-04-06T18:30:21,344000'
        self.assertNotEqual(re.match(re_s,s),None,msg='make sure the timestamp is in the correct format!')
        
    def test_can_log(self):
        """test logging works - can log things"""
        a = MyLoggingBase()

        a.logger.debug('hello world')
        a.logger.info('hello world')
        a.logger.warning('hello world')
        a.logger.error('hello world')
        a.logger.critical('hello world')

    def test_get_folders(self):
        """test resources and output folder getting"""
        #TODO: complete me: test_get_folders
        self.fail('not implemented')
        
        
        

#===============================================================================
# Run Test
#===============================================================================
def run_test():
    import os; os.chdir('..')
    import unittest
    suite = unittest.TestLoader().loadTestsFromTestCase(Test_MyLoggingBase)
    unittest.TextTestRunner().run(suite)

#===============================================================================
# Main
#===============================================================================
if __name__ == '__main__':
    run_test()