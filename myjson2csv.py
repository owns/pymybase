"""
Elias Wood (owns13927@yahoo.com)
2015-04-13
a class for simplifying flattening json (dict) objects - not just top level!
"""

import os
from csv import writer as csv_writer
from csv import QUOTE_MINIMAL as csv_QUOTE_MINIMAL
from csv import QUOTE_ALL as csv_QUOTE_ALL
from csv import QUOTE_NONE as csv_QUOTE_NONE
from json import loads as json_loads
from json import dumps as json_dumps
from decimal import Decimal

from myloggingbase import MyLoggingBase

'''class MyDict(dict):
    def __missing__(self,*args,**keys):
        return MyDict()'''
    
class MyJSON2CSV(MyLoggingBase):
    __version__ = '0.2.2'
    """
    a class for simplifying flattening json (dict) objects - not just top level!
    to use:
        a = MyJSON2CSV('filename.csv')
        #a.set_headers('key1','key2.subkey1','key2.subkey2','key2.subkey2.ssk3','key..name')
        #a.begin_writing() # alt way to set things...
        #a.add_row_number() # can add a row number
        for json_obj in json_objects:
            a.write_json_object(json_obj)
            
        a.close()
    NOTE:
        if filename already exists a number is appended,
            but there is a race condition...
        DOESN'T CHECK TO MAKE SURE ALL KEYS (& SUB KEYS) ARE USED/EXPORTED!
    TO ADD:
    """
    #TODO: add check if we've missing anything (only top lvl atm)!
    #TODO: add ability to handle lists! flatten lists!
    
    _filename = None
    _headers = None
    _top_level_headers = None
    #_header_formating = None # parallel array with _headers a calc of _headers for what to print...
    _csv = None
    _file = None
    csv_params = dict(delimiter=',',quotechar='"',doublequote=True,
                      skipinitialspace=True,quoting=csv_QUOTE_MINIMAL)
    _missed_headers = None
    _datatypes = None
    _add_row_num = False
    _cur_row_num = None
    _row_num_header_name = 'autogenerated_row_number'
    
    encoding = 'UTF-8'
    _expand_lists = False
    _LIST_FLAG = '<LIST_{0}>'
    
    
    def __init__(self,filename=None,**keys):
        """initialize class.
        keys filename: is the file name of the output file (csv)
        """
        MyLoggingBase.__init__(self,**keys)
        
        self._missed_headers = set()
        self._datatypes = set()
        self._cur_row_num = 0
        
        if filename is not None:
            self.set_filename(filename)
        
    #===========================================================================
    #============================= GETTERS/SETTERS =============================
    #===========================================================================
    def set_filename(self,filename):
        """set the filename. Returns True if success, False if failed.
        NOTE: cannot change the name if the file is already open!"""
        if not self.is_open():
            self._filename = filename
            return True
        else: return False
        
    def get_filename(self):
        """returns the set file name.  NOTE: if not set, will return None"""
        return self._filename
    
    def option_expand_lists_on(self):
        """set flag on whether to flatten lists!"""
        self.logger.warning("expanding lists isn\'t a functioning feature yet!")
        #self._expand_lists = True
        
    def option_expand_lists_off(self):
        """set flag off whether to flatten lists!"""
        self._expand_lists = False
    
    def set_separator(self,s):
        """set the separator for the csv.  Must be a one-character string,
        defaults to a comma. returns True if set correctly, False otherwise."""
        if not isinstance(s,basestring) or len(s)!=1:
            self.logger.warning('%r is not a valid separator. must be one-character string.',s)
            return False
        
        # set if open
        if self.is_open(): return False
        else:
            self.csv_params['delimiter'] = s
            return True
        
    def get_separator(self):
        """returns the current separator, defaults to a comma."""
        return self.csv_params['delimiter']
    
    def set_qouting_default(self): self.csv_params['quoting']=csv_QUOTE_MINIMAL
    def set_qouting_none(self): self.csv_params['quoting']=csv_QUOTE_NONE
    def set_qouting_minimal(self): self.csv_params['quoting']=csv_QUOTE_MINIMAL
    def set_qouting_all(self):  self.csv_params['quoting']=csv_QUOTE_ALL
    
    def add_row_number(self,b=True):
        """Makes the first column the row number with the title self._row_num_header_name.
        Fails if the file is already open.
        returns None is not set, returns b (what was passed) if successful."""
        
        if not self.is_open():
            self._add_row_num = b
            # add/remove to headers if needed
            if self._add_row_num:
                # check if need to add the row number
                if self._headers and self._row_num_header_name not in self._headers:
                    self.set_headers(*self._headers)
            else:
                # check if need to remove the row number
                if self._headers and self._row_num_header_name in self._headers:
                    self.set_headers(*self._headers[1:])     
                
            return b
        return None
    
    def set_headers(self,*headers):
        """set headers for the csv (in order).  To refer to a key in a dict
        object, for example with {'a':{'a':2,b':4},'b':1,'a.b':7}:
            'a.b' --> 4
            'b' --> 1
            'a' --> '{"a": 2,"b": 4}'
            'a..b' --> 7
        
        optionally, {key:'the key in the dict object explained above',
                     name:'rename the column header'
                     default:'default value'
                     key_fn: # function to transform the key value.
                             Must have 'key': what key to get.
                             e.g. key_fn(key='abc',value='123',default=None): return value+'abc'
                     dict_fn: # row dict and default are passed.
                              e.g. dict_fn(d={},default=None): return d.get('a',0)+d.get('b',0)
                    eval: # formula to eval. keys must be between {...}
                          Must have 'keys': finite, iterable list of keys that are used.
                          'on_error(execption,column,values,equation_w_values)': function to handle exceptions.
                          if no function is given, 'default' is returned
                          e.g. column: eval='{a}/{b}',keys=('a','b')
                            
         priority: 1: dict_fn
                   2: key_fn (must have 'key' key)
                   3. eval (must have 'keys' key!)
                   4. key
        NOTE: it's recommend to use name when using a custom function.
            otherwise, it will be blank!
        """
        # process what the headers will be
        new_headers = [self._row_num_header_name] \
                      if self._add_row_num else []
        
        for h in headers:
            if isinstance(h, basestring): new_headers.append(h)
            elif isinstance(h,dict):
                if 'dict_fn' in h:
                    if callable(h['dict_fn']): pass
                    else: # dict_fn is not callable!!!
                        raise BadCustomHeaderObject("the key 'dict_fn' must be"+
                            ' callable, not a %s. %r' % (type(h['dict_fn']),h))
                elif 'key_fn' in h:
                    if callable(h['key_fn']) and 'key' in h and \
                       isinstance(h.get('key'),basestring): pass
                    else: # key_fn is not callable or no ;key' key!!!!
                        raise BadCustomHeaderObject("the key 'key_fn' must be "+
                            "callable and 'key' must be a string valued key. "+repr(h))
                elif 'eval' in h:
                    if isinstance(h['eval'],basestring) and \
                       hasattr(h.get('keys'), '__iter__'): pass
                    else:
                        raise BadCustomHeaderObject("the key 'eval' must be "+
                            "a string and 'keys' must be an iterable object. "+repr(h))
                elif 'key' not in h or not isinstance(h['key'],basestring):
                    # at least key has to be provided!
                    raise BadCustomHeaderObject('you at least need to populate'+
                            " the 'key' string valued key... "+repr(h))
                
                # at least one passed - add the header!
                new_headers.append(h)
            else:
                raise BadCustomHeaderObject('header object must be a dict or '+
                                            'string base, not a '+str(type(h)))
        
        # convert list to tuple (a tuple is faster!)
        self._headers = tuple(new_headers)
        
        # get top level headers so we can see if we miss any
        self._top_level_headers = {i.get('name',i.get('key',''))
                                   if isinstance(i,dict)
                                   else (i[:i.find('.')]
                                         if '.' in i else i)
                                   for i in headers}
    
    def get_headers(self):
        """returns the set headers. NOTE: if not set, returns None"""
        return self._headers

    def headers_set(self):
        """returns True if the headers have been set, False otherwise"""
        return self._headers!=None and len(self._headers) != 0

    def is_open(self):
        """returns True if the file is open, i.e. writing has started"""
        return self._csv != None
    ''
    #===========================================================================
    # Helpful file Wrappers 
    #===========================================================================
    def flush(self):
        """Flushes file if open. Returns True if flushed successfully."""
        if self._file!=None:
            self._file.flush()
            return True
        else: return False
    
    def close_writing(self):
        """closes if the file is open; same as self.close()."""
        if self._file==None: self.logger.warning('nothing to close %s',self._filename)
        else:
            # {0:04} rows written + headers'.format(0 if self._cur_row_num==None else self._cur_row_num)
            self.logger.info('closing csv %s',self._filename)
            self._file.close()
        self._csv = None
    
    def close(self):
        """see self.close_writing(..."""
        self.close_writing()
    ''
    #===========================================================================
    # Begin Writing!
    #===========================================================================
    def begin_writing(self,filename=None,*headers):
        """opens the file, checks for things. the method is called automatically
        if the file isn't already open - no need to call explicitly."""
        # set filename if passed
        if filename!=None: self.set_filename(filename)
        
        # fail if not file to same to!
        if self._filename == None:
            self.logger.critical('no filename provided!!!')
            return False
        
        # if filename is used, generate a new one...
        #### if we were using python 3.3, we could do open('','x')...
        uf,fe = os.path.splitext(self._filename)
        n = ''
        while os.path.exists('{}{}{}'.format(uf,n,fe)):
            if n=='': n = 1
            else: n += 1
        
        # set the new filename if needed
        filename = '{}{}{}'.format(uf,n,fe)
        if filename != self.get_filename():
            if not self.set_filename(filename):
                filename = self.get_filename()
            
        # try to open the file
        try:f = open(filename,'wb')
        except (IOError,WindowsError) as e:
            self.logger.critical('failed to open file %s b/c %r',
                                 self.get_filename(),e)
        else:
            self._file = f
            self._csv = self._get_default_csv(self._file)
            self._cur_row_num = 0
            # write headers if given... prefer what is passed
            if not headers:
                if not self.headers_set():
                    self.logger.warning('no headers provided, ...will use keys'+
                                        ' from first records for headers! this'+
                                        ' means there may be new keys in a lat'+
                                        "er record which won't be written to t"+
                                        'he csv!')
                    return True
                
                self._write_headers() # write headers!
            else: self._write_headers(*headers) # write headers!
            
            # success!
            return True
    
    def _write_headers(self,*headers):
        """helper function - writes the headers. If headers are passed, those
        are used, else, whatever was already set.
        """
        if headers: self.set_headers(*headers)
        
        self.logger.debug('writing headers %s',self.get_headers())
        self._csv.writerow(tuple(i.get('name','') if isinstance(i,dict) else i
                                 for i in self.get_headers()))
    ''
    #===========================================================================
    # Write things
    #===========================================================================
    def writerow(self,row):
        """simply writes to the csv directly, adding row number if requested
        and formating datatypes the standard way!"""
        
        # open if needed
        if not self.is_open(): self.begin_writing()
        
        # add row number if requested!
        self._cur_row_num += 1
        if self._add_row_num: nrow = [self._cur_row_num]
        else: nrow = []
        
        # make sure writing it wont break anything - convert to strings!
        nrow.extend(self._item_to_str(i) for i in row)
        
        # write the row
        self._csv.writerow(nrow)
        
    def write_json_text(self,json_text):
        """converts to json before sending to self.write_json_object(...)"""
        return self.write_json_object(json_loads(json_text))
    
    def write_json_object(self,json_obj):
        """write to json object to the csv.  see self.set_headers(...) for how
        the headers work.
        """
        # test is dict (json object - although, lists are valid json objects...
        if not isinstance(json_obj,dict):
            self.logger.warning('%s object provided.  Only a json object '+
                                '(dict or sub-class of dict) is allowed!')
            return False
        
        # open if needed
        if not self.is_open(): self.begin_writing()
        
        # set headers if not set before...
        if self._headers == None:
            self._write_headers(*tuple(self._rec_list_headers(json_obj)))
        
        # print which keys that won't be in csv (only checks top level...)
        missed_headers = []
        for k in json_obj.iterkeys():
            # is there? and not already noted as missing?             
            if k not in self._missed_headers and \
               k not in self._top_level_headers:
                missed_headers.append(k)
        if missed_headers:
            self.logger.warning('the following headers will not be in the csv:'+
                                '%s',','.join(missed_headers))
            self._missed_headers.update(missed_headers)
        
        # add row number if requested!
        self._cur_row_num += 1
        if self._add_row_num:
            json_obj[self._row_num_header_name] = self._cur_row_num
        
        # write the row!!!
        self._csv.writerow(tuple(self._item_to_str(self.__get_item(json_obj,h))
                                                   for h in self._headers))
        return True
    ''
    #===========================================================================
    # Find All The Headers
    #===========================================================================
    def _rec_list_headers(self,obj,name='',lvl=0):
        """Go through the dict and list out the keys (going into sub keys if
        present.  e.g. {'a':{'b':1},'b':2} -- > ['a.b','b']"""
        if not isinstance(obj,(dict,list)) or len(obj)==0:
            self.logger.critical('trying to get headers for a non  list or dict object (or empty obj...)!!!')
            raise ValueError("bad value passed to _rec_list_headers obj={0} name={1} lvl={2}".format(obj,name,lvl))
        
        # if dict
        if isinstance(obj, dict):
            # yield all keys
            for k in obj:
                # if is number or string, or an emtpy list/dict - yield key
                if self.__is_simple_object(obj[k]) or len(obj[k])==0:
                    yield k if name=='' else (name+'.'+k)
                # if non-empty list
                elif isinstance(obj[k], (list,tuple)):
                    if self._expand_lists:
                        #for i in self._rec_list_headers(obj[k], k,lvl+1): yield (name+'.'+i) if name!='' else i
                        yield k if name=='' else (name+'.'+k)
                    else:  yield k if name=='' else (name+'.'+k)
                # if non-empty dict
                elif isinstance(obj[k], dict):
                    for i in self._rec_list_headers(obj[k], k,lvl+1): yield k if name=='' else (name+'.'+k) # @UnusedVariable
                # IDK what it is... assume it's simple...
                else: yield k if name=='' else (name+'.'+k)

        
        # if list (assume each item in the list is the same as the first!)
        elif isinstance(obj,(tuple,list)):
            if self.__is_simple_object(obj[0]) or len(obj[0])==0:
                yield self._LIST_FLAG.format(lvl) if name=='' else (name+'.'+self._LIST_FLAG.format(lvl))
            # if non-empty list
            elif isinstance(obj[0], (list,tuple)):
                for i in self._rec_list_headers(obj[0], self._LIST_FLAG.format(lvl),lvl+1): yield k if name=='' else (name+'.'+k) # @UnusedVariable
            # if non-empty dict
            elif isinstance(obj[0], dict):
                for i in self._rec_list_headers(obj[0], self._LIST_FLAG.format(lvl),lvl+1): yield k if name=='' else (name+'.'+k) # @UnusedVariable
            # IDK what it is... assume it's simple...
            else: yield k if name=='' else (name+'.'+k)
    
    #===========================================================================
    # Static Method
    #===========================================================================
    @staticmethod
    def __is_simple_object(obj):
        """helper function - determine if the type is simple: just write it."""
        return (obj==None or isinstance(obj,(str,unicode,int,long,float,bool,Decimal)))        
    
    @staticmethod
    def __get_item(obj,loc,default=None,loc_ind=0):
        """get the item out of dict described. by the loc (e.g. 'a.b.c')"""
        if obj is None: return None
        if isinstance(loc, dict):
            # ---------------- dict_fn ----------------
            if 'dict_fn' in loc:
                try: a = loc['dict_fn'](d=obj,
                                        default=loc.get('default',default))
                except Exception, e:
                    raise CustomColumnFunctionException(repr(loc)+'\n'+repr(e))
                else: return a
            # ---------------- key_fn -----------------
            if 'key_fn' in loc:
                try: a = loc['key_fn'](key=loc['key'],
                                     value=MyJSON2CSV.__get_item(obj,loc['key'],
                                                    loc.get('default',default)),
                                     default=loc.get('default'))
                except Exception, e:
                    raise CustomColumnFunctionException(repr(loc)+'\n'+repr(e))
                else: return a
            if 'eval' in loc:
                vals = {key:MyJSON2CSV.__get_item(obj,key)
                        for key in loc['keys']}
                eq = None
                try:
                    eq = loc['eval'].format(**vals)
                    val = eval(eq,{},{})
                except Exception, e:
                    if callable(loc.get('on_error')):
                        return loc['on_error'](e,loc,vals,eq)
                    else: return default
                else: return val
            # ---------------- key -----------------
            return MyJSON2CSV.__get_item(obj,loc['key'],
                                         loc.get('default',default))
        else:
            ind = loc.find('.',loc_ind)
            #print obj,loc,loc_ind,ind
            if ind==-1: return obj.get(loc,default)
            elif loc.find('.',ind+1) == ind+1:
                # there's a .. --> convert to 1 . but keep it
                return MyJSON2CSV.__get_item(obj,loc[:ind]+loc[ind+1:],default,ind+1)
            else:
                return MyJSON2CSV.__get_item(obj.get(loc[:ind]),loc[ind+1:],default)
                
            #if '.' in loc:
            #    a = loc.find('.')
            #    return MyJSON2CSV.__get_item(obj.get(loc[:a]),loc[a+1:])
            #else: return obj.get(loc,default)
    
    #===========================================================================
    # Turn whatever the item it into a string (writable to the csv)
    #===========================================================================
    def _item_to_str(self,value):
        """format whatever the value is into a str in a specific way!
        {'a',
        """
        # keep track of all data types! (not used however...)
        self._datatypes.add(type(value))
        
        # None
        if value == None: return ''
        # Simple (view '__is_simple_object' for details)
        elif self.__is_simple_object(value): return self._removeNonASCII(unicode(value))
        # dict
        elif isinstance(value, dict):
            if value: # non-empty
                return self._removeNonASCII(json_dumps(value))
                #return ','.join(self._removeNonASCII(k)+':'+self._removeNonASCII(v) for k,v in value.iteritems())
            else: return ''
        # list
        elif isinstance(value,(list,tuple)):
            if value: # non-empty
                return self._removeNonASCII(json_dumps(value)) #','.join(self._removeNonASCII(i) for i in value)
            else: return ''
        # just in case...
        else:
            self.logger.warning('flattening for  datatype {} has not been explicitly set... using repr(...)'.format(type(value)))
            return repr(value)
    
    def _removeNonASCII(self,s,replaceWith=''):
        if isinstance(s,basestring): return s.encode(self.encoding) #replaceWith.join(i for i in s if ord(i)<128)
        else: return self._item_to_str(s)
    
    #===========================================================================
    # Get Default CSV
    #===========================================================================
    def _get_default_csv(self,open_file):
        # quoting=csv.QUOTE_MINIMAL - default
        return csv_writer(open_file,**self.csv_params)

    #===========================================================================
    # get summary info
    #===========================================================================
    def get_summary_info(self):
        a = MyLoggingBase.get_summary_info(self)
        a.extend(('file: {}'.format(self._filename),
                 'rows: {:,}'.format(self._cur_row_num)))
        
        return a

#===============================================================================
# Exception classes
#===============================================================================
class CustomColumnFunctionException(Exception):
    """ custom column function (either dict_fn or key_fn)
        has raised an error """

class BadCustomHeaderObject(Exception):
    """ the header object doesn't have the needed key(s) """


#===============================================================================
# Main
#===============================================================================
if __name__ == '__main__':
    try: from tests import test_myjson2csv
    except ImportError: print 'no test for myjson2csv'
    else: test_myjson2csv.run_test()
    