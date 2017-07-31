import os
import sys

import datetime
import blpapi

BBG_TERM_HOST = "192.168.91.**"
BBG_TERM_PORT = ****

def get_mon_ret(bbg_symbols):

    # Fill SessionOptions
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(BBG_TERM_HOST)
    sessionOptions.setServerPort(BBG_TERM_PORT)

    # print "Connecting to %s:%d" % (BBG_TERM_HOST, BBG_TERM_PORT)

    # Create a Session
    session = blpapi.Session(sessionOptions)

    # Start a Session
    if not session.start():
        # print "Failed to start session."
        return

    if not session.openService("//blp/refdata"):
        # print "Failed to open //blp/refdata"
        return

    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("ReferenceDataRequest")

    # append securities to request
    for bbg_symbol in bbg_symbols:
        request.append("securities", bbg_symbol)

    # append fields to request
    request.append("fields", "HIST_TRR_MONTHLY")


    #print "Sending Request:", request
    session.sendRequest(request)
    count=0
    rtn_cs_list = []
    try:
        # Process received events
        while(True):
            # We provide timeout to give the chance to Ctrl+C handling:
            ev = session.nextEvent(500)
            for msg in ev:
                #print type(msg)
                #print msg.__doc__
                #print msg.correlationIds()
                #print msg.messageType()
                #print msg
                if msg.messageType() == "ReferenceDataResponse":
                    
                    for i in range(msg.getElement("securityData").numValues()):
                        _data=msg.getElement("securityData").getValueAsElement(i)
                        _symbol = _data.getElementAsString("security")
                        total_months = _data.getElement("fieldData").getElement("HIST_TRR_MONTHLY").numValues()
                        for j in range(total_months):
                            _mon_data=_data.getElement("fieldData").getElement("HIST_TRR_MONTHLY").getValueAsElement(j)
                            _rtn = _mon_data.getElementAsFloat("Total Return") if _mon_data.hasElement("Total Return") else None
                            _date = _mon_data.getElementAsString("Date") if _mon_data.hasElement("Date") else None
                            _date = datetime.datetime.strptime(_date, "%m/%d/%Y").date()
                            cs = [_symbol, _date, _rtn]
                            rtn_cs_list.append(cs)
                            if _date<datetime.date(2007,9,1):
                                break
                            

            # Response completly received, so we could exit
            if ev.eventType() == blpapi.Event.RESPONSE:
                break

    finally:
        # Stop the session
        session.stop()
        return rtn_cs_list

import math
import numpy
import scipy.stats

class Fund(object):
    
    def __init__(self, blmg_sym, mon_rtn):
        self.blmg_sym=blmg_sym
        self.mon_rtn=mon_rtn
        
    def Add_record(self, date, rtn):
        self.mon_rtn[date]=rtn
        
Fund_index={}
All_fund=[]

def mon_rtn_gen(sym):
    for i in get_mon_ret(sym):
        yield i       
Index=0

sym_set=Stock_sym
#sym_set=Bond_sym
#sym_set=Real_sym

m=0
Total=len(sym_set)
print 'Fetching data...'
for i, record in enumerate(mon_rtn_gen(sym_set)):
    
    sym=record[0]
    date=record[1]
    rtn=record[2]
    if Fund_index.has_key(sym):
        All_fund[Fund_index[sym]].Add_record(date,rtn)
    else:
        
        k = int(float(m+1)/Total*100)
        j=k-1
        Str = '>'*(j//2)+' '*((100-k)//2)
        sys.stdout.write('\r'+Str+'[%s%%]'%(k))
        sys.stdout.flush()
        
        Fund_index[sym]=Index
        All_fund.append(Fund(sym,{date:rtn}))
        Index+=1
    
        m+=1
        
n_All_fund=filter(lambda x:min(x.mon_rtn.keys())<datetime.date(2007,9,1),All_fund)
filtered_All_real_fund=filter(lambda x:len(x.mon_rtn.keys())==119,n_All_fund)

x='Stock'
with open(x+'_fund_dict.txt','wb') as f:
    cPickle.dump(filtered_All_fund,f)
