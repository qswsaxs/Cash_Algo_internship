import math
import numpy
import scipy.stats
import cPickle

MEAN_DICT={}
SD_DICT={}
SKEW_DICT={}
KURT_DICT={}
JB_DICT={}
SHARPE_DICT={}

class Fund(object):
    
    def __init__(self, blmg_sym, mon_rtn):
        self.blmg_sym=blmg_sym
        self.mon_rtn=mon_rtn

    def Mean(self):
        global MEAN_DICT
        
        if MEAN_DICT.has_key(self.blmg_sym):
            return MEAN_DICT[self.blmg_sym]
        
        MEAN_DICT[self.blmg_sym] = numpy.mean(self.mon_rtn.values())
        return MEAN_DICT[self.blmg_sym]
        
    def SD(self):
        global SD_DICT
        
        if SD_DICT.has_key(self.blmg_sym):
            return SD_DICT[self.blmg_sym]
        
        SD_DICT[self.blmg_sym] = numpy.std(self.mon_rtn.values())
        return SD_DICT[self.blmg_sym]
    
    def Kurt(self):
        global KURT_DICT
        
        if KURT_DICT.has_key(self.blmg_sym):
            return KURT_DICT[self.blmg_sym]
        
        KURT_DICT[self.blmg_sym] = scipy.stats.kurtosis(self.mon_rtn.values())
        return KURT_DICT[self.blmg_sym]
    
    def Skew(self):
        global SKEW_DICT
        
        if SKEW_DICT.has_key(self.blmg_sym):
            return SKEW_DICT[self.blmg_sym]
        
        #calculate the skewness
        SKEW_DICT[self.blmg_sym] = scipy.stats.skew(self.mon_rtn.values())
        return SKEW_DICT[self.blmg_sym]
    
    def JB_p(self):
        global JB_DICT
        
        if JB_DICT.has_key(self.blmg_sym):
            return JB_DICT[self.blmg_sym]
        
        JB_DICT[self.blmg_sym] = scipy.stats.jarque_bera(self.mon_rtn.values())[1]
        return JB_DICT[self.blmg_sym]
    
    def Sharpe(self):
        global SHARPE_DICT
        
        if SHARPE_DICT.has_key(self.blmg_sym):
            return SHARPE_DICT.has_key(self.blmg_sym)

        #calculate the sharpe ratio with the risk-free rate 0.3 
        SHARPE_DICT[self.blmg_sym] = (self.Mean()-0.3)/self.SD()
        return SHARPE_DICT[self.blmg_sym]
    
    def Measurement_Return(self):
        
        #calculate the accumulative return by the monthly return data
        def f(x,y):
            return (100+x)*(100+y)/float(100)-100
        Total_return=reduce(f,self.mon_rtn.values())
        
        return Total_return
    
    '''    
    Format for new measurements
    def Measurement_newMeasurementName(x):
        
        Use x.mon_rtn.values() to calculate the new measurement
        
        return the result
    '''    


 
def stockFundGenerator():
    '''stock'''
    with open("stock_fund_dict.txt","rb") as f:
        Dic=cPickle.load(f)
    for fund in Dic:
        yield Fund(fund['blmg_sym'],fund['mon_rtn'])

def bondFundGenerator():
    '''bond'''
    with open("bond_fund_dict.txt","rb") as f:
        Dic=cPickle.load(f)
    for fund in Dic:
        yield Fund(fund['blmg_sym'],fund['mon_rtn'])

def realEstateFundGenerator():
    '''real_estate'''
    with open("real_estate_fund_dict.txt","rb") as f:
        Dic=cPickle.load(f)
    for fund in Dic:
        yield Fund(fund['blmg_sym'],fund['mon_rtn'])

def generateReturnDistribution(fund_gen):

    fund_type=fund_gen.__doc__

    print 'Generating report for '+fund_type+' funds'

    with open('Stat_'+fund_type+'.csv','w') as f:
        
        global MEAN_DICT
        global SD_DICT
        global SKEW_DICT
        global KURT_DICT
        
        #Clear the dictionary to receive the data of funds from the specific sector
        MEAN_DICT={}
        SD_DICT={}
        SKEW_DICT={}
        KURT_DICT={}

        f.write('Time-Series Analysis by Fund Type,Mean,Median,Standard Deviation,Minimum,Maximum\n')
        
        #add the mean value, standard deviation, skewness and kurtosis data into the corresponding dictionary
        for fund in fund_gen():
            fund.Mean()
            fund.SD()
            fund.Skew()
            fund.Kurt()

        #Calculate the mean value, median, standard deviation, minimum and maximum
        fund_mean=MEAN_DICT.values()
        
        f.write('Mean value (%),{},{},{},{},{}\n'.format
                (numpy.mean(fund_mean), numpy.median(fund_mean), numpy.std(fund_mean), min(fund_mean), max(fund_mean))) 
            
        fund_SD=SD_DICT.values()
        
        f.write('Standard deviation (%),{},{},{},{},{}\n'.format
                (numpy.mean(fund_SD), numpy.median(fund_SD), numpy.std(fund_SD), min(fund_SD), max(fund_SD)))
            
        fund_Skew=SKEW_DICT.values()
        
        f.write('Skewness,{},{},{},{},{}\n'.format
                (numpy.mean(fund_Skew), numpy.median(fund_Skew), numpy.std(fund_Skew), min(fund_Skew), max(fund_Skew)))
            
        fund_Kurt=KURT_DICT.values()
        
        f.write('Excess kurtosis,{},{},{},{},{}\n'.format
                (numpy.mean(fund_Kurt), numpy.median(fund_Kurt), numpy.std(fund_Kurt), min(fund_Kurt), max(fund_Kurt)))
        
def generateRanking(sorted_list):
    list_rank=[]
    equal=[]
    for rank,fund in enumerate(sorted_list):
        if sorted_list[rank-1][1] != fund[1]:
            if equal:
                equal.append(rank)
                average=sum(equal)/float(len(equal))
                for i in equal:
                    list_rank[i-1][1]=average
                equal=[]
        else:
            equal.append(rank)
        list_rank.append([fund[0],rank+1])
    if equal:
            equal.append(rank+1)
            average=sum(equal)/float(len(equal))
            for i in equal:
                list_rank[i-1][1]=average
    return list_rank

#Generate the table which shows the rank correlations of the Sharpe ratio in relation to the other performance measures.
def generateRankCorrelationReport(fund_gen):

    fund_type=fund_gen.__doc__

    print 'Generating correlation report for '+fund_type+' funds'

    with open('Rank_corr_'+fund_type+'.csv','w') as f:

        f.write('Performance Measure,{}\n'.format(fund_type))
    
        global SHARPE_DICT
        SHARPE_DICT={}
        
        measurement_dict={}

        #add the data into the corresponding dictionary
        for measurement in filter(lambda x:x.startswith('Measurement_'),dir(Fund)):

            for fund in fund_gen():
                measurement_dict[fund.blmg_sym]=getattr(fund,measurement)()
                fund.Sharpe()

            #Get ranks
            sorted_measure=sorted(measurement_dict.iteritems(), key=lambda d:d[1], reverse = True)
            sorted_sharpe=sorted(SHARPE_DICT.iteritems(), key=lambda d:d[1], reverse = True)
            
            measure_rank=generateRanking(sorted_measure)        
            sharpe_rank=generateRanking(sorted_sharpe)

            #Calculate the rank correlation 
            N=len(sorted_measure)
            sigma_d=0
            measure_rank_sorted=sorted(measure_rank, key=lambda d:d[0])
            sharpe_rank_sorted=sorted(sharpe_rank, key=lambda d:d[0])                        
            for i in range(N):
                sigma_d+=(measure_rank_sorted[i][1]-sharpe_rank_sorted[i][1])**2

            Rank_corr = 1-6*sigma_d/float(N**3-N)
            
            f.write('{},{}\n'.format(measurement[12:], Rank_corr))
            #print Rank_corr

if __name__ == "__main__":

    for fund_gen in [stockFundGenerator, bondFundGenerator, realEstateFundGenerator]:
        generateRankCorrelationReport(fund_gen)
        #generateReturnDistribution(fund_gen)