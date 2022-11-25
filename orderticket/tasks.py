from time import sleep
from celery import shared_task
from .models import *
from nsetools import *
from datetime import datetime as dt
from truedata_ws.websocket.TD import TD
import websocket

from celery.schedules import crontab
from celery import Celery
from celery.schedules import crontab
import time
from nsetools import Nse
from ordermanagement.celery import app
from django_celery_beat.models import PeriodicTask, PeriodicTasks
from datetime import timedelta
from celery.exceptions import SoftTimeLimitExceeded
from pytz import timezone
import pendulum 
import calendar
from datetime import date
import time as te


@shared_task
def create_currency():

    from datetime import datetime, time,timedelta
    pastDate = datetime.combine(datetime.now(timezone('Asia/Kolkata')), time(9,15)).time()
    nsepadDate = datetime.combine(datetime.now(timezone('Asia/Kolkata')), time(9,15)).date()

    #LiveEquityResult.objects.all().delete()
    LiveSegment.objects.filter(time__lte = pastDate).delete()
    LiveSegment.objects.filter(date__lt = nsepadDate).delete()

    pastDate = datetime.combine(datetime.now(timezone('Asia/Kolkata')), time(9,15))
    segpastDate = datetime.combine(datetime.now(timezone('Asia/Kolkata')), time(9,15)).time()

    #LiveEquityResult.objects.all().delete()
    TestEquityResult.objects.filter(date__lte = pastDate).delete()
    LiveEquityResult.objects.filter(date__lte = pastDate).delete()
    LiveSegment.objects.filter(time__lte = segpastDate).delete()
    LiveSegment.objects.filter(date__lt = nsepadDate).delete()

    SuperLiveSegment.objects.filter(time__lte = segpastDate).delete()
    SuperLiveSegment.objects.filter(date__lt = nsepadDate).delete()

    EquityThree.objects.filter(time__lte = segpastDate).delete()
    EquityThree.objects.filter(date__lt = nsepadDate).delete()
    
    # fnolist = ['IRCTC']
    fnolist = ["MRF","HONAUT"   ,"PAGEIND"   ,"SHREECEM"  
,"ABBOTINDIA"
,"NESTLEIND" 
,"BOSCHLTD"  
,"ATUL"      
,"MARUTI"    
,"ULTRACEMCO"
,"BAJFINANCE"
,"NAUKRI"    
,"APOLLOHOSP"
,"DIXON"     
,"DRREDDY"   
,"INDIAMART" 
,"LTI"       
,"COFORGE"   
,"DIVISLAB"  
,"PERSISTENT"
,"TCS"       
,"EICHERMOT" 
,"MPHASIS"   
,"ALKEM"     
,"ASIANPAINT"
,"BRITANNIA" 
,"LTTS"      
,"MINDTREE"  
,"OFSS"      
,"NAVINFLUOR"
,"ABB"       
,"ADANIENT"  
,"BAJAJ-AUTO"
,"DEEPAKNTR" 
,"JKCEMENT"  
,"LALPATHLAB"
,"PIIND"     
,"ACC"       
,"BATAINDIA" 
,"ESCORTS"   
,"PEL"       
,"HEROMOTOCO"
,"POLYCAB"   
,"HDFC"      
,"HDFCAMC"   
,"HINDUNILVR"
,"INDIGO"    
,"INFY"      
,"LT"        
,"METROPOLIS"
,"GODREJPROP"
,"COLPAL"    
,"WHIRLPOOL" 
,"SRF"       
,"MUTHOOTFIN"
,"TITAN"     
,"KOTAKBANK" 
,"MCX"       
,"UBL"       
,"PVR"       
,"ICICIGI"   
,"INDUSINDBK"
,"GRASIM"    
,"HAL"       
,"DALBHARAT" 
,"HAVELLS"   
,"TATACOMM"  
,"VOLTAS"    
,"TATACHEM"  
,"HDFCBANK"  
,"CUMMINSIND"
,"SRTRANSFIN"
,"TECHM"     
,"ADANIPORTS"
,"MCDOWELL-N"
,"CIPLA"     
,"IPCALAB"   
,"MFSL"      
,"COROMANDEL"
,"M&M"     
,"OBEROIRLTY"
,"HCLTECH"   
,"SUNPHARMA" 
,"TVSMOTOR"  
,"TRENT"     
,"SBILIFE"   
,"INTELLECT" 
,"MGL"       
,"SBICARD"   
,"AARTIIND"  
,"LUPIN"     
,"RAMCOCEM"  
,"IRCTC"     
,"LAURUSLABS"
,"TATACONSUM"
,"BHARTIARTL"
,"CANFINHOME"
,"AUBANK"    
,"AUROPHARMA"
,"BHARATFORG"
,"CONCOR"    
,"GODREJCP"  
,"SYNGENE"   
,"AMARAJABAT"
,"WIPRO"     
,"HINDALCO"  
,"BERGEPAINT"
,"HDFCLIFE"  
,"GLENMARK"  
,"AXISBANK"  
,"MARICO"    
,"CHOLAFIN"  
,"GUJGASLTD" 
,"JINDALSTEL"
,"JUBLFOOD"  
,"DABUR"     
,"GNFC"      
,"UPL"       
,"BSOFT"     
,"JSWSTEEL"  
,"ICICIBANK" 
,"IGL"       
,"TATAMOTORS"
,"CHAMBLFERT"
,"ICICIPRULI"
,"TORNTPOWER"
,"CROMPTON"  
,"SBIN"      
,"SUNTV"     
,"VEDL"      
,"BALRAMCHIN"
,"DLF"       
,"AMBUJACEM" 
,"BANDHANBNK"
,"BPCL"      
,"GRANULES"  
,"LICHSGFIN" 
,"BIOCON"    
,"DELTACORP" 
,"GSPL"      
,"ABFRL"     
,"HINDPETRO" 
,"ZEEL"      
,"RAIN"]
    #fnolist = ['AARTIIND', 'ABBOTINDIA', 'ABFRL', 'ACC', 'ADANIPORTS', 'ALKEM', 'AMARAJABAT', 'AMBUJACEM', 'APOLLOHOSP', 'ASIANPAINT', 'ASTRAL', 'ATUL', 'AUBANK', 'AUROPHARMA', 'AXISBANK', 'BAJAJ-AUTO', 'BAJAJFINSV', 'BAJFINANCE', 'BALRAMCHIN', 'BANDHANBNK', 'BATAINDIA', 'BERGEPAINT', 'BHARATFORG', 'BHARTIARTL', 'BIOCON', 'BOSCHLTD', 'BPCL', 'BSOFT', 'CANFINHOME', 'CHAMBLFERT', 'CHOLAFIN', 'CIPLA', 'COFORGE', 'COLPAL', 'CONCOR', 'COROMANDEL', 'CROMPTON', 'CUMMINSIND', 'DABUR', 'DALBHARAT', 'DEEPAKNTR', 'DELTACORP', 'DIVISLAB', 'DIXON', 'DLF', 'DRREDDY', 'ESCORTS', 'GLENMARK', 'GNFC', 'GODREJCP', 'GODREJPROP', 'GRANULES', 'GRASIM', 'GSPL', 'GUJGASLTD', 'HAL', 'HAVELLS', 'HCLTECH', 'HDFC', 'HDFCAMC', 'HDFCBANK', 'HDFCLIFE', 'HINDALCO', 'HINDPETRO', 'HINDUNILVR', 'HONAUT', 'ICICIBANK', 'ICICIGI', 'ICICIPRULI', 'IGL', 'INDIAMART', 'INDIGO', 'INDUSINDBK', 'INDUSTOWER', 'INFY', 'INTELLECT', 'IPCALAB', 'IRCTC', 'JINDALSTEL', 'JKCEMENT', 'JSWSTEEL', 'JUBLFOOD', 'KOTAKBANK', 'LALPATHLAB', 'LAURUSLABS', 'LICHSGFIN', 'LT', 'LTI', 'LTTS', 'LUPIN', 'MARICO', 'MARUTI', 'MCDOWELL-N', 'MCX', 'MFSL', 'MGL', 'MINDTREE', 'MPHASIS', 'MRF', 'MUTHOOTFIN', 'NAUKRI', 'NAVINFLUOR', 'OBEROIRLTY', 'OFSS', 'PAGEIND', 'PERSISTENT', 'PIDILITIND', 'PIIND', 'POLYCAB', 'PVR', 'RAIN', 'RAMCOCEM', 'RELIANCE', 'SBICARD', 'SBILIFE', 'SBIN', 'SHREECEM', 'SIEMENS', 'SRF', 'SRTRANSFIN', 'SUNPHARMA', 'SUNTV', 'SYNGENE', 'TATACHEM', 'TATACOMM', 'TATACONSUM', 'TATAMOTORS', 'TATASTEEL', 'TECHM', 'TORNTPHARM', 'TORNTPOWER', 'TRENT', 'TVSMOTOR', 'UBL', 'ULTRACEMCO', 'UPL', 'VOLTAS', 'WHIRLPOOL', 'WIPRO', 'ZEEL', 'ZYDUSLIFE']
    #super_three_list = list(SuperLiveSegment.objects.all().values_list('symbol', flat=True))
    gain_list = SuperLiveSegment.objects.filter(segment__in=["gain"]).order_by('-change_perc').values_list('symbol', flat=True) 
    loss_list = SuperLiveSegment.objects.filter(segment__in=["loss"]).order_by('change_perc').values_list('symbol', flat=True) 
    super_list = list(gain_list) + list(loss_list)
    setA = set(fnolist)
    setB = set(super_list)

    # Get new set with elements that are only in a but not in b
    onlyInA = setA.difference(setB)
    fnolist = list(setB) + list(onlyInA)
    
    equitypastdate = datetime.combine(datetime.now(timezone('Asia/Kolkata')), time(9,15)).strftime('%Y-%m-%d %H:%M:%S')
    timenow = datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')

    doneToday = LiveSegment.objects.values_list('doneToday', flat=True).distinct()


    def OIPercentChange(df):
        try:
            print("Enter OIper")
            ce = df.loc[df['type'] == "CE"]
            pe = df.loc[df['type'] == "PE"]

            celtt = dt.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
            celtt = dt.strptime(str(celtt), "%Y-%m-%d %H:%M:%S").time()
            peltt = dt.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
            peltt = dt.strptime(str(peltt), "%Y-%m-%d %H:%M:%S").time()

            # ce_oipercent_df = ce.sort_values(by=['oi_change_perc'], ascending=False)
            ce_oipercent_df = ce.where(ce['oi_change_perc'] !=0 ).sort_values(by=['oi_change_perc'], ascending=False)


            # print(ce_oipercent_df)
            
            minvalue = ce.loc[ce['strike'] != 0].sort_values('strike', ascending=True)
            ceindex = minvalue.iloc[0].name
            peindex = ceindex.replace("CE", "PE")
            #pe = pe[peindex:]

            ceoi1 = ce_oipercent_df.iloc[0]['oi_change_perc']
            cestrike = ce_oipercent_df.iloc[0]['strike']
            peoi1 = pe.loc[pe['strike']==ce_oipercent_df.iloc[0]['strike']].iloc[0]['oi_change_perc']

            # print(ceoi1)
            # print(cestrike)
            # print(peoi1)

            pe_oipercent_df = pe.where(pe['oi_change_perc'] !=0 ).sort_values(by=['oi_change_perc'], ascending=False)

            ceoi2 = pe_oipercent_df.iloc[0]['oi_change_perc']
            pestrike = pe_oipercent_df.iloc[0]['strike']
            peoi2 = ce.loc[ce['strike']==pe_oipercent_df.iloc[0]['strike']].iloc[0]['oi_change_perc']

            # print(ceoi2)
            # print(pestrike)

            # print(peoi2)
            import datetime as det

            my_time_string = "15:30:00"
            my_datetime = det.datetime.strptime(my_time_string, "%H:%M:%S").time()

            if celtt > my_datetime:
                celtt = det.datetime.now().replace(hour=15,minute=30,second=00).strftime("%Y-%m-%d %H:%M:%S")
                peltt = det.datetime.now().replace(hour=15,minute=30,second=00).strftime("%Y-%m-%d %H:%M:%S")
            else:
                celtt = pe_oipercent_df.iloc[0]['ltt']
                peltt = pe_oipercent_df.iloc[0]['ltt']


            OIPercentChange = {"celtt":str(celtt),"ceoi1":ceoi1,"cestrike":cestrike,"peoi1":peoi1,"peltt":str(peltt),"peoi2":peoi2,"pestrike":pestrike,"ceoi2":ceoi2}
            print("Exit OIper")
            return OIPercentChange
        except:
            celtt = ce.iloc[0]['ltt']
            peltt = ce.iloc[0]['ltt']
            OIPercentChange = {"celtt":str(celtt),"ceoi1":0,"cestrike":0,"peoi1":0,"peltt":str(peltt),"peoi2":0,"pestrike":0,"ceoi2":0}
            print("Exit OIper")
            return OIPercentChange

    def OITotal(df,item,dte):
        print("Enter OITotl")

        ce = df.loc[df['type'] == "CE"]
        pe = df.loc[df['type'] == "PE"]

        # print("before final df")

        final_df = ce.loc[ce['oi'] != 0].sort_values('oi', ascending=False)

        minvalue = ce.loc[ce['strike'] != 0].sort_values('strike', ascending=True)

        ceindex = minvalue.iloc[0].name
        peindex = ceindex.replace("CE", "PE")
        #pe = pe[peindex:]

        peoi1 = pe.loc[pe['strike']==final_df.iloc[0]['strike']].iloc[0]['oi']
        count = 0

        while peoi1 == 0:
            count = count + 1
            peoi1 = pe.loc[pe['strike']==final_df.iloc[count]['strike']].iloc[0]['oi']

        cestrike = final_df.iloc[count]['strike']
        ceoi1 = final_df.iloc[count]['oi']
        
        coi_double = len(final_df[final_df['oi'] == ceoi1])

        import datetime as det
        celtt = final_df.iloc[count]['ltt']
        celtt = dt.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
        celtt = dt.strptime(str(celtt), "%Y-%m-%d %H:%M:%S").time()

        my_time_string = "15:30:00"
        my_datetime = det.datetime.strptime(my_time_string, "%H:%M:%S").time()

        if celtt > my_datetime:
            celtt = det.datetime.now().replace(hour=15,minute=30,second=00).strftime("%Y-%m-%d %H:%M:%S")
            peltt = det.datetime.now().replace(hour=15,minute=30,second=00).strftime("%Y-%m-%d %H:%M:%S")
        else:
            celtt = final_df.iloc[0]['ltt']
            peltt = final_df.iloc[0]['ltt']

        # print(ceoi1)
        # print(cestrike)
        # print(peoi1)

        final_df = pe.loc[pe['oi'] != 0].sort_values('oi', ascending=False)

        ceoi2 = ce.loc[ce['strike']==final_df.iloc[0]['strike']].iloc[0]['oi']
        count = 0

        while ceoi2 == 0:
            count = count + 1
            ceoi2 = ce.loc[ce['strike']==final_df.iloc[count]['strike']].iloc[0]['oi']

        pestrike = final_df.iloc[count]['strike']
        peoi2 = final_df.iloc[count]['oi']
        poi_double = len(final_df[final_df['oi'] == peoi2])
            # print(ceoi2)
        # print(pestrike)
        # print(peoi2)   
        if coi_double > 1 or poi_double > 1:
            return False
        OITot = {"celtt":celtt,"ceoi1":ceoi1,"cestrike":cestrike,"peoi1":peoi1,"peltt":peltt,"peoi2":peoi2,"pestrike":pestrike,"ceoi2":ceoi2}
        print("Exit OITotl")
        return OITot

    def OIChange(df,item,dte):
        print("inside oichange")
        try:
            ce = df.loc[df['type'] == "CE"]
            pe = df.loc[df['type'] == "PE"]

            ce_final = df.loc[df['type'] == "CE"]
            final_df_cal = ce_final.loc[ce_final['oi_change'] != 0].sort_values('oi_change', ascending=False)
            

            celtt = final_df_cal.iloc[0]['ltt']
            max_ceoi = final_df_cal.iloc[0]['oi_change']
            max_ceoi_strike = final_df_cal.iloc[0]['strike']
            
            final_df_cal = ce_final.loc[ce_final['strike'] != 0].sort_values('strike', ascending=False)
            final_df_cal.reset_index(inplace=True)
            
            ceindex = final_df_cal[final_df_cal['strike']==max_ceoi_strike].index.item()
            # print(ceindex)
            
            oneindex = final_df_cal.iloc[ceindex+1].strike 
            centerindex = final_df_cal.iloc[ceindex+0].strike 
            minusoneindex = final_df_cal.iloc[ceindex-1].strike 

            plusoneOI = final_df_cal[final_df_cal['strike']==oneindex].oi.item()
            centerOI = final_df_cal[final_df_cal['strike']==centerindex].oi.item()
            minusoneOI = final_df_cal[final_df_cal['strike']==minusoneindex].oi.item()
            
            print(oneindex)
            print(centerindex)
            print(minusoneindex)

            print(plusoneOI)
            print(centerOI)
            print(minusoneOI)

            one = float(oneindex) * float(plusoneOI)
            two = float(centerindex) * float(centerOI)
            three = float(minusoneindex) * float(minusoneOI)

            print(f"one:{one}")
            print(f"two:{two}")
            print(f"three:{three}")

            oi_strike_total = one + two + three
            oi_total = plusoneOI + centerOI + minusoneOI
            print(oi_strike_total)
            print(oi_total)
            try:
                # print(oi_strike_total)
                # print(oi_total)
                call_final = int(oi_strike_total)//oi_total
            except Exception as e:
                print(e)
            print("completed call")
            call_ceoi_total = ce_final['oi_change'].sum()
            print(f"Final_call: {int(call_final)}")
            


            #######

            # final put calculation:
            pe_final = df.loc[df['type'] == "PE"]
            final_df_put = pe_final.loc[pe_final['oi_change'] != 0].sort_values('oi_change', ascending=False)
            peltt = final_df_put.iloc[0]['ltt']
            put_max_ceoi = final_df_put.iloc[0]['oi_change']
            put_max_ceoi_strike = final_df_put.iloc[0]['strike']

            final_df_put = pe_final.loc[pe_final['strike'] != 0].sort_values('strike', ascending=False)
            final_df_put.reset_index(inplace=True)
            peindex = final_df_put[final_df_put['strike']==put_max_ceoi_strike].index.item()

            peoneindex = final_df_put.iloc[peindex+1].strike 
            pecenterindex = final_df_put.iloc[peindex].strike 
            peminusoneindex = final_df_put.iloc[peindex-1].strike 

            peplusoneOI = final_df_put[final_df_put['strike']==peoneindex].oi.item()
            pecenterOI = final_df_put[final_df_put['strike']==pecenterindex].oi.item()
            peminusoneOI = final_df_put[final_df_put['strike']==peminusoneindex].oi.item()

            print(peoneindex)
            print(pecenterindex)
            print(peminusoneindex)

            print(peplusoneOI)
            print(pecenterOI)
            print(peminusoneOI)

            one = float(peoneindex) * float(peplusoneOI)
            two = float(pecenterindex) * float(pecenterOI)
            three = float(peminusoneindex) * float(peminusoneOI)

            pe_oi_strike_total = one + two + three
            pe_oi_total = (peplusoneOI + pecenterOI + peminusoneOI)
            put_final = int(pe_oi_strike_total)//pe_oi_total
            put_ceoi_total = pe_final['oi_change'].sum()
            print(f"put_call: {int(put_final)}")


            print(max_ceoi)
            print(put_max_ceoi)
            call_percentage = (max_ceoi/put_max_ceoi)*100
            put_percentage = (put_max_ceoi/max_ceoi)*100

            print(f"call_percentage: {int(call_percentage)}")
            print(f"put_percentage: {int(put_percentage)}")
            print(f"call_ceoi_total: {int(call_ceoi_total)}")
            print(f"put_ceoi_total: {int(put_ceoi_total)}")



            final_df = ce.loc[ce['oi_change'] != 0].sort_values('oi_change', ascending=False)
            celtt = final_df.iloc[0]['ltt']
            celtt = dt.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
            celtt = dt.strptime(str(celtt), "%Y-%m-%d %H:%M:%S").time()
            peltt = final_df.iloc[0]['ltt']
            peltt = dt.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
            peltt = dt.strptime(str(peltt), "%Y-%m-%d %H:%M:%S").time()
            minvalue = ce.loc[ce['strike'] != 0].sort_values('strike', ascending=True)

            # print("2")
            ceindex = minvalue.iloc[0].strike
            # peindex = ceindex.replace("CE", "PE")
            inde = pe[pe['strike']==ceindex].index.values
            pe = pe[inde[0]:]
            print(pe)
            # ce.to_excel("ce.xlsx")
            # print("3")
            print(final_df.iloc[0]['strike'])
            print(pe.loc[pe['strike']==str(final_df.iloc[0]['strike'])])   
            peoi1 = pe.loc[pe['strike']==str(final_df.iloc[0]['strike'])].iloc[0]['oi_change']
            count = 0
            # print("4")
            while peoi1 == 0:
                count = count + 1
                peoi1 = pe.loc[pe['strike']==final_df.iloc[count]['strike']].iloc[0]['oi_change']
            # print("5")
            print(final_df)
            cestrike = final_df.iloc[count]['strike']
            ceoi1 = final_df.iloc[count]['oi_change']
            coi_double = len(final_df[final_df['oi_change'] == ceoi1])

            max_ceoi = final_df.iloc[0]['oi_change']
            max_ceoi_strike = final_df.iloc[0]['strike']

            import datetime as det
            # print("6")
            my_time_string = "15:30:00"
            my_datetime = det.datetime.strptime(my_time_string, "%H:%M:%S").time()

            if celtt > my_datetime:
                celtt = det.datetime.now().replace(hour=15,minute=30,second=00).strftime("%Y-%m-%d %H:%M:%S")
                peltt = det.datetime.now().replace(hour=15,minute=30,second=00).strftime("%Y-%m-%d %H:%M:%S")
            else:
                celtt = final_df.iloc[0]['ltt']
                peltt = final_df.iloc[0]['ltt']

            print(ceoi1)
            print(cestrike)
            print(peoi1)
            # print("7")

            final_df = pe.loc[pe['oi_change'] != 0].sort_values('oi_change', ascending=False)

            ceoi2 = ce.loc[ce['strike']==final_df.iloc[0]['strike']].iloc[0]['oi_change']
            count = 0
            # print("8")

            while ceoi2 == 0:
                count = count + 1
                ceoi2 = ce.loc[ce['strike']==final_df.iloc[count]['strike']].iloc[0]['oi_change']

            print(final_df)
            pestrike = final_df.iloc[count]['strike']
            peoi2 = final_df.iloc[count]['oi_change']
            poi_double = len(final_df[final_df['oi_change'] == peoi2])
            max_peoi = final_df.iloc[0]['oi_change']
            max_peoi_strike = final_df.iloc[0]['strike']
            if coi_double > 1 or poi_double > 1:
                return False

            # Calculations:

            OIChan = {"max_ceoi_strike":max_ceoi_strike, "put_max_ceoi_strike":put_max_ceoi_strike,"call_percentage":call_percentage,"put_percentage":put_percentage,"call_ceoi_total":call_ceoi_total,"put_ceoi_total":put_ceoi_total,"celtt":celtt,"ceoi1":ceoi1,"cestrike":cestrike,"peoi1":peoi1,"peltt":peltt,"peoi2":peoi2,"pestrike":pestrike,"ceoi2":ceoi2,"call_final":call_final, "put_final":put_final}
            print("Exit OiChnge")
            return OIChan
        except:
            celtt = ce.iloc[0]['ltt']
            peltt = ce.iloc[0]['ltt']
            OIChan = {"max_ceoi_strike":max_ceoi_strike,"put_max_ceoi_strike":put_max_ceoi_strike,"call_percentage":call_percentage,"put_percentage":put_percentage,"call_ceoi_total":call_ceoi_total,"put_ceoi_total":put_ceoi_total,"celtt":str(celtt),"ceoi1":0,"cestrike":0,"peoi1":0,"peltt":str(peltt),"peoi2":0,"pestrike":0,"ceoi2":0,"call_final":call_final, "put_final":put_final}
            print("Exit OiChnge")
            return OIChan

    def optionChainprocess(df,item,dte):
       
        # Total OI Calculation from Option chain
        
        FutureData = {}
        #df.to_excel(f"{item}.xlsx")

        # value1 = LiveOIChange.objects.all()
        # value2 = LiveOITotal.objects.all()
        # print("Before changev")
        OIChangeValue = OIChange(df,item,dte)
        print("called")
        print(OIChangeValue)

        if not OIChangeValue:
            return False
        
        OITotalValue = OITotal(df,item,dte)

        if not OITotalValue:
            return False

        percentChange = OIPercentChange(df)

        strikedf = df.loc[df['type'] == "CE"]
        strikedf['strike'] = strikedf['strike'].astype(float, errors = 'raise')
        strikedf = strikedf.sort_values(by=['strike'])
        print(strikedf)
        midvalue = round(len(strikedf['strike'].unique())/2)
        print(f"Mid value {midvalue}")

        final_list = []
        initial = 0
        for i in range(1,6):
            first_value = strikedf['strike'].unique()[midvalue+i]
            second_value = strikedf['strike'].unique()[midvalue+initial]
            strike_gap = first_value - second_value
            initial = initial + 1
            final_list.append(strike_gap)

        initial = 0
        for i in range(1,6):
            first_value = strikedf['strike'].unique()[midvalue-i]
            second_value = strikedf['strike'].unique()[midvalue-initial]
            print(f"{second_value} - {first_value}")
            strike_gap = second_value - first_value
            initial = initial + 1
            final_list.append(strike_gap)


        print(final_list)

        if len(set(final_list)) == 1:
            print("input_list has all identical elements.")
            strikeGap = float(strikedf['strike'].unique()[midvalue+1]) - float(strikedf['strike'].unique()[midvalue])
        else:
            print("not identical")
            return False
        

        print(strikedf['strike'].unique()[midvalue])
        print(strikedf['strike'].unique()[midvalue+1])
        print(f"strikegap {strikeGap}")


        FutureData[item] = [OITotalValue['cestrike'],OITotalValue['pestrike'],strikeGap]

        # print(FutureData)

        # Percentage calculation from equity data
        newDict = {}
        # for key,value in FutureData.items():
        # Call 1 percent 
        callone = float(OITotalValue['cestrike']) - (float(strikeGap))*2
        # Call 1/2 percent 
        callhalf = float(OITotalValue['cestrike']) - (float(strikeGap))*0.05
        # Put 1 percent
        putone = float(OITotalValue['pestrike']) + (float(strikeGap))*2
        # Put 1/2 percent
        puthalf = float(OITotalValue['pestrike']) + (float(strikeGap))*0.05

        newDict[item] = [float(OITotalValue['cestrike']),float(OITotalValue['pestrike']),callone,putone,callhalf,puthalf]
        
        # # Fetching today's date
        dat = dt.today()

        # print("before deletiong")

        from datetime import datetime, time
        pastDate = datetime.combine(datetime.now(timezone('Asia/Kolkata')), time(9,15))

        #LiveEquityResult.objects.all().delete()
        LiveOITotalAllSymbol.objects.filter(time__lte = pastDate).delete()

        # # Deleting past historical data in the database
        HistoryOIChange.objects.filter(time__lte = pastDate).delete()
        HistoryOITotal.objects.filter(time__lte = pastDate).delete()
        HistoryOIPercentChange.objects.filter(time__lte = pastDate).delete()

        # Deleting live data
        LiveOITotal.objects.filter(time__lte = pastDate).delete()
        LiveOIChange.objects.filter(time__lte = pastDate).delete()
        LiveOIPercentChange.objects.filter(time__lte = pastDate).delete()
       
        value1 = LiveOIChange.objects.filter(symbol=item)

        if len(value1) > 0:

            if (value1[0].callstrike != OIChangeValue['cestrike']) or (value1[0].putstrike != OIChangeValue['pestrike']):
                # Adding to history table
                ChangeOIHistory = HistoryOIChange(time=value1[0].time,call1=value1[0].call1,call2=value1[0].call2,put1=value1[0].put1,put2=value1[0].put2,callstrike=value1[0].callstrike,putstrike=value1[0].putstrike,symbol=value1[0].symbol,expiry=value1[0].expiry)
                ChangeOIHistory.save()

                # deleting live table data
                LiveOIChange.objects.filter(symbol=item).delete()

                # Creating in live data
                ChangeOICreation = LiveOIChange(call_final=OIChangeValue["call_final"], put_final=OIChangeValue["put_final"],max_ceoi_strike=OIChangeValue["max_ceoi_strike"], put_max_ceoi_strike=OIChangeValue["put_max_ceoi_strike"],call_percentage=OIChangeValue["call_percentage"],put_percentage=OIChangeValue["put_percentage"],call_ceoi_total=OIChangeValue["call_ceoi_total"],put_ceoi_total=OIChangeValue["put_ceoi_total"],time=OIChangeValue['celtt'],call1=OIChangeValue['ceoi1'],call2=OIChangeValue['ceoi2'],put1=OIChangeValue['peoi1'],put2=OIChangeValue['peoi2'],callstrike=OIChangeValue['cestrike'],putstrike=OIChangeValue['pestrike'],symbol=item,expiry=dte)
                ChangeOICreation.save() 

            else:
                # deleting live table data
                LiveOIChange.objects.filter(symbol=item).delete()

                # Creating in live data
                ChangeOICreation = LiveOIChange(call_final=OIChangeValue["call_final"], put_final=OIChangeValue["put_final"],max_ceoi_strike=OIChangeValue["max_ceoi_strike"], put_max_ceoi_strike=OIChangeValue["put_max_ceoi_strike"],call_percentage=OIChangeValue["call_percentage"],put_percentage=OIChangeValue["put_percentage"],call_ceoi_total=OIChangeValue["call_ceoi_total"],put_ceoi_total=OIChangeValue["put_ceoi_total"],time=OIChangeValue['celtt'],call1=OIChangeValue['ceoi1'],call2=OIChangeValue['ceoi2'],put1=OIChangeValue['peoi1'],put2=OIChangeValue['peoi2'],callstrike=OIChangeValue['cestrike'],putstrike=OIChangeValue['pestrike'],symbol=item,expiry=dte)
                ChangeOICreation.save() 
        else:
            ChangeOICreation = LiveOIChange(call_final=OIChangeValue["call_final"], put_final=OIChangeValue["put_final"],max_ceoi_strike=OIChangeValue["max_ceoi_strike"], put_max_ceoi_strike=OIChangeValue["put_max_ceoi_strike"],call_percentage=OIChangeValue["call_percentage"],put_percentage=OIChangeValue["put_percentage"],call_ceoi_total=OIChangeValue["call_ceoi_total"],put_ceoi_total=OIChangeValue["put_ceoi_total"],time=OIChangeValue['celtt'],call1=OIChangeValue['ceoi1'],call2=OIChangeValue['ceoi2'],put1=OIChangeValue['peoi1'],put2=OIChangeValue['peoi2'],callstrike=OIChangeValue['cestrike'],putstrike=OIChangeValue['pestrike'],symbol=item,expiry=dte)
            ChangeOICreation.save()


        # print("value1 crossed")

        value2 = LiveOITotal.objects.filter(symbol=item)

        if len(value2) > 0:

            if (value2[0].callstrike != OITotalValue['cestrike']) or (value2[0].putstrike != OITotalValue['pestrike']):
                # Adding to history table
                TotalOIHistory = HistoryOITotal(time=value2[0].time,call1=value2[0].call1,call2=value2[0].call2,put1=value2[0].put1,put2=value2[0].put2,callstrike=value2[0].callstrike,putstrike=value2[0].putstrike,symbol=value2[0].symbol,expiry=value2[0].expiry)
                TotalOIHistory.save()

                # deleting live table data
                LiveOITotal.objects.filter(symbol=item).delete()

                # Creating in live data
                TotalOICreation = LiveOITotal(time=OITotalValue['celtt'],call1=OITotalValue['ceoi1'],call2=OITotalValue['ceoi2'],put1=OITotalValue['peoi1'],put2=OITotalValue['peoi2'],callstrike=OITotalValue['cestrike'],putstrike=OITotalValue['pestrike'],symbol=item,expiry=dte,strikegap=strikeGap)
                TotalOICreation.save()

                # Live data for equity
                LiveOITotalAllSymbol.objects.filter(symbol=item).delete()
                TotalOICreationAll = LiveOITotalAllSymbol(time=OITotalValue['celtt'],call1=OITotalValue['ceoi1'],call2=OITotalValue['ceoi2'],put1=OITotalValue['peoi1'],put2=OITotalValue['peoi2'],callstrike=OITotalValue['cestrike'],putstrike=OITotalValue['pestrike'],symbol=item,expiry=dte,callone=callone,putone=putone,callhalf=callhalf,puthalf=puthalf)
                TotalOICreationAll.save()


            else:
                # deleting live table data
                LiveOITotal.objects.filter(symbol=item).delete()

                # Creating in live data
                TotalOICreation = LiveOITotal(time=OITotalValue['celtt'],call1=OITotalValue['ceoi1'],call2=OITotalValue['ceoi2'],put1=OITotalValue['peoi1'],put2=OITotalValue['peoi2'],callstrike=OITotalValue['cestrike'],putstrike=OITotalValue['pestrike'],symbol=item,expiry=dte,strikegap=strikeGap)
                TotalOICreation.save()

                # Live data for equity
                LiveOITotalAllSymbol.objects.filter(symbol=item).delete()
                TotalOICreationAll = LiveOITotalAllSymbol(time=OITotalValue['celtt'],call1=OITotalValue['ceoi1'],call2=OITotalValue['ceoi2'],put1=OITotalValue['peoi1'],put2=OITotalValue['peoi2'],callstrike=OITotalValue['cestrike'],putstrike=OITotalValue['pestrike'],symbol=item,expiry=dte,callone=callone,putone=putone,callhalf=callhalf,puthalf=puthalf)
                TotalOICreationAll.save()

        else:
            TotalOICreation = LiveOITotal(time=OITotalValue['celtt'],call1=OITotalValue['ceoi1'],call2=OITotalValue['ceoi2'],put1=OITotalValue['peoi1'],put2=OITotalValue['peoi2'],callstrike=OITotalValue['cestrike'],putstrike=OITotalValue['pestrike'],symbol=item,expiry=dte,strikegap=strikeGap)
            TotalOICreation.save()

            # Live data for equity
            LiveOITotalAllSymbol.objects.filter(symbol=item).delete()
            TotalOICreationAll = LiveOITotalAllSymbol(time=OITotalValue['celtt'],call1=OITotalValue['ceoi1'],call2=OITotalValue['ceoi2'],put1=OITotalValue['peoi1'],put2=OITotalValue['peoi2'],callstrike=OITotalValue['cestrike'],putstrike=OITotalValue['pestrike'],symbol=item,expiry=dte,callone=callone,putone=putone,callhalf=callhalf,puthalf=puthalf)
            TotalOICreationAll.save()

        value3 = LiveOIPercentChange.objects.filter(symbol=item)

        if len(value3) > 0:

            if (value3[0].callstrike != percentChange['cestrike']) or (value3[0].putstrike != percentChange['pestrike']):
                # Adding to history table
                ChangeOIPercentHistory = HistoryOIPercentChange(time=value3[0].time,call1=value3[0].call1,call2=value3[0].call2,put1=value3[0].put1,put2=value3[0].put2,callstrike=value3[0].callstrike,putstrike=value3[0].putstrike,symbol=value3[0].symbol,expiry=value3[0].expiry)
                ChangeOIPercentHistory.save()

                # deleting live table data
                LiveOIPercentChange.objects.filter(symbol=item).delete()

                # Creating in live data
                ChangeOIPercentCreation = LiveOIPercentChange(time=percentChange['celtt'],call1=percentChange['ceoi1'],call2=percentChange['ceoi2'],put1=percentChange['peoi1'],put2=percentChange['peoi2'],callstrike=percentChange['cestrike'],putstrike=percentChange['pestrike'],symbol=item,expiry=dte)
                ChangeOIPercentCreation.save() 

            else:
                # deleting live table data
                LiveOIPercentChange.objects.filter(symbol=item).delete()

                # Creating in live data
                ChangeOIPercentCreation = LiveOIPercentChange(time=percentChange['celtt'],call1=percentChange['ceoi1'],call2=percentChange['ceoi2'],put1=percentChange['peoi1'],put2=percentChange['peoi2'],callstrike=percentChange['cestrike'],putstrike=percentChange['pestrike'],symbol=item,expiry=dte)
                ChangeOIPercentCreation.save() 
        else:
            ChangeOIPercentCreation = LiveOIPercentChange(time=percentChange['celtt'],call1=percentChange['ceoi1'],call2=percentChange['ceoi2'],put1=percentChange['peoi1'],put2=percentChange['peoi2'],callstrike=percentChange['cestrike'],putstrike=percentChange['pestrike'],symbol=item,expiry=dte)
            ChangeOIPercentCreation.save()

    # Fetching the F&NO symbol list
    TrueDatausername = 'tdws127'
    TrueDatapassword = 'saaral@127'

    sampleDict = {}
    count=1
    for symbol in fnolist:
        try:
 
            # print("inside monthend")
            expiry = "29-Dec-2022"
            dte = dt.strptime(expiry, '%d-%b-%Y')

            # print("After exception")

            # td_obj = TD(TrueDatausername, TrueDatapassword, log_level= logging.WARNING )
            td_obj = TD('tdwsp127', 'saaral@127')
            first_chain = td_obj.start_option_chain( symbol , dt(dte.year , dte.month , dte.day) ,chain_length = 75)

            te.sleep(2)

            df = first_chain.get_option_chain()
            first_chain.stop_option_chain()

            td_obj.disconnect()
            td_obj.disconnect()
            sampleDict[symbol] = df

            print(df)
            # print(count)
            # print(item)
            #count = count + 1

            if optionChainprocess(df,symbol,dte) == False:
                continue


        except websocket.WebSocketConnectionClosedException as e:
            print('This caught the websocket exception in optionchain realtime')
            td_obj.disconnect()
            td_obj.disconnect()

        except IndexError as e:
            print('This caught the exception in optionchain realtime')
            print(e)
            td_obj.disconnect() 
            td_obj.disconnect()

        except Exception as e:
            print(e)
            td_obj.disconnect()
            td_obj.disconnect()
        sleep(1)

while True:
    create_currency()
