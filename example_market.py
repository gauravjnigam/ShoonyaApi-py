from api_helper import ShoonyaApiPy, get_time
import datetime
import logging
import time
import yaml
import pandas as pd
import json
import csv
import threading
import queue

#sample
logging.basicConfig(level=logging.DEBUG)

# queue for subscription
ticksQ = queue.Queue()

#flag to tell us if the websocket is open
socket_opened = False
# file_name = "tickData"
# file = ""
exch = ''
token = ''
trading_symbol = ''
ltp = ''
open = ''
high = ''
low = ''
close = ''
pc = ''

class Tick:
    def __init__(self, exchange, token, ltp, pc, vol, open, high, low, close):
        self.exchange = exchange
        self.token = token
        self.last_trade_price = ltp
        self.pc = pc
        self.vol = vol
        self.open = open
        self.high = high
        self.low = low
        self.close = close
    def __str__(self):
        strTick = "{0},{1},{2},{3},{4},{5},{6},{7},{8}".format(self.exchange, self.token, self.last_trade_price, self.pc, self.open, self.high, self.low, self.close)
        return strTick

def prepare_tick_data(msg):
    msgType = msg['t']
    global exch
    if msgType == 'tk':
        exch = msg['e']
        token = msg['ts']
        ltp = msg['lp']
        open = msg['o']
        high = msg['h']
        low = msg['l']
        close = msg['c']
    elif msgType== 'tf':
        if msg['lp']!='':
            ltp = msg['lp']
        elif msg['pc'] != '':
            pc = msg['pc']
        elif msg['o']!='':
            open = msg['o']
        elif msg['h']!='':
            high = msg['h']
        # elif msg.has_key('c'):
        #     close = msg['c']
        # elif msg.has_key('l'):
        #     low = msg['l']


    tick = Tick(exchange=exch, token=token, ltp=ltp, pc=pc, 
        vol=vol, open=open, high=high, low=low, close=close)
    print("Tick data : ", tick)
    return tick
    
#application callbacks
def event_handler_order_update(message):
    print("order event: " + str(message))


def event_handler_quote_update(message):
    #e   Exchange
    #tk  Token
    #lp  LTP
    #pc  Percentage change
    #v   volume
    #o   Open price
    #h   High price
    #l   Low price
    #c   Close price
    #ap  Average trade price

    print("quote event: {0}".format(time.strftime('%d-%m-%Y %H:%M:%S')) + str(message))
    # tick = Tick(message.get)
    tickData = prepare_tick_data(message)
    ticksQ.put(tickData)


def worker(file):
    # open the file in the write mode
    
    file_name = file + ".csv"
    print ("FILE : " + file_name)
    with open(file_name, 'a') as file:
        
    # create the csv writer
    # writer = csv.writer(f)
        #df = pd.DataFrame(dict)
        count = 0
        while True:
            count = count + 1
            writer = csv.writer(file)
            item = ticksQ.get()
            my_list = item.split(",")
            my_list.append("{0}".format(time.strftime('%d-%m-%Y %H:%M:%S')))
            
            # for i in item.values():
            #     my_list.append(i)
            # row = ",".join(my_list)
            # print(str(my_list))
            writer.writerow(my_list)
            
            # f.flush()
            ticksQ.task_done()
            if count%60 == 0:
                print("Flushing data to file")
                count = 0
                file.flush()
        # file.close()

def open_callback():
    global socket_opened
    socket_opened = True
    print('app is connected')
    print("Exchange: ", exchange)
    print("Token: ", token)
    print (file_name)
    subscription = exchange+"|"+token
    api.subscribe(subscription)
    # api.subscribe(['NSE|26009', 'NSE|26000'])

#end of callbacks

def get_time(time_string):
    data = time.strptime(time_string,'%d-%m-%Y %H:%M:%S')

    return time.mktime(data)

#start of our program
api = ShoonyaApiPy()

#use following if yaml isnt used
user    = 'FA87226'
pwd     = 'Goals@2022'
factor2 = '072606'
vc      = 'FA87226_U'
apikey  = 'aa4cff2b3742cc0eeeea60d51e311722'
imei    = 'abc1234'
pin=input('Enter pin? ')
ret = api.login(userid = user, password = pwd, twoFA=pin, vendor_code=vc, api_secret=apikey, imei=imei)

#yaml for parameters
# with open('cred.yml') as f:
#     cred = yaml.load(f, Loader=yaml.FullLoader)
#     print(cred)

# pin=input('Enter pin? ')
# ret = api.login(userid = cred['user'], password = cred['pwd'], twoFA=pin, vendor_code=cred['vc'], api_secret=cred['apikey'], imei=cred['imei'])

exchange=input('Exchange[NSE/BSE?NFO]? ')


if ret != None:   
    while True:
        print('f => find symbol')    
        print('m => get quotes')
        print('p => contract info n properties')    
        print('v => get 1 min market data')
        print('t => get today 1 min market data')
        print('d => get daily data')
        print('o => get option chain')
        print('s => start_websocket')
        print('q => quit')

        prompt1=input('what shall we do? ').lower()                    
        
        if prompt1 == 'v':
            stInput=input('Start time [20-12-2022 00:00:00]? ')
            tokenInput=input('Token[26009(bn)/26000(nifty50)]? ')
            intervalInput=input('Interval [60/120/240]? ')
            token  = tokenInput
            start_time = stInput#"20-12-2022 00:00:00"
            interval = int(intervalInput)
            end_time = time.time()
            
            start_secs = get_time(start_time)
            #end_secs = get_time(end_time)
            print("End time : ", end_time)

            ret = api.get_time_price_series(exchange=exchange, token=token, starttime=start_secs, endtime=end_time, interval=interval)
            
            df = pd.DataFrame.from_dict(ret)
            print(df)            
            
        elif prompt1 == 't':
            ret = api.get_time_price_series(exchange='NSE', token='26009')
            
            df = pd.DataFrame.from_dict(ret)
            print(df)                        

        elif prompt1 == 'f':
            exInput=input('Exchange[NSE/BSE?NFO]? ')
            exch  = exInput
            queryInput=input('QueryString ? ')
            query = queryInput
            ret = api.searchscrip(exchange=exchange, searchtext=query)
            print(ret)

            if ret != None:
                symbols = ret['values']
                for symbol in symbols:
                    print('{0} token is {1}'.format(symbol['tsym'], symbol['token']))

        elif prompt1 == 'd':
            exch  = 'NSE'
            tsym = 'RELIANCE-EQ'
            ret = api.get_daily_price_series(exchange=exchange, tradingsymbol=tsym, startdate=0)
            print(ret)

        elif prompt1 == 'p':
            exch  = 'NSE'
            token = '22'
            ret = api.get_security_info(exchange=exchange, token=token)
            print(ret)

        elif prompt1 == 'm':
            exch  = 'NSE'
            token = '22'
            ret = api.get_quotes(exchange=exchange, token=token)
            print(ret)

        elif prompt1 == 'o':
            exch  = 'MCX'
            tsym = 'CRUDEOIL18FEB22'
            chain = api.get_option_chain(exchange=exchange, tradingsymbol=tsym, strikeprice=4150, count=2)

            chainscrips = []
            for scrip in chain['values']:
                scripdata = api.get_quotes(exchange=scrip['exch'], token=scrip['token'])
                chainscrips.append(scripdata)

            print(chainscrips)

        elif prompt1 == 's':

            token=input('Token[26009(bn)/26000(nifty50)]? ')
            if socket_opened == True:
                print('websocket already opened')
                continue
            file = ''
            dumpInput = input("dump to file[filepath/no] ?")
            if dumpInput != "no":
                file = dumpInput + "_" + token
                file_name = file
                print ("#### "+file)
            
            # Turn-on the worker thread.
            threading.Thread(target=worker, args=(file,), daemon=True).start()
            
            ret = api.start_websocket(order_update_callback=event_handler_order_update, subscribe_callback=event_handler_quote_update, socket_open_callback=open_callback)
            print(" ===> ",ret)



        else:
            ret = api.logout()
            # myFile.close()
            print(ret)
            print('Fin') #an answer that wouldn't be yes or no
            break

