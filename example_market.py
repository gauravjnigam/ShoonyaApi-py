from api_helper import ShoonyaApiPy, get_time
import datetime
import logging
import time
import json
import csv
import threading
import queue
import yaml

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
open_price = ''
high = ''
low = ''
close = ''
tick_time = ''

class TickData:
    def __init__(self, exchange, token, ltp, open_price, high, low, close, tick_time):
        self.tick_time = tick_time
        self.exchange = exchange
        self.token = token
        self.ltp = ltp
        # self.pc = pc
        # self.vol = vol
        self.open_price = open_price
        self.high = high
        self.low = low
        self.close = close
        # print("creating tick object.. in init")

    def __str__(self):
        strTick = "{0},{1},{2},{3},{4},{5},{6},{7}".format(self.tick_time, self.exchange, self.token, self.ltp, self.open_price, self.high, self.low, self.close)
        return strTick

def prepare_tick_data(msg):
    msgType = msg['t']
    global exch
    global token
    global ltp
    # global pc
    # global vol
    global open_price
    global close
    global high
    global low
    global tick_time
    # print("Message type : ", msgType )
    # print("Message : " + str(msg))
    if msgType == 'tk':
        tick_time = str(int(time.time()))
        exch = msg['e']
        token = msg['tk']
        ltp = msg['lp']
        if 'o' in msg:
            open_price = msg['o']
        if 'h' in msg:
            high = msg['h']
        if 'l' in msg:
            low = msg['l']
        if 'c' in msg:
            close = msg['c']
    elif msgType== 'tf':
        if 'lp' in msg:
            ltp = msg['lp']
        if 'o' in msg:
            open_price = msg['o']
        if 'h' in msg:
            high = msg['h']
        if 'l' in msg:
            low = msg['l']
        if 'c' in msg:
            close = msg['c']

    # print("Creating tick object")
    tick = TickData(exch, token, ltp, open_price, high, low, close, tick_time)
    # print("Tick data : ", tick.__str__())
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
    ticksQ.put(message)
    # c = 100
    # while c>0:
    #     # print("Adding into queue...")
    #     ticksQ.put(message)
    #     c = c-1
    #     time.sleep(1)

def worker(fn):
    # open the file in the write mode
    
    file_name = fn + ".csv"
    with open(file_name, "a") as f:
        f.write("timestamp,Exchange,token,LTP,Open,High,Low,Close")
        f.write("\n")
        count = 0
        while True:
            count = count + 1
            item = ticksQ.get()
            tickData = prepare_tick_data(item)
            # print("##Queue item ", tickData.__str__())
            f.write(tickData.__str__())
            f.write("\n")

            ticksQ.task_done()
            if count%10 == 0:
                print("Flushing data to file")
                count = 0
                f.flush()
        # f1.close()

def open_callback():
    global socket_opened
    socket_opened = True
    print('app is connected')
    print("Exchange: ", exchange)
    print("Token: ", token)
    # print (file_name)
    subscription = exchange+"|"+str(token)
    api.subscribe(subscription)
    # api.subscribe(['NSE|26009', 'NSE|26000'])

#end of callbacks

def get_time(time_string):
    data = time.strptime(time_string,'%d-%m-%Y %H:%M:%S')

    return time.mktime(data)

#start of our program
api = ShoonyaApiPy()

#use following if yaml isnt used
# user    = '<>'
# pwd     = '<>'
# factor2 = '072606'
# vc      = '<>'
# apikey  = '<>'
# imei    = '<>'
# pin=input('Enter pin? ')
# ret = api.login(userid = user, password = pwd, twoFA=pin, vendor_code=vc, api_secret=apikey, imei=imei)

#yaml for parameters
with open('cred.yml') as f:
    cred = yaml.load(f, Loader=yaml.FullLoader)
    print(cred)

pin=input('Enter pin? ')
ret = api.login(userid = cred['user'], password = cred['pwd'], twoFA=pin, vendor_code=cred['vc'], api_secret=cred['apikey'], imei=cred['imei'])

exchange="NSE"#input('Exchange[NSE/BSE?NFO]? ')


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
            file = 'bn_tick'
            dumpInput = input("dump to file[filepath/no] ?")
            if dumpInput != "no":
                file = dumpInput + "_" + token
                file_name = file
                # print ("#### "+file)
            
            # Turn-on the worker thread.
            # threading.Thread(target=worker, daemon=True).start()
            logging.info("Main    : before creating thread")
            x = threading.Thread(target=worker, args=(file,), daemon=True)
            logging.info("Main    : before running thread")
            x.start()
            logging.info("Main    : wait for the thread to finish")
           
            
            ret = api.start_websocket(order_update_callback=event_handler_order_update, subscribe_callback=event_handler_quote_update, socket_open_callback=open_callback)
            print(" ===> ",ret)
            # x.join()
            logging.info("Main    : all done")



        else:
            ret = api.logout()
            # myFile.close()
            print(ret)
            print('Fin') #an answer that wouldn't be yes or no
            break

