from influxdb import InfluxDBClient

from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.nlp.stemmers import Stemmer
from sumy.utils import get_stop_words

from sumy.summarizers.lex_rank import LexRankSummarizer

import datetime
import time
from datetime import timezone

try:
    client = InfluxDBClient(host='127.0.0.1', port=8086, username='root', password='root', database='kml1')
except:
    print("Influxdb Error")


while True:
    presult= client.query('select * from parameter_table;')
    presult=list(presult.get_points(measurement='parameter_table'))
    if presult:
        for i in range(len(presult)):
            blevel=presult[i]['blevel']
            lsummary=presult[i]['lsummary']
            window=presult[i]['window']
    else:
        blevel='High'
        lsummary=2
        window=5



    tvalue = client.query('select * from tvalue;')
    tvalue=list(tvalue.get_points(measurement='tvalue'))
    for i in range(len(tvalue)):
        tvalue=tvalue[i]['last_tvalue']
    http_text=''
    if tvalue:
        squery='select count(Text) from kml_data where time > '+ str(tvalue) + ';'
        rcount=client.query(squery)
        #print(rcount)
        rcount=list(rcount.get_points(measurement='kml_data'))
        for i in range(len(rcount)):
            rcount=rcount[i]['count']
        #print(rcount)
        if rcount:
            if int(rcount)>int(window):
                squery='select Text from kml_data where time > '+ str(tvalue) + ';'
                result=client.query(squery,epoch='ns')
                text=list(result.get_points(measurement='kml_data'))
                for i in range(len(text)):
                    http_text=http_text+str(text[i]['Text'])+'. '

                last_tvalue=text[i]['time']
                time=datetime.datetime.utcnow()
                data = [
                {
                    "measurement": "tvalue",
                    "time": str(time),
                    "fields": {
                        "last_tvalue":last_tvalue,
                    }
                }
                ]
                client.query('drop measurement tvalue')
                client.write_points(data)
                #client.query(squery)
                #print(http_text)
                LANGUAGE = "english"
                SENTENCES_COUNT = lsummary
                parser = PlaintextParser.from_string(http_text, Tokenizer(LANGUAGE))
                stemmer = Stemmer(LANGUAGE)

                summarizer = LexRankSummarizer(stemmer)
                summarizer.stop_words = get_stop_words(LANGUAGE)

                summary=summarizer(parser.document, SENTENCES_COUNT)
                exsum=''
                for sentence in summary:
                    exsum=exsum+" "+str(sentence)

                #print(exsum.strip())
                data = [
                {
                    "measurement": "auto_summary",
                    "time": str(time),
                    "fields": {
                        "summary":str(exsum.strip()),
                    }
                }
                ]
                client.write_points(data)
                

    else:
        squery='select * from kml_data'
        rcount=client.query(squery,epoch='ns')
        rcount=list(rcount.get_points(measurement='kml_data'))
        rcount=rcount[0]['time']
        time=datetime.datetime.utcnow()
        data = [
        {
            "measurement": "tvalue",
            "time": str(time),
            "fields": {
                "last_tvalue":rcount,
            }
        }
        ]
        client.write_points(data)

    time.sleep(5)


