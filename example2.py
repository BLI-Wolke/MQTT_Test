import paho.mqtt.client as mqtt #import the client1
import time
import pandas as pd
import numpy as np
import sys
from datetime import datetime
import json

# file = pd.read_json('test.json', orient='records', encoding='utf-8')
# print(file.to_json(orient='records', indent=1))
df = pd.read_excel('Prueba Modulo de facturación.xlsx')
########## Construccion del timestamp #############
file_datetime = pd.to_datetime(df['fecha'])
df['fecha_month'] = file_datetime.dt.month
df['fecha_day'] = file_datetime.dt.day
df['fecha_year'] = file_datetime.dt.year
print(f'Largo del dataset {len(df)}')
# sys.exit()

for i in range(len(df)):
    df.loc[i, 'ts'] = datetime(df.loc[i,'fecha_year'], df.loc[i, 'fecha_month'], df.loc[i, 'fecha_day'], df.loc[i, 'Hora'], df.loc[i, 'Minutos']).timestamp()


# sys.exit()
# broker = "broker.emqx.io"
broker = "40.84.19.48"
port = 1884
# topic = "testing/data"
topic = "XenerApp/customerA/MUR32/XPM/test/Data"

clientId = "99edc7ed-c3a5-4d51-bda2-eee81adbc346"
clientUserName = "uxener"
clientPassword = "09012023*"

def on_message(client, userdata, message):
    print("message received ", str(message.payload.decode("utf-8")))
    print("message topic=",message.topic)
    print("message qos=",message.qos)
    print("message retain flag=",message.retain)

print("creating new instance")
client = mqtt.Client(clientId) #create new instance
client.username_pw_set(clientUserName, password=clientPassword)
client.on_message = on_message

print("connecting to broker")
# client.connect(broker) #connect to broker
client.connect(host=broker, port=port)
client.loop_start()

print("Subscribing to topic",topic)
client.subscribe(topic, qos=2)
# client.publish(topic, "{ \"msg\": \"Hola desde python, otro ejemplo\"  }")
# client.publish(topic, file.to_json(orient='records', indent=1)  )

str_sleep_time = input('Please input a sleep time in ms: ')
sleep_time = int(str_sleep_time)/1000

print('Sleep time in sec: ',sleep_time, '(s)')

# sys.exit()
selection = int(input('Select: 1 Random Values, 2 Input Values, (1 or 2): '))
obj = {}
if(selection==1):    
    obj["type"] = 0
    obj["V_P"] = 220*(1+0.01*np.random.randn())
    obj["A"] = 2*(1+0.01*np.random.randn())
    obj["V_P_THD"] = 300*(1+0.01*np.random.randn())
    obj["Hz"] = 60*(1+0.01*np.random.randn())
    obj["FP"] = 0.95*(1+0.01*np.random.randn())
    obj["kW"] = 0.5*(1+0.01*np.random.randn())
    obj["KVar"] = 0.01*np.random.randn()
    obj["KVA"] = 0.5*(1+0.01*np.random.randn())
    obj["KWh_I"] = 150*(1+0.01*np.random.randn())
    obj["KWh_E"] = 300*(1+0.01*np.random.randn())
    obj["KVarh"] = 300*(1+0.01*np.random.randn())
else:
    obj["type"] = int(input('Input type (0, 1, 2, 3): '))
    obj["V_P"] = int(input('Enter Voltage V_P = '))
    obj["A"] = float(input('Enter Electric Current: A = '))
    obj["V_P_THD"] = float(input('Enter V_P_THD = '))
    obj["Hz"] = float(input('Enter frequency: Hz = '))
    obj["FP"] = float(input('Enter Power Factor: FP = '))
    obj["kW"] = float(input('Enter Power: kW = '))
    obj["KVar"] = float(input('Enter Reactive Power: KVar = '))
    obj["KVA"] = float(input('Enter KVA = '))
    obj["KWh_I"] = float(input('Enter KWh_I = '))
    obj["KWh_E"] = float(input('Enter KWh_I = '))
    obj["KVarh"] = float(input('Enter KWh_I = '))

for i in range(len(df)):
    obj['kWh_T'] = df.loc[i, 'kWh_T']
    obj['kVArh_T'] = df.loc[i, 'kVArh_T']
    obj['ts'] = df.loc[i, 'ts']
    arr = [obj]
    # print(json.dumps(arr))
    print("Publishing message to topic", topic)
    client.publish(topic,json.dumps(arr, indent=1))
    time.sleep(sleep_time)

# time.sleep(10)
client.loop_stop()