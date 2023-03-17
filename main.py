#-*- coding:utf-8 -*-
import sys
import paho.mqtt.client as paho
import time


#mqtt parameters
# mqtt_broker = "broker.emqx.io"
mqtt_broker = "40.84.19.48"
mqtt_port = 1884
# topicRx = "XenerApp/company/MUR32/XPM/device1/rx"
topicRx = "XenerApp/customerA/MUR32/XPM/test/Data"
mqtt_client_id = "99edc7ed-c3a5-4d51-bda2-eee81adbc346" #
mqtt_client_username = "uxener"#
mqtt_client_password = "09012023*"#idem

relleno = "**************************************************************************************************"
client_subscribe_connected = False
subs_message_received = False
subs_message = None
subscribed = False

def sub_on_connect(client, userdata, flags, rc):
	global client_subscribe_connected
	if(rc == 0):
		print("client connected")
		client_subscribe_connected = True
	else:
		print("connection failed")
    
def sub_on_message(client, userdata, message):
    global subs_message_received,subs_message
    print("message received: %s" % message.payload)
    subs_message_received = True
    subs_message = message.payload

def sub_on_subscribe(client, userdata, mid, granted_qos):
    global subscribed 
    print("subscribed")
    subscribed = True

def main(argv=sys.argv):
	global subs_message_received,subscribed
	print("init")
	
	print(relleno)
	subscribe_attempt = False
	client_subscribe= paho.Client(mqtt_client_id)
	# client_subscribe = paho.Client(mqtt_client_id)
	client_subscribe.username_pw_set(username=mqtt_client_username, password=mqtt_client_password)
	client_subscribe.on_connect= sub_on_connect                      #attach function to callback
	client_subscribe.on_message= sub_on_message 
	client_subscribe.on_subscribe= sub_on_subscribe
	time.sleep(1)
	print("trying to connect")
	client_subscribe.connect(mqtt_broker,mqtt_port)
	client_subscribe.loop_start()

	client_subscribe.publish(topic=topicRx, payload="{ \"msg\": \"Hola desde python\"  }")
	for x in range(12):
		print(relleno)
	subscibe_correct = False
	while(1):
		if(client_subscribe_connected == True and subscribe_attempt == False):
			client_subscribe.subscribe(topicRx)
			subscribe_attempt = True
			print("subscribing to topic")
			for x in range(12):
				print(relleno)
		if(subscribed and subscibe_correct == False):
			print("subscribed correctly")
			for x in range(12):
				print(relleno)
			subscibe_correct = True
		if(subs_message_received):
			subs_message_received = False
			print("message: %s" % subs_message)
			for x in range(12):
				print(relleno)


		
if __name__=='__main__':
    main()