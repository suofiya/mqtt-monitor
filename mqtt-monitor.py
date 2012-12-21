#!/usr/bin/python
# -*- coding: utf-8 -*-
# vim tabstop=4 expandtab shiftwidth=4 softtabstop=4

#
# mmqtt-monitor
#	P
#


__author__ = "Dennis Sell"
__copyright__ = "Copyright (C) Dennis Sell"



import os
import sys
import mosquitto
import socket
import time
import logging
import signal
import threading
from config import Config


CLIENT_VERSION = "0.6"
CLIENT_NAME = "mqtt_monitor"
MQTT_TIMEOUT = 60	#seconds
CLIENT_BASE = "/clients/"


#TODO might want to add a lock file
#TODO  need to deal with no config file existing!!!
#TODO move config file to home dir


#read in configuration file
homedir = os.path.expanduser("~")
f = file(homedir + '/.mqtt-monitor.conf')
cfg = Config(f)
MQTT_HOST = cfg.MQTT_HOST
MQTT_PORT = cfg.MQTT_PORT
CLIENT_TOPIC = cfg.CLIENT_TOPIC
BASE_TOPIC = cfg.BASE_TOPIC
MONITOR_TOPIC = cfg.MONITOR_TOPIC
MONITOR_LIST = cfg.MONITOR_LIST
INTERVAL = cfg.INTERVAL


mqtt_connected = False
response = {}


#define what happens after connection
def on_connect(self, obj, rc):
	global mqtt_connected
	global running
	global response

	mqtt_connected = True
	print "MQTT Connected"
	mqttc.publish( CLIENT_TOPIC + "status" , "running", 1, 1 )
	mqttc.publish( CLIENT_TOPIC + "version", CLIENT_VERSION, 1, 1 )
	mqttc.subscribe( CLIENT_BASE + "/+/ping", 2)


def on_message(self, obj, msg):
	if (( msg.topic == CLIENT_BASE + "ping" ) and ( msg.payload == "request" )):
		mqttc.publish( CLIENT_BASE + "ping", "response", qos = 1, retain = 0 )
	else:
		topic = msg.topic.split("/")
		if (( topic[1] == "clients" ) and ( topic[3] == "ping") and ( msg.payload == "response" )):		
			response[topic[2]] = True
			print "reponse from ", topic[2]


def do_monitor_loop():
	global running
	global METAR_IDS
	global mqttc
	global response
	global mqtt_connected

	while ( running ):
		if ( mqtt_connected ):
			for client in MONITOR_LIST:
				response[client] = False
				print "Pinging ", client
				mqttc.publish( "/clients/" + client + "/ping", "request", 1, 0 )
			time.sleep(10)
			for client in MONITOR_LIST:
				if response[client] == False:
					print "No reponse from ", client
					mqttc.publish( MONITOR_TOPIC, "Client " + client + " is no longer responding.", 2, 1 )
			if ( INTERVAL ):
				print "Waiting ", INTERVAL, " minutes for next check."
				time.sleep(60 * INTERVAL)
			else:
				running = False	#do a single shot
		pass


def do_disconnect():
       global mqtt_connected

       mqttc.disconnect()
       mqtt_connected = False
       print "Disconnected"


def mqtt_disconnect():
	global mqtt_connected

	print "Disconnecting..."
	mqttc.disconnect()
	if ( mqtt_connected ):
		mqtt_connected = False 
		print "MQTT Disconnected"


def mqtt_connect():
	global mqttc

	rc = 1
	while ( rc ):
		print "Attempting connection..."
		mqttc.will_set(CLIENT_TOPIC + "status", "disconnected_", 1, 1)

		#define the mqtt callbacks
		mqttc.on_message = on_message
		mqttc.on_connect = on_connect
#		mqttc.on_disconnect = on_disconnect

		#connect
		rc = mqttc.connect( MQTT_HOST, MQTT_PORT, MQTT_TIMEOUT )
		if rc != 0:
			logging.info( "Connection failed with error code $s, Retrying in 30 seconds.", rc )
			print "Connection failed with error code ", rc, ", Retrying in 30 seconds." 
			time.sleep(30)
		else:
			print "Connect initiated OK"


def cleanup(signum, frame):
	mqtt_disconnect()
	sys.exit(signum)


for i in MONITOR_LIST:
	response[i] = False 

#create a client
mqttc = mosquitto.Mosquitto( CLIENT_NAME ) 

#trap kill signals including control-c
signal.signal(signal.SIGTERM, cleanup)
signal.signal(signal.SIGINT, cleanup)

running = True

t = threading.Thread(target=do_monitor_loop)
t.start()


def main_loop():
	global mqtt_connected

	mqttc.loop(10)
	while running:
		if ( mqtt_connected ):
			rc = mqttc.loop(10)
			if rc != 0:	
				mqtt_disconnect()
				print rc
				print "Stalling for 20 seconds to allow broker connection to time out."
				time.sleep(20)
				mqtt_connect()
				mqttc.loop(10)
		pass


mqtt_connect()
main_loop()

