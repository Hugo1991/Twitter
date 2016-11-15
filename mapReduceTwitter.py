#!/usr/bin/env python

from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import operator
# Read each line from STDIN
class mapreducetwitter(MRJob):
	
	def mapper(self, _, line):
		tweetObj=json.loads(line, encoding='utf-8')
		keys=tweetObj.keys()
		
		if('text' in keys):
			texto = tweetObj["text"]
			var=texto.split()
			region=None
			if('place' in keys):
				if(tweetObj['place'] is not None):
					if("US" in tweetObj['place']['country_code'] ):
#{u'full_name': u'Virginia, USA', u'url': u'https://api.twitter.com/1.1/geo/id/5635c19c2b5078d1.json', u'country': u'United States', u'place_type': u'admin', u'bounding_box': {u'type': u'Polygon', u'coordinates': [[[-83.67529, 36.540739], [-83.67529, 39.466012], [-75.16644, 39.466012], [-75.16644, 36.540739]]]}, u'country_code': u'US', u'attributes': {}, u'id': u'5635c19c2b5078d1', u'name': u'Virginia'}
						if(len(tweetObj['place']['full_name'].split(","))>1):
							region = tweetObj['place']['full_name'].split(",")[1]
						else:
							region = tweetObj['place']['full_name'].split(",")[0]
				else:
					#if(tweetObj['user']['lang']=="en"):
					#	region=tweetObj['user']['location']
					region=None
			else:
				region=None
			if(region is not None):
				for palabra in var:
					if(palabra[:1]=="#"):
						#print "hastag",palabra.lower()
						yield palabra.lower(),1
					fichero=open("/home/hugo/Escritorio/AFINN-111.txt","r")
					for linea in fichero:
						palabraFichero=linea.split('\t')[0]
						valor=linea.split('\t')[1][:-1]
						if (palabra == palabraFichero):
							yield str(json.dumps(region)[1:-1]),int(valor)
							#print (json.dumps(region)[1:-1]),palabra, palabraFichero, valor
							
					fichero.close()

	def reducer(self, key, values):
		print "asdasd"
		#if(key[:1]=="#"):
		#	yield key,values
		#	print "hastag"
		#else:
	        yield key, values

if __name__ == '__main__':
    mapreducetwitter.run()
