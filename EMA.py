import sys
import json
from kapacitor.udf.agent import Agent,Handler
from kapacitor.udf import udf_pb2

import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(name)s: %(message)s')
logger = logging.getLogger()

# Computes the exponential moving average of the series
# size - number of data points in window
# field - the field to operate on
# as - the name of the average field, default exp_avg 
# alpha - the discount factor of the 

class ExpAvgHandler(Handler):
	
	class state(object):
		# may need to change the privateness of alpha
		def __init__(self, size,alpha):
			self.size = size
			self._window = []
			self._avg = 0.0
			self.alpha = alpha
		
		def update(self,value):
			beta = 1 - self.alpha
			l = len(self._window)
			
			if l == self.size:
				self._avg = self.alpha*value + (self._avg - self._window[0])*beta
				self._window = self._window[1:]
			else:
				self._avg = value*self.alpha + self._avg*beta
			self._window = [point*beta for point in self._window]
			self._window.append(value*self.alpha)
			return self._avg
			
		def snapshot(self):
			return{
				'size' 	 : self.size,
				'window' : self._window,
				'exp_avg': self._avg,
				'alpha'	 : self.alpha,
			}
		
		def restore(self,data):
			self.size = int(data['size'])
			self._window = [float(d) for d in data['window']]
			self._avg = float(data['exp_avg'])
			self.alpha = float(data['alpha'])
			
	def __init__(self, agent):
		self._agent = agent
		self._field = None
		self._size = 0
		self._as = 'exp_avg'
		self._state = {}
		self._alpha = 0
	
	def info(self):
		response = udf_pb2.Response()
		response.info.wants = udf_pb2.STREAM
		response.info.provides = udf_pb2.STREAM
		response.info.options['field'].valueTypes.append(udf_pb2.STRING)
		response.info.options['size'].valueTypes.append(udf_pb2.INT)
		response.info.options['as'].valueTypes.append(udf_pb2.STRING)
		response.info.options['alpha'].valueTypes.append(udf_pb2.DOUBLE)
		return response
		
	def init(self, init_req):
		success = True
		msg = ""
		for opt in init_req.options:
			if opt.name == 'field':
				self._field = opt.values[0].stringValue
			elif opt.name == 'size':
				self._size = opt.values[0].intValue
			elif opt.name == 'as':
				self._as = opt.values[0].stringValue
			elif opt.name == 'alpha':
				self._alpha = opt.values[0].doubleValue

		if self._field is None:
			success = False
			msg += ' must supply field name'
		if self._size == 0:
			success = False
			msg += ' must supply window length'
		if self._as == '':
			success = False
			msg+= ' invalid as name'
		if self._alpha == 0:
			success = False
			msg +=' invalid as alpha'
		
			
		response = udf_pb2.Response()
		response.init.success = success
		response.init.error = msg[1:]
		
		return response
		
	def snapshot(self):
		data = {}
		for group, state in self._state.iteritems():
			data[group] = state.snapshot()
		response = udf_pb2.Response()
		response.snapshot.snapshot = json.dumps(data)
		return response
		
	def restore(self, restore_req):
		success = False
		msg = ''
		try:
			data = json.loads(restore_req.snapshot)
			for group, snapshot in data.iteritems():
				self._state[group] = ExpAvgHandler.state(0,0)
				self._state[group].restore(snapshot)
			success = True
		except Exception as e:
			success = False
			msg = str(e)
		
		response = udf_pb2.Response()
		response.restore.success = success
		response.restore.error = msg
		return response
				
				
	def begin_batch(self, begin_req):
		raise Exception("input batch is not supported")
		
	def end_batch(self, end_req):
		raise Exception("batch output is not supported")
		
	def point(self, point):
		response = udf_pb2.Response()
		response.point.CopyFrom(point)
		response.point.ClearField('fieldsInt')
		response.point.ClearField('fieldsString')
		response.point.ClearField('fieldsDouble')
		
		value = point.fieldsDouble[self._field]
		if point.group not in self._state:
			self._state[point.group] = ExpAvgHandler.state(self._size,self._alpha)
		avg = self._state[point.group].update(value)
		response.point.fieldsDouble[self._as] = avg
		self._agent.write_response(response)
		
if __name__ == '__main__':
	a = Agent()
	h = ExpAvgHandler(a)
	a.handler = h

	logger.info("Starting Agent")
	a.start()
	a.wait()
	logger.info("Agent finished")
