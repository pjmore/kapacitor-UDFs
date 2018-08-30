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

class MovAvgHandler(Handler):
	
	class state(object):
		def __init__(self, size):
			self.size = size
			self.window = []
			self._avg = 0.0 
		
		def reset(self):
			self.window = []
		
		def update(self,value,point):
			self.window.append((value,point))
			
		def myAlgorithm(self):
			return [x[1] for x in self.window]
			
		def snapshot(self):
			return{
				'size' 	 : self.size,
				'window' : self.window,
			}
		
		def restore(self,data):
			self.size = int(data['size'])
			self.window = [d for d in data['window']]
			
			
		def getLastPoint(self):
			if len(self.window) >0:
				point = self.window[-1]
				return point[-1]
			else:
				return None
				
	def __init__(self, agent):
		self._agent = agent
		self._field = None
		self._size = 0
		self._as = 'exp_avg'
		self._state = {}
		self._period = 0
		self._timeout = 0
		self._firstpoint = True
		
	
	def info(self):
		response = udf_pb2.Response()
		response.info.wants = udf_pb2.STREAM
		response.info.provides = udf_pb2.BATCH
		response.info.options['field'].valueTypes.append(udf_pb2.STRING)
		response.info.options['size'].valueTypes.append(udf_pb2.INT)
		response.info.options['as'].valueTypes.append(udf_pb2.STRING)
		response.info.options['period'].valueTypes.append(udf_pb2.DURATION)
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
			elif opt.name == 'period':
				self._period = opt.values[0].durationValue

		if self._field is None:
			success = False
			msg += ' must supply field name'
		if self._as == '':
			success = False
			msg+= ' invalid as name'
		if self._period == 0 and self._size == 0:
			success = False
			msg = ' Must supply a batch size or period to release the batch'
		
		
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
				self._state[group] = MovAvgHandler.state(0)
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
		pass
		
	def end_batch(self, end_req):
		pass
		
	def createEndBatch(self,point):
		response = udf_pb2.Response()
		response.end.group = point.group
		for k,v in point.tags.iteritems():
			response.end.tags[k]=v
		response.end.tmax = point.time
		response.end.name = point.name
		self._agent.write_response(response)
	
	def createStartBatch(self,point):
		response = udf_pb2.Response()
		response.begin.group = point.group
		#response.begin.tags = point.tags
		for k,v in point.tags.iteritems():
			response.begin.tags[k]=v
		response.begin.size = len(self._state[point.group].window)
		response.begin.name = point.name
		self._agent.write_response(response)
	
	def emptyGroup(self,point):
		self.createStartBatch(point)
		response = udf_pb2.Response()
		for myPoint in self._state[point.group].myAlgorithm():
			response.point.CopyFrom(myPoint)
			self._agent.write_response(response)
		self.createEndBatch(point)
		self._state[point.group].reset()
		
		
	def emptyAllGroups(self):
		for state in self._state.itervalues():
			point = state.getLastPoint()
			if point is not None:
				self.emptyGroup(point)
			
		
	def isTimeout(self,point):
		if point.time > self._timeout and self._timeout > 0:
			return True
		else: 
			return False
		
	def batchSizeExceeded(self,point):
		if len(self._state[point.group].window) >= self._state[point.group].size and self._size > 0:
			return True
		else:
			return False
		
	# needs a significant amount of work before it is operational
	def point(self, point):
		if self._firstpoint:
			self._timeout = point.time + self._period
			self._firstpoint = False
		response = udf_pb2.Response()
		response.point.CopyFrom(point)
		response.point.ClearField('fieldsInt')
		response.point.ClearField('fieldsString')
		response.point.ClearField('fieldsDouble')
		
		
		value = point.fieldsDouble[self._field]
		if point.group not in self._state:
			##needs to be updated
			self._state[point.group] = MovAvgHandler.state(self._size)
		if self.isTimeout(point):
			self.emptyAllGroups()
			self._timeout = self._timeout + self._period
		elif self.batchSizeExceeded(point):
			self.emptyGroup(point)
		self._state[point.group].update(value,point)

		

		
if __name__ == '__main__':
	a = Agent()
	h = MovAvgHandler(a)
	a.handler = h

	logger.info("Starting Agent")
	a.start()
	a.wait()
	logger.info("Agent finished")
