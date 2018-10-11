import sys
import json
from kapacitor.udf.agent import Agent,Handler
from kapacitor.udf import udf_pb2
import ast
from kapacitor.udf.udf_pb2 import STRING,DOUBLE,INT,DURATION,STREAM,BATCH


import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(name)s: %(message)s')
logger = logging.getLogger()



class EchoHandler(Handler):
	
	class state(object):
		def __init__(self, size):
			self.size = size
			self.window = []
			self._avg = 0.0 
		
		def reset(self):
			self.window = []
		
		def update(self,value,point):
			self.window.append((value,point))
			
		"""Here is where I would place any analysis algorithm.
			Must return points to work."""
		def myAlgorithm(self):
			return [x[1] for x in self.window]
			
		"""Uses the protobuf SerializeToString method to convert the point class to bytecode"""
		def snapshot(self):
			window = []
			strPoint = udf_pb2.Response().point
			for point in self.window:
				strPoint = point[1]
				window.append((point[0],strPoint.SerializeToString()))
			return{
				'size' 	 : self.size,
				'window' : str(window),
			}
		"""uses the protobuf ParseFromString to get a unicode reperesentation of the list and then converts that into a list"""
		def restore(self,data):
			self.size = int(data['size'])
			strPoint = udf_pb2.Response().point
			data = ast.literal_eval(data['window'])
			
			for point in data:
				myPoint = float(point[0])
				strPoint.ParseFromString(point[1])
				self.window.append((myPoint,strPoint))
			#logger.info(self.window)
		
		
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
		self._as = 'echo'
		self._state = {}
		self._period = 0
		self._timeout = 0
		self._firstpoint = True
		
	""" Tells kapacitor what kind of input that the UDF wants"""
	def info(self):
		response = udf_pb2.Response()
		response.info.wants = udf_pb2.STREAM
		response.info.provides = udf_pb2.BATCH
		response.info.options['field'].valueTypes.append(STRING)
		response.info.options['size'].valueTypes.append(udf_pb2.INT)
		response.info.options['as'].valueTypes.append(udf_pb2.STRING)
		response.info.options['period'].valueTypes.append(udf_pb2.DURATION)
		return response
	
	""" Parses input from kapacitor and checks that all values are valid"""
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
	"""Creates a json reperesentation of the data"""	
	def snapshot(self):
		data = {}
		for group, state in self._state.iteritems():
			data[group] = state.snapshot()
		response = udf_pb2.Response()
		response.snapshot.snapshot = json.dumps(data)
		return response
	""" Takes a json representation of the data and places it back into the process, usually only used on process restart."""
	def restore(self, restore_req):
		success = False
		msg = ''
		try:
			data = json.loads(restore_req.snapshot)
			for group, snapshot in data.iteritems():
				self._state[group] = EchoHandler.state(0)
				self._state[group].restore(snapshot)
			success = True
		except Exception as e:
			success = False
			msg = str(e)
			#logger.info(str(e))
		
		response = udf_pb2.Response()
		response.restore.success = success
		response.restore.error = msg
		#logger.info(success)
		return response
					
	def begin_batch(self, begin_req):
		pass
		
	def end_batch(self, end_req):
		pass
		
	""" Creates the end batch message based on what the last point to be entered into the window was"""
	def createEndBatch(self,point):
		response = udf_pb2.Response()
		response.end.group = point.group
		for k,v in point.tags.iteritems():
			response.end.tags[k]=v
		response.end.tmax = point.time
		response.end.name = point.name
		self._agent.write_response(response)
	""" Creates the start batch message based on what the last point to be entered into the window was """
	def createStartBatch(self,point):
		response = udf_pb2.Response()
		response.begin.group = point.group
		for k,v in point.tags.iteritems():
			response.begin.tags[k]=v
		response.begin.size = len(self._state[point.group].window)
		response.begin.name = point.name
		self._agent.write_response(response)
	
	""" Writes a single groups points to kapacitor.
		1. Creates start batch message
		2. Writes all points in window to kapacitor
		3. Creates end batch message
		4. Empties the window"""
	def emptyGroup(self,point):
		self.createStartBatch(point)
		response = udf_pb2.Response()
		for myPoint in self._state[point.group].myAlgorithm():
			response.point.CopyFrom(myPoint)
			self._agent.write_response(response)
		self.createEndBatch(point)
		self._state[point.group].reset()
		
	""" Empties all of the groups using emptyGroup()"""
	def emptyAllGroups(self):
		for state in self._state.itervalues():
			point = state.getLastPoint()
			if point is not None:
				self.emptyGroup(point)
			
		
	""" Checks for timeout if it has been entered. Based on when the first point comes in """
	def isTimeout(self,point):
		if point.time > self._timeout and self._timeout > 0:
			return True
		else: 
			return False
	""" Checks for a full batch."""
	def batchSizeExceeded(self,point):
		if len(self._state[point.group].window) >= self._state[point.group].size and self._size > 0:
			return True
		else:
			return False
		
	""" Called by the Agent and the only non-initilization method called by kapacitor. 
		Processes point by point and sorts into groups. 
		Then checks for conditions to empty the batch with a timeout taking priority over a full batch
		Finally appends point to proper window.
		"""
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
			self._state[point.group] = EchoHandler.state(self._size)
		if self.isTimeout(point):
			self.emptyAllGroups()
			self._timeout = self._timeout + self._period
		elif self.batchSizeExceeded(point):
			self.emptyGroup(point)
		# add the as option to update method
		self._state[point.group].update(value,point)

		

""" Entry point. Allows agent to write to handler and handler to write to agent"""
if __name__ == '__main__':
	a = Agent()
	h = EchoHandler(a)
	a.handler = h
	logger.info("Starting Agent")
	a.start()
	a.wait()
	logger.info("Agent finished")