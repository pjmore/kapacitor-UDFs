import json
from kapacitor.udf.agent import Agent, Handler
from kapacitor.udf import udf_pb2
from collections import deque


import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(name)s: %(message)s')
logger = logging.getLogger()


class SigmaHandler(Handler):
    class State(object):
        def __init__(self, size):
            self._window = deque()
            self._square_window = deque()
            self._avg = 0
            self._avg_square = 0
            self._size = size
            self._sigma = 0

        def update(self, value):
            length = len(self._window)
            if length >= self._size:
                self._avg += value / length - self._window[0] / length
                self._avg_square += ((value ** 2) / length - self._square_window[0] / length)
                self._square_window.popleft()
                self._window.popleft()
            else:
                self._avg = (value + length * self._avg) / (length + 1)
                self._avg_square = (value ** 2 + length * self._avg_square) / (length + 1)
            self._sigma = (self._avg_square - self._avg ** 2)**0.5
            self._window.append(value)
            self._square_window.append(value ** 2)

            if self._sigma == 0:
                return 0
            else:
                return (value - self._avg) / self._sigma

        def snapshot(self):
            return {
                '_window': list(self._window),
                '_square_window': list(self._square_window),
                '_avg': self._avg,
                '_avg_square': self._avg_square,
                '_size': self._size
            }

        def restore(self, snapshot):
            self._window = deque(snapshot['_window'])
            self._square_window = deque(snapshot['_square_window'])
            self._avg = snapshot['_avg']
            self._avg_square = snapshot['_avg_square']
            self._size = snapshot['_size']

        def reset(self):
            self._window = []

        def getsize(self):
            return len(self._window)

    def __init__(self, agent):
        self._agent = agent
        self._field = ''
        self._size = 100
        self._period = 0
        self._as = ''
        self._states = {}
        self._fill = False

    def info(self):
        response = udf_pb2.Response()
        response.info.wants = udf_pb2.STREAM
        response.info.provides = udf_pb2.STREAM
        response.info.options['field'].valueTypes.append(udf_pb2.STRING)
        response.info.options['window'].valueTypes.append(udf_pb2.INT)
        response.info.options['as'].valueTypes.append(udf_pb2.STRING)
        response.info.options['fillWindow'].valueTypes.append(udf_pb2.BOOL)
        return response

    def init(self, init_req):
        success = True
        msg = ''
        for opt in init_req.options:
            if opt.name == 'field':
                self._field = opt.values[0].stringValue
            elif opt.name == 'window':
                self._size = opt.values[0].intValue
            elif opt.name == 'as':
                self._as = opt.values[0].stringValue
            elif opt.name == 'fillWindow':
                self._fill = True

        if self._field == '':
            msg += ' Must input the field'
        if self._as == '':
            self._as = 'sigma_' + self._field

        response = udf_pb2.Response()
        response.init.success = success
        response.init.error = msg[1:]
        return response

    def snapshot(self):
        data = {}
        for group, state in self._states.iteritems():
            data[group] = state.snapshot()
        response = udf_pb2.Response()
        response.snapshot.snapshot = json.dumps(data).encode('latin-1')
        return response

    def restore(self, restore_req):
        success = False
        msg = ''
        try:
            data = json.loads(restore_req.snapshot)
            for group, snapshot in data.iteritems():
                self._states[group] = SigmaHandler.State(self._size)
                self._states[group].restore(snapshot)
            success = True
        except Exception as e:
            success = False
            msg = str(e)

        response = udf_pb2.Response()
        response.restore.success = success
        response.restore.error = msg
        return response

    def begin_batch(self, begin_req):
        raise ValueError("Stream -> Stream is the only NodeIO format supported")

    def end_batch(self, end_req):
        raise ValueError("Stream -> Stream is the only NodeIO format supported")

    def point(self, point):
        response = udf_pb2.Response()
        response.point.CopyFrom(point)
        value = point.fieldsDouble[self._field]
        if point.group not in self._states:
            self._states[point.group] = SigmaHandler.State(self._size)
        sigma = self._states[point.group].update(value)
        response.point.fieldsDouble[self._as] = sigma
        if self._fill and self._states[point.group].getsize() >= self._size:
            self._agent.write_response(response)
        elif not self._fill:
            self._agent.write_response(response)


""" Entry point. Allows agent to write to handler and handler to write to agent"""
if __name__ == '__main__':
    a = Agent()
    h = SigmaHandler(a)
    a.handler = h
    logger.info("Starting Agent")
    a.start()
    a.wait()
    logger.info("Agent finished")
