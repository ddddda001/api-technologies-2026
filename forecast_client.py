import grpc,forecast_pb2_grpc
from forecast_pb2 import RequestPredict

channel = grpc.insecure_channel('localhost:50051')
stub = forecast_pb2_grpc.ForecastServiceStub(channel)
request = RequestPredict(serverID = 'srv-001',horizon = '1m',mode = 'peak')
response = stub.Request(request)
print(response)