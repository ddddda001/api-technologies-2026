import grpc,forecast_pb2_grpc
from concurrent import futures
from forecast_pb2 import ForecastPredict
from forecast_pb2_grpc import ForecastServiceServicer
from forecast_prototype import get_art,get_server_history,predict_server_metrics
class ForecastService(ForecastServiceServicer):
    def __init__(self,art):
        self.art = art


    def Request(self,request,context):
        horizon = request.horizon
        mode = request.mode
        if horizon not in ['1d','1m','6m','1y']:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, 'Incorrect Horizon')
        if mode not in ['peak','mean']:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, 'Invalid mode')
        server = {"serverId" :request.serverID}
        result = get_server_history(server)

        if result is not None:
            data = predict_server_metrics(server = server,horizon = horizon, art=self.art, mode = mode, metrics = result)
        else:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, 'Insufficient history for a forecast')
        if len(data) == 3:
            return ForecastPredict(values_cpu=data['cpu'] ,values_ram=data['ram'],values_disk=data['disk'])
        else:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, 'There is no forecast for one of the metrics')

def serve():
    art = get_art()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    forecast_pb2_grpc.add_ForecastServiceServicer_to_server(ForecastService(art), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()