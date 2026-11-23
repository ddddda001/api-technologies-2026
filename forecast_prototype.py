import requests,dill, os,math,time, pandas as pd
import datetime as dt
from dotenv import load_dotenv

load_dotenv()

headers = {
    'X-API-Key': os.getenv('API_KEY'),
}

mock_scenario = os.getenv('MOCK_SCENARIO')
if mock_scenario is not None:
     headers['X-MOCK-SCENARIO'] = mock_scenario

now = dt.datetime.now(dt.timezone.utc)
end = now.replace(hour=0, minute=0, second=0, microsecond=0)
start = end - dt.timedelta(days=30)

params = {
    'step': '1d',
    'agg': 'mean',
    'from': start.isoformat(),
    'to': end.isoformat()
}
monitoring_url = os.getenv('MONITORING_URL') or 'https://api.external-services.dataprosvet.ru/monitoring'
url_servers = monitoring_url.rstrip('/') + '/v1/servers'

backoff = {'1': 5, '2' : 10, '3': 0}

# --- Получение данных из dill ---
def get_art():
    with open('capacity-forecast-v1.dill', 'rb') as f:
            art = dill.load(f)
    return art


# ---- Проверка метрик на сходство даты ---
def check_metrics(metrics_response,server, params=params):
            metrics = metrics_response.json()
            if len(metrics['points']) == 30:
                start = dt.datetime.fromisoformat(params['from'])
                end = dt.datetime.fromisoformat(params['to'])
                expected_last = end - dt.timedelta(days=1)
                first_point = dt.datetime.fromisoformat(metrics['points'][0]['ts'])
                last_point = dt.datetime.fromisoformat(metrics['points'][-1]['ts'])
                if start == first_point and expected_last == last_point:
                    for date in range(0, 29):
                        if (dt.datetime.fromisoformat(metrics['points'][date+1]['ts']) - dt.datetime.fromisoformat(metrics['points'][date]['ts'])) != dt.timedelta(days=1):
                            print(f'Error: {metrics["points"][date+1]["ts"]} - {metrics["points"][date]["ts"]}')
                            return None
                    return metrics
                else:
                        print(f'Error: Timestamps do not match the requested range for server {server["serverId"]}')
                        return None
            else:
                print(f'Expected 30 data points, but got {len(metrics["points"])} for server {server["serverId"]}')
                return None

#---Проверяет и готовит признаки для модели ----
def get_metrics_all(metrics,server,name):
    for i in range(0, 30):
        valuses_name = f'{name}Percent'
        values = metrics['points'][i].get(valuses_name)
        if (not isinstance(values, (int, float)) or isinstance(values, bool) or not math.isfinite(values) or not 0 <= values <= 100):
            print(f'Error: Invalid {valuses_name} value {values} at index {i+1} for server {server["serverId"]}')
            return None

    data = {}
    data['metric']= name
    for index, value in enumerate(metrics['points'], start=1):
            data[f'd{index}'] = value[f"{name}Percent"]
    return data


# ---- Построение прогнозирования ---
def predict(data,art, horizon,mode):
    meta = art['meta']
    df = pd.DataFrame([data], columns=meta["features"])
    predictions = art['models'][horizon][mode].predict(df)
    return float(predictions[0])

# ---Работа с сетью---
def get_with_retry(URL,params):
    for attempt in range(1,len(backoff)+1):
        try:
            response=requests.get(URL, headers=headers, timeout=5, params=params)
        except requests.exceptions.Timeout as err:
            print(f'URL: {URL} Error: {err} attempt: {attempt}')
            if attempt == len(backoff):
                break
            time.sleep(backoff[str(attempt)])
            continue

        except requests.exceptions.ConnectionError as err:
            print(f'URL {URL} Error: {err} attempt: {attempt}')
            if attempt == (len(backoff)):
                break
            time.sleep(backoff[str(attempt)])
            continue

        if response.status_code == 200:
            return response

        elif response.status_code == 429 or response.status_code == 503:
            print(f'URL: {URL} Error: {response.status_code} attempt: {attempt}')
            if attempt == len(backoff):
                return None
            else:
                try:
                    timeout = int(response.headers.get('Retry-After'))
                    if timeout < 0:
                         timeout = backoff[str(attempt)]
                except (ValueError, TypeError):
                    timeout = backoff[str(attempt)]
                time.sleep(int(timeout))
        else:
            print(f'URL: {URL} Error: {response.status_code} attempt: {attempt}')
            return None

def predict_server_metrics(metrics,server,horizon,mode,art):
    g={}
    for name in ('cpu', 'ram', 'disk'):
        data = get_metrics_all(metrics, server, name)
        if data is not None:
            g[name] = predict(data, art, horizon, mode)
    return g

def get_server_history(server):
        url_metrics = f'{url_servers}/{server["serverId"]}/metrics'
        metrics_response = get_with_retry(url_metrics,params)
        if metrics_response is not None:
            result = check_metrics(metrics_response,server)
            return result
        return None

def main():
    art = get_art()
    data={}
    servers_response = get_with_retry(url_servers,None)
    if servers_response is not None:
        servers = servers_response.json()
        for server in servers:
            result = get_server_history(server=server)
            if result is not None:
                metrics = result
                data[server["serverId"]] = predict_server_metrics(metrics=metrics,server=server,horizon='1m',mode='peak',art=art)
if __name__ == "__main__":
    main()



