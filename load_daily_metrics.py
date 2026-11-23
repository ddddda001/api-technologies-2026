from forecast_prototype import get_with_retry,url_servers
import os, psycopg, datetime as dt

def save_daily_metric(cursor,server_id,point):
    ts = point['ts']
    cpu = point['cpuPercent']
    ram = point['ramPercent']
    disk = point['diskPercent']
    day = ts[:10]
    query = "INSERT INTO server_daily_metrics (server_id, day, cpu_percent, ram_percent, disk_percent) VALUES(%s, %s, %s, %s, %s) ON CONFLICT(server_id, day) DO NOTHING"
    cursor.execute(query,[server_id, day, cpu, ram, disk])

def save_server_history(cursor, server_id, history):
    for point in history["points"]:
        save_daily_metric(cursor=cursor,server_id=server_id,point=point)

def get_year_history(server_id):
    now = dt.datetime.now(dt.timezone.utc)
    end = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - dt.timedelta(days=365)
    data = get_with_retry(URL=f'{url_servers}/{server_id}/metrics',params={'step' : '1d','agg' : 'mean' ,'from' : start.isoformat(), 'to': end.isoformat()})
    if data is None:
        print('History Year is None')
        return None
    return data.json()


def main():
    url_database = os.getenv('DATABASE_URL')
    if url_database is None:
        print('URL for databese is Not Found')
        return None
    servers = get_with_retry(URL=url_servers,params=None)
    if servers is not None:
        for server in servers.json():
            history = get_year_history(server['serverId'])
            if history is None:
                print('History is None ')
                continue
            with psycopg.connect(url_database) as conn:
                with conn.cursor() as cur:
                    save_server_history(cursor=cur,server_id=server["serverId"],history=history)

if __name__ == "__main__":
    main()