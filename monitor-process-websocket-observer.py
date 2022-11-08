#!/usr/bin/env python
import requests
import asyncio
import websockets
import json

from jsonify.convert import jsonify
from websockets.exceptions import WebSocketException, ConnectionClosedError

websocket_clients = set()
ws = ""

monitor_json = ""
cmd = "kylin"

settings = json.load(open('settings-websocket-server.json', 'r'))


def get_process_setting(paramname, filterby, valueofthis):
    return list(filter(lambda x: x[paramname] == filterby, settings))[0][valueofthis]

# __TEST_ON_SIMULATOR_REST_SERVICES (port 42001)
__TEST_OSRS = True

def get_url_process(pname):
    if __TEST_OSRS:
        tpic = "/status?name=" + pname
    else:
        tpic = pname
    return "http://" \
           + get_process_setting("name", "restserver", "host") \
           + ":" \
           + get_process_setting("name", "restserver", "port") \
           + tpic


async def get_status_all():
    r = await connect(get_url_process("all"))
    return jsonify(r)


async def connect(url):
    try:
        # response = await requests.get(url)
        response = requests.get(url)
    except requests.exceptions.RequestException as e:
        return jsonify({'connection': 'failed' + str(e)})
    return response.json()


async def handle_socket_connection(websocket, path):
    global monitor_json, cmd
    monitor_json = ""
    """Handles the whole lifecycle of each client's websocket connection."""
    websocket_clients.add(websocket)
    print(f'New connection from: {websocket.remote_address} ({len(websocket_clients)} total)')
    try:
        # This loop will keep listening on the socket until its closed. 
        async for raw_message in websocket:
            print(f'Got: [{raw_message}] from socket [{id(websocket)}]')
            try:
                m = json.loads(raw_message)
                print("content:", m["content"])
                cnt = m["content"]
                if cnt[:1] == '*':
                    cnt = cnt[1:]
                    cmd = cnt
            except Exception as e:
                print("json error:", e)
    # except websockets.exceptions.ConnectionClosedError as cce:
    except ConnectionClosedError as cce:
        print(f"error connecting to socket [{id(websocket)}]:", cce)
    else:
        print(f"Socket [{id(websocket)}] is live")
    finally:
        websocket_clients.remove(websocket)
        print(f"Socket [{id(websocket)}] was removed from stack")
    #    pass


#        print(f'Disconnected from socket [{id(websocket)}]...')

async def broadcast_monitor_services(loop):
    global monitor_json, cmd
    temp = "temp"
    is_different = True
    reset_clients = False
    while True:
        try:
            await asyncio.sleep(2)
            # response = requests.get("http://localhost:5000/kylin")
            if cmd == "":
                cmd = "kylin"
            print("cmd:", cmd)
            url = get_url_process("") + cmd
            cmd = "kylin"
            print("url:", url)
            try:
                response = requests.get(url)
            except Exception as e:
                print("request error:", e)
            try:
                temp = str(response.json())
            except Exception as e:
                print("json error (response):", e)
            if monitor_json == temp:
                is_different = False
            else:
                is_different = True
                monitor_json = temp
            print(f"is_different:", is_different)
            print(f"temp:", temp)
            print(f"monitor_json:", monitor_json)
        except requests.exceptions.RequestException as e:
            return jsonify({'connection': 'failed'})
        if is_different:
            try:
                for c in websocket_clients:
                    print(f'Sending socket [{id(c)}]')
                    await asyncio.ensure_future(
                        c.send('{"source": "' + str(id(c)) + '","content":"' + str(response.json()) + '"}'))
                    print(f"response:" + monitor_json)
            except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosedOK) as e:
                print(f"error sending to " + str(c), e)
            #                    raise websockets.exceptions.ConnectionClosedError("connection closed during sending")
            finally:
                pass
        else:
            print(f"nessun cambiamento.")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        socket_server = websockets.serve(handle_socket_connection,
                                         get_process_setting("name", "listenthis", "host"),
                                         get_process_setting("name", "listenthis", "port")
                        )
        print(f'Started socket server: {socket_server} ...')
        loop.run_until_complete(socket_server)
        loop.run_until_complete(broadcast_monitor_services(loop))
        loop.run_forever()
    finally:
        loop.close()
        print(f"Successfully shutdown [{loop}].")
