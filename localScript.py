import json
import subprocess
import threading
import time
from threading import Thread
import flask
import os
from datetime import datetime, timedelta
import copy
import requests
import jwt
import sys
from parserFunc import parser
import urllib3
import jsonpickle

app = flask.Flask(__name__)
os.environ["FLASK_APP"] = __name__ + ".py"

subprocess.check_call([sys.executable, "-m", "pip", "install", 'jsonpickle'])

class thread_gen(Thread):
    def __init__(self, kill, toll, nThread):
        Thread.__init__(self)
        self.kill = kill
        self.toll = toll
        self.nThread = nThread

    def run(self):
        stop_event = threading.Event()
        threads = []
        parts = list(split(observations, self.nThread))
        for i in range(self.nThread):
            t = working_thread(config, s.cookies.get("access_token"), parts[i], stop_event, self.kill, self.toll)
            t.start()
            threads.append(t)
        j = threading.Thread(target=joiner, args=(stop_event, threads))
        j.start()
        stop_event.wait()
        if interr == True:
            print("Caricamento interrotto a causa dei troppi errori")
        for t in threads:
            t.stopThread()
        td = time.time() - timeStart
        print("The program has uploaded " + str(exec) + " of " + str(len(observations)))
        print("Total number of error: " + str(len(failed)))
        print(f"--- Execution time : {td:.01f}s ---")
        sys.exit()


class working_thread(Thread):
    def __init__(self, conf, token, data, stop_event, kill, toll):
        Thread.__init__(self)
        self.conf = conf
        self.token = token
        self.data = data
        self.stop_event = stop_event
        self.stop = threading.Event()
        self.kill = kill
        self.toll = toll

    def stopThread(self):
        self.stop.set()

    def run(self):
        for r in self.data:
            if not self.stop.isSet():
                apiPatch(self.conf, r['id'], self.token, r, self.stop_event, self.kill, self.toll)


@app.route('/scriptBello', methods=['GET', 'POST'])
def scriptBello():
    # GET PARAMS IN INPUT (day_date,sensor_uri)
    global config, id_name, s, append_services, jsize, total, partial, exec, timeStart, interr, nFailure, tokenTime
    global observations
    global failed
    observations = []
    failed = []

    currentTime = datetime.now()
    timeStart = time.time()
    urllib3.disable_warnings()

    file_to_delete = open("failed.txt", 'w')
    file_to_delete.close()

    try:
        root_path = os.getcwd()
        params_path = root_path + "/data/conf.json"
        f = open(params_path)
        config = json.load(f)
        print("config caricato")
        f.close()
    except Exception as e:
        print(currentTime, e)
        return e

    try:
        if flask.request.method == 'GET':
            print("prendo i parametri")

        else:
            yourarg = "Nothing"
            print("Nothing: ")
            append_services.append("Nothing:---")
    except Exception as e:
        print("Error: " + str(e))
        message = "Error: " + str(e);
        return message
    s = requests.Session()
    try:
        kill = config['kill']
        toll = config['toll']
        nThread = config["threadNumber"]

    except Exception as e:
        print("Error getting: " + str(e))
        message = "Error getting: " + str(e)
        return message

    try:
        total = 0
        partial = 0
        exec = 0
        interr = False
        nFailure = 0
        observations = parser.xmlParse(config)
        jsize = len(observations)
        data = observations[0]
        id_name = idDevice(config.get("mapping").get("id"), data)  # assegna a id_name l'id nel mapping del json

        if id_name != '' and data != None:
            res = json.dumps(data)  # json for orion  #converte un oggetto python in un oggetto json
            print(res)
            currentTime = datetime.now()

            if s.cookies.get("access_token") == None or s.cookies.get("access_token") == "":
                access_token, refresh_token = accessToken(config)
                s.cookies.set("access_token", access_token)
                s.cookies.set("refresh_token", refresh_token)
                tokenTime = datetime.now()

            else:
                dateExp = verifySignature(s.cookies.get("access_token"))
                if currentTime > dateExp:
                    access_token, refresh_token = refreshToken(config, s.cookies.get("refresh_token"))
                    s.cookies.set("access_token", access_token)
                    s.cookies.set("refresh_token", refresh_token)

            # PATCH FINALE
            if type(s.cookies.get("access_token")) == str and s.cookies.get("access_token") != '':
                gen = thread_gen(kill, toll, nThread)
                gen.start()
                gen.join()


            else:
                access_token, refresh_token = accessToken(config)
                s.cookies.set("access_token", access_token)
                s.cookies.set("refresh_token", refresh_token)
                gen = thread_gen(kill, toll, nThread)
                gen.start()
                gen.join()
        return jsonpickle.encode(failed)

    except Exception as e:
        print("Error: " + str(e))
        message = "Error: " + str(e);
        return message


###############################################################################
def split(a, n):
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))


def idDevice(conf, dta):
    deviceName = ''
    if type(conf) == list:
        name = dta['name'].replace(' ', '-')
        if name != 'Pedonale-verso-Migross' and name != 'Pedonale-da-Migross':
            try:
                for item in conf:
                    try:
                        if name in item:
                            deviceName = item
                    except ValueError:
                        print(ValueError)
            except ValueError:
                print(ValueError)

    else:
        deviceName = conf

    return deviceName


###############################################################################

def accessToken(conf):
    access_token = ''
    refresh_token = ''
    payload = {
        'f': 'json',
        'client_id': conf.get('token').get('clientID'),
        'client_secret': conf.get('token').get('clientSecret'),
        'grant_type': 'password',
        'username': conf.get('token').get('username'),
        'password': conf.get('token').get('password')
    }

    header = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    urlToken = conf.get('token').get('url')
    currentTime = datetime.now()
    try:
        response = requests.request("POST", urlToken, data=payload, headers=header)
        print(response.text)
        response.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        print(currentTime, "Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print(currentTime, "Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print(currentTime, "Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print(currentTime, "OOps: Something Else", err)
    else:
        token = response.json()
        access_token = token['access_token']
        refresh_token = token['refresh_token']

    return access_token, refresh_token


def verifySignature(accessToken):
    decodeToken = jwt.decode(accessToken, options={"verify_signature": False})
    epoch_time = decodeToken['exp']
    dateExp = datetime.fromtimestamp(epoch_time)

    return dateExp


def refreshToken(conf, refreshToken):
    access_token = ''
    refresh_token = ''
    payload = json.dumps({
        'f': 'json',
        'client_id': conf.get('token').get('clientID'),
        'client_secret': conf.get('token').get('clientSecret'),
        'grant_type': 'refresh_token',
        'refresh_token': refreshToken
    })

    header = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    urlToken = conf.get('token').get('url')
    currentTime = datetime.now()
    try:
        response = requests.request("POST", urlToken, data=payload, headers=header)
        reqStatus = response.status_code

        if reqStatus == 400:
            access_token, refresh_token = accessToken(config)

        response.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        errh = ""
        #print(currentTime, "Http Error:", errh)
        #print(response.text)
    except requests.exceptions.ConnectionError as errc:
        print(currentTime, "Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print(currentTime, "Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print(currentTime, "Ops: Something Else", err)
    else:
        token = response.json()
        access_token = token['access_token']
        refresh_token = token['refresh_token']

    return access_token, refresh_token


def apiPatch(conf, device, accessToken, r, stop_event, kill, toll):
    global exec, partial, total, response, nFailure, dateExp, tokenTime
    currentTime = datetime.now()
    dateExp = verifySignature(s.cookies.get("access_token"))
    if datetime.now() - tokenTime > timedelta(days=0, seconds=0, microseconds=0, milliseconds=0, minutes=3, hours=0, weeks=0):
        tokenTime = datetime.now()
        access_token, refresh_token = refreshToken(config, s.cookies.get("refresh_token"))
        s.cookies.set("refresh_token", refresh_token)
        s.cookies.set("access_token", access_token)
        dateExp = verifySignature(s.cookies.get("access_token"))

    url = conf.get('patch').get('url') + device + '/attrs?elementid=' + device + '&type=' + conf.get('mapping').get(
        'type')
    head = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"bearer {s.cookies.get('access_token')}",
    }
    try:
        exec += 1
        if exec % 100 == 0:
            partial = 0
        if exec % 1000 == 0:
            print("--- Loading: {:.0f}".format(exec / len(observations) * 100), "% ---")
            print("--- Number of errors: " + str(len(failed)) + " ---")
        q = copy.deepcopy(r)
        for i in ['id', 'type']:
            q.pop(i)
        response = requests.request("PATCH", url, headers=head, data=json.dumps(q), verify=False)
        if len(failed) > nFailure:
            nFailure = len(failed)
            print(response.text)
        response.raise_for_status()
    except Exception as ex:
        c = 0
        success = False
        while not success and c < 3:
            time.sleep(0.01)
            success = retry(conf, device, accessToken, r)
            c += 1
        if not success:
            saveFail(json.dumps(r), ex)
            check(stop_event, total, partial, kill, toll)


def saveFail(m, err):
    global total, partial
    total += 1
    partial += 1
    js = '{"JSON": ' + m + ', "error": "' + str(err) + '"}'
    failed.append(json.loads(js))
    with open('failed.txt', 'a') as f:
        f.write(js + "\n")


def check(stop_event, total, partial, kill, toll):
    global interr
    if total > kill or partial > toll:
        stop_event.set()
        interr = True


def joiner(stop_event, threads):
    for t in threads:
        t.join()
    stop_event.set()


def retry(conf, device, accessToken, r):
    try:
        url = conf.get('patch').get('url') + device + '/attrs?elementid=' + device + '&type=' + conf.get(
            'mapping').get(
            'type')

        head = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"bearer {accessToken}",
        }
        currentTime = datetime.now()
        response = requests.request("PATCH", url, headers=head, data=json.dumps(r), verify=False)
        response.raise_for_status()
        return True
    except Exception as ex:
        return False


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)
    # app.run(host="127.0.0.1", port=5000)
    # http://127.0.0.1:500/scriptBello?start_date="2022-10-01T00:00:00"&end_date="2022-10-03T00:00:00"&sensor_uri="http://www.disit.org/km4city/resource/iot/orionToscana-UNIFI/Toscana/SMART04"
