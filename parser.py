import copy
from datetime import datetime
import os
import xml.etree.ElementTree as ET
import pandas as pd
import json

class parser:
    def xmlParse(config):
        directory_in_str = config['dataFolder']
        directory = os.fsencode(directory_in_str)

        cols = ["sensor_id", "type", "datetime", "t_time", "dens", "flow", "speed", "cong_level",
                "anom_level", "latitude", "longitude"]

        df = pd.DataFrame(columns=cols) # crea il data frame con la colonna cols

        wfs = "{http://www.opengis.net/wfs/2.0}"  # not used
        gml = "{http://www.opengis.net/gml/3.2}"
        imobi_ds = "{imobi-ds}"

        count_file = 1
        count_empty_files = 0
        number_of_file = sum([len(files) for r, d, files in os.walk(directory)])

        for root, dirs, files in os.walk(directory):
            for file in files:
                filename = os.fsdecode(file)
                if filename.endswith(".xml"):
                    if count_file % 100 == 0:
                        print("--- Conversione formato: {:.1f}".format(count_file / number_of_file * 100), "% ---")
                    count_file = count_file + 1
                    tmp_df = pd.DataFrame(columns=cols)
                    filepath = os.path.join(root, file)
                    try:
                        tree = ET.parse(filepath) # trasfonma l' xml in un tree
                    except:
                        count_empty_files = count_empty_files + 1
                        empty_in_a_row = empty_in_a_row + 1
                        if empty_in_a_row >= (6 * 6) and empty_in_a_row % (
                                6 * 3) == 0:  # se il buco è di almeno 6 ore (ogni 3 viene notificato)
                            print("Empty file in dir (" + str(empty_in_a_row) + " in a row)")
                        break
                    empty_in_a_row = 0
                    tree_root = tree.getroot()

                    for col_name in cols:
                        tmp_column = []
                        if col_name == "type":
                            for neighbor in tree_root.iter(gml + "Point"):
                                tmp_column.append(neighbor.attrib[gml + 'id'][:13])
                        elif col_name != "latitude" and col_name != "longitude":
                            for neighbor in tree_root.iter(imobi_ds + col_name): # itera sugli elementi del tree
                                tmp_column.append(neighbor.text)
                        else:
                            for neighbor in tree_root.iter(gml + "Point"):
                                if col_name == "latitude":
                                    a = neighbor[0].text. split()
                                    tmp_column.append(a[1])
                                else:
                                    a = neighbor[0].text.split()
                                    tmp_column.append(a[0])
                        tmp_df[col_name] = tmp_column
                    df = pd.concat([df, tmp_df])

        df['sensor_id'] = 'METRO' + df['sensor_id'].astype(str)
        df.index = list(range(0, len(df.index)))
        j = df.to_json(orient="records")
        j = '{"data": ' + j + '}'
        j = json.loads(j)
        memory = copy.deepcopy(config)
        observations = []
        for i in range(len(j['data'])):
            d = parser.mapper(config, j, i)
            observations.append(d)
            config = copy.deepcopy(memory)
        return observations

    def mapper(conf, data,  i):
        currentTime = datetime.now()
        separator = conf['separator']
        separatorArray = conf['arraySeparator']
        conf = conf['mapping']
        try:
            for k, v in conf.items():
                if k == "id":
                    methods = "string"
                    if separator != None and separator != '':
                        if separator in str(conf[k]):
                            path = str(conf[k]).split(separator)
                            for x, p in enumerate(path):
                                if separatorArray != None and separator != '':
                                    if separatorArray in p:
                                        path[x] = int(p.replace(separatorArray, ''))
                                        path[x] = i
                            conf[k] = parser.meth(methods, data[path[0]][path[1]][path[2]])
                elif k == "type":
                    methods = "string"
                    conf[k] = parser.meth(methods, conf['type'])
                else:
                    methods = str(conf[k]['type'])
                    if separator != None and separator != '':
                        if separator in str(conf[k]['value']):
                            path = str(conf[k]['value']).split(separator)
                            for x, p in enumerate(path):
                                if separatorArray != None and separator != '':
                                    if separatorArray in p:
                                        path[x] = int(p.replace(separatorArray, ''))
                                        path[x] = i
                            if k == 'dateObserved':
                                conf[k]['value'] = parser.meth(methods, data[path[0]][path[1]][path[2]].replace('Z', '.000Z'))
                            else:
                                conf[k]['value'] = parser.meth(methods, data[path[0]][path[1]][path[2]])
                        else:
                            if str(conf[k]['value']).strip() != "":
                                path = str(conf[k]['value'])

                                if type(conf[k]['value']) == str:
                                    conf[k]['value'] = parser.meth(methods, data[path])
                                else:
                                    conf[k]['value'] = conf[k]['value']
                            else:
                                conf[k]['value'] = ""
                    else:
                        if str(conf[k]['value']).strip() != "":
                            path = str(conf[k]['value'])

                            if type(conf[k]['value']) == str:
                                conf[k]['value'] = parser.meth(methods, data[path])
                            else:
                                conf[k]['value'] = conf[k]['value']
                        else:
                            conf[k]['value'] = ""

        except KeyError as e:
            return currentTime, 'KeyError:', e
        else:
            return conf

    def meth(methods, dta):
        currentTime = datetime.now()
        try:
            if methods == 'string':
                d = str(dta)
            elif methods == 'integer':
                d = int(dta)
            elif methods == 'float':
                d = float(dta)
            elif methods == '':
                d = dta
            else:
                print(currentTime, 'Error value :', methods)
        except:
            print('Error value type')

        return d
