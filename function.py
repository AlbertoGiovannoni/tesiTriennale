from datetime import datetime


class fun:
    def function(conf, data, i, separator, separatorArray):
        currentTime = datetime.now()
        try:
            for k, v in conf.items():
                methods = str(conf[k]['type'])
                if separator != None and separator != '':
                    if separator in str(conf[k]['value']):
                        path = str(conf[k]['value']).split(separator)
                        for x, p in enumerate(path):
                            if separatorArray != None and separator != '':
                                if separatorArray in p:
                                    path[x] = int(p.replace(separatorArray, ''))
                                    path[x] = i
                        if len(path) == 2:
                            if path[1].strip() == "":
                                conf[k]['value'] = fun.meth(methods, data[path[0]])
                            else:
                                conf[k]['value'] = fun.meth(methods, data[path[0]][path[1]])
                        if len(path) == 3:
                            conf[k]['value'] = fun.meth(methods, data[path[0]][path[1]][path[2]])
                        if len(path) == 4:
                            conf[k]['value'] = fun.meth(methods, data[path[0]][path[1]][path[2]][path[3]])
                        if len(path) == 5:
                            conf[k]['value'] = fun.meth(methods, data[path[0]][path[1]][path[2]][path[3]][path[4]])
                        if len(path) == 6:
                            conf[k]['value'] = fun.meth(methods,
                                                    data[path[0]][path[1]][path[2]][path[3]][path[4]][path[5]])
                        if len(path) == 7:
                            conf[k]['value'] = fun.meth(methods,
                                                    data[path[0]][path[1]][path[2]][path[3]][path[4]][path[5]][
                                                        path[6]])
                        if len(path) == 8:
                            conf[k]['value'] = fun.meth(methods,
                                                    data[path[0]][path[1]][path[2]][path[3]][path[4]][path[5]][
                                                        path[6]][
                                                        path[7]])
                        if len(path) == 9:
                            conf[k]['value'] = fun.meth(methods,
                                                    data[path[0]][path[1]][path[2]][path[3]][path[4]][path[5]][
                                                        path[6]][
                                                        path[7]][path[8]])
                        if len(path) == 10:
                            conf[k]['value'] = fun.meth(methods,
                                                    data[path[0]][path[1]][path[2]][path[3]][path[4]][path[5]][
                                                        path[6]][
                                                        path[7]][path[8]][path[9]])
                        if len(path) == 11:
                            conf[k]['value'] = fun.meth(methods,
                                                    data[path[0]][path[1]][path[2]][path[3]][path[4]][path[5]][
                                                        path[6]][
                                                        path[7]][path[8]][path[9]][path[10]])
                        if len(path) == 12:
                            conf[k]['value'] = fun.meth(methods,
                                                    data[path[0]][path[1]][path[2]][path[3]][path[4]][path[5]][
                                                        path[6]][
                                                        path[7]][path[8]][path[9]][path[10]][path[11]])
                        if len(path) == 13:
                            conf[k]['value'] = fun.meth(methods,
                                                    data[path[0]][path[1]][path[2]][path[3]][path[4]][path[5]][
                                                        path[6]][
                                                        path[7]][path[8]][path[9]][path[10]][path[11]][path[12]])

                    else:
                        if str(conf[k]['value']).strip() != "":
                            path = str(conf[k]['value'])

                            if type(conf[k]['value']) == str:
                                conf[k]['value'] = fun.meth(methods, data[path])
                            else:
                                conf[k]['value'] = conf[k]['value']
                        else:
                            conf[k]['value'] = ""
                else:
                    if str(conf[k]['value']).strip() != "":
                        path = str(conf[k]['value'])

                        if type(conf[k]['value']) == str:
                            conf[k]['value'] = fun.meth(methods, data[path])
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
                data = str(dta)
            elif methods == 'integer':
                data = int(dta)
            elif methods == 'float':
                data = float(dta)
            elif methods == '':
                data = dta
            else:
                print(currentTime, 'Error value :', methods)
        except:
            print('Error value type')

        return data

