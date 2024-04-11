
from flask import Flask
import socket
import threading
import select
import psutil
import redis
import time
isTransferringData = False
terminateAll = False
to_reload = False



def get_pid(port):
    connections = psutil.net_connections()
    port = int(port)
    for con in connections:
        if con.raddr != tuple():
            if con.raddr.port == port:
                return con.pid, con.status
        if con.laddr != tuple():
            if con.laddr.port == port:
                return con.pid, con.status
    return -1


class ClientThread(threading.Thread):
    def __init__(self, clientSocket, targetHost, targetPort):
        threading.Thread.__init__(self)
        self.__clientSocket = clientSocket
        self.__targetHost = targetHost
        self.__targetPort = targetPort

    def run(self):
        print("Client Thread started")

        self.__clientSocket.setblocking(0)

        targetHostSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        targetHostSocket.connect((self.__targetHost, self.__targetPort))
        targetHostSocket.setblocking(0)

        clientData = bytearray()
        targetHostData = bytearray()
        terminate = False
        while not terminate and not terminateAll:
            inputs = [self.__clientSocket, targetHostSocket]
            outputs = []

            if len(clientData) > 0:
                outputs.append(self.__clientSocket)

            if len(targetHostData) > 0:
                outputs.append(targetHostSocket)

            try:
                inputsReady, outputsReady, errorsReady = select.select(inputs, outputs, [], 1.0)
            except Exception as e:
                print(e)
                break

            for inp in inputsReady:
                if inp == self.__clientSocket:
                    try:
                        data = self.__clientSocket.recv(4096)
                    except Exception as e:
                        print(e)

                    if data != None:
                        if len(data) > 0:
                            targetHostData += data
                        else:
                            terminate = True
                elif inp == targetHostSocket:
                    try:
                        data = targetHostSocket.recv(4096)
                    except Exception as e:
                        print(e)

                    if data != None:
                        if len(data) > 0:
                            clientData += data
                        else:
                            terminate = True

            for out in outputsReady:
                if out == self.__clientSocket and len(clientData) > 0:
                    try:
                        bytesWritten = self.__clientSocket.send(clientData)
                    except Exception as e:
                        print(e)
                        break
                    if bytesWritten > 0:
                        clientData = clientData[bytesWritten:]
                elif out == targetHostSocket and len(targetHostData) > 0:
                    try:
                        bytesWritten = targetHostSocket.send(targetHostData)
                    except Exception as e:
                        print(e)
                        break
                    if bytesWritten > 0:
                        targetHostData = targetHostData[bytesWritten:]

        self.__clientSocket.close()
        targetHostSocket.close()
        print("ClientThread terminating")


def main(localHost, localPort, targetHost, targetPort, isTransferringData=isTransferringData):
    serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if isTransferringData:
        global terminateAll
        terminateAll = False
    else:
        terminateAll = True
        serverSocket.close()
    serverSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    serverSocket.bind((localHost, localPort))
    serverSocket.listen(5)
    print("Waiting for client...")
    if not isTransferringData:
        terminateAll = True
    while True:
        try:
            clientSocket, address = serverSocket.accept()
        except KeyboardInterrupt:
            print("\nTerminating...")
            terminateAll = True
            break
        ClientThread(clientSocket, targetHost, targetPort).start()
    serverSocket.close()



app = Flask(__name__)
@app.route('/server-1')
def flip():
    currentDataCenter = 'server1.redis.com'
    currentPort = 6379
    main(localHost='localhost', localPort=8000, targetHost=currentDataCenter, targetPort=6379, isTransferringData=True)
    return 'Datacenter is changed, proxying to ' + currentDataCenter + ' on port ' + str(currentPort)

@app.route('/server-2')
def flip2():
    currentDataCenter = 'server1.redis.com'
    currentPort = 5500
    main(localHost='localhost', localPort=8000, targetHost=currentDataCenter, targetPort=6380, isTransferringData=True)
    return 'Datacenter is changed, proxying to ' + currentDataCenter + ' on port ' + str(currentPort)

@app.route('/dbHealth')
def dbHealth():
    start_time_1 = time.time()
    r = redis.Redis(host='localhost', port=6380, db=0)
    r.ping()
    start_time_2 = time.time()
    r= redis.Redis(host='localhost', port=6379, db=0)
    r.ping()
    return 'DBs are healthy & their response time is ' + str(time.time() - start_time_1) + ' seconds'+ ' and ' + str(time.time() - start_time_2) + ' seconds'


def get_pid():
    connections = psutil.net_connections()
    port = 8000
    for con in connections:
        if con.raddr != tuple():
            if con.raddr.port == port:
                return con.pid, con.status
        if con.laddr != tuple():
            if con.laddr.port == port:
                return con.pid, con.status
    return -1



@app.route('/quit')
def _quit():
    pid = get_pid()
    p = psutil.Process(pid[0])
    p.terminate()
    return "Server is shutting down"



# main driver function
if __name__ == '__main__':
    # run() method of Flask class runs the application
    # on the local development server.
    app.run()

