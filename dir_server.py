#!/usr/bin/env python
import threading
import select
import signal
import socket
import time
import sys

#######global data#######
online_ds_list = []
threads = []
connections = []
clients = []
#########################

def sigint_handler(signal, frame):
    print('Directory Server shutting down...')
    sys.exit(0)

def close_threads():
    for t in threads:
        t.running = 0
        t.sock.close() 
        t.join()
    sys.exit(0)

def getAllFilesFromClients():
    filesList = []
    for c in clients:
        for f in c.shared_files:
            if f not in filesList:
                filesList.append(f)
    return filesList

def getClientTuples(fname):
    tuples = []
    for c in clients:
        if fname in c.shared_files:
            tup = (c.ip, c.port)
            tuples.append(tup)
    return tuples

###############################################################################
def auxDsSearch((sockfd, bro_addr), data):
    msg = data[:-2].split(' ')
    cmd = msg[0]
    if cmd != "find":
        sockfd.send("405 Invalid arguments")
        sockfd.close()
        return
    filename = msg[1]
    c_tupes = getClientTuples(filename)
    send_msg = ""
    if c_tupes:
        send_msg = "200 Success\r"
        for (c_ip, c_port) in c_tupes:
            send_msg += (c_ip + ' ' + str(c_port) + ' ')#ip, port info
            send_msg += (filename + '\r')
        send_msg += '\n'
    else:
        send_msg = "401 Not found\r\n"
    sockfd.send(send_msg)
    sockfd.close()

def allOnlineFiles((sockfd, c_addr), data):
    if data != "servershare\r\n":
        sockfd.send("405 Invalid arguments")
        sockfd.close()
        return
    m_files = ""
    filesList = getAllFilesFromClients()
    if not filesList:
        send_msg = "406 No shared files\r\n"
    else:
        for f in filesList:
            m_files += (f + '\r')
        else:
            send_msg = "200 Success\r"
            send_msg += m_files
            send_msg += '\n'
    sockfd.send(send_msg)
    sockfd.close()
    return

###############################################################################
class Client(threading.Thread):
    def __init__(self, (sockfd, addr), data):
        threading.Thread.__init__(self)
        self.name = "ClientConnection"
        self.ip = str(addr[0])
        self.port = addr[1]#40000

        print('CLIENT PORT: ' + str(self.port))

        self.size = 1024
        self.running = 1
        self.sock = sockfd
        self.shared_files= []
        print('initialized a client...')
        self.parse_cmd(data)

    def parse_cmd(self, data):
        msg = data.split('\r')
        if msg[-1:][0] != '\n':
            self.sock.send("400 Failure\r\n")
            return
        msg = msg[:-1][0]
        msg = msg.split(' ')
        l = len(msg)

        if (msg[0] == "share") and (l > 1):
            ##-----following needs locking
            for i in range(1, l):
                filename = msg[i]
                ##LOCK
                if filename and (filename not in self.shared_files):
                    self.shared_files.append(filename)
                ##RELEASE
            send_msg = "200 Success\r"
            for f in self.shared_files:
                send_msg += (f+'\r')
            send_msg += '\n'
            self.sock.send(send_msg)

        elif (msg[0] == "remove") and (l == 2):
            m_file = msg[1]
            ##-----following needs locking
            suc = False
            ##LOCK
            for f in self.shared_files:
                if f == m_file:
                    self.shared_files.remove(f)
                    suc = True
            ##RELEASE
            if suc:
                send_msg = "200 Success\r\n"
            else:
                send_msg = "404 File not found\r\n"
            self.sock.send(send_msg)

        elif (msg[0] == "find") and (l == 2):
            m_filename = msg[1]
            allClients = ""
            localClients = getClientTuples(m_filename)
            for (lip, lport) in localClients:
                cline = str(lip) + ' ' + str(lport) + ' ' + m_filename + '\r'
                allClients += cline
            #now get remote clients with file
            for (ds_ip, ds_port) in online_ds_list:
                #open a conn then call getClientTuples
                ds_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                ds_sock.connect((ds_ip, ds_port))
                ds_sock.send(data)
                d_msg = ds_sock.recv(self.size)
                lines = d_msg[:-2].split('\r')
                st = lines[0]
                if st.split(' ')[0] == "200":
                    for l in lines[1:]:
                        allClients += l
            if allClients:
                send_msg = "200 Success\r"
                for c in allClients:
                    send_msg += c
                send_msg += '\n'
            else:
                send_msg = "401 Not found\r\n"
            self.sock.send(send_msg)

        elif (data == "servershare\r\n"):
            files_str = ""
            send_msg = ""
            #local files first
            localFilesList = getAllFilesFromClients()
            for f in localFilesList:
                files_str += (f + '\r')
            #now remote files
            for (ds_ip, ds_port) in online_ds_list:
                #open a conn then call getClientTuples
                ds_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                ds_sock.connect((ds_ip, ds_port))
                ds_sock.send(data)
                d_msg = ds_sock.recv(self.size)
                lines = d_msg[:-2].split('\r')
                st = lines[0]
                if st.split(' ')[0] == "200":
                    for l in lines[1:]:
                        files_str += (l + '\r')

            if not files_str:
                send_msg = "406 No shared files\r\n"
            else:
                send_msg = "200 Success\r"
                send_msg += files_str
                send_msg += '\n'
            self.sock.send(send_msg)

        elif (msg[0] == "portInfo"):
            pnum = msg[1]
            self.port = int(pnum)
            print('CLIENT LISTENING PORT: ' + str(self.port))
            self.sock.send("200 Success\r\n")

        else:
            print(data)
            self.sock.send("405 Invalid arguments\r\n")

    def run(self):
        print('started a client...')
        self.sock.setblocking(True)
        while self.running:
            try:
                data = self.sock.recv(self.size)
                print('FRIM CLIENT----> '+data)
                if not data:
                    print 'closing this client, error getting message from sock'
                    return
                #print str(data) + "<----------|data|"
            except Exception:
                self.sock.close()
                print "socket closed, closing thread..."
                return
            if data:
                self.parse_cmd(data)
###############################################################################
class RegHandler(threading.Thread):
    def __init__(self, rs_ip, rs_port, ip, port):
        threading.Thread.__init__(self)
        self.name = "RegistrationServerConnection"
        self.rs_ip = rs_ip
        self.rs_port = rs_port
        self.ip = ip
        self.port = port
        self.size = 1024
        self.running = 1
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #connect is to bind to a remote socket, bind is for local socket (server will use this)
        self.sock.connect((self.rs_ip, self.rs_port))

    def run(self):
        while self.running:
            msg = 'ds_register' + ' ' + self.ip + ' ' + str(self.port) + '\r\n' #NEED TO HAVE ERROR CHECKING HERE
            self.sock.send(msg)
            data = self.sock.recv(self.size)

            data = data.split('\r')
            if data[-1:][0] != '\n':
                self.running = 0
                print 'received bad data from register server'
                print '|-------> '+str(data)
                return
            data = data[:-1]
            status_line = data[0].split(' ')
            if len(status_line) != 2:
                sys.exit(1)

            stat_code = status_line[0]
            stat_msg  = status_line[1]

            if int(stat_code) == 400:
                print 'Directory Server incorrectly formatted message : check config and restart'
                #optional return could be here...

            elif int(stat_code) == 200 and stat_msg == "Success":
                #it was a success, parse the ds's and add them as tuples to the global list
                num_servers = len(data)
                online_ds_list = [] #this is to reset the list

                # print "-----------------"
                # print data
                # print "-----------------"

                for o_ds in range(1,num_servers):
                    line = data[o_ds]
                    o_ds_info = line.split(' ')
                    ods_ip = o_ds_info[0]
                    ods_port = o_ds_info[1]
                    online_ds_list.append((ods_ip, ods_port))

            # repeat after 30 seconds
            time.sleep(15)#CHANGE TO 30
            print time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime())

        self.sock.close() #MIGHT NEED TO CHANGE THIS TO WAIT FOR REGISTRATION SERVER TO CLOSE CONNECTION

def main():
    signal.signal(signal.SIGINT, sigint_handler)
    
    ####this stuff should be in .cgf file
    m_ip = '127.0.0.1' #socket.gethostname() #wtf is this?
    m_port = 50000
    rg_server_ip = ''
    rg_server_port = 60000
    ####---------------------------------

    size = 1024

    if len(sys.argv) != 1:
        sys.exit(1)


    #bind to port to accept incoming connections from either clients or directory servers
    backlog = 5
    read_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    bound_to_port = 0
    while not bound_to_port:
        try:
            read_sock.bind(('', m_port))
            bound_to_port = 1
        except Exception, e:
            m_port += 1
            bound_to_port = 0

    read_sock.listen(backlog)

    connections = [read_sock, sys.stdin] 

    #spin up thread to register with server every 30 seconds
    rs = RegHandler(rg_server_ip, rg_server_port, m_ip, m_port) #change empty string to register server ip...
    rs.start()
    threads.append(rs)
    #connections.append(rs.sock)#might not need because independent threads...

    running = 1
    while running:
        inputready,outputready,exceptready = select.select(connections,[],[])

        for m_file in inputready:
            if m_file == read_sock:
                (c_fd, c_addr) = read_sock.accept()
                data = c_fd.recv(1024)
                msg = data.split()
                if(msg[0] == "find"):
                    auxDsSearch((c_fd, c_addr), data)
                elif(data == "servershare\r\n"):
                    allOnlineFiles((c_fd, c_addr), data)
                else:
                    client = Client((c_fd, c_addr), data)
                    client.start()
                    threads.append(client)
                    clients.append(client)
            elif m_file == sys.stdin:
                junk = sys.stdin.readline() 
                print "==========================\nSTOP TYPING ON THE KEYBOARD, I SEE YOU...\n==========================\n\n"
                #maybe do something cool in the future

    close_threads()
    read_sock.close()

if __name__ == "__main__":
    main()