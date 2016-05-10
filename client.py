#!/usr/bin/env python
from __future__ import print_function
import threading
import select
import signal
import socket
import time
import sys
import os

#######global data#######
SHARED_DIR = ""
online_ds_list = []
threads = []
connections = []
filesToClientsMap = {}
rs = None
ds = None
##this should be in .cfg file
m_ip = '127.0.0.1' #socket.gethostname() #use this or read_ip?? ...wtf is this?
m_port = 40000

#-----------------
def dline(text):
    print('+++++++++++||---------------'+ text +'-------------||++++++++++++')
    sys.stdout.flush()
#-----------------
#########################
def close_threads():
    for t in threads:
        t.running = 0
        t.sock.close()
        t.thread.exit()
        t.join()
    sys.exit(0)

def sigint_handler(signal, frame):
    print('Client shutting down...')
    close_threads()
    sys.exit(0)
###############################################################################
class DownloadServer(threading.Thread):
    def __init__(self, (sockfd, addr)):
        threading.Thread.__init__(self)
        self.name = "DownloadServer"
        self.h_ip = addr[0]
        self.h_port = addr[1]
        self.size = 1024
        self.running = 1
        self.sockfd = sockfd

    def sendPartition(self, fname, n, seq):
        path = SHARED_DIR+fname
        n = float(n)
        seq = int(seq)
        fsize = os.path.getsize(SHARED_DIR+fname)
        psize = int(fsize/n)
        fstart = seq * psize
        if (seq+1) == n:
            #get remaining part of file
            with open(path, 'r+') as fd:
                fd.seek(0)
                fd.seek(fstart)
                bytes = fd.read()
                self.sockfd.send(bytes)#change to byte array??
                self.sockfd.close()
                print('SENT PART '+str(seq)+" OF "+fname)
                print('>>>')
                return
        else:
            with open(path, 'r+') as fd:
                fd.seek(0)
                fd.seek(fstart)
                bytes = fd.read(psize)
                self.sockfd.send(bytes)#change to byte array??
                self.sockfd.close()
                print('SENT PART '+str(seq)+" OF "+fname)
                print('>>>')
                return

    def run(self):
        data = self.sockfd.recv(self.size)
        if data[-2:] != '\r\n':
            self.sockfd.close()
            return
        msg = data[:-2].split()
        if (msg[0] == "downloadPartition") and (len(msg) == 4):
            fname = msg[1]
            n = msg[2]
            seq = msg[3]
            self.sendPartition(fname, n, seq)
            return
        else:
            self.sockfd.close()
            return


###############################################################################
class DownloadHost(threading.Thread):
    def __init__(self, fname, n, seq, (ip, port)):
        threading.Thread.__init__(self)
        self.name = "DownloadHostConnection"
        self.h_ip = ip
        self.h_port = int(port)
        self.fname = fname
        self.num_hosts = n
        self.seq = seq
        self.size = 1024
        self.running = 1
        self.connected = False
        self.h_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def run(self):
        msg = "downloadPartition "+str(self.fname)+" "+str(self.num_hosts)+" "+str(self.seq)
        msg += '\r\n'
        self.h_sock.connect((self.h_ip, self.h_port))
        self.h_sock.send(msg)
        bytes = self.h_sock.recv(self.size)
        [f_id, f_ext] = self.fname.split('.')
        m_fname = (f_id +"_FFD."+ f_ext +"_part"+str(self.seq)) #change file1.txt to file1_FFD.txt_part0
        path = SHARED_DIR+m_fname
        with open(path, "a") as fd:
            fd.write(bytes)
        self.h_sock.close()
        return

###############################################################################
class Client(threading.Thread):
    def __init__(self, ip, port):
        threading.Thread.__init__(self)
        self.name = "RegistrationServerConnection"
        self.rs_ip = ""#change this...
        self.rs_port = 60000
        self.ip = ip
        self.port = port
        self.ds_ip = None
        self.ds_port = None
        self.size = 1024
        self.running = 1
        self.connected_to_ds = 0
        self.connected_to_rs = 0
        self.loggedIn = 0
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.read_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.ds_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def close_connection(self):
        self.sock.close()
        self.read_sock.close()
        self.ds_sock.close()
        sys.exit(0)

    def perform_login(self, m_msg):
        self.sock.send(m_msg)
        data = self.sock.recv(self.size)
        if not data:
            print("login fail : didnt get a message from registration server :(")
            return
        if data[-2:] != '\r\n':
            print('bad data from reg server...')
            return
        r_data = data[:-1]#stip LF
        lines = r_data.split('\r')
        lines = lines[:-1]#strip trailing empty string
        status_line = lines[0].split(' ')
        stat_code = status_line[0]
        stat_msg  = status_line[1]
        if int(stat_code) == 200 and stat_msg == "Success":
            ds_info_line = lines[1].split(' ')
            self.ds_ip = ds_info_line[0]
            self.ds_port = int(ds_info_line[1])
            #TODO connect to ds
        print('Server Response: ' + lines[0])
        self.loggedIn = 1
        ###########
        #hack for project purposes to tell directory server which port client is listening to
        self.ds_sock.connect((self.ds_ip, self.ds_port))
        self.connected_to_ds = 1
        self.ds_sock.send("portInfo "+str(self.port)+'\r\n')
        data = self.ds_sock.recv(self.size)
        if not data:
            print("port hack fail : didnt get a message from directory server :(")
            return
        if data[-2:] != '\r\n':
            print('bad data from dir server...')
            return
        if data == "200 Success\r\n":
            print('OK')
        else:
            print('NACK')
        return

    def exit_reg_server(self):
        msg = 'exit\r\n'
        self.sock.send(msg)
        data = self.sock.recv(self.size)
        if not data:
            print("exit fail : didnt get a message from registration server :(")
            return
        if data[-2:] != '\r\n':
            print('bad data from reg server...')
            return
        r_data = data[:-1]#stip LF
        lines = r_data.split('\r')
        lines = lines[:-1]#strip trailing empty string
        status_line = lines[0].split(' ')
        stat_code = status_line[0]
        stat_msg  = status_line[1]
        print('Server Response: ' + lines[0])
        if int(stat_code) == 200 and stat_msg == "Success":
            #disconnect from ds
            self.running = 0
            self.sock.close()
            if self.connected_to_ds:
                self.ds_sock.close()
            return

    def register(self, msg):
        #connect is to bind to a remote socket, bind is for local socket (server will use this)
        if not self.connected_to_rs:
            self.sock.connect(('', self.rs_port))
            self.connected_to_rs = 1
        self.sock.send(msg)
        data = self.sock.recv(self.size)
        if not data:
            print("register fail : didnt get a message from registration server :(")
            return
        if data[-2:] != '\r\n':
            print('bad data from reg server...')
            return
        r_data = data[:-1]#stip LF
        lines = r_data.split('\r')
        lines = lines[:-1]#strip trailing empty string
        status_line = lines[0].split(' ')
        stat_code = status_line[0]
        stat_msg  = status_line[1]
        print('Server Response: ' + lines[0])
        if int(stat_code) == 200 and stat_msg == "Success":
            #disconnect from ds
            return

    def share_file(self, filenames):
        #can't do this if not logged in
        if not self.loggedIn:
            print("Please log in first.")
            return
        #first check if file exists in SharedDir
        command = 'share '
        for f in filenames:
            if not os.path.isfile(SHARED_DIR + f):
                print(f + ' does not exist in the directory:\n' + SHARED_DIR)
                return
            else:
                command += (f + ' ')
        command = command[:-1]#strip extra space
        #connect to ds server if not already connected, then share file
        if not self.connected_to_ds:
            self.ds_sock.connect((self.ds_ip, self.ds_port))#change this to connect to ds_ip!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            self.connected_to_ds = 1
        command += '\r\n'
        self.ds_sock.send(command)
        data = self.ds_sock.recv(self.size)
        if not data:
            print("exit fail : didnt get a message from directory server :(")
            return
        if data[-2:] != '\r\n':
            print('bad data from reg server...')
            return
        data = data[:-1]#stip LF
        lines = data.split('\r')
        lines = lines[:-1]#strip trailing empty string
        status_line = lines[0].split(' ')
        stat_code = status_line[0]
        stat_msg  = status_line[1]
        print('Server Response: ' + lines[0])
        if int(stat_code) == 200 and stat_msg == "Success":
            for i in range(1, len(lines)):
                fname = lines[i]
                #add to global shared files list???
                if fname:
                    print('|-- ' + fname)

    def remove_file(self, msg):
        #inform the user to register or log in first, if needed
        if not self.connected_to_ds:
            print('Please log in to a directory server before removing.')
            return
        self.ds_sock.send(msg)
        data = self.ds_sock.recv(self.size)
        if not data:
            print("remove fail : didnt get a message from registration server :(")
            return
        if data[-2:] != '\r\n':
            print('bad data from reg server...')
            return
        r_data = data[:-1]#stip LF
        lines = r_data.split('\r')
        lines = lines[:-1]#strip trailing empty string
        status_line = lines[0].split(' ')
        stat_code = status_line[0]
        stat_msg  = status_line[1]
        if int(stat_code) == 200 and stat_msg == "Success":
            return
        print('Server Response: ' + lines[0])

    def find_file(self, msg):
        #can't do this if not logged in
        if not self.loggedIn:
            print("Please log in first.")
            return
        #inform the user to register or log in first, if needed
        if not self.connected_to_ds:
            self.ds_sock.connect((self.ds_ip, self.ds_port))#change this to connect to ds_ip!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            self.connected_to_ds = 1

        self.ds_sock.send(msg)
        data = None
        self.ds_sock.setblocking(True)
        while not data:
            data = self.ds_sock.recv(self.size)
        if not data:
            print("find file fail : didnt get a message from registration server :(")
            return
        if data[-2:] != '\r\n':
            print('bad data from reg server...')
            return
        data = data[:-1]#stip LF
        lines = data.split('\r')
        lines = lines[:-1]#strip trailing empty string
        status_line = lines[0].split(' ')
        stat_code = status_line[0]
        stat_msg  = status_line[1]
        print('Server Response: ' + lines[0])
        if int(stat_code) == 200 and stat_msg == "Success":
            for line in lines[1:]:
                print(line)
                #store dat ish
                line = line.split()
                ds_addr = (line[0], line[1])
                fname = line[2]
                if fname in filesToClientsMap.keys():
                    filesToClientsMap[fname].append(ds_addr)
                else:
                    filesToClientsMap[fname] = [ds_addr]

    def servershare(self):
        #can't do this if not logged in
        if not self.loggedIn:
            print("Please log in first.")
            return
        #inform the user to register or log in first, if needed
        if not self.connected_to_ds:
            self.ds_sock.connect((self.ds_ip, self.ds_port))#change this to connect to ds_ip!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            self.connected_to_ds = 1

        self.ds_sock.send("servershare\r\n")
        data = self.ds_sock.recv(self.size)
        if not data:
            print("remove fail : didnt get a message from registration server :(")
            return
        if data[-2:] != '\r\n':
            print('bad data from reg server...')
            return
        data = data[:-1]#stip LF
        lines = data.split('\r')
        lines = lines[:-1]#strip trailing empty string
        status_line = lines[0].split(' ')
        stat_code = status_line[0]
        stat_msg  = status_line[1]
        print('Server Response: ' + lines[0])
        if int(stat_code) == 200 and stat_msg == "Success":
            for line in lines[1:]:
                print('|-- ' + line)

    def fastFileDownload(self, text):
        msg = text[:-2].split()
        fname = msg[1]
        #see how many threads to create with number of hosting clients
        available_clients = filesToClientsMap[fname]
        num_hosts = len(available_clients)
        # num_hosts-1 clients will need to provide file_size/num_hosts bytes of data
        # 1 clients will need to provide file_size - file_size/(num_hosts-1) bytes of data
        #
        #create num_hosts-1 threads for the first size
        hosts = []
        for i in range(num_hosts):
            h = DownloadHost(fname, num_hosts, i, available_clients[i])
            h.start()
            hosts.append(h)
        #now reassemble file
        path = SHARED_DIR+fname
        [f_id, f_ext] = fname.split('.')
        with open(path, "a") as m_file:
            for i in range(num_hosts):
                current_file = SHARED_DIR + f_id +"_FFD."+ f_ext +"_part"+str(i)
                with open(current_file, "r+") as fd:
                    buf = fd.read()
                    m_file.write(buf)
        print('OK')

    def parse_input(self, text):
        text = text.strip()
        data = text.split(' ')
        if (data[0] == 'register') and (len(data) == 4):
            self.register(text+'\r\n')
            return (text+'\r\n')
        elif (data[0] == 'login') and (len(data) == 3):
            self.perform_login(text+'\r\n')
            return (text+'\r\n')
        elif (data[0] == 'exit') and (len(data) == 1):
            self.exit_reg_server()
            return ('exit\r\n')
        elif (data[0] == 'share') and (len(data) > 1):
            filenames = text.split(' ')[1:]
            self.share_file(filenames)
            return (text+'\r\n')
        elif (data[0] == 'remove') and (len(data) == 2):
            self.remove_file(text+'\r\n')
            return(text+'\r\n')
        elif (data[0] == 'find') and (len(data) == 2):
            self.find_file(text+'\r\n')
            return(text+'\r\n')
        elif (data[0] == 'servershare') and (len(data) == 1):
            self.servershare()
            return ('servershare\r\n')
        elif (data[0] == 'FastFileDownload') and (len(data) == 2):
            self.fastFileDownload(text+'\r\n')
            return (text+'\r\n')
        else:
            print('bad input try again...')
            return ""

    def run(self):
        #bind to port to accept incoming connections from either clients or directory servers
        backlog = 5

        bound_to_port = 0
        while not bound_to_port:
            try:
                self.read_sock.bind(('', self.port))
                bound_to_port = 1
            except Exception, e:
                self.port += 1
                bound_to_port = 0

        self.read_sock.listen(backlog)

        connections = [self.read_sock, sys.stdin]

        while self.running:
            print('>>> ', end="")
            sys.stdout.flush()
            inputready,outputready,exceptready = select.select(connections,[],[])

            for m_file in inputready:
                if m_file == self.read_sock:
                    d = DownloadServer(self.read_sock.accept())
                    d.start()
                    threads.append(d)

                elif m_file == sys.stdin:
                    data = sys.stdin.readline() 
                    msg = self.parse_input(data[:-1])

        self.close_connection()
###############################################################################
if __name__ == "__main__":
    signal.signal(signal.SIGINT, sigint_handler)
    with open('client.cfg', 'r') as fd:
        lines = fd.readlines()
        SHARED_DIR = lines[0][:-1] #CHANGE THIS TO BE SMARTER
    if len(sys.argv) != 1:
        sys.exit(1)
    c = Client(m_ip, m_port)
    c.start()
    threads.append(c)