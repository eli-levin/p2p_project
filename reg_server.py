#!/usr/bin/env python

###############################################################################
# 1. registration server (RS) accepts incoming connections
# 2. directory server (DS) connects to RS
# 3. RS creates a new entry in the hash map for the DS using the DS's hostname
#    as they key. The key is also pushed into an array
# 4. RS creates a new thread that waits for 30 seconds
# 
#  ds object should have hostname, port, and most recent time of registration
#  -- specific ds thread should (after 30 seconds) make a note of the time, go to ds object and check
#     last time registered. if the difference between the note of time and last registered is > 30 seconds remove it, else finish
###############################################################################

#############
import threading
import select
import signal
import socket
import time
import sys
##############

###################
online_ds_list = {}
clients = {}
threads = []
###################

###############################################################################
class DsData(object):
    def __init__(self):
        self.online = True
        self.clients_connected = 0      
###############################################################################
class DirServer(threading.Thread):
    def __init__(self, (newsockfd, addr), initial_msg):
        threading.Thread.__init__(self)
        signal.signal(signal.SIGINT, sigint_handler)
        self.newsockfd = newsockfd
        self.ds_ip = addr[0]
        self.ds_port = ""
        self.size = 1024
        self.running = 1
        self.parse_msg(initial_msg)
        print "started a dir server"

    def close_sock(self):
        self.running = 0
        self.newsockfd.close()

    def parse_msg(self, r_data):
        msg = r_data.split('\r')
        if msg[-1:][0] != "\n":
            self.newsockfd.send("400 Failure\r\n")
            return
        msg = msg[:-1][0]
        msg = msg.split(' ')
        l = len(msg)

        if (msg[0] == "ds_register") and (l == 3):
            #needs error checking
            ##-----following needs locking
            m_key = (msg[1], msg[2])
            self.ds_port = msg[2]
            online_ds_list[m_key] = DsData()
            ##----------------------------
            send_msg = "200 Success\r"
            for (ip, port) in online_ds_list.keys():
                send_msg += (ip + ' ' + port + '\r')
            send_msg += '\n'
            self.newsockfd.send(send_msg)
        else:
            self.newsockfd.send("400 Failure\r\n")

    def run(self):
        while self.running:
            try:
                data = self.newsockfd.recv(self.size)
            except Exception:
                self.newsockfd.close()
                print "socket closed, closing ds server thread..."
                return
            if data:
                self.parse_msg(data)
            else:
                m_tuple = (self.ds_ip, self.ds_port)
                if m_tuple in online_ds_list.keys():
                    self.newsockfd.close()
                    del online_ds_list[m_tuple]
                return

            time.sleep(15)
        print "closing thread..."
###############################################################################
class Client(threading.Thread):
    def __init__(self, (newsockfd, client_ip), initial_msg):
        threading.Thread.__init__(self)
        signal.signal(signal.SIGINT, sigint_handler)
        self.newsockfd = newsockfd
        self.client_ip = client_ip
        self.size = 1024
        self.running = 1
        self.parse_msg(initial_msg)
        self.registered = False
        self.loggedIn = False
        print "started a client thread"

    def close_sock(self):
        self.running = 0
        self.newsockfd.close()

    def parse_msg(self, r_data):
        msg = r_data.split('\r')
        if msg[-1:][0] != '\n':
            self.newsockfd.send("400 Failure\r\n")
            return
        msg = msg[:-1][0]
        msg = msg.split(' ')
        l = len(msg)

        if (msg[0] == "register") and (l == 4):
            ##-----following needs locking
            username = msg[1]
            pswd = msg[2]
            retype_pswd = msg[3]
            if username in clients.keys():
                send_msg = "401 Username already exists\r\n"
            elif pswd != retype_pswd:
                #passwords do not match
                send_msg = "402 Password does not match\r\n"
            else:
                clients[username] = pswd
                send_msg = "200 Success\r\n"
                self.registered = True
            self.newsockfd.send(send_msg)

        elif (msg[0] == "login") and (l == 3):
            username = msg[1]
            pswd = msg[2]
            if (username in clients.keys()) and (clients[username] == pswd):
                #return some DS <ip, port> using least used one in online list
                (ds_ip, ds_port) = findLeastBusyDS()
                send_msg = "200 Success\r"
                send_msg += ds_ip + ' ' + ds_port + '\r\n'
                self.loggedIn = True
            else:
                send_msg = "403 Invalid Username/Password\r\n"
            self.newsockfd.send(send_msg)

        elif (r_data == "exit\r\n") or (msg[0] == 'exit'):
            self.newsockfd.send("200 Success\r\n")
            self.newsockfd.close()
            self.runnning = 0

        else:
            self.newsockfd.send("405 Invalid arguments\r\n")

    def run(self):
        self.newsockfd.setblocking(True)
        while self.running:
            try:
                data = self.newsockfd.recv(self.size)
                if not data:
                    print 'closing this client, error getting message from sock'
                    return
                #print str(data) + "<----------|data|"
            except Exception:
                self.newsockfd.close()
                print "socket closed, closing thread..."
                return
            if data:
                self.parse_msg(data)         
###############################################################################
def findLeastBusyDS():
    #returns tuple of least busy directory server
    min_online = sys.maxint
    best_server = None
    for tup in online_ds_list.keys():
        ds = online_ds_list[tup]
        if ds.clients_connected < min_online:
            min_online = ds.clients_connected
            best_server = tup
    online_ds_list[best_server].clients_connected += 1
    return best_server

def close_threads():
    for t in threads:
        t.close_sock() 
        t.join()
    sys.exit(0)

def sigint_handler(signal, frame):
    print('Registration Server shutting down...')
    main_runnning = 0
    close_threads()
    sys.exit(0)

def main():
    signal.signal(signal.SIGINT, sigint_handler)
    #############################
    ##this should be in .cfg file
    read_ip = ''
    read_port = 60000 #not this maybe tho...
    #############################

    if len(sys.argv) != 1:
        sys.exit(1)

    backlog = 5
    size = 1024
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((read_ip, read_port))
    s.listen(backlog)

    m_input = [s, sys.stdin] 
    main_runnning = 1

    while main_runnning:
        inputready,outputready,exceptready = select.select(m_input,[],[])

        for m_file in inputready:
            if m_file == s:
                c_fd, c_ip = s.accept()
                m_data = c_fd.recv(size)
                line = m_data.split('\r\n')[0]
                command = line.split(' ')[0]

                if command == "ds_register":
                    d = DirServer((c_fd, c_ip), m_data)
                    d.start()
                    threads.append(d)
                else:
                    c = Client((c_fd, c_ip), m_data)
                    c.start()
                    threads.append(c)

            elif m_file == sys.stdin:
                junk = sys.stdin.readline() 
                print "==========================\nSTOP TYPING ON THE KEYBOARD, I SEE YOU...\n==========================\n\n"
                #maybe do something cool in the future

    close_threads()
    s.close()

if __name__ == "__main__":
	main()