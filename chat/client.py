# coding: utf-8

import sys
import socket
import threading
import os
import time
from server import Server

# global variable
isNormar = True
other_usr = ''


def recieve_msg(username, s):
    global isNormar, other_usr
    s.send('login|%s' % username)
    data = s.recv(1024)
    msg = data.split('|')
    print "recv message: %s" % msg[0]
    isNormar = True
    while (isNormar):
        data = s.recv(1024)  # 阻塞线程，接受消息
        msg = data.split('|')
        if msg[0] == 'login':
            print u'%s user has already logged in, start to chat' % msg[1]
        else:
            print "recv message: %s" % msg[0]
        isNormar = True

# 程序入口
def main():
    server = Server()
    global isNormar, other_usr
    try:
        print 'Please input your name:'
        usrname = raw_input()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", 9999))
        t = threading.Thread(target=recieve_msg, args=(usrname, s))
        t.start()
    except:
        print 'connection exception'
        isNormar = False
    finally:
        isNormar = False
    while True:
        if isNormar:
            msg = raw_input("input message: ")  # 接受用户输入
            if msg == "exit":
                break
            elif msg == "get user":
                userlist = os.listdir("status_record")
                print userlist
                if userlist == [] or userlist == None:
                    print "NoBody is online except you!"
                    continue
                other_usr = raw_input("input username who is you wanted to talk: ")
                while True:
                    if other_usr not in userlist:
                        other_usr = raw_input("user not loggin Please choose again: ")
                    else:
                        break
            else:
                if (other_usr != ''):
                    s.send("talk|%s|%s" % (other_usr, msg))  # 编码消息并发送
    s.send("exit|%s" % usrname)
    s.close()


if __name__ == "__main__":
    main()
