# coding: utf-8

import socket
import threading
import time
import copy
import os

# global variable


class User:
    def __init__(self, skt, username='none'):
        self.skt = skt
        self.username = username

    def send_msg(self, msg):
        self.skt.send(msg)

    def logout(self):
        self.skt.close()


class Server():
    def __init__(self):
        self.userlist = []

    def hand_user_con(self, usr):
        try:
            isNormar = True
            while isNormar:
                data = usr.skt.recv(1024)
                time.sleep(1)
                msg = data.split('|')  # 分析消息
                if msg[0] == 'login':
                    print 'user [%s] login' % msg[1]
                    usr.username = msg[1]
                    self.notice_other_usr(usr, msg[1])
                if msg[0] == 'talk':
                    print 'user[%s]to[%s]:%s' % (usr.username, msg[1], msg[2])
                    self.send_msg(msg[1], msg[2], usr.username)
                if msg[0] == 'exit':
                    print 'user [%s] exit' % msg[0]
                    isNormar = False
                    self.notice_other_usr(usr, msg[1], do="exit")
        except EOFError:
            isNormar = False


    # 通知其他用户以上的好友
    def notice_other_usr(self, usr, username, do='login'):
        if do == 'login':
            self.userlist.append(usr)
            file = open("status_record/%s" % username, 'w')
            file.close()
            for usr in self.userlist:
                if usr.username == username:
                    usr.skt.send(("login success"))
                else:
                    pass
                    # usr.skt.send(("login|%s" % usr.username))
        elif do == 'exit':
            for usr in self.userlist:
                if usr.username == username:
                    os.remove("status_record/%s" % username)
                    self.userlist.remove(usr)
                usr.skt.send(("%s is logout" % username))
                usr.logout()

    def send_msg(self, username, msg, from_):
        for usr in self.userlist:
            if usr.username == username:
                usr.skt.send("[%s]%s" % (from_, msg))


    # 程序入口
    def main(self):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.bind(('0.0.0.0', 9999))
        self.s.listen(5)
        print u'server start is SUCCESS...'
        while True:
            sock, addr = self.s.accept()  # 等待用户连接
            user = User(sock)
            t = threading.Thread(target=self.hand_user_con, args=(user,))
            t.start()
        self.s.close()


if (__name__ == "__main__"):
    Server().main()