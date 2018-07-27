# -*- coding:utf-8 -*-

from __future__ import unicode_literals
import sys
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import os
import openpyxl
reload(sys)
sys.setdefaultencoding('utf-8')

class send_Mail(object):
    def __init__(self):
        self.pwd = os.getcwd()

    def sendMail(self, dict_mail):
        body = dict_mail["body"]
        from_name = dict_mail["from_name"]
        from_mail = dict_mail["from_mail"]
        from_mail_passwd = dict_mail["from_mail_passwd"]
        to_mail = dict_mail["to_mail"]
        cc_mail = dict_mail["cc_mail"]
        subject = dict_mail["subject"]
        smtp_server = dict_mail["smtp_server"]

        msg = MIMEMultipart()
        msg["Subject"] = subject
        msg["From"] = "%s<%s>" % (from_name, from_mail)
        msg["To"] = ','.join(to_mail)
        msg["Cc"] = ','.join(cc_mail)
        body = MIMEText(body, "html", "utf-8")
        msg.attach(body)
        part = MIMEApplication(open("%s/temp/temp.xlsx" % self.pwd, 'rb').read())
        part.add_header('Content-Disposition', 'attachment', filename="source_data.xlsx")
        msg.attach(part)

        try:
            s = smtplib.SMTP()
            s.connect(smtp_server, 25)
            try:
                s.login(from_mail, from_mail_passwd)
            except:
                pass
            s.sendmail(from_mail, to_mail+cc_mail, msg.as_string())
            s.quit()
            print "告警邮件已成功发送！"
        except smtplib.SMTPException as e:
            print "Error: %s" % e

    def run(self, dict_mail):
        """
        :param dict_mail: 是一个字典,存储有邮件的一些设置
        :return:
        """
        self.sendMail(dict_mail)

if __name__ == '__main__':
    dict_mail = {}
    dict_mail["body"] = "<div>test</div>"
    dict_mail["smtp_server"] = "server.test.com"
    dict_mail["from_mail"] = "sender@test.com"
    dict_mail["from_name"] = "sender"
    dict_mail["from_mail_passwd"] = "testpasswd"
    dict_mail["to_mail"] = ["receiver@test.com"]
    dict_mail["cc_mail"] = ["cc@test.com"]
    dict_mail["subject"] = "testsubject"
    send_Mail().run(dict_mail)