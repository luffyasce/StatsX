from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
import smtplib
from email.mime.text import MIMEText
import pathlib


def send_mail(title_: str, content_: str, content_type: str, receiver: list, files: list = None):
    mail_host = 'smtp.163.com'
    username = "muzexlxl@163.com"
    passwd = "LSVVJIJNQXVGTYJC"
    msg = MIMEMultipart()
    # 构建正文
    part_text = MIMEText(content_, content_type)
    msg.attach(part_text)  # 把正文加到邮件体里面去

    # 构建邮件附件
    if files is not None:
        for fp in files:
            part_attach = MIMEApplication(open(fp, 'rb').read())  # 打开附件
            part_attach.add_header('Content-Disposition', 'attachment', filename=pathlib.Path(fp).name)  # 为附件命名
            msg.attach(part_attach)  # 添加附件

    msg['Subject'] = title_  # 邮件主题
    msg['From'] = username  # 发送者账号
    msg['To'] = ", ".join(receiver)  # 接收者账号列表
    smtp = smtplib.SMTP(host=mail_host, port=0)
    smtp.login(username, passwd)  # 登录
    smtp.sendmail(from_addr=username, to_addrs=receiver, msg=msg.as_string())
    smtp.quit()
