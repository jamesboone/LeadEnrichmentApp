import logging
import ipdb
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)


def send(subject, toaddr=None, message=None, filename=None, error=None):
    logger.debug('sending email')
    if not message:
        message = ''

    if error:
        fp = open('./log/latest.log', 'rb')
        error_msg = MIMEText(fp.read())
        fp.close()

    try:
        fromaddr = "james@gmail.com"
        if not toaddr:
            toaddr = ["james@ggg.com"]

        if 'james@ggg.com' not in toaddr:
            toaddr.append('james@ggg.com')

        msg = MIMEMultipart()

        msg['From'] = fromaddr
        msg['To'] = ', '.join(toaddr)
        msg['Subject'] = subject
        msg.attach(MIMEText(message, 'html'))
        if error:
            msg.attach(error_msg)

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(fromaddr, "xxx")
        server.sendmail(fromaddr, toaddr, msg.as_string())
        server.quit()
    except:
        logger.exception('failed trying to send email')
