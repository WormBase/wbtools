import logging
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


logger = logging.getLogger(__name__)


def send_email_with_attachment(subject, content, recipients, server_host, server_port, email_user, email_passwd,
                               attachment, attachment_format: str = "text/csv",
                               attachment_filename: str = 'results.csv'):
    body = MIMEText(content, "html", _charset='utf-8')
    msg = MIMEMultipart()
    msg.attach(body)

    attachment_file = MIMEApplication(attachment, attachment_format)
    attachment_file.add_header('Content-Disposition', 'attachment', filename=attachment_filename)
    msg.attach(attachment_file)
    msg['Subject'] = subject
    msg['From'] = email_user
    msg['To'] = ", ".join(recipients)

    try:
        server_ssl = smtplib.SMTP_SSL(server_host, server_port)
        server_ssl.login(email_user, email_passwd)
        server_ssl.send_message(msg)
        logger.info("Email sent to: " + ", ".join(recipients))
        server_ssl.quit()
    except:
        logger.fatal("Can't connect to smtp server. Email not sent.")