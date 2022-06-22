import logging
import pprint
import socket
import base64
import sys
import os
import smtplib
import mimetypes
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage

from email.utils import COMMASPACE, formatdate

log = logging.getLogger()

class Email():
    """\nCreate an email object.

    """
    
    ########################################################################
    def __init__(self,mailhost=None,port=25,to=[],efrom=None,message=None,
                 subject=None):
    ########################################################################
        log.debug("Initializing email object")
        self.mailhost    = mailhost
        self.port        = port
        self.to          = to
        self.efrom       = efrom
        self.message     = message
        self.subject     = subject
        self.attachments = {}     ## filename => actual_file_location
        self.recipients  = None   ## The cooked version of self.to
    ########################################################################
    def attach_mime_file(self,file=None,filename=None):
    ########################################################################
        """Determine the attachment mine type and read the contents.

             Finally, attach to self.msg
        """
        log.debug("Determine the atttachment type")
        ctype, encoding = mimetypes.guess_type(file)
        log.debug("ctype = {}".format(ctype))
        try:
            if ctype is None or encoding is not None:
            # No guess could be made, or the file is encoded (compressed), so
            # use a generic bag-of-bits type.
                ctype = 'application/octet-stream'
            maintype, subtype = ctype.split('/', 1)
            if maintype == 'text':
                fp = open(file)
                # Note: we should handle calculating the charset
                attachment = MIMEText(fp.read(), _subtype=subtype)
                fp.close()
            elif maintype == 'image':
                fp = open(file, 'rb')
                attachment = MIMEImage(fp.read(), _subtype=subtype)
                fp.close()
            elif maintype == 'audio':
                fp = open(file, 'rb')
                attachment = MIMEAudio(fp.read(), _subtype=subtype)
                fp.close()
            else:
                fp = open(file, 'rb')
                attachment = MIMEBase(maintype, subtype)
                attachment.set_payload(fp.read())
                fp.close()
            # Encode the payload using Base64
                encoders.encode_base64(attachment)
        except FileNotFoundError as err:
            log.error(err)
            return False
        attachment.add_header("Content-Disposition", "attachment", filename=filename)
        self.msg.attach(attachment)
    ########################################################################
    def attach(self,file=None,filename=None):
    ########################################################################
        """\nAttach a file to the email body.

        """

        if not file : return
        if not filename:
            filename = os.path.basename(file)
        log.info("Attaching {} to email object as {}".format(file,filename))
        self.attachments[filename] = file
    ########################################################################
    def form_message(self):
    ########################################################################
        """\nForm the message.
        """
        if not self.to:
            raise NameError("No email recipients.")
        log.debug("Forming message")
        self.msg = MIMEMultipart()
        self.msg['Subject'] = self.subject
        self.msg['From'] = self.efrom
        if isinstance(self.to,list):
             self.recipients = self.to
             #self.msg['To'] = ",".join(self.to)
             self.msg['To'] = COMMASPACE.join(self.to)
        else:
             self.msg['To'] = self.to
             self.recipients = self.to.split(",")
        self.msg['Date'] = formatdate(localtime = True)
        #self.msg.preamble = "Preamble here"
        part1 = MIMEText(self.message, 'html')
        self.msg.attach(part1)

        for filename,file in self.attachments.items():
            log.debug("Attachment filename=>{} file=>{}".format(filename,file))
            self.attach_mime_file(file,filename)

        self.msg = self.msg.as_string()
    ########################################################################
    def send(self):
    ########################################################################
        """\nSend an email.
        """
        try:
            self.form_message()
        except AttributeError as err:
            log.error(err)
            return False
        except NameError as err:
            log.warn(err)
            return False
        attempts = 8
        smtp = ""
        #while not smtp or attempts != 0:
        while not smtp and attempts !=0:
            attempts = attempts - 1
            try:
                smtp = smtplib.SMTP()
                if log.isEnabledFor(logging.DEBUG):
                    smtp.set_debuglevel(1)
                msg="Connecting to smtp server -> {}"
                log.info(msg.format(self.mailhost))
                smtp.connect(self.mailhost)
            except (smtplib.SMTPConnectError) as err:
                log.error(err)
                continue
            except (ConnectionRefusedError) as err:
                log.error(err)
                return False
            except socket.gaierror as err:
                log.error(err)
                continue
        if smtp:
             log.info("Sending email to {}".format(self.recipients))
             try:
                 smtp.sendmail(self.efrom,self.recipients,self.msg)
                 smtp.quit()
             except (smtplib.SMTPSenderRefused) as err:
                log.error(err)
                return False
             log.info("Email sent to {}".format(self.to))
             return True
        else:
             log.error("All email attempts failed")
             return False
__version__ = 0.11
###########################################################################
def history():
###########################################################################
    """\n0.10 Initial
         0.11 Loop "attempts" for send() mail
         0.12 Removed opts utilization
    """
    pass

