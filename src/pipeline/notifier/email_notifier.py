import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime


class EmailNotifier:
    def __init__(self, smtp_server, smtp_port, sender_email, sender_password):
        self.smtp_server = smtp_server  
        self.smtp_port = smtp_port    
        self.sender_email = sender_email  
        self.sender_password = sender_password 

    def notify(self, recipient_email) -> None:
        """
        Send an email notification to the recipient when the ETL process fails.

        :param recipient_email: The recipient's email address
        :param file_name: The name of the file that caused the ETL failure
        """
        date_now = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        subject="ALERT: ETL Process Failed"
        
        body=f"An error occurred during the ETL process at {date_now}, \n Please check the logs for more details."
        

        # Create the email message
        msg = MIMEMultipart()
        msg['From'] = self.sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject

        # Attach the body with the msg instance
        msg.attach(MIMEText(body, 'plain'))


        with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
            # Use TLS for security
            server.starttls()
            # Log in to the email account
            server.login(self.sender_email, self.sender_password)
            # Send the email
            server.sendmail(self.sender_email, recipient_email, msg.as_string())
