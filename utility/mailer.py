# Copyright 2023 Datametica Solutions Pvt. Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""This module contains the function to send mail.
It uses SendGrid API to send mail.
"""
import logging

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


def send_mail(report_json):
    """
    This method sends mail using SendGrid API.
    Args:
        report_json: JSON containing the report.
    Returns:
        None
    """
    message = Mail(
        from_email="sender_mail",
        to_emails="receivers_mail",
        subject="Audit Reports",
        html_content=f"<strong>{str(report_json)}</strong>",
    )
    try:
        sg = SendGridAPIClient(
            "Add SendGrid Key"
        )
        sg.send(message)
    except Exception as e:
        logging.exception(e)
