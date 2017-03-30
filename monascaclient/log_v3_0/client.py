# Copyright 2017 StackHPC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import string

from monascaclient.common import http
from monascaclient.log_v3_0 import logs


class Client(object):

    """Client for the Monasca Logs v3_0 API.

    :param string endpoint: A user-supplied endpoint URL for the monasca log api
                            service.
    :param string token: Token for authentication.
    :param integer timeout: Allows customization of the timeout for client
                            http requests. (optional)
    """

    def __init__(self, *args, **kwargs):
        """Initialize a new http client for the monasca log API."""
        if 'auth_url' in kwargs and 'v2.0' in kwargs['auth_url']:
            kwargs['auth_url'] = string.replace(
                kwargs['auth_url'], 'v2.0', 'v3')
        self.http_client = http.HTTPClient(*args, **kwargs)
        self.logs = logs.LogsManager(self.http_client)

    def replace_token(self, token):
        self.http_client.replace_token(token)
