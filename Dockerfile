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


FROM python:3.7

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
ENV PYTHONPATH "${PYTHONPATH}:/usr/local/lib/python3.7/site-packages"

COPY requirements.txt /usr/local/bin/dataflow-worker/requirements.txt
COPY jars/ /app/jars/

RUN pip install --no-cache-dir apache-beam[gcp]==2.43.0
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r /usr/local/bin/dataflow-worker/requirements.txt
RUN pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache -r /usr/local/bin/dataflow-worker/requirements.txt

COPY --from=apache/beam_python3.7_sdk:2.43.0 /opt/apache/beam /opt/apache/beam

WORKDIR /usr/local/bin/dataflow-worker

ENTRYPOINT ["/opt/apache/beam/boot"]