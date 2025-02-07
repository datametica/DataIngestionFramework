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

FROM gcr.io/dataflow-templates-base/python312-template-launcher-base

# Ensure OpenJDK 17 is installed (if needed, adjust based on actual image contents)
RUN apt-get update && apt-get install -y openjdk-17-jdk

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

# Set the working directory
ARG WORKDIR=/dataflow/python/using_flex_template_adv3
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# Copy your application code and additional files
COPY . .
COPY jars/ /jars/

# Define environment variables
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/main.py"
ENV PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"

# Install required Python packages
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r $PYTHON_REQUIREMENTS_FILE \
    && pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache -r $PYTHON_REQUIREMENTS_FILE

# Install Apache Beam and its dependencies
RUN pip install --no-cache-dir apache-beam[gcp]==2.62.0


# Set the entrypoint for your application
ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
