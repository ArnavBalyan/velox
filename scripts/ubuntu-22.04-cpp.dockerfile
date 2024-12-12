# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
ARG base=ubuntu:22.04
FROM ${base}

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Install essential dependencies
RUN apt update && apt install -y \
      git \
      build-essential \
      cmake \
      ninja-build \
      libssl-dev \
      libboost-all-dev \
      npm \
      ant \
      flex \
      bison \
      libxss1 \
      libnss3 \
      libatk-bridge2.0-0 \
      libx11-xcb1 \
      libcups2 \
      libdbus-1-3 \
      libasound2 \
      libgbm1 \
      libgtk-3-0 \
      libbz2-dev \
      liblz4-dev \
      libdouble-conversion-dev \
      liblzo2-dev \
      libzstd-dev \
      libsnappy-dev \
      zlib1g-dev \
      wget && \
      rm -rf /var/lib/apt/lists/*

# Install the latest version of CMake (3.28.0)
RUN wget -q https://github.com/Kitware/CMake/releases/download/v3.28.0/cmake-3.28.0-linux-x86_64.sh && \
    chmod +x cmake-3.28.0-linux-x86_64.sh && \
    ./cmake-3.28.0-linux-x86_64.sh --skip-license --prefix=/usr/local && \
    rm cmake-3.28.0-linux-x86_64.sh

# Ensure the correct CMake version is used
ENV PATH="/usr/local/bin:$PATH"

# Install Thrift 0.16.0
RUN git clone --branch 0.16.0 https://github.com/apache/thrift.git /tmp/thrift-0.16.0 && \
    mkdir /tmp/thrift-0.16.0-build && \
    cd /tmp/thrift-0.16.0-build && \
    cmake ../thrift-0.16.0 \
      -DCMAKE_INSTALL_PREFIX=/deps-download/arrow/cpp/_build/thrift_ep-install \
      -DWITH_CPP=ON \
      -DWITH_PYTHON=OFF \
      -DWITH_JAVA=OFF \
      -DWITH_C_GLIB=OFF \
      -DWITH_JS=ON && \
    cmake --build . --parallel && \
    cmake --build . --target install && \
    rm -rf /tmp/thrift-0.16.0 /tmp/thrift-0.16.0-build

WORKDIR /velox
