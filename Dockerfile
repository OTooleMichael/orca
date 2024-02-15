
FROM python:3.11.4-slim
RUN apt update && apt install -y curl git make build-essential cmake

ENV CMAKE_CXX_COMPILER=g++
WORKDIR /src

COPY requirements.dev.txt ./
RUN pip install --no-cache-dir -r requirements.dev.txt

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt 
ENV PYTHONPATH=/src

COPY orca_tools /orca_tools
RUN pip install --no-cache-dir -e /orca_tools

CMD ["bash", "./scripts/start_server.sh"]
