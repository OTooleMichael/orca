FROM python:3.11.4-slim
RUN apt-get update && apt-get install -y curl git make build-essential cmake

WORKDIR /src
ENV PYTHONPATH=/src
ENV CMAKE_CXX_COMPILER=g++

COPY task_servers/requirements.dev.txt ./
RUN pip install --no-cache-dir -r requirements.dev.txt

COPY task_servers/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY orca_tools /orca_tools
RUN pip install --no-cache-dir -e /orca_tools
COPY . .

CMD ["python", "task_servers/task_server_letters/__init__.py"]
