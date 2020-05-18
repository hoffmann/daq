FROM debian:10

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/ \
    USER_ID=1337 \
    USER_NAME=docker

RUN echo "root:docker" | chpasswd

# create docker user
RUN useradd \
    --create-home \
    --groups sudo \
    --home-dir /home/$USER_NAME \
    --shell /bin/bash \
    --uid $USER_ID \
    $USER_NAME \
    && echo "$USER_NAME:$USER_NAME" | chpasswd \
    && mkdir -p /app \
    && chown -R $USER_NAME:$USER_NAME /app \
    && echo "PS1='🐳  \[\033[1;36m\]\h \[\033[1;34m\]\w\[\033[0;35m\] \[\033[1;36m\]# \[\033[0m\]'" >>/home/$USER_NAME/.bashrc \
    && echo "export LANG=C.UTF-8" >>/home/$USER_NAME/.bashrc \
    && echo "export LC_ALL=C.UTF-8" >>/home/$USER_NAME/.bashrc

# install more packages
RUN apt-get update \
    && apt-get install -y unixodbc unixodbc-dev git python3.7 python3.7-dev virtualenv make build-essential libssl-dev zlib1g-dev \
    && apt-get clean all

# variable expansion in --chown does not work!
COPY --chown=docker:docker requirements.txt /tmp/requirements.txt

USER $USER_ID

# Install Python dependencies
RUN set -x \
    && virtualenv -p python3.7 /home/$USER_NAME/venv \
    && /home/$USER_NAME/venv/bin/pip install wheel \
    && /home/$USER_NAME/venv/bin/pip install -r /tmp/requirements.txt  \
    && find /usr/local -type f -name '*.pyc' -name '*.pyo' -delete \
    && rm -rf ~/.cache/ \
    && echo "source ~/venv/bin/activate" >>/home/$USER_NAME/.bashrc \
    && echo 'export PYTHONPATH="${PYTHONPATH}:/app/src"' >>/home/$USER_NAME/.bashrc


# Set our work directory to our app directory
WORKDIR /app/
