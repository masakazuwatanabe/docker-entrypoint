# docker-entrypoint (entrypoint.py)

easy and simple docker entrypoint of Python.
pid1 init script of single python file.


## requiemnt

Pyton 2.7 or letter.

if you want to use is yaml.

PyYAML


## using example

entrypoint.sh

    #!/bin/sh
    set -e
    if [ $# -ne 0 ]; then
        exec "$@"
    fi
    exec /entrypoint.py


entrypoint.yml

    service:
      rsyslog:
        type: service
        pid: /var/run/syslogd.pid
        start: "service rsyslog restart"
        stop: "service rsyslog stop"
      cron:
        type: background
        pid: /var/run/crond.pid
        start: "/usr/sbin/crond"
    stdout:
      - /var/log/cron
      - /var/log/messages
    start:
      - rsyslog
      - cron
    stop:
      - cron
      - rsyslog


Dockerfile

    FROM centos:6
    RUN yum -y install PyTAML rsyslog cronie

    COPY entrypoint.sh /
    COPY entrypoint.py /
    COPY entrypoint.yml /
    RUN chmod +x /entrypoint.sh /entrypoint.py

    ENTRYPOINT ["/entrypoint.sh"]
    CMD []
