FROM python:3.9-slim-bullseye
  
COPY entrypoint.sh /
RUN chmod +x entrypoint.sh

RUN mkdir -p /srv/mwmbl/crawler-script

RUN useradd mwmbl -r -d /srv/mwmbl && \
  chown mwmbl:mwmbl -R /srv/mwmbl

USER mwmbl
WORKDIR /srv/mwmbl/crawler-script

COPY justext justext
COPY LICENSE README.md pyproject.toml poetry.lock main.py .

RUN python -m venv venv && \
  . venv/bin/activate && \
  pip install . && \
  pip cache purge

ENTRYPOINT /entrypoint.sh
