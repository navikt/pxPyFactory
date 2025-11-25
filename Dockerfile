FROM europe-north1-docker.pkg.dev/cgr-nav/pull-through/nav.no/python:3.14.0-dev as dev
USER root
RUN apk add --update shadow
WORKDIR /pyfactory
RUN useradd -m -d /pyfactory/ -u 1069 -s /bin/bash pyfactory && \
    chown -R pyfactory:pyfactory /pyfactory/
COPY requirements.txt .
ENV PIP_TARGET=/pyfactory/.local/lib/python3.14/site-packages
ENV PYTHONPATH=$PIP_TARGET
RUN pip3 install --no-cache-dir -r requirements.txt


FROM europe-north1-docker.pkg.dev/cgr-nav/pull-through/nav.no/python:3.14
COPY pxpyfactory .
COPY run.py .
COPY --from=dev /pyfactory/.local/lib/python3.14/site-packages/ /pyfactory/.local/lib/python3.14/site-packages/
COPY --from=dev /etc/passwd /etc/passwd
USER pyfactory

ENTRYPOINT ["python", "run.py"]