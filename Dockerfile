FROM europe-north1-docker.pkg.dev/cgr-nav/pull-through/nav.no/python:3.14.0-dev as dev
USER root
RUN apk add --update shadow gcc musl-dev python3-dev
WORKDIR /pyfactory
RUN useradd -m -d /pyfactory/ -u 1069 -s /bin/bash pyfactory && \
    chown -R pyfactory:pyfactory /pyfactory/
COPY requirements.txt .
ENV PIP_TARGET=/pyfactory/lib/python3.14/site-packages
ENV PYTHONPATH=$PIP_TARGET
RUN pip3 install --no-cache-dir -r requirements.txt


FROM europe-north1-docker.pkg.dev/cgr-nav/pull-through/nav.no/python:3.14
COPY pxpyfactory pxpyfactory
COPY run.py .
COPY --from=dev /pyfactory/lib/python3.14/site-packages/ /pyfactory/lib/python3.14/site-packages/
COPY --from=dev /etc/passwd /etc/passwd
USER pyfactory
ENV PYTHONPATH=/pyfactory/lib/python3.14/site-packages

ENTRYPOINT ["python", "run.py"]
