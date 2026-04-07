FROM europe-north1-docker.pkg.dev/cgr-nav/pull-through/nav.no/python:3.14.0-dev AS dev
USER root
RUN apk add --update build-base
WORKDIR /pyfactory
RUN addgroup -g 1069 -S pyfactory && \
    adduser -S -D -H -h /pyfactory -u 1069 -G pyfactory -s /bin/sh pyfactory && \
    chown -R 1069:1069 /pyfactory/
COPY requirements.txt .
ENV PIP_TARGET=/pyfactory/lib/python3.14/site-packages
ENV PYTHONPATH=$PIP_TARGET
RUN pip3 install --no-cache-dir -r requirements.txt


FROM europe-north1-docker.pkg.dev/cgr-nav/pull-through/nav.no/python:3.14
COPY pxpyfactory pxpyfactory
COPY run.py .
COPY --from=dev /pyfactory/lib/python3.14/site-packages/ /pyfactory/lib/python3.14/site-packages/
USER 1069
ENV PYTHONPATH=/pyfactory/lib/python3.14/site-packages

ENTRYPOINT ["python", "run.py", "clean", "build=all"]
