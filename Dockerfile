# Dockerfile to build borgslave-sync image
# Prepare the base environment.
FROM ubuntu:18.04 as builder_base_borgslave
MAINTAINER asi@dbca.wa.gov.au
ENV DEBIAN_FRONTEND=noninteractive
LABEL org.opencontainers.image.source https://github.com/dbca-wa/borgslave-sync

#install required utilities
RUN apt-get update && apt-get install -y software-properties-common
RUN add-apt-repository ppa:ubuntugis/ppa \
    && apt-get update \
    && apt-get install -y --no-install-recommends wget git libmagic-dev gcc binutils vim python python-setuptools python-dev python-pip tzdata mercurial less \
    && pip install --upgrade pip

RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" >> /etc/apt/sources.list.d/pgdg.list' \
    && wget -q https://www.postgresql.org/media/keys/ACCC4CF8.asc -O - | apt-key add -  \ 
    && apt-get update  \
    && apt-get install  -y --no-install-recommends postgresql-client-9.6 \
    && apt-get install  -y --no-install-recommends openssh-client

FROM builder_base_borgslave as python_libs_borgslave

RUN groupadd  -g 1001 borg
RUN useradd -d /home/borg -m -s /bin/bash -u 1001 -g 1001 borg

WORKDIR /etc/mercurial
COPY mercurial/hgrc ./hgrc

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt 

# Install the project (ensure that frontend projects have been built prior to this step).
FROM python_libs_borgslave
COPY ./ ./
RUN rm build_image.sh

RUN chown -R borg:borg ./
RUN chmod -R 755 ./


USER borg
CMD ["python","slave_poll.py"]
