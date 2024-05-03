# Dockerfile to build borgslave-sync image
# Prepare the base environment.
FROM ubuntu:22.04 as builder_base_borgslave
MAINTAINER asi@dbca.wa.gov.au
ENV DEBIAN_FRONTEND=noninteractive
LABEL org.opencontainers.image.source https://github.com/dbca-wa/borgslave-sync

#install required utilities
RUN apt-get update && apt-get install -y software-properties-common
RUN add-apt-repository ppa:ubuntugis/ppa \
    && apt-get update \
    && apt-get install -y --no-install-recommends wget git libmagic-dev gcc binutils vim python3 python3-setuptools python3-dev python3-pip tzdata mercurial less \
    && pip install --upgrade pip

RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" >> /etc/apt/sources.list.d/pgdg.list' \
    && wget -q https://www.postgresql.org/media/keys/ACCC4CF8.asc -O - | apt-key add -  \ 
    && apt-get update  \
    && apt-get install  -y --no-install-recommends postgresql-client-9.6 \
    && apt-get install  -y --no-install-recommends openssh-client

RUN apt-get update && \
    apt-get install -yq tzdata rsync && \
    ln -fs /usr/share/zoneinfo/Australia/Perth /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata
ENV TZ="Australia/Perth"

FROM builder_base_borgslave as python_libs_borgslave

RUN ln -s /usr/bin/python3 /usr/bin/python
RUN groupadd  -g 1001 borg
RUN useradd -d /home/borg -m -s /bin/bash -u 1001 -g 1001 borg

WORKDIR /etc/mercurial
COPY mercurial/hgrc ./hgrc

RUN mkdir /app
RUN chown borg:borg /app

USER borg
WORKDIR /app
ARG CACHEBUST=1
RUN git clone https://github.com/dbca-wa/borgslave-sync

RUN echo "#!/bin/bash \n\
cd borgslave-sync && git checkout ${CODE_BRANCH:-master}  \n\
cd borgslave-sync && git pull &&   \n\
cd borgslave-sync && RUN pip install --no-cache-dir -r requirements_docker.txt \n\
cd borgslave-sync && /bin/bash start_sync.sh \n\
" > ./start_sync.sh
RUN chmod 555 ./start_sync.sh


CMD ["start_sync.sh"]
