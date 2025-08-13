FROM ubuntu:latest
RUN apt-get -y update
RUN apt-get install -y sudo
RUN apt install -y software-properties-common

RUN sudo add-apt-repository ppa:deadsnakes/ppa
RUN apt-get install -y python3.10 python3.10-venv python3.10-distutils python3-pip

RUN sudo add-apt-repository ppa:xtradeb/apps
RUN sudo apt install -y ungoogled-chromium

