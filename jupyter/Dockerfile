FROM jupyter/datascience-notebook

USER root

ENV DEBIAN_FRONTEND=noninteractive

ENV TZ=Europe/Amsterdam
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN sudo apt-get update
RUN sudo apt-get install build-essential cmake libboost-dev libexpat1-dev zlib1g-dev libbz2-dev -y

USER 1000

COPY ./requirements.txt ./requirements.txt

RUN pip install -r requirements.txt

CMD ["jupyter", "notebook", "--port=8888", "--no-browser", "--ip=0.0.0.0", "--allow-root", "--NotebookApp.token=''", "--NotebookApp.password=''"]