FROM quantiledevelopment/gdal:latest

ENV CPLUS_INCLUDE_PATH /usr/local/include/gdal
ENV C_INCLUDE_PATH /usr/local/include/gdal
ENV LD_LIBRARY_PATH=/usr/local/lib


# Install git
RUN apt-get update && apt-get install git build-essential g++ libfreexl-dev libpng16-16 libxml2 -y

# Install GDAL python wrapper
# RUN pip install GDAL==$(ogrinfo --version | grep -oP "\d+(\.\d+){2,}"

RUN curl http://bootstrap.pypa.io/get-pip.py -o get-pip.py
RUN python get-pip.py

# Manually update the pip version to fix a bug
# https://stackoverflow.com/questions/67273590/pip-21-1-cant-import-invalidschemecombination
COPY get-pip.py .
RUN python get-pip.py pip==21.0.1

# Install meltano
RUN pip install meltano==1.76.0

# The working directory is the meltano project
WORKDIR /project

# Expose default port used by `meltano ui`
EXPOSE 5000

COPY ./ .

# Install the meltano plugins
RUN meltano install

ENTRYPOINT ["meltano"]
