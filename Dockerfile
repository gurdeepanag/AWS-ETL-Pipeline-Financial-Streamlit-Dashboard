# use a base python image
FROM amazonlinux:latest

# set a working directory
WORKDIR /app

# expose port
ENV PORT 8501

# install required python packages
COPY requirements.txt /app
RUN yum update -y && \
    yum install -y python3 && \
    yum install -y python3-pip && \
    yum install -y gcc python-setuptools python3-devel postgresql-devel && \
    pip install -r requirements.txt

# copy the rest of your application code
COPY . /app

# run the streamlit application
CMD ["streamlit", "run", "Frontend.py"]
