# Container/Image name
FROM  mysql:5.7
LABEL maintainer="Michael Chou<snoopy02m@gmail.com>"

# # Install apk
RUN apt update
RUN apt -y install vim
RUN apt -y install tzdata
RUN apt -y install python3-pip

# set timezone
ENV ENV="/root/.bashrc"
ENV USERNAME="mc"
ENV PASSWORD="mc"
ENV TZ=Asia/Taipei


# Add alias
RUN echo 'alias ll="ls -al"' >> /root/.bashrc
RUN echo 'alias sql="mysql -u root -p"' >> /root/.bashrc
RUN echo 'alias python="python3"' >> /root/.bashrc
RUN echo 'alias pip="pip3"' >> /root/.bashrc
RUN echo 'alias backup="python /tmp/backup.py -b"' >> /root/.bashrc
RUN echo 'alias rebuild="python /tmp/backup.py -r"' >> /root/.bashrc

