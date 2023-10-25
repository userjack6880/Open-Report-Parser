FROM debian:bookworm-slim

# Install dependencies
# Core dependencies, Database dependencies, Oauth dependencies and container depenencies (cron)
RUN apt-get update -y \
    && apt-get install -y perl unzip cron \
        libfile-mimeinfo-perl libmail-imapclient-perl libmime-tools-perl libxml-simple-perl libio-socket-inet6-perl libio-socket-ip-perl libperlio-gzip-perl libmail-mbox-messageparser-perl libwww-perl libjson-perl \
        libdbd-mysql-perl libdbd-pg-perl \ 
        liblwp-protocol-https-perl libencode-perl libtime-piece-mysql-perl

# Copy in the tool and config file
COPY . /usr/src/parser
COPY report-parser.conf.pub-docker /usr/src/parser/report-parser.conf
WORKDIR /usr/src/parser

# Add crontab file entry to run every hour
RUN echo "0 */1 * * * root cd /usr/src/parser/ && perl /usr/src/parser/report-parser.pl -i > /proc/1/fd/1 2>&1" >> /etc/crontab

# Run the tool then start cron
ENTRYPOINT ["/usr/src/parser/run.docker.sh"]
CMD ["-i"]
