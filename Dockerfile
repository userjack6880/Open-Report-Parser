FROM perl:5-slim

# Install dependencies
# Core dependencies, Database dependencies, Oauth dependencies and container depenencies (cron)
RUN apt-get update -y \
	&& apt-get install -y libfile-mimeinfo-perl libmail-imapclient-perl libmime-tools-perl libxml-simple-perl libio-socket-inet6-perl libio-socket-ip-perl libperlio-gzip-perl libmail-mbox-messageparser-perl libwww-perl unzip \
	libdbd-mysql-perl libdbd-pg-perl \ 
	liblwp-protocol-https-perl libencode-perl libtime-piece-mysql-perl \
	cron

# Copy in the tool and config file
COPY . /usr/src/parser
COPY report-parser.conf.pub-docker /usr/src/parser/report-parser.conf
WORKDIR /usr/src/parser

# Add crontab file entry
RUN echo "0 */1 * * * root perl /usr/src/parser/report-parser.pl -i > /proc/1/fd/1 2>&1" >> /etc/crontab

# Run the tool then start cron
ENTRYPOINT ["/bin/sh", "-c" , "perl /usr/src/parser/report-parser.pl -i && cron -f"]