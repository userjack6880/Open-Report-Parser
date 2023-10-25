FROM perl:5-slim

RUN apt-get update -y \
	&& apt-get install -y libfile-mimeinfo-perl libmail-imapclient-perl libmime-tools-perl libxml-simple-perl libio-socket-inet6-perl libio-socket-ip-perl libperlio-gzip-perl libmail-mbox-messageparser-perl libwww-perl unzip \
	&& apt-get install -y libdbd-mysql-perl libdbd-pg-perl \ 
	&& apt-get install -y liblwp-protocol-https-perl libencode-perl libtime-piece-mysql-perl

COPY . /usr/src/parser
COPY report-parser.conf.pub-docker /usr/src/parser/report-parser.conf
WORKDIR /usr/src/parser

CMD [ "perl", "./report-parser.pl", "-i" ]