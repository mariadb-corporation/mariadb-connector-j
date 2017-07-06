FROM asosso/maxscale:1.4.3

# bring system up2date
#RUN yum update -y
RUN yum -y install MariaDB-client

# We copy our config creator script to the container
COPY maxscale/docker-entrypoint.sh /
COPY maxscale/cmd.sh /usr/bin/

RUN chmod 777 /docker-entrypoint.sh
RUN chmod 777 /usr/bin/cmd.sh

# We expose our set Listener Ports
EXPOSE 4006 4007

# We define the config creator as entrypoint
ENTRYPOINT ["/docker-entrypoint.sh"]

# We startup MaxScale as default command
CMD ["/usr/bin/cmd.sh"]