FROM mongo:4

RUN apt-get update -y && \
    apt-get install -y netcat && \
    apt-get clean && rm -f /var/lib/apt/lists/*_*
COPY init-inventory.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/init-inventory.sh

CMD ["mongod", "--replSet", "rs0", "--auth", "--bind_ip_all"]