FROM mongo:4.4.4 AS base

ADD deploy/hosts /tmp/
ENTRYPOINT  ["sh", "-c", "cat /tmp/hosts >> /etc/hosts && mongos --configdb cfgrs/lattice-147:27017,lattice-148:27017,lattice-149:27017 --bind_ip_all --port 27018"]

