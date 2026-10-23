#!/bin/sh
set -eu

# Keep Compose bridge networking while collecting the WSL host network.
# /proc/net resolves through self/net, so a plain host /proc bind otherwise
# reports the exporter's network namespace. PID 1 belongs to the host because
# the service uses pid: host. Other proc files retain their original sources.
mkdir -p /tmp/proc-view
for entry in /host/proc/*; do
    name=${entry##*/}
    if [ "$name" != net ]; then
        ln -sfn "$entry" "/tmp/proc-view/$name"
    fi
done
ln -sfn /host/proc/1/net /tmp/proc-view/net

# --no-collector.netdev.netlink and --no-collector.netclass.netlink are set in
# Compose so network collectors use the mounted host files, not local sockets.
exec /bin/node_exporter "$@"
