#!/bin/bash

# start sshd
/etc/init.d/ssh restart

# set so that strace can attach to other processes in this container (solution from https://bitworks.software/en/2017-07-24-docker-ptrace-attach.html) 
echo 0 > /proc/sys/kernel/yama/ptrace_scope

# this is so that CMD can run after ENTRYPOINT
exec "$@"
