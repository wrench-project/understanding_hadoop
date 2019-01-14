#!/bin/bash

# start sshd
/etc/init.d/ssh restart

# this is so that CMD can run after ENTRYPOINT
exec "$@"
