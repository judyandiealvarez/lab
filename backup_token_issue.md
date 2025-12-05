# k3s Worker Token Backup Issue

## Problem
Worker nodes fail after restore with "token CA hash does not match" error.

## Root Cause
k3s-agent stores token in `/etc/systemd/system/k3s-agent.service.env`, not in `/var/lib/rancher/k3s/agent/token`.

Current backup only includes `/etc/rancher/node`, missing the service.env file.

## Solution
Backup must include `/etc/systemd/system/k3s-agent.service.env` for each worker node.


