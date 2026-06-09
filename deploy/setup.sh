#!/bin/bash
set -e

echo "Installing RaftKV..."
mkdir -p /opt/raftkv
cp raftkv /opt/raftkv/
cp -r data /opt/raftkv/ || true

cp deploy/raftkv.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable raftkv
systemctl start raftkv

echo "RaftKV installed!"