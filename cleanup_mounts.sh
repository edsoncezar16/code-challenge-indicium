#!/bin/bash
for volume in /mnt/dockeracivolumes/*; do
    sudo umount "$volume"
done
sudo rm -r /mnt/dockeracivolumes
