#!/bin/bash -e
image_name=rohankumar1/prom-get-layers
image_tag=0.01
full_image_name=${image_name}:${image_tag}

# cd "$(dirname "$0")" 
sudo docker build -t "${full_image_name}" .
sudo docker push "$full_image_name"

# Output the strict image name, which contains the sha256 image digest
sudo docker inspect --format="{{index .RepoDigests 0}}" "${full_image_name}"
