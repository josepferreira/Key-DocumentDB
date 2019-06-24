docker stop $(docker ps -a | awk -f encontraDocker.awk)
docker rm $(docker ps -a | awk -f encontraDocker.awk)