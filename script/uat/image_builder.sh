export DOCKER_BUILDKIT=0 && export COMPOSE_DOCKER_CLI_BUILD=0
docker build -f script/Dockerfile --no-cache -t harbor-itdm.dahuatech.com/llms/domestic-analysis-report:uat-2.0.0-0114 .