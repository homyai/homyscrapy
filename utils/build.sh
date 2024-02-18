# Building the local image. From the root directory of the repo, run:
# For Intel architecture:

docker build --no-cache --platform linux/amd64 -t nicomedrano/homyscrapy:v0.3 -f ./Dockerfile .

# or for Mac Silicon architecture:

# docker build --platform linux/arm64 -t nicomedrano/homyscrapy:v0.1 -f ./Dockerfile .

# Note that you need to build the image for the right architecture. If you get the
# message "exec format error", it means you built it for the wrong architecture.

