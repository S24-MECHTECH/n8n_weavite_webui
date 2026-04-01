# Docker Build
docker build -t weaviate-minimal-ui .

# Docker Run (Port 7000:80)
docker run -d -p 7000:80 --name weaviate-minimal-ui weaviate-minimal-ui
