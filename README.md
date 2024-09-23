Docker build:
docker build -t corretor-de-rota .

Docker run:
docker run -d -v $(pwd):/app --name rotas-container corretor-de-rota