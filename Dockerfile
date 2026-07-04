FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Each service overrides `command` in docker-compose.yml, so the default CMD
# here is only a placeholder.
CMD ["python", "-c", "print('set a command in docker-compose.yml')"]
