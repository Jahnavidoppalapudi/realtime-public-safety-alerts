FROM python:3.12-slim

# Run as a non-root user
RUN useradd --create-home --uid 10001 app
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
USER app

CMD ["python", "-m", "src.producer"]
