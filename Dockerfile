FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Dependencies
COPY pyproject.toml /app/
RUN pip install --no-cache-dir .

# App
COPY . /app

# Non-root user
RUN useradd -m appuser && chown -R appuser /app
USER appuser

EXPOSE 8080

CMD ["python", "-m", "main"]