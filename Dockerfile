FROM python:3.10-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
COPY pyproject.toml ./
RUN pip install --no-cache-dir pandas==2.3.3 psycopg2-binary==2.9.11 pydantic-settings==2.12.0 tqdm==4.66.4 requests==2.32.5 beautifulsoup4==4.12.3
COPY . .
CMD ["python","main.py","full"]
