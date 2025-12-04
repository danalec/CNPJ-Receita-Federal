FROM python:3.10-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
COPY pyproject.toml poetry.lock ./
RUN pip install --no-cache-dir pip poetry pip-audit
RUN poetry export -f requirements.txt -o requirements.txt --without-hashes
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENTRYPOINT ["python","-m","src"]
CMD []
