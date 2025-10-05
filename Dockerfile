FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml poetry.lock ./ 
COPY src/ ./src/
COPY config/ ./config/
RUN pip install poetry && poetry config virtualenvs.create false && poetry install --only main
CMD ["poetry", "run", "python", "src/collector/collector.py"]
