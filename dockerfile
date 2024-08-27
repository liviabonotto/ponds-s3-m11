FROM python:3.12

WORKDIR /app

COPY pyproject.toml poetry.lock* ./

RUN pip install poetry

RUN poetry install --no-root

COPY . .

EXPOSE 5000 8501

CMD ["poetry", "run", "sh", "-c", "flask run --host=0.0.0.0 & streamlit run data_pipeline/streamlit_app.py --server.port 8501"]


