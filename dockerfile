# Use the official Python image
FROM python:3.12

# Set the working directory in the container
WORKDIR /app

# Copy the pyproject.toml and poetry.lock files to the container
COPY pyproject.toml poetry.lock* ./

# Install Poetry
RUN pip install poetry

# Install dependencies
RUN poetry install --no-root

# Copy the rest of the application code
COPY . .

# Expose necessary ports
EXPOSE 5000 8501

# Command to run both Flask and Streamlit
CMD ["poetry", "run", "sh", "-c", "flask run --host=0.0.0.0 & streamlit run streamlit_app.py --server.port 8501"]
