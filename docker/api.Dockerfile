# FastAPI serving layer.
#
# Dependencies are installed at build time so the container starts instantly;
# the application code itself is bind-mounted by docker-compose so edits are
# picked up by uvicorn --reload without a rebuild.
FROM python:3.12-slim

WORKDIR /opt/project

COPY docker/requirements-api.txt /tmp/requirements-api.txt
RUN pip install --no-cache-dir -r /tmp/requirements-api.txt

ENV PYTHONPATH=/opt/project \
    PYTHONUNBUFFERED=1

EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
