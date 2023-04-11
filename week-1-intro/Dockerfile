# ParentImage
FROM python:3.9

# Working directory for application in the container
WORKDIR /app
# Copy files from local directory to `/app` in container
COPY pipeline.py pipeline.py

# Install libraries
RUN pip install pandas

# Command executed when container is started
ENTRYPOINT [ "python", "pipeline.py"]
