# Use an official Python runtime as a parent image
FROM python:3.9

# Set the working directory in the container
WORKDIR /usr/src/app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
# Run app.py when the container launches
CMD ["python", "app.py"]
