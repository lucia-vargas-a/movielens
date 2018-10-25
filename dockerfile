# Set the python runtime to use
FROM python:3

# Set the working directory
WORKDIR /movielens

# Copy the source code into the container
COPY . /movielens

# Install the python packages required, as specified in the file requirements.txt
RUN pip3 install -r requirements.txt

# Make port 80 available outside this container
EXPOSE 80

# Define environment variable
ENV NAME World

# Run the main python application file when the container launches
CMD ["python3", "python/main.py"]
