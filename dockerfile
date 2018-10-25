# Set the python runtime to use
FROM python:3

# Set the working directory
WORKDIR /moviliens/python

# Copy the source code into the container
COPY . /moviliens

# Install the python packages required, as specified in the file requirements.txt
RUN pip3 install --trusted-host pypi.python.org -r requirements.txt

# Make port 80 available outside this container
EXPOSE 80

# Define environment variable
ENV NAME World

# Run the main python application file when the container launches
CMD ["python3", "main.py"]
