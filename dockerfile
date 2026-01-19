# which version of python to use, "Slim" means we stripped out useless tools to keep it lightweight.
FROM python:3.13-slim

# create a working directory
WORKDIR /code

# get all the necessary dependencies for this project
COPY requirements.txt .

# install those dependecies
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# copy all the code of the project
COPY . .

# once you activate the "box" the following command runs and starts the server
CMD [ "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000" ]

