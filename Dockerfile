FROM python:3.10

WORKDIR /workspaces/homyscrapy

COPY requirements.txt ./
RUN pip install -r requirements.txt

# Copy the rest of the code
COPY common/* ./common/
COPY homyscrapy/* ./homyscrapy/

CMD [ "python", "/workspaces/homyscrapy/homyscrapy/homyscrapy.py" ]
