FROM python:3.10

WORKDIR /workspaces/homyscrapy

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY common/* ./common/
COPY homyscrapy/* ./homyscrapy/
COPY data/* ./data/

CMD ["python", "/workspaces/homyscrapy/homyscrapy/homyscrapy.py"]