FROM hubdocker.aminer.cn/library/python-base:1.0.0

WORKDIR /app

COPY . /app/

RUN pip install -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com \
    && chmod +x /app/py_devlop.sh

CMD ["/app/py_devlop.sh", "/app/devlop_data/question.json", "/app/devlop_result/answer.json"]