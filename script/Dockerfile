# 使用官方 Python 镜像作为基础镜像
FROM base/python:3.11

# 将当前目录的内容复制到容器中
COPY . /app/
# 设置工作目录
WORKDIR /app

# 安装依赖项
RUN pip install --no-cache-dir -r script/requirements.txt -i http://yumserver.dahuatech.com/pypi/simple/ --trusted-host yumserver.dahuatech.com