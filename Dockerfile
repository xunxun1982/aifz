# =============================================================================
# AI病历分析系统 - Docker镜像构建文件
# =============================================================================

# 基础镜像：使用官方Python 3.12精简版
FROM python:3.12-slim

# Python运行环境配置
ENV PYTHONUNBUFFERED=1              # 禁用Python输出缓冲
ENV TZ=Asia/Shanghai                # 设置时区为北京时间

# 配置系统时区
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 设置工作目录
WORKDIR /app

# 复制依赖文件（利用Docker层缓存机制）
COPY requirements.txt .

# 复制应用程序文件
# .dockerignore 确保只复制必要文件
COPY . .

# 容器启动命令
# 程序会自动检查和安装依赖包
CMD ["python", "aifz_main.py"]