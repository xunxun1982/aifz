# AI病历分析系统

一个基于AI大模型的医疗病历自动化分析和结构化处理系统。

## 📋 项目概述

本系统是一个企业级的医疗信息化解决方案，专门用于将非结构化的电子病历文本智能转换为结构化的医疗数据。系统采用先进的AI技术，结合多策略解析算法，能够准确提取诊断编码（ICD-10）和手术编码（ICD-9-CM-3），大幅提升医疗数据的标准化程度。

### 🎯 核心价值

- **智能化处理**：使用AI大模型自动分析病历内容
- **高准确率**：多层解析策略确保数据提取准确性
- **标准化输出**：符合国际医疗编码标准（ICD-10/ICD-9-CM-3）
- **企业级可靠性**：7×24小时稳定运行，支持高并发处理
- **易于部署**：Docker容器化，一键部署

## 🚀 主要功能

### 1. AI病历分析
- 支持多种AI模型接口（提供API即可）
- 智能密钥轮换和负载均衡
- 自动重试机制和错误处理
- 429限流错误智能处理

### 2. 智能解析引擎
- **多策略解析**：区域解析 → 全局解析 → 文本回退
- **格式适应性强**：支持Markdown表格、HTML表格、纯文本等
- **中文医疗优化**：专门针对中文医疗术语优化
- **容错能力强**：能处理格式不规范的输入

### 3. 数据库管理
- 支持SQL Server数据库
- 防死锁机制和事务管理
- 数据一致性保证
- 自动维护和清理功能

### 4. 高性能处理
- 多线程并发处理
- 智能任务调度
- 实时进度监控
- 资源优化管理

### 5. 运维监控
- 详细日志记录和轮转
- 系统健康监控
- 错误自动恢复
- 性能指标统计

## 📁 项目结构

```
aifz_docker2/
├── 📄 aifz_main.py          # 主程序 - 系统核心调度器
├── 📄 aifz_logger.py        # 日志模块 - 统一日志管理
├── 📄 aifz_parser.py        # 解析模块 - 智能文本解析引擎
├── 📄 aifz_zdss_extract.py  # 提取模块 - 诊断手术信息提取器
├── 📄 test_combined.py      # 测试模块 - 综合功能测试
├── 📄 config.ini            # 配置文件 - 系统配置中心
├── 📄 requirements.txt      # 依赖清单 - Python包依赖
├── 📄 Dockerfile            # Docker构建文件
├── 📄 docker-compose.yml    # Docker编排文件
├── 📄 .dockerignore         # Docker忽略文件
├── 📄 README.md             # 项目文档
└── 📁 logs/                 # 日志目录
    ├── 2025-01-XX_main.log  # 主程序日志
    └── 2025-01-XX_zdss.log  # 提取模块日志
```

## 🔧 核心模块详解

### 1. aifz_main.py - 主程序调度器

**功能**：系统的核心控制中心，负责整体流程协调

**核心特性**：
- **任务调度**：定时获取待处理病历，支持单次和循环模式
- **AI接口管理**：多API组随机选择，密钥轮换，限流处理
- **并发控制**：线程池管理，智能延迟，资源优化
- **数据库操作**：事务管理，死锁预防，连接池管理
- **系统维护**：定期清理孤立数据，重新处理失败任务

**运行模式**：
```bash
# 定时循环模式（默认）
python aifz_main.py

# 立即执行一次
python aifz_main.py --run-now

# 处理指定病历
python aifz_main.py --syxh 123456

# 指定线程数
python aifz_main.py --threads 20
```

### 2. aifz_parser.py - 智能解析引擎

**功能**：从AI返回的非结构化文本中提取结构化医疗信息

**核心特性**：
- **多策略解析**：支持区域解析、全局解析、文本回退三层策略
- **格式兼容**：支持Markdown表格、HTML表格、纯文本、压缩表格等多种格式
- **智能识别**：自动识别表头结构，支持非标准表格格式
- **编码验证**：严格的ICD-10/ICD-9-CM-3格式验证
- **容错处理**：强大的容错能力，处理格式不规范的输入

**解析策略**：
1. **区域解析**：基于Markdown标题定位特定区域
2. **全局解析**：对整个文本进行表格解析
3. **文本回退**：使用正则表达式和关键词匹配

**支持格式**：
- 标准Markdown表格：`| 编码 | 名称 |`
- 非标准表格：`编码 | 名称` 或 `编码    名称`
- HTML表格格式
- 压缩表格：所有数据在一行中
- 纯文本列表：`编码：名称`

**编码验证**：
- ICD-10诊断编码：`A00.0-Z99.9`格式验证
- ICD-9-CM-3手术编码：`00.00-99.99`格式验证

### 3. aifz_zdss_extract.py - 诊断手术提取器

**功能**：调用解析引擎处理AI返回内容，生成标准化医疗数据

**核心特性**：
- **智能匹配**：基于医疗术语库进行智能名称匹配
- **多级策略**：精确匹配→去括号匹配→模糊匹配→编码匹配
- **重构优化**：自动重构和验证提取的诊断手术信息
- **数据标准化**：确保输出符合医疗编码标准

**处理流程**：
1. 从数据库获取AI分析结果
2. 调用解析引擎提取基础信息
3. 基于医疗术语库进行智能匹配和重构
4. 数据验证和清理
5. 写入标准化数据表

**匹配策略**：
- **精确匹配**：完全匹配医疗术语
- **去括号匹配**：移除括号内容后匹配
- **模糊匹配**：基于关键词和相似度匹配
- **编码匹配**：基于编码前缀匹配

**输出格式**：
```sql
XX_AIFZ_ZDSS表结构：
- syxh: 病历序号
- type: 类型（'zd'=诊断, 'ss'=手术）
- xh: 序号
- bm: 编码（ICD-10/ICD-9-CM-3）
- mc: 名称
- createtime: 创建时间
```

### 4. aifz_logger.py - 统一日志管理

**功能**：提供统一的日志记录和管理

**核心特性**：
- **智能轮转**：按日期自动轮转日志文件
- **多模块支持**：为不同模块创建独立日志
- **配置驱动**：根据配置文件自动调整日志级别
- **API统计**：自动记录和统计API调用情况
- **中文支持**：UTF-8编码确保中文日志正常显示

**日志类型**：
- **运行日志**：记录系统运行状态和错误信息
- **调试日志**：详细的调试信息（DEBUG模式）
- **API统计日志**：API调用成功/失败统计

**特色功能**：
- **DailyRotatingFileHandler**：自定义日志轮转处理器
- **线程安全**：支持多线程并发日志记录
- **统计功能**：自动统计API调用成功率

### 5. test_combined.py - 综合测试模块

**功能**：全面测试系统各项功能，确保代码质量

**核心特性**：
- **解析器测试**：测试各种复杂的医疗文本解析场景
- **边界测试**：测试极端情况和错误处理
- **回归测试**：确保系统更新不会影响现有功能
- **性能测试**：验证解析性能和准确性

**测试覆盖**：
- **复杂表格解析**：心力衰竭、脑缺血、感染等复杂病例
- **多种格式**：标准表格、压缩表格、纯文本格式
- **错误处理**：格式错误、数据缺失等异常情况
- **编码验证**：ICD-10/ICD-9-CM-3编码格式验证

**测试案例**：
- 急性心力衰竭复杂案例
- 短暂性脑缺血发作案例
- 上呼吸道感染案例
- 肾病血透案例
- 急性胰腺炎案例

**运行方式**：
```bash
# 运行所有测试
python test_combined.py

# 运行特定测试
python test_combined.py TestAifzParser.test_complex_case_1_heart_failure

# 详细输出
python test_combined.py -v
```

## ⚙️ 配置说明

### config.ini 配置文件

**⚠️ 重要提示：配置文件格式要求**
- 所有注释必须单独成行，不能使用内联注释（行尾注释）
- 配置值中不能包含 `#` 符号，否则会导致解析错误
- 严格按照提供的格式进行配置

**正确格式：**
```ini
# 数据库配置
[database]
# 数据库服务器地址
server = 127.0.0.1
# 用户名
user = sa
# 密码
password = sa
# 数据库名
database = SA

# API组配置（支持多个服务商）
[api_group_test1]
# 是否启用
enabled = true
url = https://test1/v1/chat/completions
# 多个密钥用逗号分隔
api_keys = sk-key1,sk-key2,sk-key3
model = testmodel1
timeout = 900

# 多线程配置
[thread]
# 并发线程数
max_workers = 10
# 最小延迟（秒）
min_delay = 0
# 最大延迟（秒）
max_delay = 20

# 系统模式
[system]
# RELEASE/DEBUG
mode = RELEASE
```

**❌ 错误格式（会导致解析失败）：**
```ini
# 不要使用内联注释！
max_workers = 10    # 这样的注释会导致错误
min_delay = 0       # 最小延迟时间
```

## 🐳 Docker部署

### 🔨 构建Docker镜像

在开始部署之前，您需要构建Docker镜像：

```bash
# 1. 确保在项目根目录下
cd /path/to/aifz_docker2

# 2. 构建Docker镜像
docker build -t aifz-medical:latest .

# 3. 验证镜像构建成功
docker images | grep aifz-medical
```

**构建参数说明：**
- `-t aifz-medical:latest`：为镜像指定名称和标签
- `.`：使用当前目录的Dockerfile进行构建

**镜像大小优化：**
```bash
# 查看构建过程的详细信息
docker build --no-cache -t aifz-medical:debug .

# 清理无用的构建缓存
docker builder prune
```

**镜像构建优化：**
```bash
# 启用BuildKit以获得更好的性能和缓存
export DOCKER_BUILDKIT=1
docker build -t aifz-medical:latest .

# 使用.dockerignore减少构建上下文
echo "*.log" >> .dockerignore
echo "__pycache__/" >> .dockerignore
echo "*.pyc" >> .dockerignore
```

**常见构建问题：**
```bash
# 如果遇到权限问题
sudo docker build -t aifz-medical:latest .

# 如果需要使用代理
docker build --build-arg HTTP_PROXY=http://proxy:8080 \
             --build-arg HTTPS_PROXY=http://proxy:8080 \
             -t aifz-medical:latest .

# 如果构建失败，查看详细错误信息
docker build --progress=plain --no-cache -t aifz-medical:latest .
```

### 方式一：Docker Compose（推荐）

1. **确保文件就绪**
```bash
# 检查必要文件
ls -la
# 应包含：Dockerfile, docker-compose.yml, config.ini, requirements.txt
```

2. **配置数据库连接**
```bash
# 编辑配置文件
vim config.ini
# 修改[database]部分的连接信息
```

3. **构建并启动服务**
```bash
# 构建镜像并启动容器
docker-compose up -d

# 查看运行状态
docker-compose ps

# 查看日志
docker-compose logs -f aifz_service
```

4. **管理操作**
  ```bash
# 停止服务
  docker-compose down

# 重启服务
docker-compose restart

# 更新代码后重新构建
docker-compose up -d --build
```

### 方式二：直接Docker命令

1. **构建镜像**
  ```bash
docker build -t aifz_service:latest .
```

2. **运行容器**
```bash
docker run -d \
  --name aifz_service \
  --restart on-failure \
  -v $(pwd)/logs:/app/logs \
  -v $(pwd)/config.ini:/app/config.ini \
  -e TZ=Asia/Shanghai \
  aifz_service:latest
```

3. **管理容器**
```bash
# 查看日志
docker logs -f aifz_service

### 🔄 镜像管理

**推送到镜像仓库：**
```bash
# 打标签（如果需要推送到私有仓库）
docker tag aifz-medical:latest your-registry.com/aifz-medical:latest

# 推送到仓库
docker push your-registry.com/aifz-medical:latest
```

**版本管理：**
```bash
# 为不同版本打标签
docker tag aifz-medical:latest aifz-medical:v2.0
docker tag aifz-medical:latest aifz-medical:v2.0.1

# 查看所有版本
docker images aifz-medical
```

**镜像清理：**
```bash
# 删除未使用的镜像
docker image prune

# 删除特定镜像
docker rmi aifz-medical:latest

# 删除所有相关镜像（谨慎使用）
docker rmi $(docker images aifz-medical -q)
```

**生产环境最佳实践：**
```bash
# 1. 为生产环境构建优化镜像
docker build --target production \
             --build-arg BUILD_ENV=production \
             -t aifz-medical:prod-$(date +%Y%m%d) .

# 2. 安全扫描（如果安装了Docker Scout）
docker scout cves aifz-medical:latest

# 3. 镜像签名（生产环境推荐）
# docker trust sign aifz-medical:latest

# 4. 健康检查
docker run --rm aifz-medical:latest python -c "import aifz_main; print('Health check passed')"

# 5. 多架构构建（如果需要支持ARM）
docker buildx build --platform linux/amd64,linux/arm64 \
                    -t aifz-medical:multiarch \
                    --push .
```

# 进入容器
docker exec -it aifz_service bash

# 停止容器
docker stop aifz_service

# 删除容器
docker rm aifz_service
```

## 💻 本地开发

### 环境要求

- Python 3.8+
- SQL Server 2008+
- 内存：建议4GB+
- 磁盘：1GB+

### 安装步骤

1. **克隆项目**
```bash
git clone <repository-url>
cd aifz_docker2
```

2. **安装依赖**
```bash
# 创建虚拟环境（推荐）
python -m venv venv
source venv/bin/activate  # Linux/Mac
# 或
venv\Scripts\activate     # Windows

# 安装依赖包
pip install -r requirements.txt
```

3. **配置数据库**
```bash
# 编辑配置文件
cp config.ini.example config.ini
vim config.ini
```

4. **运行测试**
```bash
# 运行综合测试
python test_combined.py

# 测试特定功能
python -c "from aifz_parser import parse_diagnoses_and_surgeries; print('解析器测试通过')"
```

5. **启动系统**
```bash
# 调试模式（修改config.ini中mode=DEBUG）
python aifz_main.py

# 处理单个病历
python aifz_main.py --syxh 123456

# 立即执行一次
python aifz_main.py --run-now
```

## 📊 监控和维护

### 日志系统详解

系统采用多层次的日志记录机制，确保完整的运行监控和问题追踪：

#### 1. 主要日志文件

**基础日志**（按模块分类）：
- `logs/YYYY-MM-DD_main.log` - 主程序运行日志
- `logs/YYYY-MM-DD_zdss.log` - 诊断手术提取模块日志

**分析统计日志**：
- `logs/YYYY-MM-DD_analysis.log` - **API调用统计日志**

#### 2. 分析日志(analysis.log)生成方法

**功能说明**：
`analysis.log` 是系统自动生成的API调用统计日志，记录每日的API调用成功/失败统计信息。

**生成机制**：
```python
# 在aifz_logger.py中实现
def log_api_call_success():
    """记录API调用成功，自动累计统计"""
    
def log_api_call_failure():
    """记录API调用失败，自动累计统计"""
    
def _write_api_stats_to_file():
    """内部函数：将API统计信息写入日志文件"""
```

**调用位置**：
- 在`aifz_main.py`的`call_ai_api()`函数中
- API调用成功时调用`log_api_call_success()`
- API调用失败时调用`log_api_call_failure()`

**日志格式示例**：
```
=== API调用统计 - 2025-07-12 ===
总调用次数: 2
成功: 2
失败: 0
成功率: 100.00%
最后更新: 2025-07-12 15:01:37
```

**生成时机**：
- 每次API调用后自动记录
- 日期切换时自动写入文件
- 程序退出时强制写入统计

**手动生成统计**：
```python
from aifz_logger import force_write_api_stats
force_write_api_stats()  # 强制写入API统计
```

#### 3. 日志配置

**日志级别**：
- `DEBUG模式`：输出详细调试信息，便于问题排查
- `RELEASE模式`：输出关键信息，减少日志量

**配置方式**：
```ini
# 在config.ini中配置
[system]
mode = DEBUG  # 或 RELEASE
```

**日志轮转**：
- 按日期自动轮转，每天生成新的日志文件
- 历史日志文件自动保留，便于追溯

### 日志查看

```bash
# 查看主程序日志
tail -f logs/$(date +%Y-%m-%d)_main.log

# 查看提取模块日志
tail -f logs/$(date +%Y-%m-%d)_zdss.log

# 查看API调用统计日志
cat logs/$(date +%Y-%m-%d)_analysis.log

# 查看所有日志
tail -f logs/*.log

# 查看特定日期的日志
tail -f logs/2025-07-12_*.log
```

### 性能监控

```bash
# Docker环境
docker stats aifz_service

# 系统资源
htop
iostat -x 1

# API调用统计分析
grep "成功率" logs/*_analysis.log
```

### 故障排查

1. **API调用失败**
   - 检查网络连接
   - 验证API密钥有效性
   - 查看限流情况

2. **数据库连接问题**
   - 检查数据库服务状态
   - 验证连接参数
   - 查看防火墙设置

3. **解析结果异常**
   - 检查AI返回内容格式
   - 查看解析日志
   - 验证输入数据质量

## 🔄 系统架构

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           AI病历分析系统 - 系统架构图                                │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   定时调度器     │────│   AI接口管理器    │────│   大模型API      │
│  aifz_main.py   │    │  (密钥轮换管理)   │    │  (多服务商)      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                        │                        │
         │                        │                        │
         ▼                        ▼                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   病历数据库     │────│   智能解析引擎    │────│   日志统计系统   │
│  (SQL Server)   │    │  aifz_parser.py  │    │  aifz_logger.py │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                        │                        │
         │                        │                        │
         ▼                        ▼                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  结构化数据表    │────│  诊断手术提取器   │────│   测试验证模块   │
│  XX_AIFZ_ZDSS   │    │aifz_zdss_extract.py│   │test_combined.py │
└─────────────────┘    └──────────────────┘    └─────────────────┘

                         ┌──────────────────┐
                         │   Docker容器化   │
                         │  (一键部署支持)   │
                         └──────────────────┘
```

**数据流向**：
1. 定时调度器从数据库获取待处理病历
2. 调用AI接口管理器进行智能分析
3. 智能解析引擎处理AI返回的非结构化文本
4. 诊断手术提取器进行医疗术语匹配和重构
5. 标准化数据存储到结构化数据表
6. 日志统计系统记录全过程监控信息

**核心优势**：
- **模块化设计**：各组件独立，易于维护和扩展
- **容错机制**：多层容错处理，确保系统稳定性
- **性能优化**：多线程并发，智能缓存，资源优化
- **监控完善**：全链路日志记录，实时统计分析

## 🤝 贡献指南

1. Fork 项目
2. 创建功能分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

## 📜 版权声明

本项目采用 MIT 许可证。详情请参阅 [LICENSE](LICENSE) 文件。

## 🆘 技术支持

### 常见问题解答

**Q：如何查看API调用统计？**
A：查看 `logs/YYYY-MM-DD_analysis.log` 文件，包含每日API调用次数、成功率等统计信息。

**Q：解析准确率如何提升？**
A：系统采用三层解析策略，支持多种表格格式。如遇到解析问题，可查看 `test_combined.py` 中的测试案例进行对比。

**Q：如何处理特殊格式的医疗文本？**
A：解析引擎支持标准表格、压缩表格、纯文本等多种格式。具体支持的格式请参考 `aifz_parser.py` 模块说明。

**Q：系统性能如何优化？**
A：可调整 `config.ini` 中的线程数量、API超时时间等参数。建议线程数设置为CPU核心数的1-2倍。

### 获取帮助

如有问题或建议，请：

1. 查阅本文档的故障排查部分
2. 查看日志文件了解具体错误信息
3. 运行测试模块验证系统功能
4. 查看项目 Issues
5. 创建新的 Issue 描述问题

**调试建议**：
- 设置 `mode = DEBUG` 获取详细日志
- 使用 `python test_combined.py` 验证核心功能
- 查看 `logs/*_analysis.log` 了解API调用情况
- 检查 `config.ini` 配置是否正确

