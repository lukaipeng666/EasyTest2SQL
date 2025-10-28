# 金融问答系统

## 项目简介

本项目是一个基于SQL查询的金融智能问答系统，能够自动处理用户关于股票、公司财务、市场行情等金融领域的复杂问题。系统采用多Agent架构设计，通过实体提取、数据库关联查询、结果分析等步骤，为用户提供准确的金融信息答案。

## 核心功能

- **智能实体提取**：自动从用户问题中提取公司代码、股票名称等关键实体
- **SQL查询生成**：基于实体和问题上下文生成精确的SQL查询语句
- **多库关联查询**：支持跨多个金融数据库表的关联查询操作
- **结果分析与回答**：对查询结果进行分析处理，生成自然语言回答
- **错误处理与重试**：具备完善的错误检测和自动重试机制
- **易错题识别**：自动识别并特殊处理易出错的问题类型

## 项目架构

项目采用模块化设计，主要包含以下组件：

- **主程序** (`main.py`)：负责问题加载、处理流程控制和结果保存
- **Agent节点** (`agent_node.py`)：包含各种处理节点，如实体提取、SQL执行、答案生成等
- **配置管理**：API密钥、提示词模板、数据库关系等配置集中管理
- **日志系统**：记录处理过程和错误信息

## 目录结构

```
├── Dockerfile                # Docker容器化配置
├── devlop_data/              # 输入数据目录
│   └── question.json         # 问题数据集
├── devlop_home/              # 主程序代码目录
│   ├── agent_nodes/          # Agent节点实现
│   │   └── agent_node.py     # 核心Agent节点类
│   ├── configs/              # 配置文件目录
│   │   ├── api.py            # API密钥配置
│   │   ├── prompt_config.py  # 提示词模板配置
│   │   └── sql_relation_config.py # SQL数据库关系配置
│   ├── logs/                 # 日志文件目录
│   │   └── app.log           # 应用日志
│   └── main.py               # 主程序入口
├── devlop_result/            # 结果输出目录
│   └── answer.json           # 生成的答案文件
├── py_devlop.sh              # 启动脚本
└── requirements.txt          # Python依赖包列表
```

## 技术栈

- **编程语言**：Python 3.x
- **API服务**：智谱AI API (`zhipuai`)，用于自然语言处理和理解
- **数据库访问**：通过HTTP API调用金融数据库进行SQL查询
- **容器化**：Docker，用于应用打包和部署
- **第三方库**：
  - `requests`：HTTP请求处理
  - `pydantic`：数据验证和设置管理
  - `tqdm`：进度条显示
  - `logging`：日志管理

## 安装部署

### 1. 环境要求

- Docker
- 有效的API密钥（在`devlop_home/configs/api.py`中配置）

### 2. Docker部署

```bash
# 构建Docker镜像
docker build -t financial-qa-system .

# 运行容器
docker run -v $(pwd)/devlop_result:/app/devlop_result financial-qa-system
```

### 3. 本地开发环境搭建

```bash
# 创建虚拟环境
python3 -m venv venv
source venv/bin/activate  # Linux/Mac

# 安装依赖
pip install -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/

# 运行程序
python devlop_home/main.py devlop_data/question.json devlop_result/answer.json
```

## 使用方法

### 1. 准备问题数据

在`devlop_data/question.json`中准备问题数据，格式如下：

```json
[
    {
        "tid": "问题组ID",
        "team": [
            {
                "id": "问题ID",
                "question": "问题内容",
                "answer": ""  // 留空，程序会自动填充
            }
        ]
    }
]
```

### 2. 执行程序

使用提供的启动脚本运行程序：

```bash
./py_devlop.sh devlop_data/question.json devlop_result/answer.json
```

### 3. 查看结果

程序执行完成后，答案将保存在`devlop_result/answer.json`中。

## 工作流程

1. **问题加载**：从`question.json`加载待处理的问题
2. **实体提取**：识别问题中的关键金融实体（如公司代码、股票名称等）
3. **数据库查询**：
   - 确定相关的数据表和字段
   - 生成并执行SQL查询语句
   - 处理查询结果
4. **答案生成**：基于查询结果和问题上下文生成自然语言答案
5. **结果保存**：将答案保存到`answer.json`文件

## 关键功能模块

### 1. 问题处理器 (`QuestionProcessor`)

负责加载问题、管理处理流程、保存结果，具备错误检测和重试机制。

### 2. Agent节点集合 (`all_agent_node`)

包含多个功能节点：

- **实体提取**：从问题中提取关键实体
- **SQL执行**：执行数据库查询并处理结果
- **样本搜索**：获取数据表结构和样例数据
- **递归搜索**：处理复杂的多步骤查询
- **易错题检测**：识别和特殊处理易出错的问题

### 3. 配置管理

集中管理API密钥、提示词模板和数据库关系，便于维护和更新。

## 日志系统

程序运行过程中的日志记录在`devlop_home/logs/app.log`中，包含：
- 问题处理状态
- SQL查询语句和结果
- 错误信息和异常处理

## 注意事项

1. 确保API密钥有效且具有足够的权限
2. 问题数据格式必须符合要求
3. 数据库查询可能受到网络环境影响，程序内置了重试机制
4. 对于复杂问题，处理时间可能较长

## 故障排除

- **API错误**：检查`api.py`中的API密钥是否正确
- **数据库连接超时**：程序会自动重试，如持续失败请检查网络连接
- **内存错误**：对于大量数据查询，可能需要增加内存限制

## 作者

lkp

---

*项目持续更新中，如有问题或建议，请联系开发团队*
lukaipeng1998@163.com
