#!/bin/bash

# =================================================================
# TS-Share 一键启动脚本 (Mac/Linux)
# =================================================================

# 颜色定义
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}🚀 正在准备 TS-Share 环境...${NC}"

# 1. 检查环境变量 (虚拟环境)
if [[ "$VIRTUAL_ENV" != "" ]]; then
    echo -e "${GREEN}✅ 已检测到虚拟环境: $VIRTUAL_ENV${NC}"
else
    echo -e "${YELLOW}⚠️ 警告: 未检测到激活的虚拟环境，建议在虚拟环境中运行。${NC}"
fi

# 2. 安装/更新依赖
echo -e "${BLUE}📦 正在检查并安装必要依赖...${NC}"
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ 依赖安装成功${NC}"
else
    echo -e "${RED}❌ 依赖安装失败，请检查网络或 pip 配置${NC}"
    exit 1
fi

# 3. 检查配置文件
if [ ! -f "config.py" ]; then
    echo -e "${YELLOW}⚠️ 警告: 未找到 config.py。如果您是第一次运行，请参考 config.py.example 进行配置。${NC}"
else
    echo -e "${GREEN}✅ 配置文件已就绪${NC}"
fi

# 4. 启动程序
echo -e "${BLUE}🎯 正在启动 Streamlit Web 服务...${NC}"
echo -e "${YELLOW}提示: 如果浏览器没有自动打开，请手动访问终端输出的 Local URL${NC}"

python3 -m streamlit run app.py
