# 远程数据同步脚本使用指南

## 📋 文件说明

```
scripts/
├── remote_sync.py      # 主同步脚本
├── requirements.txt    # 依赖清单
└── README.md          # 本说明文档
```

## 🚀 快速部署到远程服务器

### 1. 上传脚本

```bash
# 在本地执行
scp -r scripts/ user@your-server:~/ts-share-sync/
```

### 2. 在服务器上安装依赖

```bash
# SSH 登录服务器
ssh user@your-server

cd ~/ts-share-sync
pip install -r requirements.txt
```

### 3. 运行全量同步

```bash
# 低频模式，每次请求间隔3秒（推荐）
python remote_sync.py --mode full --days 120 --delay 3

# 更保守的设置（间隔5秒）
python remote_sync.py --mode full --days 120 --delay 5
```

### 4. 使用 screen 或 nohup 后台运行

```bash
# 使用 screen（推荐）
screen -S ts-sync
python remote_sync.py --mode full --days 120 --delay 3
# 按 Ctrl+A, 然后按 D 退出 screen（保持运行）
# 重新进入: screen -r ts-sync

# 或使用 nohup
nohup python remote_sync.py --mode full --days 120 --delay 3 > sync.log 2>&1 &
tail -f sync.log  # 查看日志
```

### 5. 打包下载数据

```bash
# 在服务器上
python remote_sync.py --mode export

# 在本地下载
scp user@your-server:~/ts-share-sync/export/ts_share_data_*.tar.gz ./
```

### 6. 解压到项目目录

```bash
# 在本地 ts-share 项目目录
cd /Users/mac/Gits/skills/ts-share
tar -xzvf ts_share_data_XXXXXXXX_XXXXXX.tar.gz -C data/
```

---

## 📊 命令参考

| 命令 | 说明 |
|------|------|
| `--mode full` | 全量同步（首次使用） |
| `--mode incremental` | 增量更新（日常更新） |
| `--mode export` | 打包数据用于下载 |
| `--mode status` | 查看同步状态 |
| `--days N` | 同步N天历史数据（默认120） |
| `--delay N` | 每次请求间隔N秒（默认3） |
| `--no-resume` | 禁用断点续传，从头开始 |

---

## ⚠️ 限流保护机制

脚本内置了多重限流保护：

1. **随机延迟**: 每次请求间隔 = delay + 随机(0~1秒)
2. **健康检查**: 每100只股票检查一次API状态
3. **连续失败保护**: 连续失败5次后自动冷却10分钟
4. **断点续传**: 中断后可继续，不丢失进度

---

## 📁 数据结构

同步后的数据结构：

```
data/
├── stocks/              # 股票历史数据（Parquet格式）
│   ├── 000001.parquet   # 平安银行
│   ├── 000002.parquet   # 万科A
│   └── ...
├── metadata.json        # 元数据（同步时间/范围等）
└── sync_progress.json   # 同步进度（用于断点续传）
```

---

## 🔄 日常增量更新流程

全量同步完成后，后续只需增量更新：

```bash
# 在服务器上
python remote_sync.py --mode incremental --delay 2

# 打包增量后的完整数据
python remote_sync.py --mode export

# 下载到本地并解压覆盖
```

---

## 💡 建议

1. **首次同步选择非高峰时段**（如凌晨或周末）
2. **延迟设置 3~5 秒**较为安全
3. **使用 screen** 保证 SSH 断开后继续运行
4. **定期增量更新**（如每周一次）
