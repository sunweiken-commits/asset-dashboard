# 个人资产看板

这是一个资产看板应用，既支持直接读取 `Personal Assets.xlsx` 第一个工作表，也支持切换到 Supabase 云数据库。

## 功能

- 只读取第一个工作表 `资产管理`
- 自动从这张表计算总资产趋势
- 展示最新资产分类汇总
- 展示各账户最新快照
- 提供统一的“月度更新入口”，可直接在页面里填写并保存回 Excel
- 可直接在页面里创建新的月份列
- 支持切换到 Supabase 云数据库，后续直接在网页读写数据

## 运行方式

1. 安装依赖

```bash
python3 -m pip install -r requirements.txt
```

2. 启动应用

```bash
streamlit run app.py
```

3. 打开浏览器后，确认左侧的 Excel 路径是否为：

```text
/Users/bytedance/Downloads/Personal Assets.xlsx
```

## 切换到 Supabase

1. 在 Supabase SQL Editor 执行 [schema.sql](/Users/bytedance/Documents/资产管理/schema.sql)
2. 配置环境变量

```bash
export SUPABASE_URL="https://your-project-id.supabase.co"
export SUPABASE_KEY="your-service-role-or-secret-key"
```

3. 一次性导入 Excel 历史数据

```bash
python3 import_excel_to_supabase.py
```

4. 重新启动应用后，页面会自动切换到数据库模式

## 部署到 Streamlit Community Cloud

1. 把当前目录初始化为 Git 仓库并推到 GitHub
2. 在 Streamlit Community Cloud 选择该 GitHub 仓库
3. Main file path 填 `app.py`
4. 在 Streamlit 的 `Secrets` 中配置：

```toml
SUPABASE_URL = "https://your-project-id.supabase.co"
SUPABASE_KEY = "your-secret-key"
```

5. 点击 Deploy，之后电脑和手机都可以通过同一个网址访问

## 适合下一步继续增强的方向

- 接入实时股票/基金行情，不再只看手工录入值
- 增加“交易流水”模式，减少每个月手工维护整张快照表
- 增加月度收益率、资产净流入、目标配置偏离提醒
- 增加手机端更友好的首页卡片
- 后续把 Excel 迁移到数据库，支持多端同步
