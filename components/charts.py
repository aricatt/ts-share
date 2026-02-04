"""
PyEcharts 图表组件
"""
import pandas as pd
from pyecharts import options as opts
from pyecharts.charts import Pie, Bar, Kline, Line, Grid
from pyecharts.globals import ThemeType
from pyecharts.commons.utils import JsCode
from streamlit.components.v1 import html


def render_chart(chart, height: int = 400):
    """
    渲染 PyEcharts 图表到 Streamlit
    
    Args:
        chart: PyEcharts 图表对象
        height: 图表高度
    """
    chart_html = chart.render_embed()
    html(chart_html, height=height)


def create_industry_pie(df: pd.DataFrame, title: str = "行业分布") -> Pie:
    """
    创建行业分布饼图
    
    Args:
        df: 股票数据，需包含 '行业' 列
        title: 图表标题
    
    Returns:
        PyEcharts Pie 对象
    """
    # 兼容性处理：尝试获取 '行业' 或 '所属行业'
    col = '行业' if '行业' in df.columns else '所属行业'
    if col not in df.columns:
        return Pie() # 返回空图表避免报错
        
    industry_count = df[col].value_counts()
    data = [list(z) for z in zip(industry_count.index.tolist(), industry_count.values.tolist())]
    
    pie = (
        Pie(init_opts=opts.InitOpts(theme=ThemeType.DARK, width="100%", height="500px"))
        .add(
            "",
            data,
            radius=["30%", "65%"],
            center=["50%", "45%"], # 身体稍微上移，给底部图例留空间
            label_opts=opts.LabelOpts(
                formatter="{b}: {c}",
                position="outside"
            ),
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(title=title, pos_left="center", pos_top="0"),
            legend_opts=opts.LegendOpts(
                pos_bottom="5%", 
                pos_left="center", 
                orient="horizontal",
                is_show=len(data) < 25 # 如果行业太多（超过25个）就不显示图例以免拥挤
            ),
        )
    )
    return pie


def create_turnover_bar(df: pd.DataFrame, top_n: int = 10, title: str = "换手率排行") -> Bar:
    """
    创建换手率柱状图
    
    Args:
        df: 股票数据，需包含 '名称' 和 '换手率' 列
        top_n: 显示前 N 只
        title: 图表标题
    
    Returns:
        PyEcharts Bar 对象
    """
    df_sorted = df.nlargest(top_n, '换手率')[['名称', '换手率']]
    
    bar = (
        Bar(init_opts=opts.InitOpts(theme=ThemeType.DARK, width="100%", height="400px"))
        .add_xaxis(df_sorted['名称'].tolist())
        .add_yaxis(
            "换手率 (%)",
            df_sorted['换手率'].round(2).tolist(),
            itemstyle_opts=opts.ItemStyleOpts(color="#667eea"),
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(title=title),
            xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=45)),
        )
    )
    return bar


def create_market_cap_bar(df: pd.DataFrame, top_n: int = 10, title: str = "市值分布") -> Bar:
    """
    创建市值柱状图
    
    Args:
        df: 股票数据，需包含 '名称' 和 '总市值' 列
        top_n: 显示前 N 只
        title: 图表标题
    
    Returns:
        PyEcharts Bar 对象
    """
    df_sorted = df.nsmallest(top_n, '总市值')[['名称', '总市值']].copy()
    df_sorted['市值(亿)'] = (df_sorted['总市值'] / 1e8).round(2)
    
    bar = (
        Bar(init_opts=opts.InitOpts(theme=ThemeType.DARK, width="100%", height="400px"))
        .add_xaxis(df_sorted['名称'].tolist())
        .add_yaxis(
            "市值 (亿)",
            df_sorted['市值(亿)'].tolist(),
            itemstyle_opts=opts.ItemStyleOpts(color="#764ba2"),
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(title=title),
            xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=45)),
        )
    )
    return bar


def create_kline_chart(df: pd.DataFrame, title: str = "K线图") -> Grid:
    """
    创建K线图（带成交量）
    
    Args:
        df: K线数据，需包含 日期、开盘、收盘、最低、最高、成交量
        title: 图表标题
    
    Returns:
        PyEcharts Grid 对象
    """
    dates = df['日期'].tolist()
    # 增强鲁棒性：寻找换手率列
    turnover_col = None
    for c in ['换手率', 'turnover_rate', '换手率(%)']:
        if c in df.columns:
            turnover_col = c
            break
    
    # 构造 Kline 数据，固定索引以保证 Tooltip 稳定
    # 索引: 0:日期, 1:开盘, 2:收盘, 3:最低, 4:最高, 5:涨跌幅, 6:换手率
    df_chart = df.copy()
    if turnover_col and turnover_col != '换手率':
        df_chart['换手率'] = df_chart[turnover_col]
    elif not turnover_col:
        df_chart['换手率'] = 0 # 垫片数据
        
    kline_data = df_chart[['开盘', '收盘', '最低', '最高', '涨跌幅', '换手率']].values.tolist()
    volumes = df_chart['成交量'].tolist()
    
    # K线主图
    kline = (
        Kline()
        .add_xaxis(dates)
        .add_yaxis(
            "K线",
            kline_data,
            itemstyle_opts=opts.ItemStyleOpts(color="red", color0="green"),
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(title=title),
            xaxis_opts=opts.AxisOpts(is_scale=True),
            yaxis_opts=opts.AxisOpts(is_scale=True),
            datazoom_opts=[
                opts.DataZoomOpts(is_show=True, type_="inside", xaxis_index=[0, 1], range_start=0, range_end=100),
                opts.DataZoomOpts(is_show=True, type_="slider", xaxis_index=[0, 1], range_start=0, range_end=100, pos_bottom="10%"),
            ],
            legend_opts=opts.LegendOpts(pos_top="5%"),
            tooltip_opts=opts.TooltipOpts(
                trigger="axis", 
                axis_pointer_type="cross",
                formatter=JsCode("""
                    function (params) {
                        try {
                            var res = params[0].name;
                            for (var i = 0; i < params.length; i++) {
                                var item = params[i];
                                var type = item.seriesType || '';
                                if (type.indexOf('kline') !== -1 || type.indexOf('candlestick') !== -1) {
                                    var val = item.value;
                                    var open = val[1];
                                    var close = val[2];
                                    var low = val[3];
                                    var high = val[4];
                                    var pct = val[5];
                                    var turnover = val[6];
                                    
                                    var color = close >= open ? '#ef5350' : '#26a69a';
                                    res += '<br/>' + item.marker + ' <b>' + item.seriesName + '</b>:';
                                    res += '<br/>&nbsp;&nbsp;开盘: ' + open + ' &nbsp;&nbsp;收盘: ' + close;
                                    res += '<br/>&nbsp;&nbsp;最低: ' + low + ' &nbsp;&nbsp;最高: ' + high;
                                    
                                    var extra = '';
                                    if (pct !== undefined && pct !== null) {
                                        extra += '涨跌幅: <span style="color:' + color + '">' + pct + '%</span>';
                                    }
                                    if (turnover !== undefined && turnover !== null) {
                                        if (extra) extra += ' &nbsp;&nbsp;';
                                        extra += '换手率: ' + turnover + '%';
                                    }
                                    if (extra) {
                                        res += '<br/>&nbsp;&nbsp;' + extra;
                                    }
                                } else {
                                    var val = Array.isArray(item.value) ? item.value[1] : item.value;
                                    if (val !== undefined && val !== null) {
                                        var displayVal = typeof val === 'number' ? val.toFixed(2) : val;
                                        res += '<br/>' + item.marker + ' ' + item.seriesName + ': ' + displayVal;
                                    }
                                }
                            }
                            return res;
                        } catch (e) {
                            return 'Tooltip Error';
                        }
                    }
                """)
            ),
        )
    )
    
    # 均线
    line = (
        Line()
        .add_xaxis(dates)
        .add_yaxis("MA5", df['ma5'].tolist() if 'ma5' in df.columns else [], is_smooth=True, 
                  linestyle_opts=opts.LineStyleOpts(color="#5470c6", opacity=0.8, width=1), 
                  label_opts=opts.LabelOpts(is_show=False))
        .add_yaxis("MA20", df['ma20'].tolist() if 'ma20' in df.columns else [], is_smooth=True,
                  linestyle_opts=opts.LineStyleOpts(color="#ee6666", opacity=0.8, width=1), 
                  label_opts=opts.LabelOpts(is_show=False))
        .add_yaxis("MA60", df['ma60'].tolist() if 'ma60' in df.columns else [], is_smooth=True,
                  linestyle_opts=opts.LineStyleOpts(color="#91cc75", opacity=0.8, width=1), 
                  label_opts=opts.LabelOpts(is_show=False))
    )
    
    kline.overlap(line)
    
    # 成交量柱状图
    bar = (
        Bar()
        .add_xaxis(dates)
        .add_yaxis("成交量", volumes, 
                  itemstyle_opts=opts.ItemStyleOpts(color="#7fbe9e", opacity=0.5),
                  label_opts=opts.LabelOpts(is_show=False))
        .set_global_opts(
            legend_opts=opts.LegendOpts(is_show=False),
            yaxis_opts=opts.AxisOpts(
                axislabel_opts=opts.LabelOpts(is_show=False), # 隐藏 Y 轴标签
                axistick_opts=opts.AxisTickOpts(is_show=False), # 隐藏刻度
                splitline_opts=opts.SplitLineOpts(is_show=False) # 隐藏横向网格线线
            )
        )
    )
    
    # 均量线
    line_v = (
        Line()
        .add_xaxis(dates)
        .add_yaxis("VMA5", df['vma5'].tolist() if 'vma5' in df.columns else [], is_smooth=True,
                  linestyle_opts=opts.LineStyleOpts(color="#5470c6", width=1), label_opts=opts.LabelOpts(is_show=False))
        .add_yaxis("VMA10", df['vma10'].tolist() if 'vma10' in df.columns else [], is_smooth=True,
                  linestyle_opts=opts.LineStyleOpts(color="#fac858", width=1), label_opts=opts.LabelOpts(is_show=False))
        .add_yaxis("VMA20", df['vma20'].tolist() if 'vma20' in df.columns else [], is_smooth=True,
                  linestyle_opts=opts.LineStyleOpts(color="#ee6666", width=1), label_opts=opts.LabelOpts(is_show=False))
    )
    bar.overlap(line_v)
    
    grid = (
        Grid(init_opts=opts.InitOpts(theme=ThemeType.DARK, width="100%", height="550px"))
        .add(kline, grid_opts=opts.GridOpts(pos_left="8%", pos_right="5%", pos_top="8%", height="55%"))
        .add(bar, grid_opts=opts.GridOpts(pos_left="8%", pos_right="5%", pos_top="70%", height="15%"))
    )
    
    return grid
