# V3.6.5 单文件部署版
# 启动: streamlit run app_v3.6.5.py
import pandas as pd
import datetime
import calendar
import copy
import io
import streamlit as st

# 设置宽布局（必须是第一个 streamlit 命令）
st.set_page_config(page_title="北美全渠道智能分仓系统 V3.6.5", layout="wide")
# ============================================================
# 全局常量
# ============================================================
REGIONS = ['美西', '美东', 'GA', 'TX', 'CG']
TRANSFER_REGIONS = ['美西', '美东', 'GA', 'TX']  # CG 不参与调拨


# ============================================================
# 通用辅助函数
# ============================================================
def ratio_col_name(r):
    """返回占比列名"""
    return f'理论_{r.replace("美", "")}%' if r in ['美西', '美东'] else f'理论_{r}%'


def parse_in_transit(val):
    """解析 '2026-04-25:2000; 2026-05-10:500' → {date: qty}"""
    if pd.isna(val) or str(val).strip() == '':
        return {}
    res = {}
    clean = str(val).replace('；', ';').replace('：', ':')
    for part in clean.split(';'):
        if ':' in part:
            d_str, q_str = part.split(':')
            try:
                y, m, d = map(int, d_str.strip().split('-'))
                dt = datetime.date(y, m, d)
                res[dt] = res.get(dt, 0) + int(float(q_str.strip()))
            except Exception:
                pass
    return res


def dict_to_transit_str(d):
    """{date: qty} → '2026-04-25:2000; 2026-05-10:500'"""
    if not d:
        return ''
    return "; ".join([f"{dt.strftime('%Y-%m-%d')}:{int(round(q))}"
                      for dt, q in sorted(d.items()) if q > 0.5])


def merge_in_transits(series):
    """合并多行的在途批次"""
    merged = {}
    for val in series:
        for dt, q in parse_in_transit(val).items():
            merged[dt] = merged.get(dt, 0) + q
    return dict_to_transit_str(merged)


def aggregate_data(df):
    """
    同 SKU + 组别 的多行数据合并为一行
    （用于"同组别同SKU 汇总计算"开关）
    """
    grouped_records = []
    df_copy = df.copy()
    df_copy['SKU'] = df_copy['SKU'].fillna('Unknown')
    df_copy['组别'] = df_copy['组别'].fillna('Unknown')

    for (sku, group), group_df in df_copy.groupby(['SKU', '组别']):
        row = {'SKU': sku, '组别': group}
        # 店铺/运营 拼接去重
        row['店铺'] = ", ".join(list(dict.fromkeys(group_df['店铺'].dropna().astype(str))))
        row['运营'] = ", ".join(list(dict.fromkeys(group_df['运营'].dropna().astype(str))))
        # 发货量求和
        row['本次总发货量'] = group_df['本次总发货量'].sum()
        # 理论占比按发货量加权平均
        total_qty = row['本次总发货量']
        ratios = {}
        for r in REGIONS:
            col = ratio_col_name(r)
            if total_qty > 0:
                ratios[r] = (group_df[col] * group_df['本次总发货量']).sum() / total_qty
            else:
                ratios[r] = group_df[col].mean()
        # 归一化到 100
        tr = sum(ratios.values())
        for r in REGIONS:
            row[ratio_col_name(r)] = ratios[r] * 100 / tr if tr > 0 else 20
        # 在仓求和、在途合并
        for r in REGIONS:
            row[f'{r}_在仓'] = group_df[f'{r}_在仓'].sum()
            row[f'{r}_多批次在途'] = merge_in_transits(group_df[f'{r}_多批次在途'])
        # 月度预测求和
        for m in ['M1预测(当月)', 'M2预测(次月)', 'M3预测(第3月)', 'M4预测(第4月)', 'M5预测(第5月)']:
            row[m] = group_df[m].sum()
        grouped_records.append(row)
    return pd.DataFrame(grouped_records)


def round_preserve_sum(float_dict, target_sum):
    """
    四舍五入各区分配量，同时保证和 = target_sum
    用最大余数法补 1
    """
    clean = {k: (0.0 if pd.isna(v) else float(v)) for k, v in float_dict.items()}
    int_dict = {k: int(v) for k, v in clean.items()}
    remainder = {k: clean[k] - int_dict[k] for k in clean}
    diff = int(target_sum - sum(int_dict.values()))
    sorted_keys = sorted(remainder.keys(), key=lambda k: remainder[k], reverse=True)
    for i in range(min(diff, len(sorted_keys))):
        int_dict[sorted_keys[i]] += 1
    return int_dict


def row_to_key(row):
    """组别-运营-店铺 作为行唯一标识"""
    return f"{row.get('组别', '-')}-{row.get('运营', '-')}-{row.get('店铺', '-')}"


# ============================================================
# 推演前置：计算到港日与日销函数
# ============================================================
def compute_arrivals(transit_dict, earliest_etd, target_eta):
    """
    计算本次发货各区到港日
    不迟到区: arrival = target_eta
    迟到区:   arrival = earliest_etd + transit_days
    """
    d_diff = (target_eta - earliest_etd).days
    arrivals = {}
    for r in REGIONS:
        if transit_dict[r] <= d_diff:
            # 不迟到：逆向排期
            deadline = target_eta - datetime.timedelta(days=transit_dict[r])
            arrivals[r] = deadline + datetime.timedelta(days=transit_dict[r])  # = target_eta
        else:
            # 迟到：极速抢发
            arrivals[r] = earliest_etd + datetime.timedelta(days=transit_dict[r])
    return arrivals


def compute_deadlines(transit_dict, earliest_etd, target_eta):
    """计算本次发货各区最晚发货日"""
    d_diff = (target_eta - earliest_etd).days
    deadlines = {}
    for r in REGIONS:
        if transit_dict[r] <= d_diff:
            deadlines[r] = target_eta - datetime.timedelta(days=transit_dict[r])
        else:
            deadlines[r] = earliest_etd
    return deadlines


def build_daily_sales_fn(row, today):
    """根据行的 M1-M5 预测，返回一个 daily_sales(date) 函数"""
    forecasts = (
        float(row.get('M1预测(当月)', 0) or 0),
        float(row.get('M2预测(次月)', 0) or 0),
        float(row.get('M3预测(第3月)', 0) or 0),
        float(row.get('M4预测(第4月)', 0) or 0),
        float(row.get('M5预测(第5月)', 0) or 0),
    )

    def daily_sales(d_obj):
        m_diff = (d_obj.year - today.year) * 12 + d_obj.month - today.month
        m_idx = min(max(m_diff, 0), 4)
        days_in_m = calendar.monthrange(d_obj.year, d_obj.month)[1]
        return max(forecasts[m_idx] / days_in_m, 0.1)

    return daily_sales


# ============================================================
# V3.5 水池分配算法（完全保留，仅作内部使用）
# ============================================================
def waterpool_allocation(row, transit_dict, earliest_etd, target_eta,
                         south_linkage=False, q_ship_override=None):
    """
    V3.5 原版水池分配算法（含虚拟负债）
    输入：row（一行数据的 dict）、时效、日期、南部联动开关
    输出：{区: 分配量} 的整数字典，和 = q_ship

    算法核心:
    1. 先跑虚拟沙盘（含虚拟负债），得到 v_stock 末态
    2. 用 v_stock 做水池分配：让 (v_stock[r] + alloc[r]) / sum_total 接近理论占比
    3. 出现负数分配时剔除该区（已富余），重新分配

    注意: 本函数只负责决定"各区发多少"，不负责计算"最终占比/跨区/耗尽日"
          后者由 physical_simulation() 完成
    """
    today = datetime.date.today()  # 调用方用自己的 today；这里临时用
    # 注意：真实使用时 today 会由调用方通过上下文传入，此处占位
    # 实现上，我们让调用方外部先固定 today，函数内不再用系统时间
    raise NotImplementedError("请用 waterpool_allocation_v2，显式传入 today")


def waterpool_allocation_v2(row, transit_dict, earliest_etd, target_eta,
                            today, south_linkage=False, q_ship_override=None):
    """
    V3.5 原版水池分配算法（含虚拟负债），显式传入 today 参数。

    Returns:
        alloc_int: {区: 整数发货量} 满足 sum = q_ship
    """
    arrivals = compute_arrivals(transit_dict, earliest_etd, target_eta)
    daily_sales = build_daily_sales_fn(row, today)

    q_ship = float(row.get('本次总发货量', 0) or 0) if q_ship_override is None else q_ship_override

    raw_ratios = {r: float(row.get(ratio_col_name(r), 0) or 0) for r in REGIONS}
    tr = sum(raw_ratios.values())
    ratios = {r: raw_ratios[r] / tr if tr > 0 else 0.2 for r in REGIONS}

    in_wh = {r: float(row.get(f'{r}_在仓', 0) or 0) for r in REGIONS}
    in_transits = {r: parse_in_transit(row.get(f'{r}_多批次在途', '')) for r in REGIONS}

    max_arrival = max(arrivals.values()) if arrivals else today
    for r in REGIONS:
        if in_transits[r]:
            max_arrival = max(max_arrival, max(in_transits[r].keys()))

    # ----------- 步骤1：跑虚拟沙盘，拿到 v_stock 末态 -----------
    v_stock = in_wh.copy()
    days_to_sim = (max_arrival - today).days

    for d_idx in range(1, days_to_sim + 1):
        sim_date = today + datetime.timedelta(days=d_idx)
        # 到港入库（仅在途，不加本次发货 alloc，因为还没算出来）
        for r in REGIONS:
            if sim_date in in_transits[r]:
                v_stock[r] += in_transits[r][sim_date]

        # 激活区
        active_regions = [r for r in REGIONS if v_stock[r] > 0 or arrivals[r] <= sim_date]
        if not active_regions:
            continue

        ds = daily_sales(sim_date)
        unmet = 0.0
        for r in REGIONS:
            demand = ds * ratios[r]
            if r in active_regions:
                if arrivals[r] <= sim_date:
                    v_stock[r] -= demand  # 允许负（虚拟负债）
                else:
                    if v_stock[r] >= demand:
                        v_stock[r] -= demand
                    else:
                        unmet += (demand - v_stock[r])
                        v_stock[r] = 0.0
            else:
                unmet += demand

        if unmet > 0.001:
            capable_donors = [r for r in active_regions
                              if arrivals[r] <= sim_date or v_stock[r] > 0]
            if capable_donors:
                split = unmet / len(capable_donors)
                for r in capable_donors:
                    if arrivals[r] <= sim_date:
                        v_stock[r] -= split
                    else:
                        if v_stock[r] >= split:
                            v_stock[r] -= split
                        else:
                            v_stock[r] = 0.0

    # ----------- 步骤2：水池分配 -----------
    allocations = {r: 0.0 for r in REGIONS}
    pool = q_ship
    unallocated = list(REGIONS)
    wf_v_stock = v_stock.copy()
    wf_ratios = ratios.copy()
    ga_gets_all, tx_gets_all = False, False

    if south_linkage:
        total_sys = (sum(in_wh.values())
                     + sum(sum(v.values()) for v in in_transits.values())
                     + q_ship)
        if total_sys > 0:
            ga_target = total_sys * raw_ratios['GA'] / 100
            tx_target = total_sys * raw_ratios['TX'] / 100
            ga_actual = in_wh['GA'] + sum(in_transits['GA'].values())
            tx_actual = in_wh['TX'] + sum(in_transits['TX'].values())
            if ga_actual > ga_target or tx_actual > tx_target:
                unallocated.remove('GA')
                unallocated.remove('TX')
                unallocated.append('South')
                wf_v_stock['South'] = wf_v_stock['GA'] + wf_v_stock['TX']
                wf_ratios['South'] = wf_ratios['GA'] + wf_ratios['TX']
                if ga_actual > ga_target:
                    tx_gets_all = True
                else:
                    ga_gets_all = True

    while pool > 0.001 and unallocated:
        target_total = sum(wf_v_stock[r] for r in unallocated) + pool
        ratio_sum = sum(wf_ratios[r] for r in unallocated)
        if ratio_sum <= 0:
            break
        theoretical = {r: target_total * (wf_ratios[r] / ratio_sum) - wf_v_stock[r]
                       for r in unallocated}
        negatives = [r for r, v in theoretical.items() if v < 0]
        if negatives:
            for r in negatives:
                if r == 'South':
                    allocations['GA'] = 0.0
                    allocations['TX'] = 0.0
                else:
                    allocations[r] = 0.0
                unallocated.remove(r)
        else:
            for r in unallocated:
                if r == 'South':
                    if ga_gets_all:
                        allocations['GA'] = theoretical[r]
                        allocations['TX'] = 0.0
                    elif tx_gets_all:
                        allocations['TX'] = theoretical[r]
                        allocations['GA'] = 0.0
                else:
                    allocations[r] = theoretical[r]
                pool -= theoretical[r]
            break

    alloc_int = round_preserve_sum(allocations, q_ship)
    return alloc_int


# ============================================================
# 🆕 物理真实推演（V3.6.5 新引擎·核心）
# ============================================================
def physical_simulation(row, transit_dict, earliest_etd, target_eta,
                        today, alloc_int, sales_cutoff, end_date=None):
    """
    物理真实推演（唯一口径，替换 V3.5 所有输出指标计算）

    入参:
        row: 一行数据
        alloc_int: 本次发货各区分配量 (由 waterpool_allocation_v2 算出)
        sales_cutoff: 销售截止日
        end_date: 推演终点（None 表示推演至全网物理耗尽）

    返回 dict:
        final_ratio: {区: 占比%} 在 real_final_arrival 当天截取
        real_final_arrival: 最后一批"有货量"到港日
        cz_before_cutoff: 销售截止日前累计跨区订单数
        cz_to_end: 推演到 end_date（若给定）的累计跨区
        oos_date: 物理耗尽日
        sim_stock_at_end: end_date 那天的 sim_stock（若 end_date 给定）
        sim_stock_at_real_final: real_final_arrival 那天的 sim_stock

    算法要点（你确认过的）:
    - 起点：今天
    - 不允许虚拟负债：sim_stock 扣到 0 停
    - 全网零库存时 while 退出，剩余 unmet 丢单不计跨区
    - 跨区只在 donor 端累加（不双算）
    - real_final_arrival = max(在途批次有货的日期 ∪ alloc>0 的 arrival 日)
    """
    arrivals = compute_arrivals(transit_dict, earliest_etd, target_eta)
    daily_sales = build_daily_sales_fn(row, today)

    raw_ratios = {r: float(row.get(ratio_col_name(r), 0) or 0) for r in REGIONS}
    tr = sum(raw_ratios.values())
    ratios = {r: raw_ratios[r] / tr if tr > 0 else 0.2 for r in REGIONS}

    in_wh = {r: float(row.get(f'{r}_在仓', 0) or 0) for r in REGIONS}
    in_transits = {r: parse_in_transit(row.get(f'{r}_多批次在途', '')) for r in REGIONS}

    # 计算 real_final_arrival：只考虑"有货量"的到港事件
    candidate_dates = []
    for r in REGIONS:
        if alloc_int.get(r, 0) > 0:
            candidate_dates.append(arrivals[r])
        for dt, qty in in_transits[r].items():
            if qty > 0:
                candidate_dates.append(dt)

    if candidate_dates:
        real_final_arrival = max(candidate_dates)
    else:
        real_final_arrival = today  # 无未来进货事件

    # 推演终点：取 max(end_date 或 sales_cutoff, real_final_arrival) 保证能截到 final_ratio
    hard_end = end_date if end_date is not None else sales_cutoff
    sim_until = max(hard_end, real_final_arrival, sales_cutoff) + datetime.timedelta(days=30)

    sim_stock = in_wh.copy()
    cz_before_cutoff = 0.0
    cz_to_end = 0.0
    sim_stock_at_end = None
    sim_stock_at_real_final = None
    oos_date = None

    max_days = 3500  # 硬上限防死循环
    sim_date = today

    for d_idx in range(1, max_days + 1):
        sim_date = today + datetime.timedelta(days=d_idx)

        # ---- 到港入库 ----
        for r in REGIONS:
            if sim_date in in_transits[r]:
                sim_stock[r] += in_transits[r][sim_date]
            if sim_date == arrivals[r]:
                sim_stock[r] += alloc_int.get(r, 0)

        # ---- 按理论占比分配当日需求 ----
        ds = daily_sales(sim_date)
        ask = {r: 0.0 for r in REGIONS}
        for r in REGIONS:
            demand = ds * ratios[r]
            if sim_stock[r] >= demand:
                sim_stock[r] -= demand
            else:
                ask[r] = demand - sim_stock[r]
                sim_stock[r] = 0.0  # 扣到 0 就停（无虚拟负债）

        # ---- 跨区均摊 ----
        unmet = sum(ask.values())
        cz_today = 0.0
        while unmet > 0.001 and sum(sim_stock.values()) > 0.001:
            donors = [r for r in REGIONS if sim_stock[r] > 0]
            if not donors:
                break
            split = unmet / len(donors)
            unmet = 0.0
            for r in donors:
                if sim_stock[r] >= split:
                    sim_stock[r] -= split
                    cz_today += split
                else:
                    cz_today += sim_stock[r]
                    unmet += (split - sim_stock[r])
                    sim_stock[r] = 0.0
        # 若 donors 空了，剩余 unmet 即丢单，不计跨区

        # ---- 累加跨区 ----
        if sim_date <= sales_cutoff:
            cz_before_cutoff += cz_today
        if end_date is not None and sim_date <= end_date:
            cz_to_end += cz_today

        # ---- 截取 sim_stock 快照 ----
        if sim_date == real_final_arrival:
            sim_stock_at_real_final = {r: max(0, sim_stock[r]) for r in REGIONS}
        if end_date is not None and sim_date == end_date:
            sim_stock_at_end = {r: max(0, sim_stock[r]) for r in REGIONS}

        # ---- 耗尽日判定 ----
        total_stock = sum(sim_stock.values())
        if total_stock <= 0.001 and sim_date >= real_final_arrival and oos_date is None:
            oos_date = sim_date

        # ---- 终止条件 ----
        if sim_date >= sim_until and oos_date is not None:
            break

    # 边界处理
    if oos_date is None:
        oos_date = sim_date
    if sim_stock_at_real_final is None:
        sim_stock_at_real_final = {r: max(0, sim_stock[r]) for r in REGIONS}
    if end_date is not None and sim_stock_at_end is None:
        sim_stock_at_end = {r: max(0, sim_stock[r]) for r in REGIONS}

    # 最终占比
    total_final = sum(sim_stock_at_real_final.values())
    if total_final > 0:
        final_ratio = {r: sim_stock_at_real_final[r] / total_final * 100 for r in REGIONS}
    else:
        final_ratio = {r: 0.0 for r in REGIONS}

    return {
        'final_ratio': final_ratio,
        'real_final_arrival': real_final_arrival,
        'cz_before_cutoff': cz_before_cutoff,
        'cz_to_end': cz_to_end,
        'oos_date': oos_date,
        'sim_stock_at_end': sim_stock_at_end,
        'sim_stock_at_real_final': sim_stock_at_real_final,
    }


# ============================================================
# 便捷包装：计算一行的完整输出指标
# ============================================================
def compute_row_metrics(row, transit_dict, earliest_etd, target_eta,
                        today, sales_cutoff, south_linkage=False,
                        q_ship_override=None):
    """
    一站式计算一行的所有输出指标（主看板展示用）

    Returns dict:
        alloc: {区: 发货量} 整数
        deadlines: {区: 最晚发货日}
        arrivals: {区: 到港日}
        final_ratio: {区: 占比%}
        real_final_arrival: 日期
        cz_before_cutoff: 销售截止日前跨区单数
        oos_date: 耗尽日
    """
    alloc_int = waterpool_allocation_v2(
        row, transit_dict, earliest_etd, target_eta,
        today, south_linkage, q_ship_override
    )
    deadlines = compute_deadlines(transit_dict, earliest_etd, target_eta)
    arrivals = compute_arrivals(transit_dict, earliest_etd, target_eta)

    # 若被 override，临时改 row 的 '本次总发货量' 供推演用（不污染原 row）
    row_for_sim = row.copy() if isinstance(row, dict) else row.to_dict()
    if q_ship_override is not None:
        row_for_sim['本次总发货量'] = q_ship_override

    sim = physical_simulation(
        row_for_sim, transit_dict, earliest_etd, target_eta,
        today, alloc_int, sales_cutoff
    )

    return {
        'alloc': alloc_int,
        'deadlines': deadlines,
        'arrivals': arrivals,
        'final_ratio': sim['final_ratio'],
        'real_final_arrival': sim['real_final_arrival'],
        'cz_before_cutoff': sim['cz_before_cutoff'],
        'oos_date': sim['oos_date'],
    }


def compute_sandbox_metrics(row, alloc_int, transit_dict, earliest_etd, target_eta,
                            today, sales_cutoff, query_date):
    """
    沙盘查询：返回 query_date 那天的库存分布和累计跨区
    使用 compute_row_metrics 计算出的 alloc（已分配好的发货量）
    """
    row_for_sim = row.copy() if isinstance(row, dict) else row.to_dict()
    sim = physical_simulation(
        row_for_sim, transit_dict, earliest_etd, target_eta,
        today, alloc_int, sales_cutoff, end_date=query_date
    )

    sim_stock = sim['sim_stock_at_end']
    total = sum(sim_stock.values())
    pct = {r: (sim_stock[r] / total * 100 if total > 0 else 0) for r in REGIONS}

    return {
        'query_date': query_date,
        'sim_stock': sim_stock,
        'total': total,
        'pct': pct,
        'cz_to_end': sim['cz_to_end'],
    }


# ============================================================
# Step 1 自测
# ============================================================


# ============================================================
# 状态度量：SD/RQ/CZ
# ============================================================
def compute_row_status(row, transit_dict, earliest_etd, target_eta,
                       today, sales_cutoff, south_linkage=False):
    """
    计算一行的核心状态三元组：
    - SD: 实际可售天数（从今天到销售截止日，全网物理库存 > 0 的天数）
    - RQ: 剩余冗余量（销售截止日当天的全网物理库存）
    - CZ: 销售截止日前累计跨区订单数
    """
    # 先算水池分配（用于推演）
    alloc = waterpool_allocation_v2(
        row, transit_dict, earliest_etd, target_eta, today, south_linkage
    )
    # 物理推演
    sim = physical_simulation(
        row if isinstance(row, dict) else row.to_dict(),
        transit_dict, earliest_etd, target_eta,
        today, alloc, sales_cutoff, end_date=sales_cutoff
    )

    # SD 需要额外推演（精确统计"有货天数"）
    sd = compute_sd(row, alloc, transit_dict, earliest_etd, target_eta,
                    today, sales_cutoff)

    # RQ = 销售截止日当天的全网物理库存
    sim_stock_at_cutoff = sim['sim_stock_at_end']
    rq = sum(sim_stock_at_cutoff.values()) if sim_stock_at_cutoff else 0.0

    # CZ = 销售截止日前累计跨区订单
    cz = sim['cz_before_cutoff']

    return {
        'SD': sd,
        'RQ': rq,
        'CZ': cz,
        'alloc': alloc,
        'sim_stock_at_cutoff': sim_stock_at_cutoff,
    }


def compute_sd(row, alloc, transit_dict, earliest_etd, target_eta,
               today, sales_cutoff):
    """
    精确计算"实际可售天数" SD
    定义：从今天到销售截止日期间，全网物理库存 > 0 的天数
    """
    arrivals = compute_arrivals(transit_dict, earliest_etd, target_eta)
    daily_sales = build_daily_sales_fn(row, today)

    raw_ratios = {r: float(row.get(ratio_col_name(r), 0) or 0) for r in REGIONS}
    tr = sum(raw_ratios.values())
    ratios = {r: raw_ratios[r] / tr if tr > 0 else 0.2 for r in REGIONS}

    in_wh = {r: float(row.get(f'{r}_在仓', 0) or 0) for r in REGIONS}
    in_transits = {r: parse_in_transit(row.get(f'{r}_多批次在途', '')) for r in REGIONS}

    sim_stock = in_wh.copy()
    sd_count = 0
    sales_window = (sales_cutoff - today).days

    for d_idx in range(1, sales_window + 1):
        sim_date = today + datetime.timedelta(days=d_idx)

        # 到港入库
        for r in REGIONS:
            if sim_date in in_transits[r]:
                sim_stock[r] += in_transits[r][sim_date]
            if sim_date == arrivals[r]:
                sim_stock[r] += alloc.get(r, 0)

        # 消耗前先看全网总库存
        if sum(sim_stock.values()) > 0.001:
            sd_count += 1

        # 消耗
        ds = daily_sales(sim_date)
        ask = {r: 0.0 for r in REGIONS}
        for r in REGIONS:
            demand = ds * ratios[r]
            if sim_stock[r] >= demand:
                sim_stock[r] -= demand
            else:
                ask[r] = demand - sim_stock[r]
                sim_stock[r] = 0.0

        unmet = sum(ask.values())
        while unmet > 0.001 and sum(sim_stock.values()) > 0.001:
            donors = [r for r in REGIONS if sim_stock[r] > 0]
            if not donors:
                break
            split = unmet / len(donors)
            unmet = 0.0
            for r in donors:
                if sim_stock[r] >= split:
                    sim_stock[r] -= split
                else:
                    unmet += (split - sim_stock[r])
                    sim_stock[r] = 0.0

    return sd_count


# ============================================================
# 调拨执行函数（带物理上限保护）
# ============================================================
def apply_transfer(df, out_idx, in_idx, src_type, region, source_date, qty):
    """
    执行一次调拨（原地修改 df）
    带物理上限保护：qty 自动截断为实际可调量
    返回实际调拨量（可能 < qty）
    """
    if qty <= 0.5:
        return 0

    if src_type == '本次发货量':
        out_available = float(df.at[out_idx, '本次总发货量'])
        actual = min(qty, max(0, out_available))
        if actual <= 0.5:
            return 0
        df.at[out_idx, '本次总发货量'] = out_available - actual
        df.at[in_idx, '本次总发货量'] = float(df.at[in_idx, '本次总发货量']) + actual
        return actual

    elif src_type == '在仓':
        out_available = float(df.at[out_idx, f'{region}_在仓'])
        actual = min(qty, max(0, out_available))
        if actual <= 0.5:
            return 0
        df.at[out_idx, f'{region}_在仓'] = out_available - actual
        df.at[in_idx, f'{region}_在仓'] = float(df.at[in_idx, f'{region}_在仓']) + actual
        return actual

    elif src_type == '在途':
        out_tr = parse_in_transit(df.at[out_idx, f'{region}_多批次在途'])
        available = out_tr.get(source_date, 0)
        actual = min(qty, max(0, available))
        if actual <= 0.5:
            return 0
        out_tr[source_date] = available - actual
        if out_tr[source_date] <= 0.5:
            del out_tr[source_date]
        in_tr = parse_in_transit(df.at[in_idx, f'{region}_多批次在途'])
        in_tr[source_date] = in_tr.get(source_date, 0) + actual
        df.at[out_idx, f'{region}_多批次在途'] = dict_to_transit_str(out_tr)
        df.at[in_idx, f'{region}_多批次在途'] = dict_to_transit_str(in_tr)
        return actual

    return 0


def backup_rows(df, indices):
    """深拷贝指定行，用于回滚"""
    return {idx: df.loc[idx].to_dict() for idx in indices}


def restore_rows(df, backup):
    """恢复行"""
    for idx, row_dict in backup.items():
        for k, v in row_dict.items():
            df.at[idx, k] = v


# ============================================================
# 获取一行的可调出批次列表
# ============================================================
def get_shipment_source(row_dict):
    """获取"本次发货量"作为调拨源（仅一个）"""
    q = float(row_dict.get('本次总发货量', 0))
    if q > 0.5:
        return [('本次发货量', None, None, q)]  # (type, region, date, qty)
    return []


def get_stock_sources(row_dict):
    """获取所有"在仓+在途"的可调拨批次（不含本次发货量）"""
    sources = []
    for r in TRANSFER_REGIONS:
        in_wh = float(row_dict.get(f'{r}_在仓', 0))
        if in_wh > 0.5:
            sources.append(('在仓', r, None, in_wh))
        for dt, qty in parse_in_transit(row_dict.get(f'{r}_多批次在途', '')).items():
            if qty > 0.5:
                sources.append(('在途', r, dt, qty))
    return sources


# ============================================================
# 阶段1：冗余调拨
# ============================================================
def stage1_redundancy_transfer(df, transit_dict, earliest_etd, target_eta,
                               today, sales_cutoff, south_linkage=False):
    """
    阶段1 全局贪心冗余调拨
    返回: (transfer_records, df_after)
    """
    df = df.copy().reset_index(drop=True)
    transfer_records = []
    sales_window = (sales_cutoff - today).days

    for sku, sku_group in df.groupby('SKU'):
        sku_indices = sku_group.index.tolist()
        if len(sku_indices) < 2:
            continue

        max_iter = 50
        for iter_count in range(max_iter):
            # 计算当前所有行的状态
            status = {}
            for idx in sku_indices:
                st = compute_row_status(
                    df.loc[idx].to_dict(), transit_dict, earliest_etd, target_eta,
                    today, sales_cutoff, south_linkage
                )
                status[idx] = st

            # 识别冗余方 & 缺货方
            # 冗余方：RQ > 0（销售截止日还有剩余）
            redundant = [idx for idx in sku_indices if status[idx]['RQ'] > 0.5]
            # 缺货方：SD < 销售窗口天数（过程中存在断货，不论 RQ）
            #   - 包含真·缺货方（RQ=0，整体卖空）
            #   - 包含等待期伪缺货方（RQ>0，但启动期/中途有空窗）
            shortage = [idx for idx in sku_indices
                        if status[idx]['SD'] < sales_window - 0.5]

            if not redundant or not shortage:
                break

            # 全局贪心：枚举所有候选（本次发货量 + 在仓 + 在途 同台竞技），选 ΔSD 最大的
            best_action = None
            best_delta_sd = 0.5  # 最小改善阈值

            for g_idx in redundant:
                g_row = df.loc[g_idx].to_dict()

                # 收集所有可调批次（策略 A：本次发货量与在仓/在途平等竞争，由 ΔSD 决定胜负）
                all_sources = get_shipment_source(g_row) + get_stock_sources(g_row)
                if not all_sources:
                    continue

                for r_idx in shortage:
                    if r_idx == g_idx:
                        continue
                    r_status_old = status[r_idx]

                    for src_type, src_region, src_date, src_max in all_sources:
                        # 二分搜索最大可调量（满足 3 重锁）
                        best_qty = binary_search_max_transfer(
                            df, g_idx, r_idx, src_type, src_region, src_date, src_max,
                            transit_dict, earliest_etd, target_eta,
                            today, sales_cutoff, south_linkage,
                            sales_window, status[g_idx], status[r_idx]
                        )

                        if best_qty < 1:
                            continue

                        # 试探性执行，评估 ΔSD
                        backup = backup_rows(df, [g_idx, r_idx])
                        actual_q = apply_transfer(df, g_idx, r_idx,
                                                  src_type, src_region, src_date, best_qty)

                        if actual_q < 1:
                            restore_rows(df, backup)
                            continue

                        r_status_new = compute_row_status(
                            df.loc[r_idx].to_dict(), transit_dict, earliest_etd, target_eta,
                            today, sales_cutoff, south_linkage
                        )
                        delta_sd = r_status_new['SD'] - r_status_old['SD']

                        # 回滚
                        restore_rows(df, backup)

                        if delta_sd > best_delta_sd:
                            best_delta_sd = delta_sd
                            best_action = {
                                'g_idx': g_idx, 'r_idx': r_idx,
                                'src_type': src_type, 'src_region': src_region,
                                'src_date': src_date, 'qty': actual_q,
                                'delta_sd': delta_sd,
                            }

            if best_action is None:
                break

            # 执行最优动作
            ba = best_action
            actual_q = apply_transfer(df, ba['g_idx'], ba['r_idx'],
                                      ba['src_type'], ba['src_region'], ba['src_date'], ba['qty'])

            if actual_q < 1:
                break

            # 记录
            batch_str = ('本次发货量' if ba['src_type'] == '本次发货量'
                         else '在仓' if ba['src_type'] == '在仓'
            else f'在途 {ba["src_date"].strftime("%Y-%m-%d")}')

            transfer_records.append({
                'SKU': sku,
                '调拨类型': '冗余调拨',
                '调拨区域': ba['src_region'] if ba['src_region'] else '全局分配池',
                '调拨批次': batch_str,
                '调出方': row_to_key(df.loc[ba['g_idx']]),
                '调入方': row_to_key(df.loc[ba['r_idx']]),
                '调拨数量': int(round(actual_q)),
                '备注': f"挽回断货 {int(ba['delta_sd'])} 天",
            })

    return transfer_records, df


def binary_search_max_transfer(df, g_idx, r_idx, src_type, src_region, src_date, src_max,
                               transit_dict, earliest_etd, target_eta,
                               today, sales_cutoff, south_linkage,
                               sales_window, g_status, r_status):
    """
    二分搜索：找出"3 重锁"下最大的可调拨量
    锁1: qty <= src_max （物理上限）  → 已在 apply_transfer 保护
    锁2: 调出方调出后 SD 不下降（不变得"更缺货"）
    锁3: 调入方调入后 RQ 不增加（在缺货等待期内消耗的货才合法）
         - 若调入量 ≤ 等待期销量需求 → 货全部在等待期消耗 → RQ 不变 → 通过
         - 若调入量 > 等待期销量需求 → 多余货进入售卖期 → RQ 增加 → 拒绝
    """
    lo = 0.0
    hi = float(src_max)
    best = 0.0
    g_baseline_sd = g_status['SD']
    r_baseline_rq = r_status['RQ']

    for _ in range(15):
        if hi - lo < 1:
            break
        mid = (lo + hi) / 2

        # 试探
        backup = backup_rows(df, [g_idx, r_idx])
        actual = apply_transfer(df, g_idx, r_idx, src_type, src_region, src_date, mid)

        if actual < 1:
            restore_rows(df, backup)
            hi = mid
            continue

        # 评估锁2 & 锁3
        g_new = compute_row_status(
            df.loc[g_idx].to_dict(), transit_dict, earliest_etd, target_eta,
            today, sales_cutoff, south_linkage
        )
        r_new = compute_row_status(
            df.loc[r_idx].to_dict(), transit_dict, earliest_etd, target_eta,
            today, sales_cutoff, south_linkage
        )

        restore_rows(df, backup)

        # 锁2: 调出方 SD 不下降（不变得更缺货）
        lock2_ok = (g_new['SD'] >= g_baseline_sd - 0.5)
        # 锁3: 调入方 RQ 不超过基线（保证等待期内消耗）
        lock3_ok = (r_new['RQ'] <= r_baseline_rq + 0.5)

        if lock2_ok and lock3_ok:
            # 通过锁，继续尝试更大
            best = actual
            lo = actual
        else:
            # 超了，缩小
            hi = mid

    return best


# ============================================================
# 阶段2：独立减量
# ============================================================
def stage2_independent_reduction(df, transit_dict, earliest_etd, target_eta,
                                 today, sales_cutoff, south_linkage=False):
    """
    阶段2 独立减量
    公式: new_q_ship = q_ship - min(RQ, q_ship)
    """
    df = df.copy().reset_index(drop=True)
    reduce_records = []

    for idx, row in df.iterrows():
        row_dict = row.to_dict()
        q_ship = float(row_dict.get('本次总发货量', 0))

        if q_ship <= 0.5:
            continue

        # 计算当前 RQ
        status = compute_row_status(
            row_dict, transit_dict, earliest_etd, target_eta,
            today, sales_cutoff, south_linkage
        )
        rq = status['RQ']

        if rq <= 0.5:
            continue  # 不冗余，无需减量

        # 应用简化公式
        delta = min(rq, q_ship)
        new_q_ship = q_ship - delta

        if delta > 0.5:
            df.at[idx, '本次总发货量'] = new_q_ship
            reduce_records.append({
                'SKU': row_dict.get('SKU', '-'),
                '组别': row_dict.get('组别', '-'),
                '运营': row_dict.get('运营', '-'),
                '店铺': row_dict.get('店铺', '-'),
                '原发货量': int(round(q_ship)),
                '减量后发货量': int(round(new_q_ship)),
                '减量原因': f"冗余调拨后剩余 RQ={int(round(rq))}",
            })

    return reduce_records, df


# ============================================================
# 组合入口：阶段1+2 流水线
# ============================================================
def run_stage_1_and_2(df_baseline, transit_dict, earliest_etd, target_eta,
                      today, sales_cutoff, south_linkage=False):
    """
    执行阶段1（冗余调拨）+ 阶段2（独立减量）
    返回:
        transfer_records_s1: 阶段1 冗余调拨记录
        reduce_records_s2: 阶段2 减量记录
        df_after_s2: 完成阶段1+2 后的 DataFrame
    """
    # 确保数值列是 float 类型（避免 pandas int64 不接受浮点赋值）
    df = df_baseline.copy().reset_index(drop=True)
    numeric_cols = (['本次总发货量']
                    + [ratio_col_name(r) for r in REGIONS]
                    + [f'{r}_在仓' for r in REGIONS]
                    + ['M1预测(当月)', 'M2预测(次月)', 'M3预测(第3月)', 'M4预测(第4月)', 'M5预测(第5月)'])
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).astype(float)

    # 阶段1
    transfer_records_s1, df_after_s1 = stage1_redundancy_transfer(
        df, transit_dict, earliest_etd, target_eta,
        today, sales_cutoff, south_linkage
    )
    # 阶段2
    reduce_records_s2, df_after_s2 = stage2_independent_reduction(
        df_after_s1, transit_dict, earliest_etd, target_eta,
        today, sales_cutoff, south_linkage
    )

    return transfer_records_s1, reduce_records_s2, df_after_s2


# ============================================================
# Step 2 自测
# ============================================================


# ============================================================
# 候选预筛：哪些 (行A, 行B, 区域 X) 组合值得评估？
# ============================================================
def get_row_region_donor_capacity(row_dict, region):
    """
    返回该行在指定区域的可调出物理库存量
    （在仓 + 在途，不含本次发货量，因为本次发货量是分区前的）
    """
    capacity = 0.0
    capacity += float(row_dict.get(f'{region}_在仓', 0) or 0)
    for dt, qty in parse_in_transit(row_dict.get(f'{region}_多批次在途', '')).items():
        capacity += qty
    return capacity


def filter_partition_candidates(df, sku_indices, status_dict):
    """
    候选预筛（选项 C 简单预筛）：
    - 预筛1：行A 在区 X 有可调出物理库存 > 0
    - 预筛2：行B 当前 CZ > 0

    返回：[(行A_idx, 行B_idx, 区域 X)]
    """
    candidates = []
    for a_idx in sku_indices:
        a_row = df.loc[a_idx].to_dict()
        for b_idx in sku_indices:
            if a_idx == b_idx:
                continue
            # 预筛2：行B CZ > 0
            if status_dict[b_idx]['CZ'] <= 0.5:
                continue
            for region in TRANSFER_REGIONS:
                # 预筛1：行A 在 X 有可调出库存
                a_capacity = get_row_region_donor_capacity(a_row, region)
                if a_capacity > 0.5:
                    candidates.append((a_idx, b_idx, region))
    return candidates


# ============================================================
# 阶段3 分区调拨核心：评估单向 + 双向对调
# ============================================================
def evaluate_single_transfer(df, out_idx, in_idx, src_type, src_region, src_date, qty,
                             transit_dict, earliest_etd, target_eta,
                             today, sales_cutoff, south_linkage,
                             baseline_status):
    """
    评估单向调拨的合法性 + ΔCZ 改善
    锁1: qty <= 物理上限（apply 自带保护）
    锁2: 调出方 SD 不下降
    锁3: 调入方 SD 不下降
    锁4: 调入方调入后 RQ 不增加
    锁5: 调出方 CZ 不增加 且 调入方 CZ 严格下降

    返回 (ΔCZ_total, is_valid)
    """
    backup = backup_rows(df, [out_idx, in_idx])

    actual = apply_transfer(df, out_idx, in_idx, src_type, src_region, src_date, qty)
    if actual < 1:
        restore_rows(df, backup)
        return 0, False

    out_new = compute_row_status(
        df.loc[out_idx].to_dict(), transit_dict, earliest_etd, target_eta,
        today, sales_cutoff, south_linkage
    )
    in_new = compute_row_status(
        df.loc[in_idx].to_dict(), transit_dict, earliest_etd, target_eta,
        today, sales_cutoff, south_linkage
    )

    restore_rows(df, backup)

    out_old = baseline_status[out_idx]
    in_old = baseline_status[in_idx]

    # 锁2: 调出方 SD 不下降
    if out_new['SD'] < out_old['SD'] - 0.5:
        return 0, False
    # 锁3: 调入方 SD 不下降
    if in_new['SD'] < in_old['SD'] - 0.5:
        return 0, False
    # 锁4: 调入方 RQ 不增加
    if in_new['RQ'] > in_old['RQ'] + 0.5:
        return 0, False
    # 锁5: 调出方 CZ 不增加 且 调入方 CZ 严格下降
    if out_new['CZ'] > out_old['CZ'] + 0.5:
        return 0, False
    if in_new['CZ'] >= in_old['CZ'] - 0.5:
        return 0, False

    delta_cz = (out_old['CZ'] + in_old['CZ']) - (out_new['CZ'] + in_new['CZ'])
    return delta_cz, True


def evaluate_swap_transfer(df, a_idx, b_idx,
                           type_x, region_x, date_x, qty_x,
                           type_y, region_y, date_y, qty_y,
                           transit_dict, earliest_etd, target_eta,
                           today, sales_cutoff, south_linkage,
                           baseline_status):
    """
    评估双向对调原子动作: A 调 X → B + B 调 Y → A，两步必须同时合法
    锁同 evaluate_single_transfer，但对 a_idx 和 b_idx 各自检查

    返回 (ΔCZ_total, is_valid)
    """
    backup = backup_rows(df, [a_idx, b_idx])

    # 两步同时执行
    actual_x = apply_transfer(df, a_idx, b_idx, type_x, region_x, date_x, qty_x)
    actual_y = apply_transfer(df, b_idx, a_idx, type_y, region_y, date_y, qty_y)

    if actual_x < 1 or actual_y < 1:
        restore_rows(df, backup)
        return 0, False

    a_new = compute_row_status(
        df.loc[a_idx].to_dict(), transit_dict, earliest_etd, target_eta,
        today, sales_cutoff, south_linkage
    )
    b_new = compute_row_status(
        df.loc[b_idx].to_dict(), transit_dict, earliest_etd, target_eta,
        today, sales_cutoff, south_linkage
    )

    restore_rows(df, backup)

    a_old = baseline_status[a_idx]
    b_old = baseline_status[b_idx]

    # 锁2/3: 双方 SD 不下降
    if a_new['SD'] < a_old['SD'] - 0.5:
        return 0, False
    if b_new['SD'] < b_old['SD'] - 0.5:
        return 0, False
    # 锁4: 双方 RQ 不增加（对调中两边都既是调出方又是调入方）
    if a_new['RQ'] > a_old['RQ'] + 0.5:
        return 0, False
    if b_new['RQ'] > b_old['RQ'] + 0.5:
        return 0, False
    # 锁5: 双方 CZ 至少有一方严格下降，且总 ΔCZ > 0
    delta_cz = (a_old['CZ'] + b_old['CZ']) - (a_new['CZ'] + b_new['CZ'])
    if delta_cz <= 0.5:
        return 0, False

    return delta_cz, True


# ============================================================
# 二分搜索：找单向 / 对调的最大有效量
# ============================================================
def binary_search_single_transfer(df, out_idx, in_idx, src_type, src_region, src_date, src_max,
                                  transit_dict, earliest_etd, target_eta,
                                  today, sales_cutoff, south_linkage,
                                  baseline_status):
    """
    多档位扫描单向调拨最大有效量
    （ΔCZ 关于调拨量可能非单调，避免二分错失）
    """
    if src_max < 1:
        return 0.0, 0.0

    best_qty = 0.0
    best_delta = 0.0

    fracs = [0.05, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 0.95, 1.00]
    for frac in fracs:
        q = src_max * frac
        if q < 1:
            continue
        delta, valid = evaluate_single_transfer(
            df, out_idx, in_idx, src_type, src_region, src_date, q,
            transit_dict, earliest_etd, target_eta,
            today, sales_cutoff, south_linkage, baseline_status
        )
        if valid and delta > best_delta:
            best_delta = delta
            best_qty = q

    # 精细化（最优点附近 ±10%）
    if best_qty > 0:
        for adj in [-0.10, -0.05, -0.02, 0.02, 0.05, 0.10]:
            q = best_qty * (1 + adj)
            if q < 1 or q > src_max:
                continue
            delta, valid = evaluate_single_transfer(
                df, out_idx, in_idx, src_type, src_region, src_date, q,
                transit_dict, earliest_etd, target_eta,
                today, sales_cutoff, south_linkage, baseline_status
            )
            if valid and delta > best_delta:
                best_delta = delta
                best_qty = q

    return best_qty, best_delta


def binary_search_swap_transfer(df, a_idx, b_idx,
                                type_x, region_x, date_x, max_x,
                                type_y, region_y, date_y, max_y,
                                transit_dict, earliest_etd, target_eta,
                                today, sales_cutoff, south_linkage,
                                baseline_status):
    """
    多档位扫描双向对调最大有效量

    重要：ΔCZ 关于调拨量呈 V 字曲线（先降后升），不能用二分。
    这里用 12 档线性扫描，找最优点。
    """
    cap = min(max_x, max_y)
    if cap < 1:
        return 0.0, 0.0

    best_qty = 0.0
    best_delta = 0.0

    # 12 档线性扫描：5%, 10%, 20%, 30%, ..., 100%
    fracs = [0.05, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 0.95, 1.00]
    for frac in fracs:
        q = cap * frac
        if q < 1:
            continue
        delta, valid = evaluate_swap_transfer(
            df, a_idx, b_idx,
            type_x, region_x, date_x, q,
            type_y, region_y, date_y, q,
            transit_dict, earliest_etd, target_eta,
            today, sales_cutoff, south_linkage, baseline_status
        )
        if valid and delta > best_delta:
            best_delta = delta
            best_qty = q

    # 在最优档位附近做精细化搜索（前后 ±10% 范围内 5 档）
    if best_qty > 0:
        for adj in [-0.10, -0.05, -0.02, 0.02, 0.05, 0.10]:
            q = best_qty * (1 + adj)
            if q < 1 or q > cap:
                continue
            delta, valid = evaluate_swap_transfer(
                df, a_idx, b_idx,
                type_x, region_x, date_x, q,
                type_y, region_y, date_y, q,
                transit_dict, earliest_etd, target_eta,
                today, sales_cutoff, south_linkage, baseline_status
            )
            if valid and delta > best_delta:
                best_delta = delta
                best_qty = q

    return best_qty, best_delta


# ============================================================
# 阶段3：分区调拨主循环
# ============================================================
def stage3_partition_transfer(df, transit_dict, earliest_etd, target_eta,
                              today, sales_cutoff, south_linkage=False):
    """
    阶段3 全局贪心分区调拨
    返回: (transfer_records, df_after)
    """
    df = df.copy().reset_index(drop=True)
    transfer_records = []

    for sku, sku_group in df.groupby('SKU'):
        sku_indices = sku_group.index.tolist()
        if len(sku_indices) < 2:
            continue

        max_iter = 20  # 单SKU迭代上限（避免浮点误差导致的伪迭代）
        for iter_count in range(max_iter):
            # 计算所有行的基线状态
            baseline_status = {}
            for idx in sku_indices:
                baseline_status[idx] = compute_row_status(
                    df.loc[idx].to_dict(), transit_dict, earliest_etd, target_eta,
                    today, sales_cutoff, south_linkage
                )

            # 候选预筛
            single_candidates = filter_partition_candidates(df, sku_indices, baseline_status)

            best_action = None
            best_delta_cz = 5.0  # 改善阈值至少 5 单（避免浮点驱动的伪改善）

            # ----- 评估单向候选 -----
            for out_idx, in_idx, region in single_candidates:
                out_row = df.loc[out_idx].to_dict()
                # 该区可调批次：在仓 + 在途
                sources = []
                in_wh = float(out_row.get(f'{region}_在仓', 0))
                if in_wh > 0.5:
                    sources.append(('在仓', None, in_wh))
                for dt, qty in parse_in_transit(out_row.get(f'{region}_多批次在途', '')).items():
                    if qty > 0.5:
                        sources.append(('在途', dt, qty))

                for src_type, src_date, src_max in sources:
                    qty, delta = binary_search_single_transfer(
                        df, out_idx, in_idx, src_type, region, src_date, src_max,
                        transit_dict, earliest_etd, target_eta,
                        today, sales_cutoff, south_linkage, baseline_status
                    )
                    if qty < 1:
                        continue
                    if delta > best_delta_cz:
                        best_delta_cz = delta
                        best_action = {
                            'kind': 'single',
                            'a_idx': out_idx, 'b_idx': in_idx,
                            'type_x': src_type, 'region_x': region, 'date_x': src_date, 'qty_x': qty,
                            'delta_cz': delta,
                        }

            # ----- 评估双向对调候选 -----
            # 对每对行 (a, b)，每对不同区域 (X, Y) 试探
            for i_a, a_idx in enumerate(sku_indices):
                for b_idx in sku_indices[i_a + 1:]:
                    a_row = df.loc[a_idx].to_dict()
                    b_row = df.loc[b_idx].to_dict()

                    # 双方都需要 CZ > 0 才有对调价值
                    if baseline_status[a_idx]['CZ'] <= 0.5 or baseline_status[b_idx]['CZ'] <= 0.5:
                        continue

                    # 收集双方各区的所有可调批次
                    a_sources_by_region = {}
                    b_sources_by_region = {}
                    for region in TRANSFER_REGIONS:
                        a_in_wh = float(a_row.get(f'{region}_在仓', 0))
                        a_sources_by_region[region] = []
                        if a_in_wh > 0.5:
                            a_sources_by_region[region].append(('在仓', None, a_in_wh))
                        for dt, qty in parse_in_transit(a_row.get(f'{region}_多批次在途', '')).items():
                            if qty > 0.5:
                                a_sources_by_region[region].append(('在途', dt, qty))
                        b_in_wh = float(b_row.get(f'{region}_在仓', 0))
                        b_sources_by_region[region] = []
                        if b_in_wh > 0.5:
                            b_sources_by_region[region].append(('在仓', None, b_in_wh))
                        for dt, qty in parse_in_transit(b_row.get(f'{region}_多批次在途', '')).items():
                            if qty > 0.5:
                                b_sources_by_region[region].append(('在途', dt, qty))

                    # 枚举区域对 (X, Y)，X < Y 避免 (X,Y) 和 (Y,X) 重复
                    for ix, region_x in enumerate(TRANSFER_REGIONS):
                        if not a_sources_by_region[region_x]:
                            continue
                        for region_y in TRANSFER_REGIONS[ix + 1:]:
                            if not b_sources_by_region[region_y]:
                                continue

                            # 枚举源批次（A 的 X、B 的 Y）
                            for type_x, date_x, max_x in a_sources_by_region[region_x]:
                                for type_y, date_y, max_y in b_sources_by_region[region_y]:
                                    qty, delta = binary_search_swap_transfer(
                                        df, a_idx, b_idx,
                                        type_x, region_x, date_x, max_x,
                                        type_y, region_y, date_y, max_y,
                                        transit_dict, earliest_etd, target_eta,
                                        today, sales_cutoff, south_linkage,
                                        baseline_status
                                    )
                                    if qty < 1:
                                        continue
                                    if delta > best_delta_cz:
                                        best_delta_cz = delta
                                        best_action = {
                                            'kind': 'swap',
                                            'a_idx': a_idx, 'b_idx': b_idx,
                                            'type_x': type_x, 'region_x': region_x, 'date_x': date_x, 'qty_x': qty,
                                            'type_y': type_y, 'region_y': region_y, 'date_y': date_y, 'qty_y': qty,
                                            'delta_cz': delta,
                                        }
                            # 镜像：A 的 Y → B + B 的 X → A
                            for type_y, date_y, max_y in a_sources_by_region[region_y]:
                                for type_x, date_x, max_x in b_sources_by_region[region_x]:
                                    qty, delta = binary_search_swap_transfer(
                                        df, b_idx, a_idx,
                                        type_x, region_x, date_x, max_x,
                                        type_y, region_y, date_y, max_y,
                                        transit_dict, earliest_etd, target_eta,
                                        today, sales_cutoff, south_linkage,
                                        baseline_status
                                    )
                                    if qty < 1:
                                        continue
                                    if delta > best_delta_cz:
                                        best_delta_cz = delta
                                        best_action = {
                                            'kind': 'swap',
                                            'a_idx': b_idx, 'b_idx': a_idx,
                                            'type_x': type_x, 'region_x': region_x, 'date_x': date_x, 'qty_x': qty,
                                            'type_y': type_y, 'region_y': region_y, 'date_y': date_y, 'qty_y': qty,
                                            'delta_cz': delta,
                                        }

            if best_action is None:
                break

            # 执行最优动作
            ba = best_action
            if ba['kind'] == 'single':
                actual = apply_transfer(df, ba['a_idx'], ba['b_idx'],
                                        ba['type_x'], ba['region_x'], ba['date_x'], ba['qty_x'])
                if actual < 1:
                    break
                bs = ('在仓' if ba['type_x'] == '在仓'
                      else f'在途 {ba["date_x"].strftime("%Y-%m-%d")}')
                transfer_records.append({
                    'SKU': sku,
                    '调拨类型': '分区调拨',
                    '调拨区域': ba['region_x'],
                    '调拨批次': bs,
                    '调出方': row_to_key(df.loc[ba['a_idx']]),
                    '调入方': row_to_key(df.loc[ba['b_idx']]),
                    '调拨数量': int(round(actual)),
                    '备注': f"降跨区 {int(round(ba['delta_cz']))} 单",
                })
            else:  # swap
                # X 方向: A→B
                actual_x = apply_transfer(df, ba['a_idx'], ba['b_idx'],
                                          ba['type_x'], ba['region_x'], ba['date_x'], ba['qty_x'])
                # Y 方向: B→A
                actual_y = apply_transfer(df, ba['b_idx'], ba['a_idx'],
                                          ba['type_y'], ba['region_y'], ba['date_y'], ba['qty_y'])
                if actual_x < 1 or actual_y < 1:
                    break
                bs_x = ('在仓' if ba['type_x'] == '在仓'
                        else f'在途 {ba["date_x"].strftime("%Y-%m-%d")}')
                bs_y = ('在仓' if ba['type_y'] == '在仓'
                        else f'在途 {ba["date_y"].strftime("%Y-%m-%d")}')
                transfer_records.append({
                    'SKU': sku,
                    '调拨类型': '分区调拨(对调)',
                    '调拨区域': ba['region_x'],
                    '调拨批次': bs_x,
                    '调出方': row_to_key(df.loc[ba['a_idx']]),
                    '调入方': row_to_key(df.loc[ba['b_idx']]),
                    '调拨数量': int(round(actual_x)),
                    '备注': f"对调降运费（合计降 {int(round(ba['delta_cz']))} 单）",
                })
                transfer_records.append({
                    'SKU': sku,
                    '调拨类型': '分区调拨(对调)',
                    '调拨区域': ba['region_y'],
                    '调拨批次': bs_y,
                    '调出方': row_to_key(df.loc[ba['b_idx']]),
                    '调入方': row_to_key(df.loc[ba['a_idx']]),
                    '调拨数量': int(round(actual_y)),
                    '备注': f"对调降运费（合计降 {int(round(ba['delta_cz']))} 单）",
                })

    return transfer_records, df


# ============================================================
# 阶段4：死冗余报告
# ============================================================
def stage4_dead_redundancy_report(df, transit_dict, earliest_etd, target_eta,
                                  today, sales_cutoff, south_linkage=False):
    """
    阶段4 死冗余检测：经过阶段1+2+3 后仍 RQ > 0 的行
    """
    dead_records = []
    for idx, row in df.iterrows():
        row_dict = row.to_dict()
        st = compute_row_status(row_dict, transit_dict, earliest_etd, target_eta,
                                today, sales_cutoff, south_linkage)
        if st['RQ'] > 0.5:
            forecasts = (
                float(row_dict.get('M1预测(当月)', 0) or 0),
                float(row_dict.get('M2预测(次月)', 0) or 0),
                float(row_dict.get('M3预测(第3月)', 0) or 0),
                float(row_dict.get('M4预测(第4月)', 0) or 0),
                float(row_dict.get('M5预测(第5月)', 0) or 0),
            )
            avg_daily = max(sum(forecasts) / 150, 0.1)
            dead_days = int(round(st['RQ'] / avg_daily))
            dead_records.append({
                'SKU': row_dict.get('SKU', '-'),
                '组别': row_dict.get('组别', '-'),
                '运营': row_dict.get('运营', '-'),
                '店铺': row_dict.get('店铺', '-'),
                '死冗余量': int(round(st['RQ'])),
                '死冗余天数': dead_days,
            })
    return dead_records


# ============================================================
# 完整流水线：阶段1+2+3+4
# ============================================================
def run_full_pipeline(df_baseline, transit_dict, earliest_etd, target_eta,
                      today, sales_cutoff, south_linkage=False):
    """
    完整调拨流水线
    返回:
        s1_transfer: 阶段1 冗余调拨记录
        s2_reduce: 阶段2 减量记录
        s3_transfer: 阶段3 分区调拨记录
        s4_dead: 阶段4 死冗余记录
        df_after: 最终 DataFrame
    """
    # 数值列类型规范
    df = df_baseline.copy().reset_index(drop=True)
    numeric_cols = (['本次总发货量']
                    + [ratio_col_name(r) for r in REGIONS]
                    + [f'{r}_在仓' for r in REGIONS]
                    + ['M1预测(当月)', 'M2预测(次月)', 'M3预测(第3月)', 'M4预测(第4月)', 'M5预测(第5月)'])
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).astype(float)

    # 阶段1
    s1_transfer, df = stage1_redundancy_transfer(
        df, transit_dict, earliest_etd, target_eta, today, sales_cutoff, south_linkage
    )
    # 阶段2
    s2_reduce, df = stage2_independent_reduction(
        df, transit_dict, earliest_etd, target_eta, today, sales_cutoff, south_linkage
    )
    # 阶段3
    s3_transfer, df = stage3_partition_transfer(
        df, transit_dict, earliest_etd, target_eta, today, sales_cutoff, south_linkage
    )
    # 阶段4
    s4_dead = stage4_dead_redundancy_report(
        df, transit_dict, earliest_etd, target_eta, today, sales_cutoff, south_linkage
    )

    return s1_transfer, s2_reduce, s3_transfer, s4_dead, df


# ============================================================
# Step 3 自测
# ============================================================


# Step 4 UI
# ============================================================


# ============================================================
# 引入引擎模块（直接展开内联，方便单文件部署）
# ============================================================
# (本文件实际部署时，会把 step1/2/3 的代码全部内联进来)

st.title("📦 北美全渠道智能分仓控制塔 V3.6.5")
st.caption("分层双轨调拨版 · 物理真实推演 · 状态机交互")

with st.expander("📖 核心指标说明", expanded=False):
    st.markdown("""
    **核心日期**：
    - 📅 销售截止日：这批货应在该日期前售罄的业务底线日
    - 📅 最早可发货日期 / 目标上架时间：物流时间窗

    **状态指标**（V3.6.5 物理真实推演口径）：
    - 🚚 预估跨区订单数量：从今天到销售截止日的累计物理跨区订单
    - 🎯 最终全网占比估值：在 real_final_arrival 当天截取的物理库存占比
    - 📅 最终全网到货日：最后一批"有货量"到港的事件日期
    - 📅 预估全网耗尽日：物理全部卖空的日期

    **调拨流程**（4 阶段）：
    1. 阶段1 冗余调拨（救命）：把冗余方的货给缺货方
    2. 阶段2 独立减量：剩余冗余直接减本次发货量
    3. 阶段3 分区调拨（降本）：单向 + 双向对调，降低跨区运费
    4. 阶段4 死冗余报告：仍卖不掉的货
    """)

# ============================================================
# 侧边栏：日期与时效设置
# ============================================================
with st.sidebar:
    st.header("⚙️ 1. 全局时间与排期控制")
    today = datetime.date.today()
    st.info(f"今天 (Day 0): {today.strftime('%Y-%m-%d')}")

    default_sales_cutoff = today + datetime.timedelta(days=37 + 60)
    sales_cutoff = st.date_input(
        "📅 本批次发货最晚销售截止日期",
        value=default_sales_cutoff,
        help="这批货最晚应在该日期前售罄（默认目标上架日+60天）"
    )
    earliest_etd = st.date_input("📅 本批次最早可发货日期",
                                 value=today + datetime.timedelta(days=7))
    target_eta = st.date_input("🎯 本轮发货目标上架时间",
                               value=today + datetime.timedelta(days=37))

    if sales_cutoff <= target_eta:
        st.error("🚨 销售截止日必须晚于目标上架日！请调整。")
        st.stop()

    d_diff = (target_eta - earliest_etd).days
    if d_diff < 0:
        st.error("上架时间不能早于最早发货日期！")
        st.stop()
    st.success(f"⏳ 物流 D差: {d_diff} 天")
    st.success(f"📅 销售窗口: {(sales_cutoff - target_eta).days} 天")

    st.markdown("---")
    st.subheader("🚢 各区海运在途时效（天）")
    transit_times = {
        '美西': st.number_input("美西 (LA/LB)", value=25, step=1),
        '美东': st.number_input("美东 (NY/NJ)", value=45, step=1),
        'GA': st.number_input("美南 (GA)", value=45, step=1),
        'TX': st.number_input("美南 (TX)", value=45, step=1),
        'CG': st.number_input("CG多渠道", value=50, step=1)
    }
    d_diff_invalid = d_diff < min(transit_times.values())
    if d_diff_invalid:
        st.error(f"🚨 极速熔断：D差 ({d_diff}天) 小于最短海运时效 ({min(transit_times.values())}天)！")

# ============================================================
# 数据上传与编辑
# ============================================================
st.header("📥 2. 上传/输入业务数据")


def generate_excel_template():
    template = {
        'SKU': ['SKU-A', 'SKU-A'],
        '店铺': ['Shop-A', 'Shop-B'], '组别': ['二部', '三部'], '运营': ['张三', '李四'],
        '本次总发货量': [1000, 1000],
        '理论_西%': [25, 25], '理论_东%': [25, 25], '理论_GA%': [25, 25], '理论_TX%': [25, 25], '理论_CG%': [0, 0],
        '美西_在仓': [0, 0], '美东_在仓': [0, 0], 'GA_在仓': [0, 0], 'TX_在仓': [0, 0], 'CG_在仓': [0, 0],
        '美西_多批次在途': [f'{(today + datetime.timedelta(days=8)).strftime("%Y-%m-%d")}:2000', ''],
        '美东_多批次在途': ['', f'{(today + datetime.timedelta(days=8)).strftime("%Y-%m-%d")}:4000'],
        'GA_多批次在途': ['', ''], 'TX_多批次在途': ['', ''], 'CG_多批次在途': ['', ''],
        'M1预测(当月)': [1000, 1000], 'M2预测(次月)': [1000, 1000], 'M3预测(第3月)': [1000, 1000],
        'M4预测(第4月)': [1000, 1000], 'M5预测(第5月)': [1000, 1000]
    }
    df_tpl = pd.DataFrame(template)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_tpl.to_excel(writer, index=False, sheet_name='发货需求表')
    return output.getvalue()


col1, col2 = st.columns([1, 2])
with col1:
    st.download_button(
        "⬇️ 下载标准模板",
        data=generate_excel_template(),
        file_name='北美智能分仓模板_V3.6.5.xlsx',
        type="primary"
    )
with col2:
    uploaded_file = st.file_uploader("⬆️ 上传 Excel/CSV", type=["xlsx", "csv"])

if uploaded_file is not None:
    df_input = (pd.read_excel(uploaded_file)
                if uploaded_file.name.endswith('.xlsx')
                else pd.read_csv(uploaded_file))
else:
    # 默认示例：张三-李四经典互补场景
    df_input = pd.DataFrame({
        'SKU': ['SKU-A', 'SKU-A'],
        '店铺': ['Shop-A', 'Shop-B'], '组别': ['二部', '三部'], '运营': ['张三', '李四'],
        '本次总发货量': [1000, 1000],
        '理论_西%': [25, 25], '理论_东%': [25, 25], '理论_GA%': [25, 25], '理论_TX%': [25, 25], '理论_CG%': [0, 0],
        '美西_在仓': [0, 0], '美东_在仓': [0, 0], 'GA_在仓': [0, 0], 'TX_在仓': [0, 0], 'CG_在仓': [0, 0],
        '美西_多批次在途': [f'{(today + datetime.timedelta(days=8)).strftime("%Y-%m-%d")}:2000', ''],
        '美东_多批次在途': ['', f'{(today + datetime.timedelta(days=8)).strftime("%Y-%m-%d")}:4000'],
        'GA_多批次在途': ['', ''], 'TX_多批次在途': ['', ''], 'CG_多批次在途': ['', ''],
        'M1预测(当月)': [1000, 1000], 'M2预测(次月)': [1000, 1000], 'M3预测(第3月)': [1000, 1000],
        'M4预测(第4月)': [1000, 1000], 'M5预测(第5月)': [1000, 1000]
    })

edited_df = st.data_editor(df_input, num_rows="dynamic", use_container_width=True)


# ============================================================
# 计算主看板 + 沙盘所需的核心函数（基于 compute_row_metrics）
# ============================================================
def compute_main_board(df, transit_dict, earliest_etd, target_eta, today, sales_cutoff,
                       south_linkage=False):
    """
    输入：dataframe（含一行或多行 SKU 数据）
    输出：每行的主看板指标
    """
    results = []
    sales_window = (sales_cutoff - today).days

    for _, row in df.iterrows():
        row_dict = row.to_dict()
        m = compute_row_metrics(row_dict, transit_dict, earliest_etd, target_eta,
                                today, sales_cutoff, south_linkage)

        alloc = m['alloc']
        deadlines = m['deadlines']
        final_ratio = m['final_ratio']

        in_wh = {r: float(row_dict.get(f'{r}_在仓', 0) or 0) for r in REGIONS}
        in_transits = {r: parse_in_transit(row_dict.get(f'{r}_多批次在途', ''))
                       for r in REGIONS}
        q_ship = float(row_dict.get('本次总发货量', 0) or 0)
        total_sys = (sum(in_wh.values())
                     + sum(sum(v.values()) for v in in_transits.values())
                     + q_ship)

        if total_sys > 0:
            init_ratio_str = " : ".join([
                f"{(in_wh[r] + sum(in_transits[r].values()) + alloc[r]) / total_sys * 100:.0f}%"
                for r in REGIONS
            ])
        else:
            init_ratio_str = "0% : 0% : 0% : 0% : 0%"

        final_str = " : ".join([f"{final_ratio[r]:.0f}%" for r in REGIONS])

        def fmt_date(d_obj, amt):
            return d_obj.strftime('%Y-%m-%d') if amt > 0 else "-"

        # 建议减量：基于 RQ 的简化公式 q' = q - min(RQ, q)
        st_data = compute_row_status(row_dict, transit_dict, earliest_etd, target_eta,
                                     today, sales_cutoff, south_linkage)
        rq = st_data['RQ']
        is_redundant = (rq > 0.5)
        suggest_qty_str = "-"
        if is_redundant and q_ship > 0.5:
            new_q = q_ship - min(rq, q_ship)
            if new_q < 0.5:
                suggest_qty_str = "建议不发货"
            else:
                suggest_qty_str = str(int(round(new_q)))

        results.append({
            'SKU': row_dict.get('SKU', '-'),
            '店铺': row_dict.get('店铺', '-'),
            '组别': row_dict.get('组别', '-'),
            '运营': row_dict.get('运营', '-'),
            '👉 美西发货': alloc['美西'],
            '📅 美西最晚发货': fmt_date(deadlines['美西'], alloc['美西']),
            '👉 美东发货': alloc['美东'],
            '📅 美东最晚发货': fmt_date(deadlines['美东'], alloc['美东']),
            '👉 GA发货': alloc['GA'],
            '📅 GA最晚发货': fmt_date(deadlines['GA'], alloc['GA']),
            '👉 TX发货': alloc['TX'],
            '📅 TX最晚发货': fmt_date(deadlines['TX'], alloc['TX']),
            '👉 CG发货': alloc['CG'],
            '📅 CG最晚发货': fmt_date(deadlines['CG'], alloc['CG']),
            '📊 期初分区占比': init_ratio_str,
            '🎯 最终全网占比估值': final_str,
            '📅 最终全网到货日': m['real_final_arrival'].strftime('%Y-%m-%d'),
            '🚚 预估跨区订单数量': int(round(m['cz_before_cutoff'])),
            '📅 预估全网耗尽日': m['oos_date'].strftime('%Y-%m-%d'),
            '💡 建议减量至': suggest_qty_str,
            '_is_redundant': is_redundant
        })
    return pd.DataFrame(results)


# ============================================================
# 主看板交互
# ============================================================
st.header("🚀 3. 智能分仓指令看板")

col_btn1, col_btn2, col_btn3, col_btn4 = st.columns([2, 2, 2, 1])
with col_btn1:
    # 启用调拨时不允许聚合（汇总后无法做行间调拨）
    transfer_disabled_for_agg = False  # 占位，后续判断
    agg_on = st.checkbox("🔄 同组别同SKU 汇总计算",
                         value=False,
                         help="启用后，相同 SKU+组别 的多行数据会自动合并为一行计算")
with col_btn2:
    south_linkage = st.checkbox("🔘 美南仓 (GA/TX) 联动合并优化", value=False)
with col_btn3:
    transfer_on = st.checkbox("🔄 启用跨店库存调拨分析", value=False,
                              help="启用后，运算时同步计算 4 阶段调拨方案")
    if transfer_on and agg_on:
        st.warning("⚠️ 汇总计算和调拨分析互斥（汇总后只剩一行，无法做行间调拨）")
with col_btn4:
    btn_run = st.button("🚀 开始逆向推演运算", type="primary")

# Session State 初始化
SESSION_KEYS = [
    'baseline_df',  # S0 数据源（原始数据）
    'stage12_df',  # S1 数据源（阶段1+2 完成）
    'stage34_df',  # S2 数据源（阶段3+4 完成）
    'alloc_result_s0',  # S0 主看板
    'alloc_result_s1',  # S1 主看板
    'alloc_result_s2',  # S2 主看板
    'current_stage',  # 'S0' / 'S1' / 'S2'
    's12_records',  # 阶段1+2 记录（表1+表2-A）
    's34_records',  # 阶段3+4 记录（表2-B + 表4 + 对比表）
]
for k in SESSION_KEYS:
    if k not in st.session_state:
        st.session_state[k] = None

# ============================================================
# 执行运算
# ============================================================
if btn_run:
    if d_diff_invalid:
        st.error("❌ D差小于最短海运时效，无法计算！")
    else:
        # 数据规范化
        working_df = edited_df.copy()
        numeric_cols = (['本次总发货量']
                        + [ratio_col_name(r) for r in REGIONS]
                        + [f'{r}_在仓' for r in REGIONS]
                        + ['M1预测(当月)', 'M2预测(次月)', 'M3预测(第3月)',
                           'M4预测(第4月)', 'M5预测(第5月)'])
        for col in numeric_cols:
            if col in working_df.columns:
                working_df[col] = pd.to_numeric(working_df[col], errors='coerce').fillna(0.0).astype(float)
        for col in ['SKU', '店铺', '组别', '运营']:
            if col in working_df.columns:
                working_df[col] = working_df[col].fillna('-').astype(str)

        # 🆕 同组别同SKU 汇总计算（启用调拨时不允许聚合）
        if agg_on and not transfer_on:
            try:
                working_df = aggregate_data(working_df)
                # 聚合后再次保证数值列类型
                for col in numeric_cols:
                    if col in working_df.columns:
                        working_df[col] = pd.to_numeric(working_df[col], errors='coerce').fillna(0.0).astype(float)
                st.info(f"✅ 已按【SKU + 组别】汇总，共 {len(working_df)} 行")
            except Exception as e:
                st.error(f"汇总计算失败: {e}")
                st.stop()

        # 校验：理论占比和 = 100
        error_skus = []
        for _, row in working_df.iterrows():
            total_pct = sum([float(row[ratio_col_name(r)]) for r in REGIONS])
            if not (99.99 <= total_pct <= 100.01):
                error_skus.append(f"• 【{row['SKU']}】 理论占比和: {total_pct:.1f}%")
        if error_skus:
            st.error("❌ 数据校验失败！以下 SKU 的理论分区占比之和不等于 100%：")
            st.warning("\n".join(error_skus))
        else:
            # 清空旧状态
            for k in SESSION_KEYS:
                st.session_state[k] = None

            # 计算 S0：基线方案
            with st.spinner("正在计算 S0 基线方案..."):
                st.session_state['baseline_df'] = working_df.copy()
                st.session_state['alloc_result_s0'] = compute_main_board(
                    working_df, transit_times, earliest_etd, target_eta,
                    today, sales_cutoff, south_linkage
                )
                st.session_state['current_stage'] = 'S0'

            # 若启用调拨：跑阶段1+2，把结果暂存（等用户点击"确认救命方案"才进入 S1）
            if transfer_on:
                with st.spinner("正在计算阶段1+2（救命+减量）..."):
                    s1_records, s2_records, df_after_s12 = run_stage_1_and_2(
                        working_df, transit_times, earliest_etd, target_eta,
                        today, sales_cutoff, south_linkage
                    )
                    st.session_state['stage12_df'] = df_after_s12
                    st.session_state['s12_records'] = {
                        's1_transfer': s1_records,
                        's2_reduce': s2_records,
                    }

            st.success("✅ 计算完成！")
            st.rerun()

# ============================================================
# 主看板状态显示
# ============================================================
if st.session_state['alloc_result_s0'] is not None:
    stage = st.session_state['current_stage']

    # 状态标签
    stage_label_map = {
        'S0': "📊 当前显示：**原始基线方案**",
        'S1': "📊 当前显示：**救命+减量后的方案**（阶段1+2 完成）",
        'S2': "📊 当前显示：**完整调拨方案**（阶段3 完成）"
    }
    st.info(stage_label_map[stage])

    # 选择展示哪一份数据
    if stage == 'S0':
        cached_result = st.session_state['alloc_result_s0']
    elif stage == 'S1':
        cached_result = st.session_state['alloc_result_s1']
    else:
        cached_result = st.session_state['alloc_result_s2']

    # 过滤检索
    st.markdown("### 🔍 极速检索与结果过滤")
    col_f1, col_f2, col_f3, col_f4 = st.columns(4)
    with col_f1:
        search_sku = st.text_input("模糊搜索 SKU", placeholder="敲击回车即搜...")
    with col_f2:
        sel_shop = st.multiselect("过滤 店铺", options=cached_result['店铺'].unique())
    with col_f3:
        sel_group = st.multiselect("过滤 组别", options=cached_result['组别'].unique())
    with col_f4:
        sel_op = st.multiselect("过滤 运营", options=cached_result['运营'].unique())

    filtered = cached_result.copy()
    if search_sku:
        filtered = filtered[filtered['SKU'].str.contains(search_sku, case=False, na=False, regex=False)]
    if sel_shop:
        filtered = filtered[filtered['店铺'].apply(lambda x: any(s in x for s in sel_shop))]
    if sel_group:
        filtered = filtered[filtered['组别'].isin(sel_group)]
    if sel_op:
        filtered = filtered[filtered['运营'].apply(lambda x: any(o in x for o in sel_op))]


    def highlight(row):
        styles = [''] * len(row)
        try:
            if '🚚 预估跨区订单数量' in row.index and row['🚚 预估跨区订单数量'] > 0:
                styles[row.index.get_loc('🚚 预估跨区订单数量')] = (
                    'background-color: #fff3cd; color: #cc0000; font-weight: bold')
            if '_is_redundant' in row.index and row['_is_redundant']:
                styles[row.index.get_loc('📅 预估全网耗尽日')] = (
                    'background-color: #ffcccc; color: #990000; font-weight: bold')
        except (KeyError, ValueError):
            pass
        return styles


    display_cols = [c for c in filtered.columns if not c.startswith('_')]
    styled = filtered.style.apply(highlight, axis=1)
    hide_cols = [c for c in filtered.columns if c.startswith('_')]
    if hide_cols:
        styled = styled.hide(axis='columns', subset=hide_cols)
    st.dataframe(styled, use_container_width=True)

    csv_data = filtered[display_cols].to_csv(index=False).encode('utf-8-sig')
    st.download_button(
        f"📥 导出当前视图（{stage}）",
        data=csv_data,
        file_name=f'装柜排期_{stage}_{today.strftime("%Y%m%d")}.csv',
        mime='text/csv'
    )

# ============================================================
# 调拨板块第1部分（S0 状态展示）
# ============================================================
if (st.session_state['current_stage'] == 'S0'
        and st.session_state.get('s12_records') is not None):
    st.markdown("---")
    st.header("🔴 4. 阶段1+2：救命与减量方案")

    s12 = st.session_state['s12_records']
    s1_transfer = s12['s1_transfer']
    s2_reduce = s12['s2_reduce']

    # 表1：减量明细
    st.markdown("#### 📉 表1·减量明细（阶段2）")
    if s2_reduce:
        df_reduce = pd.DataFrame(s2_reduce)
        st.dataframe(df_reduce, use_container_width=True)
    else:
        st.info("✅ 无需减量：所有 SKU 整体不冗余，或冗余已被冗余调拨完全消化。")

    # 表2-A：冗余调拨记录
    st.markdown("#### 🔄 表2-A·冗余调拨指令（阶段1）")
    if s1_transfer:
        df_t1 = pd.DataFrame(s1_transfer)
        st.dataframe(df_t1, use_container_width=True)
    else:
        st.info("ℹ️ 无冗余调拨发生（无 SKU 同时存在冗余方与缺货方）。")

    # 效果摘要
    if s1_transfer or s2_reduce:
        st.markdown("#### 📊 阶段1+2 效果摘要")
        baseline = st.session_state['baseline_df']
        after = st.session_state['stage12_df']
        sales_window = (sales_cutoff - today).days

        baseline_total_rq = 0
        baseline_total_short = 0
        after_total_rq = 0
        after_total_short = 0
        for _, row in baseline.iterrows():
            st_b = compute_row_status(row.to_dict(), transit_times, earliest_etd,
                                      target_eta, today, sales_cutoff, south_linkage)
            baseline_total_rq += st_b['RQ']
            baseline_total_short += max(0, sales_window - st_b['SD'])
        for _, row in after.iterrows():
            st_a = compute_row_status(row.to_dict(), transit_times, earliest_etd,
                                      target_eta, today, sales_cutoff, south_linkage)
            after_total_rq += st_a['RQ']
            after_total_short += max(0, sales_window - st_a['SD'])

        col_s1, col_s2 = st.columns(2)
        with col_s1:
            st.metric("全网总冗余量",
                      f"{int(baseline_total_rq)} → {int(after_total_rq)}",
                      delta=f"{int(after_total_rq - baseline_total_rq)}")
        with col_s2:
            st.metric("全网总缺货天数（累计）",
                      f"{int(baseline_total_short)} → {int(after_total_short)}",
                      delta=f"{int(after_total_short - baseline_total_short)}",
                      delta_color="inverse")

    # 控制按钮
    st.markdown("---")
    col_b1, col_b2 = st.columns([1, 1])
    with col_b1:
        if st.button("✅ 确认救命方案，主看板刷新并继续优化", type="primary"):
            with st.spinner("正在刷新主看板 + 计算阶段3+4..."):
                # 主看板基于 stage12_df 刷新
                st.session_state['alloc_result_s1'] = compute_main_board(
                    st.session_state['stage12_df'], transit_times,
                    earliest_etd, target_eta, today, sales_cutoff, south_linkage
                )

                # 阶段3+4
                s3_transfer, df_after_s3 = stage3_partition_transfer(
                    st.session_state['stage12_df'], transit_times,
                    earliest_etd, target_eta, today, sales_cutoff, south_linkage
                )
                s4_dead = stage4_dead_redundancy_report(
                    df_after_s3, transit_times, earliest_etd, target_eta,
                    today, sales_cutoff, south_linkage
                )
                st.session_state['stage34_df'] = df_after_s3
                st.session_state['s34_records'] = {
                    's3_transfer': s3_transfer,
                    's4_dead': s4_dead,
                }
                st.session_state['current_stage'] = 'S1'
            st.rerun()

# ============================================================
# 调拨板块第2部分（S1/S2 状态展示）
# ============================================================
if st.session_state['current_stage'] in ('S1', 'S2'):
    st.markdown("---")
    st.header("⚖️ 5. 阶段3+4：降本与死冗余报告")

    s34 = st.session_state.get('s34_records', {})
    s3_transfer = s34.get('s3_transfer', [])
    s4_dead = s34.get('s4_dead', [])

    # 表2-B：分区调拨指令
    st.markdown("#### 🔄 表2-B·分区调拨指令（阶段3）")
    if s3_transfer:
        df_t3 = pd.DataFrame(s3_transfer)
        st.dataframe(df_t3, use_container_width=True)
    else:
        st.info("✅ 无可行分区调拨：当前方案在跨区维度已接近最优。")

    # 表3：调拨前后对比
    st.markdown("#### 📊 表3·调拨前后占比与跨区订单对比（基准 = S0 原始基线）")
    baseline = st.session_state['baseline_df']
    final_df = st.session_state['stage34_df']
    compare_records = []
    for idx in baseline.index:
        if idx not in final_df.index:
            continue
        old_row = baseline.loc[idx].to_dict()
        new_row = final_df.loc[idx].to_dict()
        # 用 compute_row_metrics 拿最终占比和跨区
        m_old = compute_row_metrics(old_row, transit_times, earliest_etd, target_eta,
                                    today, sales_cutoff, south_linkage)
        m_new = compute_row_metrics(new_row, transit_times, earliest_etd, target_eta,
                                    today, sales_cutoff, south_linkage)
        old_ratio = m_old['final_ratio']
        new_ratio = m_new['final_ratio']
        old_cz = int(round(m_old['cz_before_cutoff']))
        new_cz = int(round(m_new['cz_before_cutoff']))
        cz_improve = old_cz - new_cz

        theory = {r: float(old_row.get(ratio_col_name(r), 0) or 0) for r in REGIONS}
        compare_records.append({
            'SKU': old_row['SKU'],
            '运营-店铺': f"{old_row.get('运营', '-')}-{old_row.get('店铺', '-')}",
            '理论占比 (西:东:GA:TX:CG)': ":".join([f"{theory[r]:.0f}" for r in REGIONS]),
            '调拨前最终占比': ":".join([f"{old_ratio[r]:.0f}" for r in REGIONS]),
            '调拨后最终占比': ":".join([f"{new_ratio[r]:.0f}" for r in REGIONS]),
            '调拨前跨区单数': old_cz,
            '调拨后跨区单数': new_cz,
            '跨区单数改善': f"{-cz_improve:+d}" if cz_improve != 0 else "0",
        })

    if compare_records:
        # 加全局汇总
        total_old = sum(r['调拨前跨区单数'] for r in compare_records)
        total_new = sum(r['调拨后跨区单数'] for r in compare_records)
        compare_records.append({
            'SKU': '🌎 全局汇总',
            '运营-店铺': '-',
            '理论占比 (西:东:GA:TX:CG)': '-',
            '调拨前最终占比': '-',
            '调拨后最终占比': '-',
            '调拨前跨区单数': total_old,
            '调拨后跨区单数': total_new,
            '跨区单数改善': f"{total_old - total_new:+d}".replace('+-', '-') if total_old != total_new else "0",
        })
        df_cmp = pd.DataFrame(compare_records)


        def color_cz(row):
            styles = [''] * len(row)
            if row['SKU'] == '🌎 全局汇总':
                styles = ['background-color: #e6f2ff; font-weight: bold'] * len(row)
            cz_str = str(row['跨区单数改善'])
            if cz_str.startswith('-') and cz_str != '0':
                idx_loc = row.index.get_loc('跨区单数改善')
                styles[idx_loc] += '; color: #70AD47; font-weight: bold'
            elif cz_str.startswith('+'):
                idx_loc = row.index.get_loc('跨区单数改善')
                styles[idx_loc] += '; color: #C00000; font-weight: bold'
            return styles


        st.dataframe(df_cmp.style.apply(color_cz, axis=1), use_container_width=True)

    # 表4：死冗余报告
    st.markdown("#### 🔴 表4·死冗余预警（阶段4）")
    if s4_dead:
        df_dead = pd.DataFrame(s4_dead)
        st.dataframe(
            df_dead.style.apply(lambda r: ['background-color: #ffe6e6; color: #990000'] * len(r), axis=1),
            use_container_width=True
        )
        st.warning("⚠️ 上述 SKU 在调拨完成后仍有无法消化的库存，请关注。")
    else:
        st.success("✅ 无死冗余：所有行都能在销售截止日前售罄。")

    # 控制按钮
    st.markdown("---")
    if st.session_state['current_stage'] == 'S1':
        col_a1, col_a2 = st.columns([1, 1])
        with col_a1:
            if st.button("✅ 应用全部方案，主看板刷新为最终状态", type="primary"):
                with st.spinner("正在应用最终方案..."):
                    st.session_state['alloc_result_s2'] = compute_main_board(
                        st.session_state['stage34_df'], transit_times,
                        earliest_etd, target_eta, today, sales_cutoff, south_linkage
                    )
                    st.session_state['current_stage'] = 'S2'
                st.rerun()
        with col_a2:
            if st.button("⏪ 全部撤销，回到原始基线"):
                st.session_state['current_stage'] = 'S0'
                st.rerun()
    else:  # S2
        col_a1, col_a2 = st.columns([1, 1])
        with col_a1:
            if st.button("↩️ 撤销分区调拨，回到救命方案"):
                st.session_state['current_stage'] = 'S1'
                st.rerun()
        with col_a2:
            if st.button("⏪ 全部撤销，回到原始基线"):
                st.session_state['current_stage'] = 'S0'
                st.rerun()

# ============================================================
# 时空沙盘
# ============================================================
st.markdown("---")
st.header("🕰️ 6. 时空沙盘：穿越任意日期推演")

col_d1, col_d2 = st.columns([1, 4])
with col_d1:
    target_query = st.date_input("选择查询日期", value=target_eta, key='sandbox_date')
    sandbox_btn = st.button("🚀 穿越至该日推演", type="secondary")

with col_d2:
    if sandbox_btn:
        if st.session_state['alloc_result_s0'] is None:
            st.warning("⚠️ 请先点击【开始逆向推演运算】生成方案！")
        else:
            stage = st.session_state['current_stage']

            # 取当前 stage 对应的 working_df
            if stage == 'S0':
                working_df = st.session_state['baseline_df']
            elif stage == 'S1':
                working_df = st.session_state['stage12_df']
            else:
                working_df = st.session_state['stage34_df']

            sandbox_results = []
            for _, row in working_df.iterrows():
                row_dict = row.to_dict()
                # 用主看板的 alloc 算沙盘
                m = compute_row_metrics(row_dict, transit_times, earliest_etd, target_eta,
                                        today, sales_cutoff, south_linkage)
                sb = compute_sandbox_metrics(
                    row_dict, m['alloc'], transit_times,
                    earliest_etd, target_eta, today, sales_cutoff, target_query
                )

                pct = sb['pct']
                sim_stock = sb['sim_stock']
                sandbox_results.append({
                    'SKU': row_dict.get('SKU', '-'),
                    '店铺': row_dict.get('店铺', '-'),
                    '组别': row_dict.get('组别', '-'),
                    '运营': row_dict.get('运营', '-'),
                    f'📅 {target_query} 总库存': int(sb['total']),
                    '🇺🇸 实际占比 (西:东:GA:TX:CG)': " : ".join([f"{pct[r]:.0f}%" for r in REGIONS]),
                    '🚚 累计跨区订单': int(round(sb['cz_to_end'])),
                    '美西结存': int(sim_stock['美西']),
                    '美东结存': int(sim_stock['美东']),
                    'GA结存': int(sim_stock['GA']),
                    'TX结存': int(sim_stock['TX']),
                    'CG结存': int(sim_stock['CG']),
                })

            if sandbox_results:
                st.success(f"✅ 已推演至 {target_query}（{stage} 数据）")
                st.dataframe(pd.DataFrame(sandbox_results), use_container_width=True)
                st.caption(f"💡 自洽性提示：当查询日期 = 销售截止日 ({sales_cutoff}) 时，"
                           f"沙盘累计跨区应等于主看板预估跨区订单数。")