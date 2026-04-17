import streamlit as st
import pandas as pd
import datetime
import calendar
import io
import copy

# ==========================================
# 页面基础配置与标题
# ==========================================
st.set_page_config(page_title="北美全渠道智能分仓系统 V3.6", layout="wide")
st.title("📦 北美全渠道智能分仓控制塔 (V3.6 冗余提醒与跨店调拨版)")

with st.expander("📖 核心指标与业务定义说明 (点击展开/收起)", expanded=False):
    st.markdown("""
    * **📅 本批次发货最晚销售截止日期**：这批货最晚应在该日期前售罄的业务底线日（新增）。
    * **本批次最早可发货日期**：业务/工厂能将货物发出（离厂/离港）的最早物理底线日。
    * **🎯 本轮发货目标上架时间**：业务期望的这批货在全美各仓完成上架的目标锚点日。
    * **⏳ 物流 D差 (操作时间窗)**：`目标上架时间 - 最早可发货日期`。
    * **🚚 预估跨区订单数量**：从今天起至推演日历结束，因各仓断货不均触发的【跨区代发单量】总和。
    * **📊 期初分区占比**：`(本区在仓 + 本区所有在途 + 本次分配给该区的发货量) / 全网总盘子`。
    * **🎯 最终全网占比估值**：沙盘推演至【最后一批货全部到港的那一天】的动态各区占比。
    * **💡 建议减量至**：若耗尽日晚于销售截止日（冗余），反向推演得出让耗尽日恰好等于销售截止日的最优发货量（新增）。
    * **🔄 跨店库存调拨**：同SKU跨组别/运营/店铺的虚拟货权调拨，用于拉平各方占比偏差（含"一方冗余一方缺货"互补场景）（新增）。
    """)

# ==========================================
# 侧边栏
# ==========================================
with st.sidebar:
    st.header("⚙️ 1. 全局时间与排期控制")
    today = datetime.date.today()
    st.info(f"今天 (Day 0): {today.strftime('%Y-%m-%d')}")

    # 🆕 V3.6：销售截止日（最顶部）
    default_sales_cutoff = today + datetime.timedelta(days=37 + 60)
    sales_cutoff = st.date_input(
        "📅 本批次发货最晚销售截止日期",
        value=default_sales_cutoff,
        help="这批货最晚应在该日期前售罄（默认目标上架日+60天）"
    )

    earliest_etd = st.date_input("本批次最早可发货日期", value=today + datetime.timedelta(days=7))
    target_eta = st.date_input("🎯 本轮发货目标上架时间", value=today + datetime.timedelta(days=37))

    if sales_cutoff <= target_eta:
        st.error("🚨 销售截止日必须晚于目标上架日！请调整。")
        st.stop()

    d_diff = (target_eta - earliest_etd).days
    if d_diff < 0:
        st.error("上架时间不能早于最早发货日期！")
        st.stop()

    st.success(f"⏳ 物流 D差 (操作时间窗): {d_diff} 天")
    st.success(f"📅 销售窗口 (目标上架→销售截止): {(sales_cutoff - target_eta).days} 天")

    st.markdown("---")
    st.subheader("🚢 各区海运在途时效 (天)")
    transit_times = {
        '美西': st.number_input("美西 (LA/LB)", value=25, step=1),
        '美东': st.number_input("美东 (NY/NJ)", value=45, step=1),
        'GA': st.number_input("美南 (GA)", value=45, step=1),
        'TX': st.number_input("美南 (TX)", value=45, step=1),
        'CG': st.number_input("CG多渠道", value=50, step=1)
    }

    d_diff_invalid = d_diff < min(transit_times.values())
    if d_diff_invalid:
        st.error(f"🚨 极速熔断：D差 ({d_diff}天) 小于全网最短海运时效 ({min(transit_times.values())}天)！")

regions = ['美西', '美东', 'GA', 'TX', 'CG']
transfer_regions = ['美西', '美东', 'GA', 'TX']  # 🆕 CG不参与调拨


# ==========================================
# 通用辅助函数
# ==========================================
def ratio_col_name(r):
    """返回理论占比列名"""
    return f'理论_{r.replace("美", "")}%' if r in ['美西', '美东'] else f'理论_{r}%'


def parse_in_transit(val):
    if pd.isna(val) or str(val).strip() == '': return {}
    res = {}
    clean_str = str(val).replace('；', ';').replace('：', ':')
    for part in clean_str.split(';'):
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
    if not d: return ''
    return "; ".join([f"{dt.strftime('%Y-%m-%d')}:{int(round(q))}" for dt, q in sorted(d.items()) if q > 0.5])


def merge_in_transits(series):
    merged = {}
    for val in series:
        parsed = parse_in_transit(val)
        for d, q in parsed.items():
            merged[d] = merged.get(d, 0) + q
    return dict_to_transit_str(merged)


def aggregate_data(df):
    grouped_records = []
    df_copy = df.copy()
    df_copy['SKU'] = df_copy['SKU'].fillna('Unknown')
    df_copy['组别'] = df_copy['组别'].fillna('Unknown')

    for (sku, group), group_df in df_copy.groupby(['SKU', '组别']):
        row = {'SKU': sku, '组别': group}
        row['店铺'] = ", ".join(list(dict.fromkeys(group_df['店铺'].dropna().astype(str))))
        row['运营'] = ", ".join(list(dict.fromkeys(group_df['运营'].dropna().astype(str))))
        row['本次总发货量'] = group_df['本次总发货量'].sum()

        total_qty = row['本次总发货量']
        ratios = {}
        for r in regions:
            col = ratio_col_name(r)
            if total_qty > 0:
                ratios[r] = (group_df[col] * group_df['本次总发货量']).sum() / total_qty
            else:
                ratios[r] = group_df[col].mean()

        tr = sum(ratios.values())
        for r in regions:
            row[ratio_col_name(r)] = ratios[r] * 100 / tr if tr > 0 else 20

        for r in regions:
            row[f'{r}_在仓'] = group_df[f'{r}_在仓'].sum()
            row[f'{r}_多批次在途'] = merge_in_transits(group_df[f'{r}_多批次在途'])

        for m in ['M1预测(当月)', 'M2预测(次月)', 'M3预测(第3月)', 'M4预测(第4月)', 'M5预测(第5月)']:
            row[m] = group_df[m].sum()

        grouped_records.append(row)
    return pd.DataFrame(grouped_records)


def generate_excel_template():
    template_data = {
        'SKU': ['SKU-A', 'SKU-A'],
        '店铺': ['Shop-A', 'Shop-B'], '组别': ['二部', '三部'], '运营': ['张三', '李四'],
        '本次总发货量': [1000, 1000],
        '理论_西%': [25, 25], '理论_东%': [25, 25], '理论_GA%': [25, 25], '理论_TX%': [25, 25], '理论_CG%': [0, 0],
        '美西_在仓': [0, 0], '美东_在仓': [0, 0], 'GA_在仓': [0, 0], 'TX_在仓': [0, 0], 'CG_在仓': [0, 0],
        '美西_多批次在途': [f'{(today + datetime.timedelta(days=8)).strftime("%Y-%m-%d")}:2000', ''],
        '美东_多批次在途': ['', f'{(today + datetime.timedelta(days=8)).strftime("%Y-%m-%d")}:2000'],
        'GA_多批次在途': ['', ''], 'TX_多批次在途': ['', ''], 'CG_多批次在途': ['', ''],
        'M1预测(当月)': [3000, 3000], 'M2预测(次月)': [3000, 3000], 'M3预测(第3月)': [3000, 3000],
        'M4预测(第4月)': [3000, 3000], 'M5预测(第5月)': [3000, 3000]
    }
    df_tpl = pd.DataFrame(template_data)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_tpl.to_excel(writer, index=False, sheet_name='发货需求表')
    return output.getvalue()


def round_preserve_sum(float_dict, target_sum):
    clean_dict = {k: (0.0 if pd.isna(v) else float(v)) for k, v in float_dict.items()}
    int_dict = {k: int(v) for k, v in clean_dict.items()}
    remainder = {k: clean_dict[k] - int_dict[k] for k, v in clean_dict.items()}
    diff = int(target_sum - sum(int_dict.values()))
    sorted_keys = sorted(remainder.keys(), key=lambda k: remainder[k], reverse=True)
    for i in range(min(diff, len(sorted_keys))):
        int_dict[sorted_keys[i]] += 1
    return int_dict


def row_to_key(row):
    return f"{row.get('组别', '-')}-{row.get('运营', '-')}-{row.get('店铺', '-')}"


# ==========================================
# 主界面：数据上传/输入
# ==========================================
st.header("📥 2. 上传/输入业务数据")

col1, col2 = st.columns([1, 2])
with col1:
    st.download_button(label="⬇️ 下载标准 Excel 模板", data=generate_excel_template(),
                       file_name='北美拉式智能分仓模板_V3.6.xlsx', type="primary")
with col2:
    uploaded_file = st.file_uploader("⬆️ 上传填写好的 Excel 表格", type=["xlsx", "csv"])

if uploaded_file is not None:
    df_input = pd.read_excel(uploaded_file) if uploaded_file.name.endswith('.xlsx') else pd.read_csv(uploaded_file)
else:
    df_input = pd.DataFrame({
        'SKU': ['SKU-A', 'SKU-A'],
        '店铺': ['Shop-A', 'Shop-B'], '组别': ['二部', '三部'], '运营': ['张三', '李四'],
        '本次总发货量': [1000, 1000],
        '理论_西%': [25, 25], '理论_东%': [25, 25], '理论_GA%': [25, 25], '理论_TX%': [25, 25], '理论_CG%': [0, 0],
        '美西_在仓': [0, 0], '美东_在仓': [0, 0], 'GA_在仓': [0, 0], 'TX_在仓': [0, 0], 'CG_在仓': [0, 0],
        '美西_多批次在途': [f'{(today + datetime.timedelta(days=8)).strftime("%Y-%m-%d")}:2000', ''],
        '美东_多批次在途': ['', f'{(today + datetime.timedelta(days=8)).strftime("%Y-%m-%d")}:2000'],
        'GA_多批次在途': ['', ''], 'TX_多批次在途': ['', ''], 'CG_多批次在途': ['', ''],
        'M1预测(当月)': [3000, 3000], 'M2预测(次月)': [3000, 3000], 'M3预测(第3月)': [3000, 3000],
        'M4预测(第4月)': [3000, 3000], 'M5预测(第5月)': [3000, 3000]
    })

edited_df = st.data_editor(df_input, num_rows="dynamic", use_container_width=True)


# ==========================================
# 核心算法：V3.5 原引擎（扩展 sales_cutoff 参数）
# ==========================================
def calculate_allocation_v34(df, transit_dict, d_diff, earliest_etd, target_eta, south_linkage, sales_cutoff=None):
    deadlines, arrivals = {}, {}
    for r in regions:
        if transit_dict[r] <= d_diff:
            deadlines[r] = target_eta - datetime.timedelta(days=transit_dict[r])
        else:
            deadlines[r] = earliest_etd
        arrivals[r] = deadlines[r] + datetime.timedelta(days=transit_dict[r])

    date_to_m_idx = {}
    date_to_days_in_m = {}
    for d_offset in range(3500):
        sim_d = today + datetime.timedelta(days=d_offset)
        m_diff = (sim_d.year - today.year) * 12 + sim_d.month - today.month
        date_to_m_idx[sim_d] = min(max(m_diff, 0), 4)
        date_to_days_in_m[sim_d] = calendar.monthrange(sim_d.year, sim_d.month)[1]

    records = df.to_dict('records')
    results = []

    for row in records:
        sku = str(row.get('SKU', 'Unknown'))
        q_ship = float(row.get('本次总发货量', 0.0) or 0.0)

        forecasts = (
            float(row.get('M1预测(当月)', 0)), float(row.get('M2预测(次月)', 0)),
            float(row.get('M3预测(第3月)', 0)), float(row.get('M4预测(第4月)', 0)), float(row.get('M5预测(第5月)', 0))
        )

        def fast_daily_sales(d_obj):
            return max(forecasts[date_to_m_idx[d_obj]] / date_to_days_in_m[d_obj], 0.1)

        raw_ratios = {r: float(row.get(ratio_col_name(r), 20)) for r in regions}
        tr = sum(raw_ratios.values())
        ratios = {r: raw_ratios[r] / tr if tr > 0 else 1 / 5 for r in regions}

        in_wh = {r: float(row.get(f'{r}_在仓', 0)) for r in regions}
        in_transits = {r: parse_in_transit(row.get(f'{r}_多批次在途', '')) for r in regions}

        v_stock = in_wh.copy()
        cross_zone_orders = 0.0

        max_arrival = max(arrivals.values()) if arrivals else today
        for r in regions:
            if in_transits[r]: max_arrival = max(max_arrival, max(in_transits[r].keys()))

        days_to_sim = (max_arrival - today).days

        for d_idx in range(1, days_to_sim + 1):
            sim_date = today + datetime.timedelta(days=d_idx)
            for r in regions:
                if sim_date in in_transits[r]: v_stock[r] += in_transits[r][sim_date]

            active_regions = [r for r in regions if v_stock[r] > 0 or arrivals[r] <= sim_date]
            if not active_regions: continue

            daily_sales = fast_daily_sales(sim_date)
            unmet = 0.0

            for r in regions:
                demand = daily_sales * ratios[r]
                if r in active_regions:
                    if arrivals[r] <= sim_date:
                        v_stock[r] -= demand
                    else:
                        if v_stock[r] >= demand:
                            v_stock[r] -= demand
                        else:
                            unmet += (demand - v_stock[r]); v_stock[r] = 0.0
                else:
                    unmet += demand

            if unmet > 0.001:
                capable_donors = [r for r in active_regions if arrivals[r] <= sim_date or v_stock[r] > 0]
                if capable_donors:
                    split = unmet / len(capable_donors)
                    for r in capable_donors:
                        if arrivals[r] <= sim_date:
                            v_stock[r] -= split;
                            cross_zone_orders += split
                        else:
                            if v_stock[r] >= split:
                                v_stock[r] -= split; cross_zone_orders += split
                            else:
                                cross_zone_orders += v_stock[r]; v_stock[r] = 0.0

        allocations = {r: 0.0 for r in regions}
        pool = q_ship
        unallocated_regions = list(regions)
        wf_v_stock, wf_ratios = v_stock.copy(), ratios.copy()
        ga_gets_all, tx_gets_all = False, False

        if south_linkage:
            total_sys_init = sum(in_wh.values()) + sum(sum(v.values()) for v in in_transits.values()) + q_ship
            if total_sys_init > 0:
                ga_target_init = total_sys_init * raw_ratios['GA'] / 100
                tx_target_init = total_sys_init * raw_ratios['TX'] / 100
                ga_actual_init = in_wh['GA'] + sum(in_transits['GA'].values())
                tx_actual_init = in_wh['TX'] + sum(in_transits['TX'].values())

                if ga_actual_init > ga_target_init or tx_actual_init > tx_target_init:
                    unallocated_regions.remove('GA')
                    unallocated_regions.remove('TX')
                    unallocated_regions.append('South')
                    wf_v_stock['South'] = wf_v_stock['GA'] + wf_v_stock['TX']
                    wf_ratios['South'] = wf_ratios['GA'] + wf_ratios['TX']
                    if ga_actual_init > ga_target_init:
                        tx_gets_all = True
                    else:
                        ga_gets_all = True

        while pool > 0.001 and unallocated_regions:
            target_total = sum(wf_v_stock[r] for r in unallocated_regions) + pool
            ratio_sum = sum(wf_ratios[r] for r in unallocated_regions)
            if ratio_sum <= 0: break

            theoretical_alloc = {r: target_total * (wf_ratios[r] / ratio_sum) - wf_v_stock[r] for r in
                                 unallocated_regions}
            negatives = [r for r, val in theoretical_alloc.items() if val < 0]

            if negatives:
                for r in negatives:
                    if r == 'South':
                        allocations['GA'], allocations['TX'] = 0.0, 0.0
                    else:
                        allocations[r] = 0.0
                    unallocated_regions.remove(r)
            else:
                for r in unallocated_regions:
                    if r == 'South':
                        if ga_gets_all:
                            allocations['GA'], allocations['TX'] = theoretical_alloc[r], 0.0
                        elif tx_gets_all:
                            allocations['TX'], allocations['GA'] = theoretical_alloc[r], 0.0
                    else:
                        allocations[r] = theoretical_alloc[r]
                    pool -= theoretical_alloc[r]
                break

        alloc_int = round_preserve_sum(allocations, q_ship)

        global_stock = sum(float(in_wh[r]) for r in regions) + q_ship
        for r in regions:
            for qty in in_transits[r].values(): global_stock += qty

        oos_date = today
        if global_stock > 0:
            sim_date_oos = today
            for _ in range(3000):
                sim_date_oos += datetime.timedelta(days=1)
                ds = fast_daily_sales(sim_date_oos)
                if global_stock >= ds:
                    global_stock -= ds
                else:
                    global_stock = 0.0
                if sim_date_oos >= max_arrival and global_stock <= 0.001:
                    oos_date = sim_date_oos
                    break
            oos_date = sim_date_oos

        final_physical_stock = {r: v_stock[r] + alloc_int[r] for r in regions}
        total_final = sum(final_physical_stock.values())
        final_str = f"{final_physical_stock['美西'] / total_final * 100:.0f}% : {final_physical_stock['美东'] / total_final * 100:.0f}% : {final_physical_stock['GA'] / total_final * 100:.0f}% : {final_physical_stock['TX'] / total_final * 100:.0f}% : {final_physical_stock['CG'] / total_final * 100:.0f}%" if total_final > 0 else "0% : 0% : 0% : 0% : 0%"

        total_sys = sum(in_wh.values()) + sum(sum(v.values()) for v in in_transits.values()) + q_ship
        init_ratio_str = f"{(in_wh['美西'] + sum(in_transits['美西'].values()) + alloc_int['美西']) / total_sys * 100:.0f}% : {(in_wh['美东'] + sum(in_transits['美东'].values()) + alloc_int['美东']) / total_sys * 100:.0f}% : {(in_wh['GA'] + sum(in_transits['GA'].values()) + alloc_int['GA']) / total_sys * 100:.0f}% : {(in_wh['TX'] + sum(in_transits['TX'].values()) + alloc_int['TX']) / total_sys * 100:.0f}% : {(in_wh['CG'] + sum(in_transits['CG'].values()) + alloc_int['CG']) / total_sys * 100:.0f}%" if total_sys > 0 else "0% : 0% : 0% : 0% : 0%"

        def format_date(d_obj, amt):
            return d_obj.strftime('%Y-%m-%d') if amt > 0 else "-"

        # 🆕 V3.6：计算建议减量至 + 冗余标记
        suggest_qty_str = "-"
        is_redundant = False
        if sales_cutoff is not None and oos_date > sales_cutoff and q_ship > 0:
            suggest_qty = calculate_suggested_qty(row, transit_dict, earliest_etd, target_eta, sales_cutoff, q_ship)
            if suggest_qty <= 0:
                suggest_qty_str = "建议不发货"
                is_redundant = True
            elif suggest_qty >= q_ship:
                suggest_qty_str = "-"
            else:
                suggest_qty_str = str(int(suggest_qty))
                is_redundant = True

        results.append({
            'SKU': sku, '店铺': row.get('店铺', '-'), '组别': row.get('组别', '-'), '运营': row.get('运营', '-'),
            '👉 美西发货': alloc_int['美西'], '📅 美西最晚发货': format_date(deadlines['美西'], alloc_int['美西']),
            '👉 美东发货': alloc_int['美东'], '📅 美东最晚发货': format_date(deadlines['美东'], alloc_int['美东']),
            '👉 GA发货': alloc_int['GA'], '📅 GA最晚发货': format_date(deadlines['GA'], alloc_int['GA']),
            '👉 TX发货': alloc_int['TX'], '📅 TX最晚发货': format_date(deadlines['TX'], alloc_int['TX']),
            '👉 CG发货': alloc_int['CG'], '📅 CG最晚发货': format_date(deadlines['CG'], alloc_int['CG']),
            '📊 期初分区占比': init_ratio_str, '🎯 最终全网占比估值': final_str,
            '📅 最终全网到货日': max_arrival.strftime('%Y-%m-%d'),
            '🚚 预估跨区订单数量': int(round(cross_zone_orders)),
            '📅 预估全网耗尽日': oos_date.strftime('%Y-%m-%d'),
            '💡 建议减量至': suggest_qty_str,
            '_is_redundant': is_redundant
        })

    return pd.DataFrame(results)


# ==========================================
# 🆕 V3.6：建议减量至（反向二分）& 每日缺口推演
# ==========================================
def simulate_oos_date_only(row, transit_dict, earliest_etd, target_eta, q_ship_override):
    """给定发货量，快速推演返回全网耗尽日"""
    deadlines, arrivals = {}, {}
    d_diff = (target_eta - earliest_etd).days
    for r in regions:
        if transit_dict[r] <= d_diff:
            deadlines[r] = target_eta - datetime.timedelta(days=transit_dict[r])
        else:
            deadlines[r] = earliest_etd
        arrivals[r] = deadlines[r] + datetime.timedelta(days=transit_dict[r])

    forecasts = (
        float(row.get('M1预测(当月)', 0)), float(row.get('M2预测(次月)', 0)),
        float(row.get('M3预测(第3月)', 0)), float(row.get('M4预测(第4月)', 0)), float(row.get('M5预测(第5月)', 0))
    )
    in_wh = {r: float(row.get(f'{r}_在仓', 0)) for r in regions}
    in_transits = {r: parse_in_transit(row.get(f'{r}_多批次在途', '')) for r in regions}

    max_arrival = max(arrivals.values()) if arrivals else today
    for r in regions:
        if in_transits[r]: max_arrival = max(max_arrival, max(in_transits[r].keys()))

    global_stock = sum(in_wh.values()) + q_ship_override
    for r in regions:
        for qty in in_transits[r].values(): global_stock += qty

    if global_stock <= 0: return today

    sim_date = today
    for _ in range(3000):
        sim_date += datetime.timedelta(days=1)
        m_diff = (sim_date.year - today.year) * 12 + sim_date.month - today.month
        m_idx = min(max(m_diff, 0), 4)
        days_in_m = calendar.monthrange(sim_date.year, sim_date.month)[1]
        ds = max(forecasts[m_idx] / days_in_m, 0.1)
        if global_stock >= ds:
            global_stock -= ds
        else:
            global_stock = 0.0
        if sim_date >= max_arrival and global_stock <= 0.001:
            return sim_date
    return sim_date


def calculate_suggested_qty(row, transit_dict, earliest_etd, target_eta, sales_cutoff, original_qty):
    """二分搜索：让耗尽日等于销售截止日的发货量"""
    oos_zero = simulate_oos_date_only(row, transit_dict, earliest_etd, target_eta, 0)
    if oos_zero > sales_cutoff:
        return 0  # 连不发货都冗余

    lo, hi = 0.0, float(original_qty)
    best = hi
    for _ in range(20):
        if hi - lo < 1: break
        mid = (lo + hi) / 2
        oos_mid = simulate_oos_date_only(row, transit_dict, earliest_etd, target_eta, mid)
        diff_days = (oos_mid - sales_cutoff).days
        if abs(diff_days) <= 1:
            return int(round(mid))
        if oos_mid > sales_cutoff:
            hi = mid;
            best = mid
        else:
            lo = mid
    return int(round(best))


def simulate_daily_shortage(row, transit_dict, earliest_etd, target_eta, sales_cutoff):
    """推演至销售截止日，返回每日【缺口量】字典（用于调拨约束验证）"""
    deadlines, arrivals = {}, {}
    d_diff = (target_eta - earliest_etd).days
    for r in regions:
        if transit_dict[r] <= d_diff:
            deadlines[r] = target_eta - datetime.timedelta(days=transit_dict[r])
        else:
            deadlines[r] = earliest_etd
        arrivals[r] = deadlines[r] + datetime.timedelta(days=transit_dict[r])

    forecasts = (
        float(row.get('M1预测(当月)', 0)), float(row.get('M2预测(次月)', 0)),
        float(row.get('M3预测(第3月)', 0)), float(row.get('M4预测(第4月)', 0)), float(row.get('M5预测(第5月)', 0))
    )
    in_wh = {r: float(row.get(f'{r}_在仓', 0)) for r in regions}
    in_transits = {r: parse_in_transit(row.get(f'{r}_多批次在途', '')) for r in regions}
    q_ship = float(row.get('本次总发货量', 0))

    global_stock = sum(in_wh.values())
    future_arrivals = {}
    for r in regions:
        for dt, qty in in_transits[r].items():
            future_arrivals[dt] = future_arrivals.get(dt, 0) + qty
    max_arrival = max(arrivals.values()) if arrivals else today
    future_arrivals[max_arrival] = future_arrivals.get(max_arrival, 0) + q_ship

    days_to_sim = max(1, (sales_cutoff - today).days)
    daily_shortage = {}

    for d_idx in range(1, days_to_sim + 1):
        sim_date = today + datetime.timedelta(days=d_idx)
        if sim_date in future_arrivals:
            global_stock += future_arrivals[sim_date]
        m_diff = (sim_date.year - today.year) * 12 + sim_date.month - today.month
        m_idx = min(max(m_diff, 0), 4)
        days_in_m = calendar.monthrange(sim_date.year, sim_date.month)[1]
        ds = max(forecasts[m_idx] / days_in_m, 0.1)
        if global_stock >= ds:
            global_stock -= ds
            daily_shortage[sim_date] = 0.0
        else:
            daily_shortage[sim_date] = ds - global_stock
            global_stock = 0.0
    return daily_shortage


# ==========================================
# 🆕 V3.6：跨店库存调拨引擎
# ==========================================
def calc_row_deviation(row, transfer_regions):
    """计算一行在调拨参与区的占比偏差总和（以 (在仓+在途+按理论比例的本次发货) 近似）"""
    raw_r = {r: float(row.get(ratio_col_name(r), 0)) for r in transfer_regions}
    sr_t = sum(raw_r.values())
    theory = {r: raw_r[r] / sr_t if sr_t > 0 else 0.25 for r in transfer_regions}

    all_raw_sum = sum(float(row.get(ratio_col_name(rr), 0)) for rr in regions)
    q_ship = float(row.get('本次总发货量', 0))

    stock = {}
    for r in transfer_regions:
        in_wh = float(row.get(f'{r}_在仓', 0))
        in_tr = sum(parse_in_transit(row.get(f'{r}_多批次在途', '')).values())
        raw_pct = float(row.get(ratio_col_name(r), 0))
        stock[r] = in_wh + in_tr + (q_ship * raw_pct / all_raw_sum if all_raw_sum > 0 else 0)

    total = sum(stock.values())
    cur_ratios = {r: (stock[r] / total if total > 0 else 0) for r in transfer_regions}

    dev = sum(abs(cur_ratios[r] - theory[r]) for r in transfer_regions)
    return dev, cur_ratios, theory, stock


def apply_transfer_to_df(df, out_idx, in_idx, region, source_type, source_date, qty):
    """执行一次调拨（修改 df 原地）"""
    if source_type == '在仓':
        df.at[out_idx, f'{region}_在仓'] = float(df.at[out_idx, f'{region}_在仓']) - qty
        df.at[in_idx, f'{region}_在仓'] = float(df.at[in_idx, f'{region}_在仓']) + qty
    elif source_type == '在途':
        out_tr = parse_in_transit(df.at[out_idx, f'{region}_多批次在途'])
        in_tr = parse_in_transit(df.at[in_idx, f'{region}_多批次在途'])
        if source_date in out_tr:
            out_tr[source_date] = out_tr[source_date] - qty
            if out_tr[source_date] <= 0.5:
                del out_tr[source_date]
        in_tr[source_date] = in_tr.get(source_date, 0) + qty
        df.at[out_idx, f'{region}_多批次在途'] = dict_to_transit_str(out_tr)
        df.at[in_idx, f'{region}_多批次在途'] = dict_to_transit_str(in_tr)
    elif source_type == '本次发货量':
        # 按调出方的该区理论占比折算回总发货量
        out_row = df.loc[out_idx]
        raw_pct = float(out_row.get(ratio_col_name(region), 0))
        all_raw = sum(float(out_row.get(ratio_col_name(rr), 0)) for rr in regions)
        if all_raw > 0 and raw_pct > 0:
            total_move = qty * all_raw / raw_pct
            df.at[out_idx, '本次总发货量'] = float(df.at[out_idx, '本次总发货量']) - total_move
            df.at[in_idx, '本次总发货量'] = float(df.at[in_idx, '本次总发货量']) + total_move


def calculate_transfer_plan(working_df, transit_dict, earliest_etd, target_eta, sales_cutoff):
    """
    核心调拨引擎
    返回：(reduce_records, transfer_records, transferred_df)
    """
    df = working_df.copy().reset_index(drop=True)
    reduce_records = []
    transfer_records = []

    for sku, sku_group in df.groupby('SKU'):
        if len(sku_group) < 1: continue
        sku_indices = sku_group.index.tolist()

        # ---- 步骤A：SKU虚拟合并 ----
        agg_row = {'SKU': sku, '本次总发货量': sku_group['本次总发货量'].sum()}
        for r in regions:
            agg_row[f'{r}_在仓'] = sku_group[f'{r}_在仓'].sum()
            agg_row[f'{r}_多批次在途'] = merge_in_transits(sku_group[f'{r}_多批次在途'])
        for m in ['M1预测(当月)', 'M2预测(次月)', 'M3预测(第3月)', 'M4预测(第4月)', 'M5预测(第5月)']:
            agg_row[m] = sku_group[m].sum()
        total_qty = agg_row['本次总发货量']
        for r in regions:
            col = ratio_col_name(r)
            if total_qty > 0:
                agg_row[col] = (sku_group[col] * sku_group['本次总发货量']).sum() / total_qty
            else:
                agg_row[col] = sku_group[col].mean()

        Q_sum = agg_row['本次总发货量']
        if Q_sum <= 0: continue

        Q_optimal = calculate_suggested_qty(agg_row, transit_dict, earliest_etd, target_eta, sales_cutoff, Q_sum)

        # ---- 步骤B：按"冗余最大优先"分摊减量 ----
        delta_reduce = max(0, Q_sum - Q_optimal)
        if delta_reduce > 0.5:
            row_redundancy = []
            for idx in sku_indices:
                row_dict = df.loc[idx].to_dict()
                orig_q = float(row_dict.get('本次总发货量', 0))
                oos = simulate_oos_date_only(row_dict, transit_dict, earliest_etd, target_eta, orig_q)
                red_days = (oos - sales_cutoff).days
                row_redundancy.append((idx, red_days, orig_q))

            row_redundancy.sort(key=lambda x: -x[1])  # 冗余度降序
            remaining = delta_reduce

            for idx, red_days, orig_q in row_redundancy:
                if remaining <= 0.5 or red_days <= 0: break
                row_dict = df.loc[idx].to_dict()
                row_target = calculate_suggested_qty(row_dict, transit_dict, earliest_etd, target_eta, sales_cutoff,
                                                     orig_q)
                row_target = max(0, row_target)
                actual_reduce = min(orig_q - row_target, remaining)
                if actual_reduce > 0.5:
                    new_q = orig_q - actual_reduce
                    df.at[idx, '本次总发货量'] = new_q
                    reduce_records.append({
                        'SKU': sku,
                        '组别': df.at[idx, '组别'],
                        '运营': df.at[idx, '运营'],
                        '店铺': df.at[idx, '店铺'],
                        '原发货量': int(orig_q),
                        '减量后发货量': int(new_q),
                        '减量原因': f"独立推演冗余 {red_days} 天"
                    })
                    remaining -= actual_reduce

        # ---- 步骤C/D：贪心调拨（减量后） ----
        sku_indices = df[df['SKU'] == sku].index.tolist()
        if len(sku_indices) < 2: continue

        # 基线：各行调拨前的每日缺口（约束用）
        baseline_shortage = {}
        for idx in sku_indices:
            baseline_shortage[idx] = simulate_daily_shortage(df.loc[idx].to_dict(), transit_dict, earliest_etd,
                                                             target_eta, sales_cutoff)

        max_iter = 50
        for iter_count in range(max_iter):
            # 计算当前总偏差
            current_total_dev = 0.0
            row_infos = {}
            for idx in sku_indices:
                row_dict = df.loc[idx].to_dict()
                dev, cur_ratios, theory, stock = calc_row_deviation(row_dict, transfer_regions)
                current_total_dev += dev
                row_infos[idx] = (cur_ratios, theory, stock)

            # 寻找最优调拨
            best_action = None
            best_improvement = 0.001  # 最小改善阈值

            for out_idx in sku_indices:
                out_cur, out_theory, out_stock = row_infos[out_idx]
                out_row = df.loc[out_idx].to_dict()

                for r in transfer_regions:
                    if out_cur[r] <= out_theory[r] + 0.005:  # 该区不过剩，跳过
                        continue

                    # 该区域可调出的三种来源
                    sources = []
                    in_wh_qty = float(out_row.get(f'{r}_在仓', 0))
                    if in_wh_qty > 0.5:
                        sources.append(('在仓', None, in_wh_qty))
                    for dt, qty in parse_in_transit(out_row.get(f'{r}_多批次在途', '')).items():
                        if qty > 0.5:
                            sources.append(('在途', dt, qty))
                    q_ship = float(out_row.get('本次总发货量', 0))
                    raw_pct = float(out_row.get(ratio_col_name(r), 0))
                    all_raw = sum(float(out_row.get(ratio_col_name(rr), 0)) for rr in regions)
                    ship_in_region = q_ship * raw_pct / all_raw if all_raw > 0 else 0
                    if ship_in_region > 0.5:
                        sources.append(('本次发货量', None, ship_in_region))

                    # 该区的"过剩量"上限
                    total_out_stock = sum(out_stock.values())
                    excess_qty = (out_cur[r] - out_theory[r]) * total_out_stock

                    for in_idx in sku_indices:
                        if in_idx == out_idx: continue
                        in_cur, in_theory, in_stock = row_infos[in_idx]
                        if in_cur[r] >= in_theory[r] - 0.005:
                            continue  # 该区不缺，跳过

                        # 调入方的"缺口量"上限
                        total_in_stock = sum(in_stock.values())
                        deficit_qty = (in_theory[r] - in_cur[r]) * total_in_stock

                        # 取一个合理的调拨量
                        for source_type, source_date, source_qty in sources:
                            trial = min(source_qty, excess_qty, deficit_qty)
                            if trial < 1: continue

                            # 试探
                            improvement = try_action(df, out_idx, in_idx, r, source_type, source_date, trial,
                                                     transit_dict, earliest_etd, target_eta, sales_cutoff,
                                                     baseline_shortage, current_total_dev)
                            if improvement > best_improvement:
                                best_improvement = improvement
                                best_action = (out_idx, in_idx, r, source_type, source_date, trial)

            if best_action is None: break

            # 执行最优动作
            out_idx, in_idx, r, source_type, source_date, qty = best_action
            apply_transfer_to_df(df, out_idx, in_idx, r, source_type, source_date, qty)

            if source_type == '在仓':
                batch_str = '在仓'
            elif source_type == '本次发货量':
                batch_str = '本次发货量'
            else:
                batch_str = f'在途 {source_date.strftime("%Y-%m-%d")}'

            transfer_records.append({
                'SKU': sku,
                '调出方': row_to_key(df.loc[out_idx]),
                '调入方': row_to_key(df.loc[in_idx]),
                '调拨区域': r,
                '调拨批次': batch_str,
                '调拨数量': int(round(qty))
            })

    return reduce_records, transfer_records, df


def try_action(df, out_idx, in_idx, region, source_type, source_date, qty,
               transit_dict, earliest_etd, target_eta, sales_cutoff,
               baseline_shortage, current_total_dev):
    """试探调拨，返回总偏差改善量（违反约束返回 -1）"""
    backup_out = df.loc[out_idx].to_dict()
    backup_in = df.loc[in_idx].to_dict()

    try:
        apply_transfer_to_df(df, out_idx, in_idx, region, source_type, source_date, qty)

        # 约束：调出方每日缺口不能比原来大
        out_row_dict = df.loc[out_idx].to_dict()
        new_shortage = simulate_daily_shortage(out_row_dict, transit_dict, earliest_etd, target_eta, sales_cutoff)
        base = baseline_shortage.get(out_idx, {})
        for dt, sh in new_shortage.items():
            if sh > base.get(dt, 0) + 0.5:
                for k, v in backup_out.items(): df.at[out_idx, k] = v
                for k, v in backup_in.items(): df.at[in_idx, k] = v
                return -1.0

        # 计算新偏差（仅重算 out_idx 和 in_idx）
        new_dev = 0.0
        for idx in [out_idx, in_idx]:
            dev, _, _, _ = calc_row_deviation(df.loc[idx].to_dict(), transfer_regions)
            new_dev += dev

        old_dev = 0.0
        for bak in [backup_out, backup_in]:
            dev, _, _, _ = calc_row_deviation(bak, transfer_regions)
            old_dev += dev

        improvement = old_dev - new_dev

        # 回滚
        for k, v in backup_out.items(): df.at[out_idx, k] = v
        for k, v in backup_in.items(): df.at[in_idx, k] = v

        return improvement
    except Exception:
        for k, v in backup_out.items(): df.at[out_idx, k] = v
        for k, v in backup_in.items(): df.at[in_idx, k] = v
        return -1.0


def calculate_row_final_ratio(row, transit_dict, earliest_etd, target_eta):
    """单行最终占比（对比用，含全部5个区）"""
    in_wh = {r: float(row.get(f'{r}_在仓', 0)) for r in regions}
    in_transits = {r: parse_in_transit(row.get(f'{r}_多批次在途', '')) for r in regions}
    q_ship = float(row.get('本次总发货量', 0))
    raw_ratios = {r: float(row.get(ratio_col_name(r), 0)) for r in regions}
    tr = sum(raw_ratios.values())
    ratios = {r: raw_ratios[r] / tr if tr > 0 else 0.2 for r in regions}

    stock = {r: in_wh[r] + sum(in_transits[r].values()) + q_ship * ratios[r] for r in regions}
    total = sum(stock.values())
    return {r: (stock[r] / total * 100 if total > 0 else 0) for r in regions}


# ==========================================
# 主看板交互区
# ==========================================
st.header("🚀 3. 智能分仓指令看板")

col_btn1, col_btn2, col_btn3, col_btn4 = st.columns([1.3, 1.5, 1.5, 1])

# 用 session_state 维护互斥
if 'agg_on' not in st.session_state: st.session_state['agg_on'] = False
if 'transfer_on' not in st.session_state: st.session_state['transfer_on'] = False

with col_btn1:
    agg_disabled = st.session_state.get('transfer_on', False)
    agg_checkbox = st.checkbox(
        "🔄 开启【同组别同SKU】汇总计算",
        value=st.session_state.get('agg_on', False),
        disabled=agg_disabled,
        help="相同组别和SKU的数据合并为一条记录计算。（开启调拨开关时自动置灰）",
        key='agg_on'
    )

with col_btn2:
    south_linkage = st.checkbox("🔘 开启美南仓 (GA/TX) 联动合并优化", value=False, help="GA/TX偏仓时合并计算并定向补贴。")

with col_btn3:
    transfer_disabled = st.session_state.get('agg_on', False)
    transfer_on = st.checkbox(
        "🔄 开启跨店库存调拨分析",
        value=st.session_state.get('transfer_on', False),
        disabled=transfer_disabled,
        help="按SKU跨组别/运营/店铺检测减量+调拨机会。（与【同组别同SKU汇总】互斥）",
        key='transfer_on'
    )

with col_btn4:
    btn_run = st.button("开始逆向推演运算", type="primary")

if btn_run:
    if d_diff_invalid:
        st.error("❌ D差小于最短海运时效，无法进行计算！请在左侧调整日期。")
    else:
        working_df = edited_df.copy()

        numeric_cols = (
                ['本次总发货量'] + [ratio_col_name(r) for r in regions] + [f'{r}_在仓' for r in regions] +
                ['M1预测(当月)', 'M2预测(次月)', 'M3预测(第3月)', 'M4预测(第4月)', 'M5预测(第5月)']
        )
        for col in numeric_cols:
            if col in working_df.columns:
                working_df[col] = pd.to_numeric(working_df[col], errors='coerce').fillna(0.0)
        for col in ['SKU', '店铺', '组别', '运营']:
            if col in working_df.columns:
                working_df[col] = working_df[col].fillna('-').astype(str)

        if agg_checkbox:
            working_df = aggregate_data(working_df)

        error_skus = []
        for _, row in working_df.iterrows():
            total_pct = sum([float(row[ratio_col_name(r)]) for r in regions])
            if not (99.99 <= total_pct <= 100.01):
                error_skus.append(f"• 【{row['SKU']}】 理论占比总和为: {total_pct:.1f}%")

        if error_skus:
            st.error("❌ **防呆拦截：数据校验失败！**")
            st.warning("以下 SKU 的【理论分区占比】之和不等于 100%：\n\n" + "\n".join(error_skus))
            for k in ['alloc_result', 'transfer_result', 'applied_transfer', 'alloc_result_transferred']:
                if k in st.session_state: del st.session_state[k]
        else:
            with st.spinner("启动逆向排期引擎..."):
                df_result = calculate_allocation_v34(working_df, transit_times, d_diff, earliest_etd, target_eta,
                                                     south_linkage, sales_cutoff)
                st.session_state['alloc_result'] = df_result
                st.session_state['working_df'] = working_df
                st.session_state['applied_transfer'] = False
                for k in ['transfer_result', 'alloc_result_transferred']:
                    if k in st.session_state: del st.session_state[k]

                # 🆕 V3.6：若开启调拨，同步算调拨方案（但不应用到主看板）
                if transfer_on:
                    with st.spinner("正在计算跨店调拨方案..."):
                        reduce_records, transfer_records, transferred_df = calculate_transfer_plan(
                            working_df, transit_times, earliest_etd, target_eta, sales_cutoff
                        )
                        compare_records = []
                        for idx in working_df.index:
                            if idx not in transferred_df.index: continue
                            old_row = working_df.loc[idx].to_dict()
                            new_row = transferred_df.loc[idx].to_dict()
                            old_ratio = calculate_row_final_ratio(old_row, transit_times, earliest_etd, target_eta)
                            new_ratio = calculate_row_final_ratio(new_row, transit_times, earliest_etd, target_eta)
                            theory = {r: float(old_row.get(ratio_col_name(r), 0)) for r in regions}
                            old_dev = sum(abs(old_ratio[r] - theory[r]) for r in regions)
                            new_dev = sum(abs(new_ratio[r] - theory[r]) for r in regions)
                            compare_records.append({
                                'SKU': old_row['SKU'],
                                '组别-运营-店铺': row_to_key(old_row),
                                '理论占比 (西:东:GA:TX:CG)': f"{theory['美西']:.0f}:{theory['美东']:.0f}:{theory['GA']:.0f}:{theory['TX']:.0f}:{theory['CG']:.0f}",
                                '调拨前占比': f"{old_ratio['美西']:.0f}:{old_ratio['美东']:.0f}:{old_ratio['GA']:.0f}:{old_ratio['TX']:.0f}:{old_ratio['CG']:.0f}",
                                '调拨后占比': f"{new_ratio['美西']:.0f}:{new_ratio['美东']:.0f}:{new_ratio['GA']:.0f}:{new_ratio['TX']:.0f}:{new_ratio['CG']:.0f}",
                                '偏差改善量': f"{old_dev - new_dev:+.1f}%"
                            })
                        st.session_state['transfer_result'] = {
                            'reduce': reduce_records,
                            'transfer': transfer_records,
                            'compare': compare_records,
                            'transferred_df': transferred_df
                        }

# ==========================================
# 主看板展示（支持调拨前/后切换）
# ==========================================
if 'alloc_result' in st.session_state:
    applied = st.session_state.get('applied_transfer', False)

    if applied and 'alloc_result_transferred' in st.session_state:
        cached_result = st.session_state['alloc_result_transferred']
        st.success("✅ 当前显示：**调拨后**的分仓指令（含调拨生效的新归属）")
    else:
        cached_result = st.session_state['alloc_result']
        if 'transfer_result' in st.session_state:
            st.info("ℹ️ 当前显示：**调拨前**的原始分仓指令。可在下方调拨板块应用方案。")

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

    filtered_result = cached_result.copy()
    if search_sku:
        filtered_result = filtered_result[
            filtered_result['SKU'].str.contains(search_sku, case=False, na=False, regex=False)]
    if sel_shop: filtered_result = filtered_result[
        filtered_result['店铺'].apply(lambda x: any(s in x for s in sel_shop))]
    if sel_group: filtered_result = filtered_result[filtered_result['组别'].isin(sel_group)]
    if sel_op: filtered_result = filtered_result[filtered_result['运营'].apply(lambda x: any(o in x for o in sel_op))]


    def highlight_row(row):
        styles = [''] * len(row)
        try:
            if '🚚 预估跨区订单数量' in row.index and row['🚚 预估跨区订单数量'] > 0:
                styles[row.index.get_loc(
                    '🚚 预估跨区订单数量')] = 'background-color: #fff3cd; color: #cc0000; font-weight: bold'
            if '_is_redundant' in row.index and row['_is_redundant']:
                styles[row.index.get_loc(
                    '📅 预估全网耗尽日')] = 'background-color: #ffcccc; color: #990000; font-weight: bold'
        except (KeyError, ValueError):
            pass
        return styles


    display_cols = [c for c in filtered_result.columns if not c.startswith('_')]
    # 为了让高亮函数能读到 _is_redundant，先 style 再转 display
    styled = filtered_result.style.apply(highlight_row, axis=1)
    # 隐藏内部列
    hide_cols = [c for c in filtered_result.columns if c.startswith('_')]
    if hide_cols:
        styled = styled.hide(axis='columns', subset=hide_cols)
    st.dataframe(styled, use_container_width=True)

    csv_result = filtered_result[display_cols].to_csv(index=False).encode('utf-8-sig')
    st.download_button(
        "📥 导出物流装柜/发货指令表 (当前视图)",
        data=csv_result,
        file_name=f'装柜排期计划_{today.strftime("%Y%m%d")}{"_调拨后" if applied else ""}.csv',
        mime='text/csv'
    )

# ==========================================
# 🆕 V3.6 新板块：跨店库存调拨建议
# ==========================================
if 'transfer_result' in st.session_state:
    st.markdown("---")
    st.header("🔄 5. 跨店库存调拨建议")

    tr_res = st.session_state['transfer_result']
    reduce_records = tr_res['reduce']
    transfer_records = tr_res['transfer']
    compare_records = tr_res['compare']

    # --- 表1：减量明细 ---
    st.markdown("#### 📉 表1·减量明细")
    if reduce_records:
        df_reduce = pd.DataFrame(reduce_records)
        st.dataframe(df_reduce, use_container_width=True)
    else:
        st.info("✅ 无需减量：所有SKU整体不冗余或已平衡。")

    # --- 表2：调拨明细 ---
    st.markdown("#### 🔀 表2·调拨明细")
    if transfer_records:
        df_transfer = pd.DataFrame(transfer_records)
        st.dataframe(df_transfer, use_container_width=True)
    else:
        st.info("ℹ️ 无可行调拨动作：各行占比已接近理论值或无可调出资源。")

    # --- 表3：调拨前后对比 ---
    st.markdown("#### 📊 表3·调拨前后占比对比")
    if compare_records:
        df_compare = pd.DataFrame(compare_records)
        st.dataframe(df_compare, use_container_width=True)

    # --- 应用/撤销按钮 ---
    st.markdown("---")
    applied = st.session_state.get('applied_transfer', False)
    col_apply1, col_apply2 = st.columns([1, 4])
    with col_apply1:
        if not applied:
            if st.button("✅ 应用调拨方案并重算分仓指令", type="primary"):
                # 用调拨后的 df 重跑主看板
                transferred_df = tr_res['transferred_df']
                with st.spinner("正在基于调拨后数据重算主看板..."):
                    df_result_new = calculate_allocation_v34(
                        transferred_df, transit_times, d_diff, earliest_etd, target_eta, south_linkage, sales_cutoff
                    )
                    st.session_state['alloc_result_transferred'] = df_result_new
                    st.session_state['applied_transfer'] = True
                st.rerun()
        else:
            if st.button("↩️ 撤销调拨，恢复原始方案", type="secondary"):
                st.session_state['applied_transfer'] = False
                st.rerun()
    with col_apply2:
        if applied:
            st.success("🎯 调拨方案已生效，主看板已刷新为调拨后的归属与数量。点击左侧按钮可随时撤销回切。")
        else:
            if transfer_records or reduce_records:
                st.info("👈 审阅上述调拨方案无误后，点击左侧按钮应用。")

# ==========================================
# 🕰️ 时空沙盘（沿用 V3.5）
# ==========================================
st.markdown("---")
st.header("🕰️ 4. 时空沙盘：特定日期库存分布穿越")

col_d1, col_d2 = st.columns([1, 4])
with col_d1:
    target_date = st.date_input("选择目标查询日期", value=target_eta, key='sandbox_date')
    check_btn = st.button("🚀 穿越至该日推演", type="secondary")


def get_daily_sales_for_date(target_date, row):
    month_diff = (target_date.year - today.year) * 12 + target_date.month - today.month
    if month_diff <= 0:
        forecast = row.get('M1预测(当月)', 0)
    elif month_diff == 1:
        forecast = row.get('M2预测(次月)', 0)
    elif month_diff == 2:
        forecast = row.get('M3预测(第3月)', 0)
    elif month_diff == 3:
        forecast = row.get('M4预测(第4月)', 0)
    else:
        forecast = row.get('M5预测(第5月)', 0)
    days_in_month = calendar.monthrange(target_date.year, target_date.month)[1]
    return max(forecast / days_in_month, 0.1)


with col_d2:
    if check_btn:
        if 'alloc_result' not in st.session_state:
            st.warning("⚠️ 请先点击【开始逆向推演运算】生成分仓方案！")
        else:
            # 根据当前是否应用调拨，决定用哪份数据
            applied_now = st.session_state.get('applied_transfer', False)
            if applied_now and 'transfer_result' in st.session_state:
                alloc_df = st.session_state.get('alloc_result_transferred', st.session_state['alloc_result'])
                working_df = st.session_state['transfer_result']['transferred_df']
            else:
                alloc_df = st.session_state['alloc_result']
                working_df = st.session_state['working_df']

            time_machine_results = []
            filtered_edited_df = working_df.copy()

            for _, row in filtered_edited_df.iterrows():
                sku = row['SKU']
                shop = row.get('店铺', '-')
                group = row.get('组别', '-')
                op = row.get('运营', '-')
                matched = alloc_df[
                    (alloc_df['SKU'] == sku) & (alloc_df['店铺'] == shop) & (alloc_df['组别'] == group) & (
                                alloc_df['运营'] == op)]
                if matched.empty: continue
                alloc_row = matched.iloc[0]

                raw_ratios = {r: float(row[ratio_col_name(r)]) for r in regions}
                tr = sum(raw_ratios.values())
                ratios = {r: raw_ratios[r] / tr if tr > 0 else 1 / 5 for r in regions}

                sim_stock = {r: float(row[f'{r}_在仓']) for r in regions}
                in_transits = {r: parse_in_transit(row.get(f'{r}_多批次在途', '')) for r in regions}

                arrivals = {}
                for r in regions:
                    if transit_times[r] <= d_diff:
                        arrivals[r] = target_eta
                    else:
                        arrivals[r] = earliest_etd + datetime.timedelta(days=transit_times[r])

                tz_cross = 0.0
                days_to_sim = (target_date - today).days
                for d in range(1, days_to_sim + 1):
                    sim_d = today + datetime.timedelta(days=d)
                    for r in regions:
                        if sim_d in in_transits[r]: sim_stock[r] += in_transits[r][sim_d]
                        if sim_d == arrivals[r]: sim_stock[r] += alloc_row[f'👉 {r}发货']

                    ask = {r: 0 for r in regions}
                    ds = get_daily_sales_for_date(sim_d, row)
                    for r in regions:
                        demand = ds * ratios[r]
                        if sim_stock[r] >= demand:
                            sim_stock[r] -= demand
                        else:
                            ask[r], sim_stock[r] = demand - sim_stock[r], 0

                    unmet = sum(ask.values())
                    while unmet > 0.001 and sum(sim_stock.values()) > 0.001:
                        donors = [r for r in regions if sim_stock[r] > 0]
                        if not donors: break
                        split = unmet / len(donors)
                        unmet = 0
                        for r in donors:
                            if sim_stock[r] >= split:
                                sim_stock[r] -= split; tz_cross += split
                            else:
                                tz_cross += sim_stock[r]; unmet += (split - sim_stock[r]); sim_stock[r] = 0

                total_inv = sum(sim_stock.values())
                pct = {r: (sim_stock[r] / total_inv * 100) if total_inv > 0 else 0 for r in regions}

                time_machine_results.append({
                    'SKU': sku, '店铺': shop, '组别': group, '运营': op,
                    f'📅 {target_date} 总库存': int(total_inv),
                    '🇺🇸 实际占比 (西:东:GA:TX:CG)': f"{pct['美西']:.0f}% : {pct['美东']:.0f}% : {pct['GA']:.0f}% : {pct['TX']:.0f}% : {pct['CG']:.0f}",
                    '🚚 累计跨区订单数量': int(round(tz_cross)),
                    '美西结存': int(sim_stock['美西']), '美东结存': int(sim_stock['美东']),
                    'GA结存': int(sim_stock['GA']), 'TX结存': int(sim_stock['TX']), 'CG结存': int(sim_stock['CG'])
                })
            st.success(f"✅ 已推演至 {target_date}（{'调拨后' if applied_now else '原始'}数据）")
            st.dataframe(pd.DataFrame(time_machine_results), use_container_width=True)
