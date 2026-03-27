import streamlit as st
import pandas as pd
import datetime
import calendar
import io

# ==========================================
# 页面基础配置与标题
# ==========================================
st.set_page_config(page_title="北美全渠道智能分仓系统 V3.5", layout="wide")
st.title("📦 北美全渠道智能分仓控制塔 (V3.5 极速向量版)")

with st.expander("📖 核心指标与业务定义说明 (点击展开/收起)", expanded=False):
    st.markdown("""
    * **本批次最早可发货日期**：业务/工厂能将货物发出（离厂/离港）的最早物理底线日。
    * **🎯 本轮发货目标上架时间**：业务期望的这批货在全美各仓完成上架的目标锚点日。
    * **⏳ 物流 D差 (操作时间窗)**：`目标上架时间 - 最早可发货日期`。留给物流发货和在途的理论最大时间窗口。
    * **🚚 预估跨区订单数量**：从今天起至推演日历结束，因各仓断货不均触发的【跨区代发单量】总和。
    * **📊 期初分区占比**：`(本区在仓 + 本区所有在途 + 本次分配给该区的发货量) / 全网总盘子`。反映静态视角的资源分配情况。
    * **🎯 最终全网占比估值**：沙盘推演至【最后一批货(含在途)全部到港的那一天】，截取当天的真实物理库存计算的动态各区占比。
    """)

# ==========================================
# 侧边栏：全局时间与高阶控制参数
# ==========================================
with st.sidebar:
    st.header("⚙️ 1. 全局时间与排期控制")
    today = datetime.date.today()
    st.info(f"今天 (Day 0): {today.strftime('%Y-%m-%d')}")

    earliest_etd = st.date_input("本批次最早可发货日期", value=today + datetime.timedelta(days=7))
    target_eta = st.date_input("🎯 本轮发货目标上架时间", value=today + datetime.timedelta(days=37))

    d_diff = (target_eta - earliest_etd).days
    if d_diff < 0:
        st.error("上架时间不能早于最早发货日期！")
        st.stop()

    st.success(f"⏳ 物流 D差 (操作时间窗): {d_diff} 天")

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
        st.error(
            f"🚨 极速熔断：D差 ({d_diff}天) 小于全网最短海运时效 ({min(transit_times.values())}天)！该批次必然全网迟到，无法进行逆向排期计算！")

regions = ['美西', '美东', 'GA', 'TX', 'CG']


# ==========================================
# 辅助函数定义 (高性能优化版)
# ==========================================
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


def merge_in_transits(series):
    merged = {}
    for val in series:
        parsed = parse_in_transit(val)
        for d, q in parsed.items():
            merged[d] = merged.get(d, 0) + q
    if not merged: return ''
    return "; ".join([f"{d.strftime('%Y-%m-%d')}:{q}" for d, q in sorted(merged.items())])


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
            col = f'理论_{r.replace("美", "")}%' if r in ['美西', '美东'] else f'理论_{r}%'
            if total_qty > 0:
                ratios[r] = (group_df[col] * group_df['本次总发货量']).sum() / total_qty
            else:
                ratios[r] = group_df[col].mean()

        tr = sum(ratios.values())
        for r in regions:
            col = f'理论_{r.replace("美", "")}%' if r in ['美西', '美东'] else f'理论_{r}%'
            row[col] = ratios[r] * 100 / tr if tr > 0 else 20

        for r in regions:
            row[f'{r}_在仓'] = group_df[f'{r}_在仓'].sum()
            row[f'{r}_多批次在途'] = merge_in_transits(group_df[f'{r}_多批次在途'])

        for m in ['M1预测(当月)', 'M2预测(次月)', 'M3预测(第3月)', 'M4预测(第4月)', 'M5预测(第5月)']:
            row[m] = group_df[m].sum()

        grouped_records.append(row)
    return pd.DataFrame(grouped_records)


def generate_excel_template():
    template_data = {
        'SKU': ['1-Group-Test(测汇总)', '1-Group-Test(测汇总)'],
        '店铺': ['Shop-A', 'Shop-B'], '组别': ['一部', '一部'], '运营': ['张三', '李四'],
        '本次总发货量': [2000, 3000],
        '理论_西%': [20, 20], '理论_东%': [20, 20], '理论_GA%': [20, 20], '理论_TX%': [20, 20], '理论_CG%': [20, 20],
        '美西_在仓': [0, 0], '美东_在仓': [0, 1000], 'GA_在仓': [0, 0], 'TX_在仓': [2000, 0], 'CG_在仓': [0, 0],
        '美西_多批次在途': ['', ''],
        '美东_多批次在途': [f'{(today + datetime.timedelta(days=15)).strftime("%Y-%m-%d")}:1000', ''],
        'GA_多批次在途': ['', ''], 'TX_多批次在途': ['', ''], 'CG_多批次在途': ['', ''],
        'M1预测(当月)': [1500, 1500], 'M2预测(次月)': [1500, 1500], 'M3预测(第3月)': [1500, 1500],
        'M4预测(第4月)': [1500, 1500], 'M5预测(第5月)': [1500, 1500]
    }
    df_tpl = pd.DataFrame(template_data)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_tpl.to_excel(writer, index=False, sheet_name='发货需求表')
    return output.getvalue()


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


def round_preserve_sum(float_dict, target_sum):
    clean_dict = {k: (0.0 if pd.isna(v) else float(v)) for k, v in float_dict.items()}
    int_dict = {k: int(v) for k, v in clean_dict.items()}
    remainder = {k: clean_dict[k] - int_dict[k] for k, v in clean_dict.items()}
    diff = int(target_sum - sum(int_dict.values()))
    sorted_keys = sorted(remainder.keys(), key=lambda k: remainder[k], reverse=True)
    for i in range(min(diff, len(sorted_keys))):
        int_dict[sorted_keys[i]] += 1
    return int_dict


# ==========================================
# 主界面：数据上传与输入区
# ==========================================
st.header("📥 2. 上传/输入业务数据")

col1, col2 = st.columns([1, 2])
with col1:
    st.download_button(label="⬇️ 下载标准 Excel 模板", data=generate_excel_template(),
                       file_name='北美拉式智能分仓模板_V3.5.xlsx', type="primary")
with col2:
    uploaded_file = st.file_uploader("⬆️ 上传填写好的 Excel 表格", type=["xlsx", "csv"])

if uploaded_file is not None:
    df_input = pd.read_excel(uploaded_file) if uploaded_file.name.endswith('.xlsx') else pd.read_csv(uploaded_file)
else:
    df_input = pd.DataFrame({
        'SKU': ['1-Group-Test(测汇总)', '1-Group-Test(测汇总)'],
        '店铺': ['Shop-A', 'Shop-B'], '组别': ['一部', '一部'], '运营': ['张三', '李四'],
        '本次总发货量': [2000, 3000],
        '理论_西%': [20, 20], '理论_东%': [20, 20], '理论_GA%': [20, 20], '理论_TX%': [20, 20], '理论_CG%': [20, 20],
        '美西_在仓': [0, 0], '美东_在仓': [0, 1000], 'GA_在仓': [0, 0], 'TX_在仓': [2000, 0], 'CG_在仓': [0, 0],
        '美西_多批次在途': ['', ''],
        '美东_多批次在途': [f'{(today + datetime.timedelta(days=15)).strftime("%Y-%m-%d")}:1000', ''],
        'GA_多批次在途': ['', ''], 'TX_多批次在途': ['', ''], 'CG_多批次在途': ['', ''],
        'M1预测(当月)': [1500, 1500], 'M2预测(次月)': [1500, 1500], 'M3预测(第3月)': [1500, 1500],
        'M4预测(第4月)': [1500, 1500], 'M5预测(第5月)': [1500, 1500]
    })

edited_df = st.data_editor(df_input, num_rows="dynamic", use_container_width=True)


# ==========================================
# 核心算法引擎模块 (V3.5 极速向量化与哈希表优化版)
# ==========================================
def calculate_allocation_v34(df, transit_dict, d_diff, earliest_etd, target_eta, south_linkage):
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
        q_ship_val = row.get('本次总发货量', 0.0)
        q_ship = float(q_ship_val) if pd.notna(q_ship_val) else 0.0

        forecasts = (
            float(row.get('M1预测(当月)', 0)), float(row.get('M2预测(次月)', 0)),
            float(row.get('M3预测(第3月)', 0)), float(row.get('M4预测(第4月)', 0)), float(row.get('M5预测(第5月)', 0))
        )

        def fast_daily_sales(d_obj):
            return max(forecasts[date_to_m_idx[d_obj]] / date_to_days_in_m[d_obj], 0.1)

        raw_ratios = {r: float(row.get(f'理论_{r.replace("美", "")}%' if r in ['美西', '美东'] else f'理论_{r}%', 20))
                      for r in regions}
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
            for safety_counter in range(3000):
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
            '📅 预估全网耗尽日': oos_date.strftime('%Y-%m-%d')
        })

    return pd.DataFrame(results)


# ==========================================
# 结果展示区 & 智能联动极速检索
# ==========================================
st.header("🚀 3. 智能分仓指令看板")

col_btn1, col_btn2, col_btn3 = st.columns([1.2, 1.5, 1])
with col_btn1:
    agg_checkbox = st.checkbox("🔄 开启【同组别同SKU】汇总计算", value=False,
                               help="勾选后，相同组别和SKU的数据将合并为一条记录进行全盘推演计算。")
with col_btn2:
    south_linkage = st.checkbox("🔘 开启美南仓 (GA/TX) 联动合并优化", value=False,
                                help="开启后，若GA/TX其中之一偏仓，将计算美南总缺口，并定向补贴给缺货方。")
with col_btn3:
    btn_run = st.button("开始逆向推演运算", type="primary")

if btn_run:
    if d_diff_invalid:
        st.error("❌ D差小于最短海运时效，无法进行计算！请在左侧调整日期。")
    else:
        working_df = edited_df.copy()

        # 1. 清理所有数值列 (采用隐式换行写法，杜绝语法错误)
        numeric_cols = (
                ['本次总发货量'] + [f'理论_{r.replace("美", "")}%' if r in ['美西', '美东'] else f'理论_{r}%' for r in
                                    regions] + [f'{r}_在仓' for r in regions] + ['M1预测(当月)', 'M2预测(次月)',
                                                                                 'M3预测(第3月)', 'M4预测(第4月)',
                                                                                 'M5预测(第5月)']
        )

        for col in numeric_cols:
            if col in working_df.columns:
                working_df[col] = pd.to_numeric(working_df[col], errors='coerce').fillna(0.0)

        # 2. 清理所有文本列 (遇到空值转为 '-')
        for col in ['SKU', '店铺', '组别', '运营']:
            if col in working_df.columns:
                working_df[col] = working_df[col].fillna('-').astype(str)

        if agg_checkbox:
            working_df = aggregate_data(working_df)

        error_skus = []
        for index, row in working_df.iterrows():
            total_pct = sum(
                [float(row[f'理论_{r.replace("美", "")}%' if r in ['美西', '美东'] else f'理论_{r}%']) for r in
                 regions])
            if not (99.99 <= total_pct <= 100.01): error_skus.append(
                f"• 【{row['SKU']}】 理论占比总和为: {total_pct:.1f}%")

        if len(error_skus) > 0:
            st.error("❌ **防呆拦截：数据校验失败！**")
            st.warning("以下 SKU 的【理论分区占比】之和不等于 100%（或比例未填写完全）。请在上方表格修正：\n\n" + "\n".join(
                error_skus))
            if 'alloc_result' in st.session_state: del st.session_state['alloc_result']
        else:
            with st.spinner("启动高性能拉式逆向排期与水池引擎 (支持 20,000+ SKU 秒级运算)..."):
                df_result = calculate_allocation_v34(working_df, transit_times, d_diff, earliest_etd, target_eta,
                                                     south_linkage)
                st.session_state['alloc_result'] = df_result
                st.session_state['working_df'] = working_df

if 'alloc_result' in st.session_state:
    cached_result = st.session_state['alloc_result']

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


    def highlight_risk(row):
        styles = [''] * len(row)
        try:
            if row['🚚 预估跨区订单数量'] > 0:
                styles[row.index.get_loc(
                    '🚚 预估跨区订单数量')] = 'background-color: #fff3cd; color: #cc0000; font-weight: bold'
        except KeyError:
            pass
        return styles


    st.dataframe(filtered_result.style.apply(highlight_risk, axis=1), use_container_width=True)
    csv_result = filtered_result.to_csv(index=False).encode('utf-8-sig')
    st.download_button("📥 导出物流装柜/发货指令表 (当前视图)", data=csv_result,
                       file_name=f'装柜排期计划_{today.strftime("%Y%m%d")}.csv', mime='text/csv')

# ==========================================
# 🚀 4. 时空沙盘 (全局联动推演)
# ==========================================
st.markdown("---")
st.header("🕰️ 4. 时空沙盘：特定日期库存分布穿越")

col_d1, col_d2 = st.columns([1, 4])
with col_d1:
    target_date = st.date_input("选择目标查询日期", value=target_eta)
    check_btn = st.button("🚀 穿越至该日推演", type="secondary")

with col_d2:
    if check_btn:
        if 'alloc_result' not in st.session_state:
            st.warning("⚠️ 请先在上方修复数据并点击【开始逆向推演运算】生成分仓方案！")
        else:
            alloc_df = st.session_state['alloc_result']
            working_df = st.session_state['working_df']
            time_machine_results = []

            filtered_edited_df = working_df.copy()
            if 'search_sku' in locals() and search_sku:
                filtered_edited_df = filtered_edited_df[
                    filtered_edited_df['SKU'].str.contains(search_sku, case=False, na=False, regex=False)]
            if 'sel_shop' in locals() and sel_shop:
                filtered_edited_df = filtered_edited_df[
                    filtered_edited_df['店铺'].apply(lambda x: any(s in x for s in sel_shop))]
            if 'sel_group' in locals() and sel_group:
                filtered_edited_df = filtered_edited_df[filtered_edited_df['组别'].isin(sel_group)]
            if 'sel_op' in locals() and sel_op:
                filtered_edited_df = filtered_edited_df[
                    filtered_edited_df['运营'].apply(lambda x: any(o in x for o in sel_op))]

            if filtered_edited_df.empty:
                st.info("ℹ️ 当前筛选条件下没有数据，请调整上方的搜索条件。")
            else:
                for index, row in filtered_edited_df.iterrows():
                    sku, shop, group, op = row['SKU'], row.get('店铺', '-'), row.get('组别', '-'), row.get('运营', '-')
                    alloc_row = alloc_df[
                        (alloc_df['SKU'] == sku) & (alloc_df['店铺'] == shop) & (alloc_df['组别'] == group) & (
                                    alloc_df['运营'] == op)].iloc[0]

                    raw_ratios = {
                        r: float(row[f'理论_{r.replace("美", "")}%' if r in ['美西', '美东'] else f'理论_{r}%']) for r
                        in regions}
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

                    tz_cross_zone_orders = 0.0
                    days_to_sim = (target_date - today).days
                    for d in range(1, days_to_sim + 1):
                        sim_d = today + datetime.timedelta(days=d)
                        for r in regions:
                            if sim_d in in_transits[r]: sim_stock[r] += in_transits[r][sim_d]
                            if sim_d == arrivals[r]: sim_stock[r] += alloc_row[f'👉 {r}发货']

                        ask = {r: 0 for r in regions}
                        daily_sales = get_daily_sales_for_date(sim_d, row)
                        for r in regions:
                            demand = daily_sales * ratios[r]
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
                                    sim_stock[r] -= split;
                                    tz_cross_zone_orders += split
                                else:
                                    tz_cross_zone_orders += sim_stock[r];
                                    unmet += (split - sim_stock[r]);
                                    sim_stock[r] = 0

                    total_inv = sum(sim_stock.values())
                    pct = {r: (sim_stock[r] / total_inv * 100) if total_inv > 0 else 0 for r in regions}

                    time_machine_results.append({
                        'SKU': sku, '店铺': shop, '组别': group, '运营': op,
                        f'📅 {target_date} 总库存': int(total_inv),
                        '🇺🇸 实际占比 (西:东:GA:TX:CG)': f"{pct['美西']:.0f}% : {pct['美东']:.0f}% : {pct['GA']:.0f}% : {pct['TX']:.0f}% : {pct['CG']:.0f}%",
                        '🚚 累计跨区订单数量': int(round(tz_cross_zone_orders)),
                        '美西结存': int(sim_stock['美西']), '美东结存': int(sim_stock['美东']),
                        'GA结存': int(sim_stock['GA']), 'TX结存': int(sim_stock['TX']), 'CG结存': int(sim_stock['CG'])
                    })
                st.success(f"✅ 已推演至 {target_date} 的平行时空！(已同步应用检索条件)")
                st.dataframe(pd.DataFrame(time_machine_results), use_container_width=True)