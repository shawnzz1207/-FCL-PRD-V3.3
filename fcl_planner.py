# V3.6.5 单文件部署版
# 启动: streamlit run app_v3.6.5.py
import pandas as pd
import datetime
import calendar
import copy
import io
import math
import streamlit as st

# 设置宽布局（必须是第一个 streamlit 命令）
st.set_page_config(page_title="北美全渠道智能分仓系统 V3.6.5", layout="wide")
# ============================================================
# 全局常量
# ============================================================
REGIONS = ['美西', '美东', 'GA', 'TX', 'CG']
TRANSFER_REGIONS = ['美西', '美东', 'GA', 'TX']  # CG 不参与调拨
FORECAST_COLS = [
    'M1预测(当月)', 'M2预测(次月)', 'M3预测(第3月)',
    'M4预测(第4月)', 'M5预测(第5月)', 'M6预测(第6月)'
]
CONTRACT_SOURCE_COLS = {
    'SKU': '产品编号',
    '店铺': '店铺',
    '运营': '运营专员',
    '组别': '运营组别',
    'PO号': '采购合同',
    '合同可用数量': '可供应总数',
    '交货日期': '出货日期',
}
CONTRACT_DERIVED_COLS = [
    '本轮理论发货量', '本轮可用合同量', '本轮合同缺口',
    '剩余合同数量', '推荐下单量', '最晚下单日', '备货状态'
]
DEFAULT_LEAD_DAYS = 60


# ============================================================
# 通用辅助函数
# ============================================================
def ratio_col_name(r):
    """返回占比列名"""
    return f'理论_{r.replace("美", "")}%' if r in ['美西', '美东'] else f'理论_{r}%'


def parse_in_transit(val):
    """解析 '2026-04-25:2000; 2026-05-10:500' → {date: qty}

    兼容多种分隔符：
    - 中英文分号：; ；
    - 中英文逗号：, ，
    - 中文冒号：：
    """
    if pd.isna(val) or str(val).strip() == '':
        return {}
    res = {}
    clean = str(val).replace('；', ';').replace('：', ':').replace('，', ',')
    # 把所有分隔符统一替换成 ;
    clean = clean.replace(',', ';')
    for part in clean.split(';'):
        part = part.strip()
        if ':' in part:
            try:
                d_str, q_str = part.split(':', 1)  # maxsplit=1 防多个冒号
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
    """同 SKU + 组别 的多行数据合并为一行
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
        for m in FORECAST_COLS:
            row[m] = group_df[m].sum() if m in group_df.columns else 0.0
        grouped_records.append(row)
    return pd.DataFrame(grouped_records)


def round_preserve_sum(float_dict, target_sum):
    """四舍五入各区分配量，同时保证和 = target_sum
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
    """组别-运营/店铺 作为行唯一标识"""
    return f"{row.get('组别', '-')}-{row.get('运营', '-')}/{row.get('店铺', '-')}"


def append_note(existing, note):
    """追加备注，避免覆盖推荐发货量或合同校验已有提示。"""
    existing = '' if pd.isna(existing) else str(existing).strip()
    if not note:
        return existing
    if not existing:
        return note
    if note in existing:
        return existing
    return f"{existing}; {note}"


def clean_text(val):
    """把 Excel / dataframe 值转成用于匹配的干净文本。"""
    if pd.isna(val):
        return ''
    return str(val).strip()


def parse_date_value(val):
    """兼容 Excel 日期、字符串日期与空值。"""
    if pd.isna(val) or str(val).strip() == '':
        return None
    if isinstance(val, datetime.datetime):
        return val.date()
    if isinstance(val, datetime.date):
        return val
    parsed = pd.to_datetime(val, errors='coerce')
    if pd.isna(parsed):
        return None
    return parsed.date()


def normalize_contract_qty(val):
    qty = pd.to_numeric(pd.Series([val]), errors='coerce').fillna(0).iloc[0]
    return max(0, int(round(float(qty))))


def contract_match_key_from_values(sku, operator, shop, group):
    return tuple(clean_text(v) for v in [sku, operator, shop, group])


def contract_match_key_from_row(row):
    return contract_match_key_from_values(
        row.get('SKU', ''), row.get('运营', ''),
        row.get('店铺', ''), row.get('组别', '')
    )


def contract_match_key_from_contract(contract):
    return contract_match_key_from_values(
        contract.get('SKU', ''), contract.get('运营', ''),
        contract.get('店铺', ''), contract.get('组别', '')
    )


def load_contract_workbook(uploaded_contract_file):
    """读取合同明细文件：Sheet1 合同明细，Sheet2 SPU交期。"""
    contracts = []
    lead_days_by_spu = {}
    errors = []

    if uploaded_contract_file is None:
        errors.append({
            'SKU': '-', 'SPU': '-', '组别': '-', '运营': '-', '店铺': '-',
            '异常类型': '缺少合同文件',
            '异常说明': '请上传包含合同明细和交期表的 Excel 文件'
        })
        return contracts, lead_days_by_spu, errors

    try:
        if hasattr(uploaded_contract_file, 'seek'):
            uploaded_contract_file.seek(0)
        xls = pd.ExcelFile(uploaded_contract_file)
    except Exception as e:
        errors.append({
            'SKU': '-', 'SPU': '-', '组别': '-', '运营': '-', '店铺': '-',
            '异常类型': '合同文件读取失败',
            '异常说明': str(e)
        })
        return contracts, lead_days_by_spu, errors

    if len(xls.sheet_names) < 2:
        errors.append({
            'SKU': '-', 'SPU': '-', '组别': '-', '运营': '-', '店铺': '-',
            '异常类型': '缺少交期表',
            '异常说明': '合同文件需包含 Sheet1 合同明细和 Sheet2 交期表'
        })
        return contracts, lead_days_by_spu, errors

    try:
        contract_df = pd.read_excel(xls, sheet_name=0)
        lead_df = pd.read_excel(xls, sheet_name=1)
    except Exception as e:
        errors.append({
            'SKU': '-', 'SPU': '-', '组别': '-', '运营': '-', '店铺': '-',
            '异常类型': '合同文件解析失败',
            '异常说明': str(e)
        })
        return contracts, lead_days_by_spu, errors

    missing_contract_cols = [src for src in CONTRACT_SOURCE_COLS.values()
                             if src not in contract_df.columns]
    if missing_contract_cols:
        errors.append({
            'SKU': '-', 'SPU': '-', '组别': '-', '运营': '-', '店铺': '-',
            '异常类型': '合同明细缺少字段',
            '异常说明': f"缺少列: {', '.join(missing_contract_cols)}"
        })

    if 'SPU' not in lead_df.columns or '交期' not in lead_df.columns:
        errors.append({
            'SKU': '-', 'SPU': '-', '组别': '-', '运营': '-', '店铺': '-',
            '异常类型': '交期表缺少字段',
            '异常说明': 'Sheet2 必须包含 SPU、交期 两列表头'
        })

    if missing_contract_cols or 'SPU' not in lead_df.columns or '交期' not in lead_df.columns:
        return contracts, lead_days_by_spu, errors

    for row_idx, row in contract_df.iterrows():
        sku = clean_text(row.get(CONTRACT_SOURCE_COLS['SKU']))
        shop = clean_text(row.get(CONTRACT_SOURCE_COLS['店铺']))
        operator = clean_text(row.get(CONTRACT_SOURCE_COLS['运营']))
        group = clean_text(row.get(CONTRACT_SOURCE_COLS['组别']))
        po = clean_text(row.get(CONTRACT_SOURCE_COLS['PO号']))
        qty = normalize_contract_qty(row.get(CONTRACT_SOURCE_COLS['合同可用数量']))
        delivery_date = parse_date_value(row.get(CONTRACT_SOURCE_COLS['交货日期']))

        if not sku or not shop or not operator or not group:
            errors.append({
                'SKU': sku or '-', 'SPU': '-', '组别': group or '-',
                '运营': operator or '-', '店铺': shop or '-',
                '异常类型': '合同归属字段缺失',
                '异常说明': f"合同明细第 {row_idx + 2} 行缺少 SKU/店铺/运营/组别"
            })
            continue
        if qty <= 0:
            continue
        if delivery_date is None:
            errors.append({
                'SKU': sku, 'SPU': '-', '组别': group, '运营': operator, '店铺': shop,
                '异常类型': '合同交货日期异常',
                '异常说明': f"合同明细第 {row_idx + 2} 行出货日期为空或格式异常"
            })
            continue

        contracts.append({
            'SKU': sku,
            '店铺': shop,
            '运营': operator,
            '组别': group,
            'PO号': po or '-',
            '合同可用数量': qty,
            '交货日期': delivery_date,
        })

    for row_idx, row in lead_df.iterrows():
        spu = clean_text(row.get('SPU'))
        lead_days = pd.to_numeric(pd.Series([row.get('交期')]), errors='coerce').iloc[0]
        if not spu:
            continue
        if pd.isna(lead_days) or float(lead_days) < 0:
            errors.append({
                'SKU': '-', 'SPU': spu, '组别': '-', '运营': '-', '店铺': '-',
                '异常类型': '交期异常',
                '异常说明': f"交期表第 {row_idx + 2} 行交期为空或非数字"
            })
            continue
        lead_days_by_spu[spu] = int(round(float(lead_days)))

    return contracts, lead_days_by_spu, errors


def get_global_supply_events(row):
    """合同备货只看全网总供应，不参与分区水池分配。"""
    initial_stock = sum(float(row.get(f'{r}_在仓', 0) or 0) for r in REGIONS)
    events = []
    for r in REGIONS:
        for dt, qty in parse_in_transit(row.get(f'{r}_多批次在途', '')).items():
            if qty > 0:
                events.append((dt, float(qty)))
    return initial_stock, events


def forecast_demand_between(row, today, start_date, end_date, daily_sales_fn=None):
    if end_date < start_date:
        return 0.0
    daily_sales = daily_sales_fn or build_daily_sales_fn(row, today)
    total = 0.0
    d_obj = start_date
    while d_obj <= end_date:
        total += daily_sales(d_obj)
        d_obj += datetime.timedelta(days=1)
    return total


def simulate_global_stock(row, initial_stock, arrival_events, today, until=None, max_days=2200,
                          daily_sales_fn=None):
    """全网日粒度库存推演，返回首次补货需求日和最终耗尽日。"""
    daily_sales = daily_sales_fn or build_daily_sales_fn(row, today)
    stock = float(initial_stock)
    events_by_date = {}
    for dt, qty in arrival_events:
        if qty > 0:
            events_by_date[dt] = events_by_date.get(dt, 0.0) + float(qty)

    last_event_date = max(events_by_date.keys(), default=today)
    sim_until = until or today + datetime.timedelta(days=max_days)
    sim_until = max(sim_until, last_event_date + datetime.timedelta(days=1))

    first_need_date = None
    final_oos_date = None
    d_obj = today + datetime.timedelta(days=1)
    while d_obj <= sim_until:
        stock += events_by_date.get(d_obj, 0.0)
        demand = daily_sales(d_obj)
        if stock + 1e-9 >= demand:
            stock -= demand
        else:
            if first_need_date is None:
                first_need_date = d_obj
            if d_obj >= last_event_date and final_oos_date is None:
                final_oos_date = d_obj
            stock = 0.0

        if until is None and final_oos_date is not None and d_obj > last_event_date:
            break
        d_obj += datetime.timedelta(days=1)

    return {
        'first_need_date': first_need_date,
        'final_oos_date': final_oos_date,
        'stock_at_end': max(0.0, stock),
    }


def deduct_current_contracts(row, row_contracts, theoretical_qty, earliest_etd):
    """本轮发货只扣交货日期不晚于最早可发货日的合同，FIFO + PO排序。"""
    need = max(0, int(round(float(theoretical_qty or 0))))
    eligible = [copy.deepcopy(c) for c in row_contracts
                if c['交货日期'] <= earliest_etd and c['合同可用数量'] > 0]
    future = [copy.deepcopy(c) for c in row_contracts
              if c['交货日期'] > earliest_etd and c['合同可用数量'] > 0]
    eligible.sort(key=lambda c: (c['交货日期'], c['PO号']))
    future.sort(key=lambda c: (c['交货日期'], c['PO号']))

    eligible_total = sum(c['合同可用数量'] for c in eligible)
    details = []
    remaining_contracts = future[:]
    deducted = 0

    for seq, contract in enumerate(eligible, 1):
        use_qty = min(need, contract['合同可用数量'])
        deducted += use_qty
        need -= use_qty
        remain_qty = contract['合同可用数量'] - use_qty
        details.append({
            'SKU': contract['SKU'],
            '组别': contract['组别'],
            '运营': contract['运营'],
            '店铺': contract['店铺'],
            'PO号': contract['PO号'],
            '合同交货日期': contract['交货日期'].strftime('%Y-%m-%d'),
            '合同原可用数量': contract['合同可用数量'],
            '本轮扣减数量': use_qty,
            '扣减后剩余数量': remain_qty,
            '扣减顺序': seq,
            '备注': '' if use_qty > 0 else '本轮理论发货已满足，未扣减',
        })
        if remain_qty > 0:
            remaining = copy.deepcopy(contract)
            remaining['合同可用数量'] = remain_qty
            remaining_contracts.append(remaining)
        if need <= 0:
            remaining_contracts.extend(copy.deepcopy(c) for c in eligible[seq:])
            break

    remaining_contracts.sort(key=lambda c: (c['交货日期'], c['PO号']))
    shortage = max(0, max(0, int(round(float(theoretical_qty or 0)))) - deducted)
    return {
        '本轮可用合同量': eligible_total,
        '本轮实际发货量': deducted,
        '本轮合同缺口': shortage,
        '剩余合同': remaining_contracts,
        '扣减明细': details,
    }


def contract_plan_qty(row, today, need_date, next_arrival_date, sales_cycle_days,
                      daily_sales_fn=None):
    if next_arrival_date and next_arrival_date > need_date:
        end_date = next_arrival_date - datetime.timedelta(days=1)
    else:
        end_date = need_date + datetime.timedelta(days=max(0, int(sales_cycle_days) - 1))
    daily_sales_fn = daily_sales_fn or build_contract_daily_sales_fn(row, today)
    return max(0, int(math.ceil(forecast_demand_between(
        row, today, need_date, end_date, daily_sales_fn=daily_sales_fn
    ))))


def schedule_remaining_contracts(row, remaining_contracts, initial_stock, base_events,
                                 today, earliest_etd, ocean_cycle_days, sales_cycle_days,
                                 daily_sales_fn=None):
    """剩余合同允许拆分 PO，按需求日倒推建议发货日。"""
    daily_sales_fn = daily_sales_fn or build_contract_daily_sales_fn(row, today)
    pool = [copy.deepcopy(c) for c in remaining_contracts if c['合同可用数量'] > 0]
    pool.sort(key=lambda c: (c['交货日期'], c['PO号']))
    scheduled_events = list(base_events)
    plan_records = []
    first_need_date = None
    first_uncovered_gap = None
    seq = 1
    loop_guard = 0

    while pool and loop_guard < 2000:
        loop_guard += 1
        sim = simulate_global_stock(
            row, initial_stock, scheduled_events, today, daily_sales_fn=daily_sales_fn
        )
        raw_first_need = sim['first_need_date']
        if raw_first_need and first_need_date is None:
            first_need_date = raw_first_need

        # 先处理首个补货需求日，避免“先断货、后续在途接上”的空窗被最终耗尽日掩盖。
        if raw_first_need and first_uncovered_gap is None:
            need_date = raw_first_need
        else:
            need_date = sim['final_oos_date'] or raw_first_need
        if need_date is None:
            for contract in pool:
                plan_records.append({
                    'SKU': contract['SKU'],
                    'SPU': clean_text(row.get('SPU', '-')) or '-',
                    '组别': contract['组别'],
                    '运营': contract['运营'],
                    '店铺': contract['店铺'],
                    'PO号': contract['PO号'],
                    '合同交货日期': contract['交货日期'].strftime('%Y-%m-%d'),
                    '可用剩余数量': contract['合同可用数量'],
                    '本次计划发货数量': 0,
                    '发货后PO剩余': contract['合同可用数量'],
                    '建议合同发货日': '-',
                    '预计到美日': '-',
                    '是否延后发货': '-',
                    '延后原因': '无销售预测或无需发货',
                    '是否覆盖缺口': '-',
                })
            break

        contract = pool.pop(0)
        latest_ship_date = need_date - datetime.timedelta(days=int(ocean_cycle_days))
        available_ship_date = max(contract['交货日期'], earliest_etd)
        ship_date = max(available_ship_date, latest_ship_date)
        arrival_date = ship_date + datetime.timedelta(days=int(ocean_cycle_days))
        next_arrival = pool[0]['交货日期'] + datetime.timedelta(days=int(ocean_cycle_days)) if pool else None
        need_qty = contract_plan_qty(
            row, today, need_date, next_arrival, sales_cycle_days,
            daily_sales_fn=daily_sales_fn
        )
        ship_qty = min(contract['合同可用数量'], max(1, need_qty))
        remain_qty = contract['合同可用数量'] - ship_qty
        covers_gap = arrival_date <= need_date
        if not covers_gap and first_uncovered_gap is None:
            first_uncovered_gap = need_date

        scheduled_events.append((arrival_date, ship_qty))
        plan_records.append({
            'SKU': contract['SKU'],
            'SPU': clean_text(row.get('SPU', '-')) or '-',
            '组别': contract['组别'],
            '运营': contract['运营'],
            '店铺': contract['店铺'],
            'PO号': contract['PO号'],
            '合同交货日期': contract['交货日期'].strftime('%Y-%m-%d'),
            '可用剩余数量': contract['合同可用数量'],
            '本次计划发货数量': ship_qty,
            '发货后PO剩余': remain_qty,
            '建议合同发货日': ship_date.strftime('%Y-%m-%d'),
            '预计到美日': arrival_date.strftime('%Y-%m-%d'),
            '是否延后发货': '是' if ship_date > contract['交货日期'] else '否',
            '延后原因': '海外库存充足，延后发货' if ship_date > contract['交货日期'] else '',
            '是否覆盖缺口': '是' if covers_gap else '否，合同交货偏晚会产生空窗',
        })

        if remain_qty > 0:
            remaining = copy.deepcopy(contract)
            remaining['合同可用数量'] = remain_qty
            pool.insert(0, remaining)

    if loop_guard >= 2000:
        plan_records.append({
            'SKU': clean_text(row.get('SKU', '-')) or '-',
            'SPU': clean_text(row.get('SPU', '-')) or '-',
            '组别': clean_text(row.get('组别', '-')) or '-',
            '运营': clean_text(row.get('运营', '-')) or '-',
            '店铺': clean_text(row.get('店铺', '-')) or '-',
            'PO号': '-',
            '合同交货日期': '-',
            '可用剩余数量': 0,
            '本次计划发货数量': 0,
            '发货后PO剩余': 0,
            '建议合同发货日': '-',
            '预计到美日': '-',
            '是否延后发货': '-',
            '延后原因': '剩余合同拆分次数超过上限，请检查数据',
            '是否覆盖缺口': '-',
        })

    return scheduled_events, plan_records, {
        '首次补货需求日': first_need_date,
        '首个未覆盖空窗日': first_uncovered_gap,
    }


def compute_contract_recommendation(row, contracts, lead_days_by_spu, theoretical_qty,
                                    today, earliest_etd, target_eta,
                                    ocean_cycle_days, sales_cycle_days):
    row_key = contract_match_key_from_row(row)
    row_contracts = [c for c in contracts if contract_match_key_from_contract(c) == row_key]
    deduction = deduct_current_contracts(row, row_contracts, theoretical_qty, earliest_etd)
    initial_stock, current_events = get_global_supply_events(row)
    base_events = list(current_events)
    if deduction['本轮实际发货量'] > 0:
        base_events.append((target_eta, deduction['本轮实际发货量']))

    spu = clean_text(row.get('SPU'))
    lead_days = lead_days_by_spu.get(spu)
    exception_records = []
    remarks = []

    if theoretical_qty > 0 and not row_contracts:
        remarks.append('未匹配到合同明细')
        exception_records.append({
            'SKU': row.get('SKU', '-'), 'SPU': spu or '-',
            '组别': row.get('组别', '-'), '运营': row.get('运营', '-'), '店铺': row.get('店铺', '-'),
            '异常类型': '缺少合同',
            '异常说明': '未匹配到 SKU+运营+店铺+组别 对应的合同明细'
        })
    if deduction['本轮合同缺口'] > 0:
        remarks.append(f"理论发货{int(round(theoretical_qty))}，合同数量不足")
    if not spu or lead_days is None:
        lead_days = DEFAULT_LEAD_DAYS
        remarks.append(f'默认{DEFAULT_LEAD_DAYS}天交期')

    contract_daily_sales_fn = build_contract_daily_sales_fn(row, today)
    scheduled_events, plan_records, hidden = schedule_remaining_contracts(
        row, deduction['剩余合同'], initial_stock, base_events,
        today, earliest_etd, int(ocean_cycle_days), int(sales_cycle_days),
        daily_sales_fn=contract_daily_sales_fn
    )

    coverage_days = int(lead_days) + int(ocean_cycle_days) + int(sales_cycle_days)
    coverage_cutoff = today + datetime.timedelta(days=coverage_days)
    demand_to_cutoff = forecast_demand_between(
        row, today, today + datetime.timedelta(days=1), coverage_cutoff,
        daily_sales_fn=contract_daily_sales_fn
    )
    supply_to_cutoff = initial_stock + sum(qty for dt, qty in scheduled_events
                                           if dt <= coverage_cutoff)
    recommend_order_qty = max(0, int(math.ceil(demand_to_cutoff - supply_to_cutoff)))

    final_sim = simulate_global_stock(
        row, initial_stock, scheduled_events, today,
        daily_sales_fn=contract_daily_sales_fn
    )
    final_oos = final_sim['final_oos_date']
    latest_order_date = (final_oos - datetime.timedelta(days=int(lead_days) + int(ocean_cycle_days))
                         if final_oos else None)

    # 历史最晚下单日无法再覆盖此前需求，只保留当前订单可正常到美后的销售周期需求。
    if latest_order_date and latest_order_date < today:
        current_order_arrival = today + datetime.timedelta(
            days=int(lead_days) + int(ocean_cycle_days)
        )
        current_order_cutoff = current_order_arrival + datetime.timedelta(
            days=max(0, int(sales_cycle_days) - 1)
        )
        before_arrival = simulate_global_stock(
            row, initial_stock, scheduled_events, today,
            until=current_order_arrival - datetime.timedelta(days=1),
            daily_sales_fn=contract_daily_sales_fn
        )
        supply_after_arrival = before_arrival['stock_at_end'] + sum(
            qty for dt, qty in scheduled_events
            if current_order_arrival <= dt <= current_order_cutoff
        )
        demand_after_arrival = forecast_demand_between(
            row, today, current_order_arrival, current_order_cutoff,
            daily_sales_fn=contract_daily_sales_fn
        )
        recommend_order_qty = max(0, int(math.ceil(demand_after_arrival - supply_after_arrival)))
        latest_order_date = None

    if 0 < recommend_order_qty < 10:
        remarks.append(f'推荐下单量{recommend_order_qty}pcs已归零')
        recommend_order_qty = 0

    if hidden['首个未覆盖空窗日'] is not None:
        remarks.append('部分剩余合同到美晚于补货需求日，详见剩余合同发货计划')

    if deduction['本轮合同缺口'] > 0:
        status = '合同不足'
    elif not row_contracts and theoretical_qty > 0:
        status = '缺少合同'
    elif hidden['首个未覆盖空窗日'] is not None:
        status = '存在库存空窗'
    elif recommend_order_qty > 0:
        status = '建议下单'
    else:
        status = '覆盖期内无需下单'

    return {
        '本轮实际发货量': deduction['本轮实际发货量'],
        '本轮可用合同量': deduction['本轮可用合同量'],
        '本轮合同缺口': deduction['本轮合同缺口'],
        '剩余合同数量': sum(c['合同可用数量'] for c in deduction['剩余合同']),
        '推荐下单量': recommend_order_qty,
        '最晚下单日': latest_order_date.strftime('%Y-%m-%d') if latest_order_date else '-',
        '备货状态': status,
        '备注': '; '.join(remarks),
        '合同扣减明细': deduction['扣减明细'],
        '剩余合同发货计划': plan_records,
        '异常记录': exception_records,
        '_首次补货需求日': hidden['首次补货需求日'],
        '_首个未覆盖空窗日': hidden['首个未覆盖空窗日'],
        '_含剩余合同预估耗尽日': final_oos,
    }


# ============================================================
# 推演前置：计算到港日与日销函数
# ============================================================
def compute_arrivals(transit_dict, earliest_etd, target_eta):
    """计算本次发货各区到港日
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
    """根据行的 M1-M6 预测，返回一个 daily_sales(date) 函数"""
    forecasts = tuple(float(row.get(col, 0) or 0) for col in FORECAST_COLS)

    # 全段无销售预测时按真实零日销处理，避免被误判为缺货方。
    if sum(forecasts) <= 0:
        return lambda d_obj: 0.0

    def daily_sales(d_obj):
        m_diff = (d_obj.year - today.year) * 12 + d_obj.month - today.month
        m_idx = min(max(m_diff, 0), len(forecasts) - 1)
        days_in_m = calendar.monthrange(d_obj.year, d_obj.month)[1]
        return max(forecasts[m_idx] / days_in_m, 0.01)

    return daily_sales


def build_contract_daily_sales_fn(row, today):
    """合同备货只使用实际填写的 M1-M6 预测，空白/0 及 M6 后均为 0 日销。"""
    forecasts = []
    for col in FORECAST_COLS:
        try:
            value = float(row.get(col, 0) or 0)
        except (TypeError, ValueError):
            value = 0.0
        forecasts.append(value if math.isfinite(value) and value > 0 else 0.0)

    def daily_sales(d_obj):
        m_diff = (d_obj.year - today.year) * 12 + d_obj.month - today.month
        if m_diff < 0 or m_diff >= len(forecasts):
            return 0.0
        days_in_m = calendar.monthrange(d_obj.year, d_obj.month)[1]
        return forecasts[m_diff] / days_in_m

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

    注意: 本函数只负责决定"各区发多少"，不负责计算"最终占比/跨区/耗尽日"后者由 physical_simulation() 完成
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
# 物理真实推演（V3.6.5 新引擎·核心）
# ============================================================
def physical_simulation(row, transit_dict, earliest_etd, target_eta,
                        today, alloc_int, sales_cutoff, end_date=None):
    """物理真实推演（唯一口径，替换 V3.5 所有输出指标计算）

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
    """一站式计算一行的所有输出指标（主看板展示用）

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

    # 若被 override，临时改 row 的 '本次总发货量'供推演用（不污染原 row）
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
    """沙盘查询：返回 query_date 那天的库存分布和累计跨区
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

    性能优化：单次推演同时输出 SD/RQ/CZ（替代旧版的 2 次推演）
    """
    # 先算水池分配
    alloc = waterpool_allocation_v2(
        row, transit_dict, earliest_etd, target_eta, today, south_linkage
    )
    # 单次推演同时输出 SD/RQ/CZ
    row_dict = row if isinstance(row, dict) else row.to_dict()
    sd, rq, cz, sim_stock_at_cutoff = _compute_sd_rq_cz_in_one_pass(
        row_dict, alloc, transit_dict, earliest_etd, target_eta,
        today, sales_cutoff
    )

    return {
        'SD': sd,
        'RQ': rq,
        'CZ': cz,
        'alloc': alloc,
        'sim_stock_at_cutoff': sim_stock_at_cutoff,
    }


def _compute_sd_rq_cz_in_one_pass(row, alloc, transit_dict, earliest_etd, target_eta,
                                  today, sales_cutoff):
    """
    单次物理推演同时算出 SD / RQ / CZ
    （替代分开调用 physical_simulation 和 compute_sd，性能优化）
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
    cz_total = 0.0
    sales_window = (sales_cutoff - today).days
    sim_stock_at_cutoff = None

    for d_idx in range(1, sales_window + 1):
        sim_date = today + datetime.timedelta(days=d_idx)
        # 到港入库
        for r in REGIONS:
            if sim_date in in_transits[r]:
                sim_stock[r] += in_transits[r][sim_date]
            if sim_date == arrivals[r]:
                sim_stock[r] += alloc.get(r, 0)

        # 消耗前先看全网总库存（决定 SD）
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

        # 跨区均摊
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
                    cz_total += split
                else:
                    cz_total += sim_stock[r]
                    unmet += (split - sim_stock[r])
                    sim_stock[r] = 0.0

        # 销售截止日当天截取库存（即 sales_window 那天）
        if d_idx == sales_window:
            sim_stock_at_cutoff = {r: max(0, sim_stock[r]) for r in REGIONS}

    if sim_stock_at_cutoff is None:
        sim_stock_at_cutoff = {r: max(0, sim_stock[r]) for r in REGIONS}

    rq = sum(sim_stock_at_cutoff.values())
    return sd_count, rq, cz_total, sim_stock_at_cutoff


def compute_sd(row, alloc, transit_dict, earliest_etd, target_eta,
               today, sales_cutoff):
    """精确计算"实际可售天数" SD
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
# V3.7a 新增: 推荐发货量引擎(解析法)
# ============================================================
def recommend_q_ship(row_dict, transit_dict, earliest_etd, target_eta,
                     today, sales_cutoff, south_linkage=False,
                     tol=1.0, max_iter=4):
    """推荐发货量: 目标为销售截止日当天 RQ ≈ 0(刚好售罄)

    算法(解析法, 每行仅 2-4 次推演):
      步骤1: q=0 推演 → RQ>0 说明现有库存已冗余, 推荐 0
      步骤2: q=Q_max(销售窗口总需求) 推演 → q* = Q_max - RQ_max
             (超出网络消化能力的部分 1:1 变成剩余, 解析反推)
      步骤3: 用 q* 验证, 若 RQ 仍超容差则牛顿式修正 q* -= RQ

    Returns:
        (q_final, q_raw, status)
        q_final: 最终推荐量(已应用 1-9 归零规则)
        q_raw:   应用 1-9 归零前的整数推荐量(用于归零提示)
        status:  'redundant' 已冗余 / 'ok' 正常 / 'small' 1-9被归零
    """
    work = dict(row_dict)

    # ---- 步骤1: q=0 ----
    work['本次总发货量'] = 0.0
    st0 = compute_row_status(work, transit_dict, earliest_etd, target_eta,
                             today, sales_cutoff, south_linkage)
    if st0['RQ'] > 0.5:
        return 0, 0, 'redundant'

    # ---- 步骤2: q=Q_max 解析反推 ----
    daily_sales = build_daily_sales_fn(row_dict, today)
    sales_window = (sales_cutoff - today).days
    q_max = sum(daily_sales(today + datetime.timedelta(days=i))
                for i in range(1, sales_window + 1))
    q_max = float(int(q_max) + 10)

    if q_max <= 0.5:
        return 0, 0, 'ok'

    work['本次总发货量'] = q_max
    st_max = compute_row_status(work, transit_dict, earliest_etd, target_eta,
                                today, sales_cutoff, south_linkage)
    q_star = q_max - st_max['RQ']

    # ---- 步骤3: 验证 + 牛顿式修正 ----
    for _ in range(max_iter):
        if q_star <= 0.5:
            q_star = 0.0
            break
        work['本次总发货量'] = q_star
        st_v = compute_row_status(work, transit_dict, earliest_etd, target_eta,
                                  today, sales_cutoff, south_linkage)
        if st_v['RQ'] <= tol:
            break
        q_star -= st_v['RQ']

    # ---- 步骤4: 整数取整后复核 ----
    # 四舍五入可能重新产生 0.5-1 件余量；按整数件下调，避免阶段2再次减 1。
    q_raw = max(0, int(round(q_star)))
    for _ in range(max_iter):
        if q_raw <= 0:
            break
        work['本次总发货量'] = float(q_raw)
        st_int = compute_row_status(work, transit_dict, earliest_etd, target_eta,
                                    today, sales_cutoff, south_linkage)
        if st_int['RQ'] <= 0.5:
            break
        adjustment = max(1, int(math.ceil(st_int['RQ'] - 0.5)))
        q_raw = max(0, q_raw - adjustment)

    if 1 <= q_raw <= 9:
        return 0, q_raw, 'small'
    return q_raw, q_raw, 'ok'

# ============================================================
# 调拨执行函数（带物理上限保护）
# ============================================================
def apply_transfer(df, out_idx, in_idx, src_type, region, source_date, qty):
    """执行一次调拨（原地修改 df）
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


def _row_status_cache_key(row_dict):
    """提取影响库存状态的字段，供阶段1复用相同试探结果。"""
    status_cols = (
        ['本次总发货量']
        + [ratio_col_name(r) for r in REGIONS]
        + [f'{r}_在仓' for r in REGIONS]
        + [f'{r}_多批次在途' for r in REGIONS]
        + FORECAST_COLS
    )
    values = []
    for col in status_cols:
        value = row_dict.get(col)
        try:
            if pd.isna(value):
                value = None
        except (TypeError, ValueError):
            value = repr(value)
        try:
            hash(value)
        except TypeError:
            value = repr(value)
        values.append(value)
    return tuple(values)


def _compute_row_status_cached(row_dict, transit_dict, earliest_etd, target_eta,
                               today, sales_cutoff, south_linkage, status_cache):
    key = _row_status_cache_key(row_dict)
    if key not in status_cache:
        status_cache[key] = compute_row_status(
            row_dict, transit_dict, earliest_etd, target_eta,
            today, sales_cutoff, south_linkage
        )
    return status_cache[key]


# ============================================================
# 阶段1：冗余调拨
# ============================================================
def stage1_redundancy_transfer(df, transit_dict, earliest_etd, target_eta,
                               today, sales_cutoff, south_linkage=False):
    """阶段1 全局贪心冗余调拨
    返回: (transfer_records, df_after)
    """
    df = df.copy().reset_index(drop=True)
    transfer_records = []
    sales_window = (sales_cutoff - today).days

    for sku, sku_group in df.groupby('SKU'):
        sku_indices = sku_group.index.tolist()
        if len(sku_indices) < 2:
            continue
        status_cache = {}

        max_iter = 50
        for iter_count in range(max_iter):
            # 计算当前所有行的状态
            status = {}
            for idx in sku_indices:
                st = _compute_row_status_cached(
                    df.loc[idx].to_dict(), transit_dict, earliest_etd, target_eta,
                    today, sales_cutoff, south_linkage, status_cache
                )
                status[idx] = st

            # 识别冗余方 & 缺货方
            # 冗余方：RQ > 0（销售截止日还有剩余）
            redundant = [idx for idx in sku_indices if status[idx]['RQ'] > 0.5]
            # 缺货方：SD < 销售窗口天数（过程中存在断货，不论 RQ）
            #   - 包含真·缺货方（RQ=0，整体卖空）
            #   - 包含等待期伪缺货方（RQ>0，但启动期/中途有空窗）
            # 全段预测为 0 的行没有销售需求，不进入调入候选。
            shortage = [idx for idx in sku_indices
                        if sum(float(df.at[idx, col]) for col in FORECAST_COLS) > 0
                        and status[idx]['SD'] < sales_window - 0.5]

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
                            sales_window, status[g_idx], status[r_idx], status_cache
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

                        r_status_new = _compute_row_status_cached(
                            df.loc[r_idx].to_dict(), transit_dict, earliest_etd, target_eta,
                            today, sales_cutoff, south_linkage, status_cache
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
                               sales_window, g_status, r_status, status_cache=None):
    """二分搜索：找出"3 重锁"下最大的可调拨量
    锁1: qty <= src_max （物理上限）  → 已在 apply_transfer 保护
    锁2: 调出方调出后 SD 不下降（不变得"更缺货"）
    锁3: 调入方调入后不产生新的可减冗余
         - 原 RQ ≤ 0.5 时，调入后 RQ 最高为 0.5
         - 原 RQ > 0.5 时，调入后 RQ 不得超过原值
    """
    lo = 0.0
    hi = float(src_max)
    best = 0.0
    g_baseline_sd = g_status['SD']
    r_baseline_rq = r_status['RQ']
    r_rq_limit = max(r_baseline_rq, 0.5)
    if status_cache is None:
        status_cache = {}

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
        g_new = _compute_row_status_cached(
            df.loc[g_idx].to_dict(), transit_dict, earliest_etd, target_eta,
            today, sales_cutoff, south_linkage, status_cache
        )
        r_new = _compute_row_status_cached(
            df.loc[r_idx].to_dict(), transit_dict, earliest_etd, target_eta,
            today, sales_cutoff, south_linkage, status_cache
        )

        restore_rows(df, backup)

        # 锁2: 调出方 SD 不下降（不变得更缺货）
        lock2_ok = (g_new['SD'] >= g_baseline_sd - 0.5)
        # 锁3: 不让本阶段新增需要在阶段2处理的冗余。
        lock3_ok = (r_new['RQ'] <= r_rq_limit + 1e-6)

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
    """阶段2 独立减量
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
    """执行阶段1（冗余调拨）+ 阶段2（独立减量）
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
                    + FORECAST_COLS)
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
    """返回该行在指定区域的可调出物理库存量
    （在仓 + 在途，不含本次发货量，因为本次发货量是分区前的）
    """
    capacity = 0.0
    capacity += float(row_dict.get(f'{region}_在仓', 0) or 0)
    for dt, qty in parse_in_transit(row_dict.get(f'{region}_多批次在途', '')).items():
        capacity += qty
    return capacity


def filter_partition_candidates(df, sku_indices, status_dict):
    """候选预筛（选项 C 简单预筛）：
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
    """评估单向调拨的合法性 + ΔCZ 改善
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
    """评估双向对调原子动作: A 调 X → B + B 调 Y → A，两步必须同时合法
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
    多档位扫描单向调拨最大有效量（性能优化：6 档 + 3 档精细化）
    """
    if src_max < 1:
        return 0.0, 0.0

    best_qty = 0.0
    best_delta = 0.0

    fracs = [0.10, 0.30, 0.50, 0.70, 0.90, 1.00]
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

    # 精细化（最优点附近 3 档）
    if best_qty > 0:
        for adj in [-0.05, 0.05, -0.10]:
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
    多档位扫描双向对调最大有效量（性能优化：6 档 + 3 档精细化）

    重要：ΔCZ 关于调拨量呈 V 字曲线（先降后升），不能用二分。
    """
    cap = min(max_x, max_y)
    if cap < 1:
        return 0.0, 0.0

    best_qty = 0.0
    best_delta = 0.0

    fracs = [0.10, 0.30, 0.50, 0.70, 0.90, 1.00]
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

    # 精细化（最优点附近 3 档）
    if best_qty > 0:
        for adj in [-0.05, 0.05, -0.10]:
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
    阶段3 全局贪心分区调拨（性能优化版）
    返回: (transfer_records, df_after)

    优化点:
    1. 单 SKU 总 CZ <= 改善阈值 直接跳过（最关键的早退）
    2. 单 SKU < 2 行直接跳过
    3. 同区域多批次按到港日聚合代表，避免重复评估
    4. 降低扫描档位：6 档 + 3 档精细化
    """
    df = df.copy().reset_index(drop=True)
    transfer_records = []

    DELTA_THRESHOLD = 5.0  # 改善阈值

    for sku, sku_group in df.groupby('SKU'):
        sku_indices = sku_group.index.tolist()
        if len(sku_indices) < 2:
            continue

        # ===== 早退1: 整 SKU 跨区总和已经很小，没优化空间 =====
        total_cz_check = 0
        skip_status_cache = {}
        for idx in sku_indices:
            st = compute_row_status(
                df.loc[idx].to_dict(), transit_dict, earliest_etd, target_eta,
                today, sales_cutoff, south_linkage
            )
            skip_status_cache[idx] = st
            total_cz_check += st['CZ']
        if total_cz_check < DELTA_THRESHOLD * 2:  # 整 SKU 跨区不到 10，根本不值得调拨
            continue

        max_iter = 15  # 迭代上限
        last_baseline = skip_status_cache  # 复用第一次计算的 baseline

        for iter_count in range(max_iter):
            # 计算所有行的基线状态（首轮已缓存，后续轮重算）
            if iter_count == 0:
                baseline_status = last_baseline
            else:
                baseline_status = {}
                for idx in sku_indices:
                    baseline_status[idx] = compute_row_status(
                        df.loc[idx].to_dict(), transit_dict, earliest_etd, target_eta,
                        today, sales_cutoff, south_linkage
                    )

            # 候选预筛
            single_candidates = filter_partition_candidates(df, sku_indices, baseline_status)

            best_action = None
            best_delta_cz = DELTA_THRESHOLD

            # ----- 评估单向候选 -----
            for out_idx, in_idx, region in single_candidates:
                out_row = df.loc[out_idx].to_dict()
                # 该区可调批次：在仓 + 在途（按到港日先后聚合：只取最早可用的代表）
                # 优化：单向调拨在该区内不同批次只在"到港日"上有差异，
                #       但锁判定本身已包含日期影响，简化只取总量代表
                sources = []
                in_wh = float(out_row.get(f'{region}_在仓', 0))
                if in_wh > 0.5:
                    sources.append(('在仓', None, in_wh))
                # 在途批次按到港日排序，只评估前 2 个（最早 + 最晚），减少候选
                in_transits_list = [(dt, qty) for dt, qty in
                                    parse_in_transit(out_row.get(f'{region}_多批次在途', '')).items()
                                    if qty > 0.5]
                in_transits_list.sort(key=lambda x: x[0])
                if in_transits_list:
                    sources.append(('在途', in_transits_list[0][0], in_transits_list[0][1]))
                    if len(in_transits_list) > 1:
                        sources.append(('在途', in_transits_list[-1][0], in_transits_list[-1][1]))

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
            # 优化：每行 × 每区只保留一个"代表批次"（最早可用且量最大的）
            for i_a, a_idx in enumerate(sku_indices):
                for b_idx in sku_indices[i_a + 1:]:
                    a_row = df.loc[a_idx].to_dict()
                    b_row = df.loc[b_idx].to_dict()

                    if baseline_status[a_idx]['CZ'] <= 0.5 or baseline_status[b_idx]['CZ'] <= 0.5:
                        continue

                    # 收集双方各区的代表批次（每区最多 1 个）
                    def get_rep_source(row, region):
                        """获取该行该区的代表批次：优先在仓，否则最早到港的在途"""
                        in_wh = float(row.get(f'{region}_在仓', 0))
                        if in_wh > 0.5:
                            return ('在仓', None, in_wh)
                        in_t = [(dt, qty) for dt, qty in
                                parse_in_transit(row.get(f'{region}_多批次在途', '')).items()
                                if qty > 0.5]
                        if in_t:
                            in_t.sort(key=lambda x: x[0])
                            return ('在途', in_t[0][0], in_t[0][1])
                        return None

                    a_reps = {r: get_rep_source(a_row, r) for r in TRANSFER_REGIONS}
                    b_reps = {r: get_rep_source(b_row, r) for r in TRANSFER_REGIONS}

                    # 枚举区域对 (X, Y)，X < Y 避免重复
                    for ix, region_x in enumerate(TRANSFER_REGIONS):
                        if a_reps[region_x] is None:
                            continue
                        for region_y in TRANSFER_REGIONS[ix + 1:]:
                            if b_reps[region_y] is None:
                                continue

                            # 主调拨方向: A 的 X → B + B 的 Y → A
                            type_x, date_x, max_x = a_reps[region_x]
                            type_y, date_y, max_y = b_reps[region_y]
                            qty, delta = binary_search_swap_transfer(
                                df, a_idx, b_idx,
                                type_x, region_x, date_x, max_x,
                                type_y, region_y, date_y, max_y,
                                transit_dict, earliest_etd, target_eta,
                                today, sales_cutoff, south_linkage, baseline_status
                            )
                            if qty >= 1 and delta > best_delta_cz:
                                best_delta_cz = delta
                                best_action = {
                                    'kind': 'swap',
                                    'a_idx': a_idx, 'b_idx': b_idx,
                                    'type_x': type_x, 'region_x': region_x, 'date_x': date_x, 'qty_x': qty,
                                    'type_y': type_y, 'region_y': region_y, 'date_y': date_y, 'qty_y': qty,
                                    'delta_cz': delta,
                                }

                            # 镜像：A 的 Y → B + B 的 X → A
                            if a_reps[region_y] is None or b_reps[region_x] is None:
                                continue
                            type_y2, date_y2, max_y2 = a_reps[region_y]
                            type_x2, date_x2, max_x2 = b_reps[region_x]
                            qty, delta = binary_search_swap_transfer(
                                df, b_idx, a_idx,
                                type_x2, region_x, date_x2, max_x2,
                                type_y2, region_y, date_y2, max_y2,
                                transit_dict, earliest_etd, target_eta,
                                today, sales_cutoff, south_linkage, baseline_status
                            )
                            if qty >= 1 and delta > best_delta_cz:
                                best_delta_cz = delta
                                best_action = {
                                    'kind': 'swap',
                                    'a_idx': b_idx, 'b_idx': a_idx,
                                    'type_x': type_x2, 'region_x': region_x, 'date_x': date_x2, 'qty_x': qty,
                                    'type_y': type_y2, 'region_y': region_y, 'date_y': date_y2, 'qty_y': qty,
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
                actual_x = apply_transfer(df, ba['a_idx'], ba['b_idx'],
                                          ba['type_x'], ba['region_x'], ba['date_x'], ba['qty_x'])
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
    """阶段4 死冗余检测：经过阶段1+2+3 后仍 RQ > 0 的行
    """
    dead_records = []
    for idx, row in df.iterrows():
        row_dict = row.to_dict()
        st = compute_row_status(row_dict, transit_dict, earliest_etd, target_eta,
                                today, sales_cutoff, south_linkage)
        if st['RQ'] > 0.5:
            forecasts = tuple(float(row_dict.get(col, 0) or 0)
                              for col in FORECAST_COLS)
            avg_daily = max(sum(forecasts) / (30 * len(FORECAST_COLS)), 0.1)
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
    """完整调拨流水线
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
                    + FORECAST_COLS)
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

st.title("📦 北美全渠道智能库存计算器 V3.7.0")
st.caption("🎯 分层双轨调拨版 · 物理真实推演 · 状态机交互")

with st.expander("📖 核心指标说明", expanded=False):
    st.markdown("""
    **核心日期**：
    - 销售截止日：这批货应在该日期前售罄的业务底线日
    - 最早可发货日期 / 目标上架时间：物流时间窗

    **状态指标**：
    - 预估跨区订单数量：从今天到销售截止日的累计物理跨区订单
    - 最终全网占比估值：在 real_final_arrival 当天截取的物理库存占比
    - 最终全网到货日：最后一批"有货量"到港的事件日期
    - 预估全网耗尽日：物理全部卖空的日期

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
        st.error("销售截止日必须晚于目标上架日！请调整。")
        st.stop()

    d_diff = (target_eta - earliest_etd).days
    if d_diff < 0:
        st.error("上架时间不能早于最早发货日期！")
        st.stop()
    st.success(f"物流 D差: {d_diff} 天")
    st.success(f"销售窗口: {(sales_cutoff - target_eta).days} 天")

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
        st.error(f"极速熔断：D差 ({d_diff}天) 小于最短海运时效 ({min(transit_times.values())}天)！")

    st.markdown("---")
    st.subheader("📦 合同备发货参数")
    default_ocean_cycle = int(round(sum(transit_times[r] for r in ['美西', '美东', 'GA', 'TX']) / 4))
    contract_ocean_cycle_days = st.number_input(
        "合同海运周期（天）", min_value=0, value=default_ocean_cycle, step=1,
        help="用于剩余合同到美日、推荐下单量和最晚下单日；不改变上方分仓海运时效。"
    )
    contract_sales_cycle_days = st.number_input(
        "销售周期（天）", min_value=0, value=30, step=1,
        help="用于后续推荐下单量覆盖窗口，不加安全缓冲。"
    )

# ============================================================
# 数据上传与编辑
# ============================================================
st.header("📥 2. 上传/输入业务数据")


def generate_excel_template():
    template = {
        'SKU': ['SKU-A', 'SKU-A'],
        'SPU': ['SPU-A', 'SPU-A'],
        '店铺': ['Shop-A', 'Shop-B'], '组别': ['二部', '三部'], '运营': ['张三', '李四'],
        '本次总发货量': [1000, 1000],
        '理论_西%': [25, 25], '理论_东%': [25, 25], '理论_GA%': [25, 25], '理论_TX%': [25, 25], '理论_CG%': [0, 0],
        '美西_在仓': [0, 0], '美东_在仓': [0, 0], 'GA_在仓': [0, 0], 'TX_在仓': [0, 0], 'CG_在仓': [0, 0],
        '美西_多批次在途': [f'{(today + datetime.timedelta(days=8)).strftime("%Y-%m-%d")}:2000', ''],
        '美东_多批次在途': ['', f'{(today + datetime.timedelta(days=8)).strftime("%Y-%m-%d")}:4000'],
        'GA_多批次在途': ['', ''], 'TX_多批次在途': ['', ''], 'CG_多批次在途': ['', ''],
        'M1预测(当月)': [1000, 1000], 'M2预测(次月)': [1000, 1000], 'M3预测(第3月)': [1000, 1000],
        'M4预测(第4月)': [1000, 1000], 'M5预测(第5月)': [1000, 1000], 'M6预测(第6月)': [1000, 1000]
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
    if uploaded_file.name.endswith('.xlsx'):
        df_input = pd.read_excel(uploaded_file)
    else:
        df_input = pd.read_csv(uploaded_file)

    # 智能表头探测：处理第一行有空行/合并单元格的情况
    # 检测条件：超过一半的列名是 "Unnamed: N" 形式
    unnamed_count = sum(1 for c in df_input.columns if str(c).startswith('Unnamed:'))
    if unnamed_count > len(df_input.columns) / 2:
        # 找到第一个非全空的行作为真正的表头
        for header_row_idx in range(min(5, len(df_input))):
            row_vals = df_input.iloc[header_row_idx].astype(str).tolist()
            non_empty = sum(1 for v in row_vals if v not in ('nan', '', 'None'))
            if non_empty > len(df_input.columns) / 2:
                # 把这行作为表头
                df_input.columns = [str(v).strip() if str(v) not in ('nan', '', 'None') else f'col_{i}'
                                    for i, v in enumerate(df_input.iloc[header_row_idx])]
                # 跳过表头行及之前的空行
                df_input = df_input.iloc[header_row_idx + 1:].reset_index(drop=True)
                st.info(f"已自动探测表头位置（原文件第 {header_row_idx + 2} 行为表头）")
                break

    # 兼容旧版五个月模板：缺少的预测月份自动按 0 补齐。
    for col in FORECAST_COLS:
        if col not in df_input.columns:
            df_input[col] = 0.0

    # 校验：必需列是否齐全
    required_cols = ['SKU', '店铺', '组别', '运营', '本次总发货量']
    missing = [c for c in required_cols if c not in df_input.columns]
    if missing:
        st.error(f"上传的文件缺少必需列：{missing}")
        st.write("当前列名：", list(df_input.columns))
        st.stop()
else:
    # 默认示例：张三-李四经典互补场景
    df_input = pd.DataFrame({
        'SKU': ['SKU-A', 'SKU-A'],
        'SPU': ['SPU-A', 'SPU-A'],
        '店铺': ['Shop-A', 'Shop-B'], '组别': ['二部', '三部'], '运营': ['张三', '李四'],
        '本次总发货量': [1000, 1000],
        '理论_西%': [25, 25], '理论_东%': [25, 25], '理论_GA%': [25, 25], '理论_TX%': [25, 25], '理论_CG%': [0, 0],
        '美西_在仓': [0, 0], '美东_在仓': [0, 0], 'GA_在仓': [0, 0], 'TX_在仓': [0, 0], 'CG_在仓': [0, 0],
        '美西_多批次在途': [f'{(today + datetime.timedelta(days=8)).strftime("%Y-%m-%d")}:2000', ''],
        '美东_多批次在途': ['', f'{(today + datetime.timedelta(days=8)).strftime("%Y-%m-%d")}:4000'],
        'GA_多批次在途': ['', ''], 'TX_多批次在途': ['', ''], 'CG_多批次在途': ['', ''],
        'M1预测(当月)': [1000, 1000], 'M2预测(次月)': [1000, 1000], 'M3预测(第3月)': [1000, 1000],
        'M4预测(第4月)': [1000, 1000], 'M5预测(第5月)': [1000, 1000], 'M6预测(第6月)': [1000, 1000]
    })

# ============================================================
# V3.7a: 推荐发货量结果优先作为编辑器数据源
# 注意: recommended_df / rec_version / recommend_msgs 三个 key
#       不要加入 SESSION_KEYS, 否则点运算按钮会清掉推荐结果
# ============================================================
if st.session_state.get('recommended_df') is not None:
    df_input = st.session_state['recommended_df']

# 归零/冗余提示(精简为一行汇总, 明细看表格「备注」列)
rec_msgs = st.session_state.get('recommend_msgs')
if rec_msgs:
    parts = []
    if rec_msgs.get('small_count'):
        parts.append(f"{rec_msgs['small_count']} 行需求 1-9 pcs 已归零(如需发货请手动改为 ≥10)")
    if rec_msgs.get('redundant_count'):
        parts.append(f"{rec_msgs['redundant_count']} 行库存已冗余(推荐 0)")
    if rec_msgs.get('contract_short_count'):
        parts.append(f"{rec_msgs['contract_short_count']} 行合同数量不足")
    if rec_msgs.get('gap_count'):
        parts.append(f"{rec_msgs['gap_count']} 行存在库存空窗")
    if rec_msgs.get('missing_contract_count'):
        parts.append(f"{rec_msgs['missing_contract_count']} 行未匹配到合同")
    if rec_msgs.get('missing_lead_count'):
        parts.append(f"{rec_msgs['missing_lead_count']} 行缺少 SPU 交期")
    if parts:
        st.warning("⚠️ " + "; ".join(parts) + "。明细见表格「备注」列。")

edited_df = st.data_editor(df_input, num_rows="dynamic", use_container_width=True,
                           key=f"main_editor_v{st.session_state.get('rec_version', 0)}")

contract_file = st.file_uploader(
    "⬆️ 上传合同明细 + SPU交期 Excel（Sheet1 合同明细，Sheet2 交期表）",
    type=["xlsx"], key="contract_workbook"
)

# ============================================================
# V3.7b: 生成推荐备发货量按钮
# ============================================================
col_rec1, col_rec2 = st.columns([1, 3])
with col_rec1:
    btn_recommend = st.button(
        "📦 生成推荐备发货量", type="secondary",
        help="基于销售预测、在仓在途、本轮排期、合同明细和 SPU 交期，"
             "生成本轮推荐发货量、合同扣减、后续推荐下单量和最晚下单日。"
    )
with col_rec2:
    st.caption("生成后会回写「本次总发货量」为合同约束后的本轮实际发货量；可人工检查、修改后再点「开始逆向推演运算」。")

if btn_recommend:
    if d_diff_invalid:
        st.error("D差小于最短海运时效, 无法计算推荐备发货量!")
    elif contract_file is None:
        st.error("请先上传合同明细 + SPU交期 Excel。")
    else:
        contract_rows, lead_days_by_spu, load_errors = load_contract_workbook(contract_file)
        fatal_types = {
            '缺少合同文件', '合同文件读取失败', '合同文件解析失败',
            '缺少交期表', '合同明细缺少字段', '交期表缺少字段'
        }
        fatal_errors = [e for e in load_errors if e.get('异常类型') in fatal_types]
        if fatal_errors:
            st.error("合同文件格式校验失败，请先修正后再生成推荐备发货量。")
            st.dataframe(pd.DataFrame(fatal_errors), use_container_width=True)
            st.stop()

        rec_df = edited_df.copy()
        drop_cols = [c for c in (CONTRACT_DERIVED_COLS + ['备注']) if c in rec_df.columns]
        if drop_cols:
            rec_df = rec_df.drop(columns=drop_cols)

        # 数值规范化(与主运算同口径)
        rec_numeric_cols = (['本次总发货量']
                            + [ratio_col_name(r) for r in REGIONS]
                            + [f'{r}_在仓' for r in REGIONS]
                            + FORECAST_COLS)
        for col in rec_numeric_cols:
            if col in rec_df.columns:
                rec_df[col] = pd.to_numeric(rec_df[col], errors='coerce').fillna(0.0).astype(float)
        for col in ['SKU', '店铺', '组别', '运营']:
            if col in rec_df.columns:
                rec_df[col] = rec_df[col].fillna('-').astype(str)
        if 'SPU' in rec_df.columns:
            rec_df['SPU'] = rec_df['SPU'].fillna('').astype(str)

        # 校验理论占比和 = 100
        rec_errors = []
        for _, row in rec_df.iterrows():
            total_pct = sum([float(row[ratio_col_name(r)]) for r in REGIONS])
            if not (99.99 <= total_pct <= 100.01):
                rec_errors.append(f"【{row['SKU']}】理论占比和: {total_pct:.1f}%")
        if rec_errors:
            st.error("数据校验失败! 以下 SKU 的理论分区占比之和不等于 100%:")
            st.warning("\n".join(rec_errors))
        else:
            n_rows = len(rec_df)
            prog = st.progress(0, text=f"正在计算推荐备发货量... 0/{n_rows}")
            notes = []
            derived_values = {col: [] for col in CONTRACT_DERIVED_COLS}
            deduct_records = []
            plan_records = []
            exception_records = list(load_errors)
            small_count = 0
            redundant_count = 0
            contract_short_count = 0
            gap_count = 0
            missing_contract_count = 0
            missing_lead_count = 0
            for i, (idx, row) in enumerate(rec_df.iterrows()):
                q_final, q_raw, status = recommend_q_ship(
                    row.to_dict(), transit_times, earliest_etd, target_eta,
                    today, sales_cutoff, False
                )
                note = ''
                if status == 'small':
                    note = f"本轮发货需求{int(round(q_raw))}pcs已归零"
                    small_count += 1
                elif status == 'redundant':
                    note = "库存已冗余"
                    redundant_count += 1

                contract_result = compute_contract_recommendation(
                    row.to_dict(), contract_rows, lead_days_by_spu, q_final,
                    today, earliest_etd, target_eta,
                    contract_ocean_cycle_days, contract_sales_cycle_days
                )
                rec_df.at[idx, '本次总发货量'] = float(contract_result['本轮实际发货量'])

                derived_values['本轮理论发货量'].append(int(round(q_final)))
                for col in CONTRACT_DERIVED_COLS:
                    if col != '本轮理论发货量':
                        derived_values[col].append(contract_result[col])

                note = append_note(note, contract_result.get('备注', ''))
                notes.append(note)
                deduct_records.extend(contract_result.get('合同扣减明细', []))
                plan_records.extend(contract_result.get('剩余合同发货计划', []))
                exception_records.extend(contract_result.get('异常记录', []))

                if contract_result['本轮合同缺口'] > 0:
                    contract_short_count += 1
                if contract_result['备货状态'] == '存在库存空窗':
                    gap_count += 1
                if contract_result['备货状态'] == '缺少合同':
                    missing_contract_count += 1
                if contract_result['备货状态'] == '缺少交期':
                    missing_lead_count += 1

                prog.progress((i + 1) / n_rows,
                              text=f"正在计算推荐备发货量... {i + 1}/{n_rows}")
            prog.empty()

            # 派生列插入「本次总发货量」之后，保持后续主运算仍读取本次总发货量。
            insert_pos = rec_df.columns.get_loc('本次总发货量') + 1
            for col in CONTRACT_DERIVED_COLS:
                rec_df.insert(insert_pos, col, derived_values[col])
                insert_pos += 1
            rec_df.insert(insert_pos, '备注', notes)

            st.session_state['recommended_df'] = rec_df
            st.session_state['rec_version'] = st.session_state.get('rec_version', 0) + 1
            st.session_state['recommend_msgs'] = {
                'small_count': small_count,
                'redundant_count': redundant_count,
                'contract_short_count': contract_short_count,
                'gap_count': gap_count,
                'missing_contract_count': missing_contract_count,
                'missing_lead_count': missing_lead_count,
            }
            st.session_state['contract_stocking_detail'] = {
                'deduct_records': deduct_records,
                'plan_records': plan_records,
                'exception_records': exception_records,
            }
            st.rerun()

detail = st.session_state.get('contract_stocking_detail')
if detail:
    st.markdown("#### 📦 合同备发货测算明细")
    tab_deduct, tab_plan, tab_errors = st.tabs(["合同扣减明细", "剩余合同发货计划", "异常检查"])
    with tab_deduct:
        records = detail.get('deduct_records', [])
        if records:
            st.dataframe(pd.DataFrame(records), use_container_width=True)
        else:
            st.info("无本轮合同扣减记录。")
    with tab_plan:
        records = detail.get('plan_records', [])
        if records:
            st.dataframe(pd.DataFrame(records), use_container_width=True)
        else:
            st.info("无剩余合同发货计划。")
    with tab_errors:
        records = detail.get('exception_records', [])
        if records:
            st.dataframe(pd.DataFrame(records), use_container_width=True)
        else:
            st.success("未发现合同/交期异常。")


# ============================================================
# 计算主看板 + 沙盘所需的核心函数（基于 compute_row_metrics）
# ============================================================
def compute_main_board(df, transit_dict, earliest_etd, target_eta, today, sales_cutoff,
                       south_linkage=False):
    """输入：dataframe（含一行或多行 SKU 数据）
    输出：每行的主看板指标
    """
    results = []

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
            '美西发货': alloc['美西'],
            '美西最晚发货': fmt_date(deadlines['美西'], alloc['美西']),
            '美东发货': alloc['美东'],
            '美东最晚发货': fmt_date(deadlines['美东'], alloc['美东']),
            'GA发货': alloc['GA'],
            'GA最晚发货': fmt_date(deadlines['GA'], alloc['GA']),
            'TX发货': alloc['TX'],
            'TX最晚发货': fmt_date(deadlines['TX'], alloc['TX']),
            'CG发货': alloc['CG'],
            'CG最晚发货': fmt_date(deadlines['CG'], alloc['CG']),
            '期初分区占比': init_ratio_str,
            '最终全网占比估值': final_str,
            '最终全网到货日': m['real_final_arrival'].strftime('%Y-%m-%d'),
            '预估跨区订单数量': int(round(m['cz_before_cutoff'])),
            '预估全网耗尽日': m['oos_date'].strftime('%Y-%m-%d'),
            '建议减量至': suggest_qty_str,
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
        st.warning("汇总计算和调拨分析互斥（汇总后只剩一行，无法做行间调拨）")
with col_btn4:
    btn_run = st.button("🚀 开始逆向推演运算", type="primary")

# Session State 初始化
SESSION_KEYS = [
    'baseline_df',
    'stage12_df',
    'stage34_df',
    'alloc_result_s0',
    'alloc_result_s1',
    'alloc_result_s2',
    'current_stage',
    's12_records',
    's34_records',
    'independent_mode',  # 新增:标记是否走"仅运算分区调拨"独立路径
]

for k in SESSION_KEYS:
    if k not in st.session_state:
        st.session_state[k] = None

# ============================================================
# 执行运算
# ============================================================
if btn_run:
    if d_diff_invalid:
        st.error("D差小于最短海运时效，无法计算！")
    else:
        # 数据规范化
        working_df = edited_df.copy()

        numeric_cols = (['本次总发货量']
                        + [ratio_col_name(r) for r in REGIONS]
                        + [f'{r}_在仓' for r in REGIONS]
                        + FORECAST_COLS)
        for col in numeric_cols:
            if col in working_df.columns:
                working_df[col] = pd.to_numeric(working_df[col], errors='coerce').fillna(0.0).astype(float)
        for col in ['SKU', '店铺', '组别', '运营']:
            if col in working_df.columns:
                working_df[col] = working_df[col].fillna('-').astype(str)

        # V3.7a: 提取并剔除「备注」列(算法引擎不使用; 仅 S0 主看板回显)
        row_notes = None
        if '备注' in working_df.columns:
            row_notes = working_df['备注'].fillna('').astype(str).tolist()
            working_df = working_df.drop(columns=['备注'])

        # 同组别同SKU 汇总计算（启用调拨时不允许聚合）
        if agg_on and not transfer_on:
            try:
                working_df = aggregate_data(working_df)
                row_notes = None  # V3.7a: 聚合后行数变化, 备注失效
                # 聚合后再次保证数值列类型
                for col in numeric_cols:
                    if col in working_df.columns:
                        working_df[col] = pd.to_numeric(working_df[col], errors='coerce').fillna(0.0).astype(float)
                st.info(f"已按【SKU + 组别】汇总，共 {len(working_df)} 行")
            except Exception as e:
                st.error(f"汇总计算失败: {e}")
                st.stop()

        # 校验：理论占比和 = 100
        error_skus = []
        for _, row in working_df.iterrows():
            total_pct = sum([float(row[ratio_col_name(r)]) for r in REGIONS])
            if not (99.99 <= total_pct <= 100.01):
                error_skus.append(f"【{row['SKU']}】 理论占比和: {total_pct:.1f}%")
        if error_skus:
            st.error("数据校验失败！以下 SKU 的理论分区占比之和不等于 100%：")
            st.warning("\n".join(error_skus))
        else:
            # 清空旧状态
            for k in SESSION_KEYS:
                st.session_state[k] = None

            # 计算 S0：基线方案
            with st.spinner("正在计算 S0 基线方案..."):
                st.session_state['baseline_df'] = working_df.copy()
                board_s0 = compute_main_board(
                    working_df, transit_times, earliest_etd, target_eta,
                    today, sales_cutoff, south_linkage
                )
                # V3.7a: S0 主看板回显备注(放「运营」列后); S1/S2 刷新后自然消失
                if row_notes is not None and len(row_notes) == len(board_s0):
                    board_s0.insert(board_s0.columns.get_loc('运营') + 1, '备注', row_notes)
                st.session_state['alloc_result_s0'] = board_s0
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

            st.success("计算完成！")
            st.rerun()

# ============================================================
# 主看板状态显示
# ============================================================
if st.session_state['alloc_result_s0'] is not None:
    stage = st.session_state['current_stage']

    # 状态标签(独立模式和完整流程用不同文案)
    if st.session_state.get('independent_mode'):
        stage_label_map = {
            'S0': "📊 当前显示：**原始基线方案**",
            'S1': "📊 当前显示：**S0 原始基线**（独立模式,主看板未变化,分区调拨方案见下方）",
            'S2': "📊 当前显示：**S0 + 分区调拨方案**（独立模式,跳过了阶段1+2）"
        }
    else:
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
            if '预估跨区订单数量' in row.index and row['预估跨区订单数量'] > 0:
                styles[row.index.get_loc('预估跨区订单数量')] = (
                    'background-color: #fff3cd; color: #cc0000; font-weight: bold')
            if '_is_redundant' in row.index and row['_is_redundant']:
                styles[row.index.get_loc('预估全网耗尽日')] = (
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
        f"导出当前视图（{stage}）",
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
        st.info("无需减量：所有 SKU 整体不冗余，或冗余已被冗余调拨完全消化。")

    # 表2-A：冗余调拨记录
    st.markdown("#### 🔄 表2-A·冗余调拨指令（阶段1）")
    if s1_transfer:
        df_t1 = pd.DataFrame(s1_transfer)
        st.dataframe(df_t1, use_container_width=True)
    else:
        st.info("无冗余调拨发生（无 SKU 同时存在冗余方与缺货方）。")

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

    # 控制按钮（必须在 if s1_transfer or s2_reduce 外面,否则没救命方案时按钮消失）
    st.markdown("---")
    col_b1, col_b2 = st.columns([1, 1])
    with col_b1:
        if st.button("✅ 确认救命方案,主看板刷新并继续优化", type="primary"):
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
                st.session_state['independent_mode'] = False  # 完整流程
            st.rerun()
    with col_b2:
        if st.button("⚡ 仅运算分区调拨", type="secondary",
                     help="跳过救命方案,直接基于 S0 原始数据运算分区调拨"):
            with st.spinner("正在基于 S0 原始数据运算分区调拨..."):
                # 主看板复用 S0 数据(剔除备注: 进入后续运算后备注消失)
                _s1_board = st.session_state['alloc_result_s0'].copy()
                if '备注' in _s1_board.columns:
                    _s1_board = _s1_board.drop(columns=['备注'])
                st.session_state['alloc_result_s1'] = _s1_board

                # 直接对 baseline_df 跑阶段3+4
                s3_transfer, df_after_s3 = stage3_partition_transfer(
                    st.session_state['baseline_df'], transit_times,
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
                st.session_state['independent_mode'] = True  # 独立路径标记
            st.rerun()

# ============================================================
# 调拨板块第2部分（S1/S2 状态展示）
# ============================================================
if st.session_state['current_stage'] in ('S1', 'S2'):
    st.markdown("---")
    st.header("⚖️ 5. 阶段3+4:降本与死冗余报告")

    # 状态提示:区分独立路径和完整路径
    if st.session_state.get('independent_mode'):
        st.warning("🔵 当前为【仅运算分区调拨】独立模式:基于 S0 原始数据,跳过了阶段1+2 救命方案。")

    s34 = st.session_state.get('s34_records', {})
    s3_transfer = s34.get('s3_transfer', [])
    s4_dead = s34.get('s4_dead', [])

    # 表2-B：分区调拨指令
    st.markdown("#### 🔄 表2-B·分区调拨指令（阶段3）")
    if s3_transfer:
        df_t3 = pd.DataFrame(s3_transfer)
        st.dataframe(df_t3, use_container_width=True)
    else:
        st.info("无可行分区调拨：当前方案在跨区维度已接近最优。")

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
            '跨区单数改善': f"{cz_improve:+d}" if cz_improve != 0 else "0",
        })

    if compare_records:
        total_old = sum(r['调拨前跨区单数'] for r in compare_records)
        total_new = sum(r['调拨后跨区单数'] for r in compare_records)
        compare_records.append({
            'SKU': '全局汇总',
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
            if row['SKU'] == '全局汇总':
                styles = ['background-color: #e6f2ff; font-weight: bold'] * len(row)
            cz_str = str(row['跨区单数改善'])
            if cz_str.startswith('+'):
                # 正数 = 变好 → 绿色
                idx_loc = row.index.get_loc('跨区单数改善')
                styles[idx_loc] += '; color: #70AD47; font-weight: bold'
            elif cz_str.startswith('-') and cz_str != '0':
                # 负数 = 变坏 → 红色
                idx_loc = row.index.get_loc('跨区单数改善')
                styles[idx_loc] += '; color: #C00000; font-weight: bold'
            return styles


        st.dataframe(df_cmp.style.apply(color_cz, axis=1), use_container_width=True)

    # 表4:死冗余报告（必须在 if compare_records 外面,独立显示）
    if st.session_state.get('independent_mode'):
        st.markdown("#### 🟠 表4·剩余冗余报告(阶段4,独立模式)")
        st.info("⚠️ 当前为独立模式,未经阶段2 减量,以下「剩余冗余量」包含原始冗余,数据可能虚高,仅供参考。")
    else:
        st.markdown("#### 🔴 表4·死冗余预警(阶段4)")

    if s4_dead:
        df_dead = pd.DataFrame(s4_dead)
        st.dataframe(
            df_dead.style.apply(lambda r: ['background-color: #ffe6e6; color: #990000'] * len(r), axis=1),
            use_container_width=True
        )
        if not st.session_state.get('independent_mode'):
            st.warning("上述 SKU 在调拨完成后仍有无法消化的库存,请关注。")
    else:
        st.success("无死冗余:所有行都能在销售截止日前售罄。")

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
        if st.session_state.get('independent_mode'):
            # 独立模式:只有"全部撤销"按钮(因为没有救命方案可回)
            if st.button("⏪ 全部撤销,回到原始基线"):
                st.session_state['current_stage'] = 'S0'
                st.rerun()
        else:
            # 完整流程:两个撤销按钮都显示
            col_a1, col_a2 = st.columns([1, 1])
            with col_a1:
                if st.button("↩️ 撤销分区调拨,回到救命方案"):
                    st.session_state['current_stage'] = 'S1'
                    st.rerun()
            with col_a2:
                if st.button("全部撤销,回到原始基线"):
                    st.session_state['current_stage'] = 'S0'
                    st.rerun()

# ============================================================
# 时空沙盘
# ============================================================
st.markdown("---")
st.header("🕰️ 6. 时空沙盘：穿越任意日期推演")

col_d1, col_d2 = st.columns([1, 4])
with col_d1:
    target_query = st.date_input("📅 选择查询日期", value=target_eta, key='sandbox_date')
    sandbox_btn = st.button("🚀 穿越至该日推演", type="secondary")

with col_d2:
    if sandbox_btn:
        if st.session_state['alloc_result_s0'] is None:
            st.warning("请先点击【开始逆向推演运算】生成方案！")
        else:
            stage = st.session_state['current_stage']

            # 取当前 stage 对应的 working_df
            if stage == 'S0':
                working_df = st.session_state['baseline_df']
            elif stage == 'S1':
                # 独立模式下没经过阶段1+2,stage12_df 是 None,要用 baseline_df
                if st.session_state.get('independent_mode'):
                    working_df = st.session_state['baseline_df']
                else:
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
                    f'{target_query} 总库存': int(sb['total']),
                    '实际占比 (西:东:GA:TX:CG)': " : ".join([f"{pct[r]:.0f}%" for r in REGIONS]),
                    '累计跨区订单': int(round(sb['cz_to_end'])),
                    '美西结存': int(sim_stock['美西']),
                    '美东结存': int(sim_stock['美东']),
                    'GA结存': int(sim_stock['GA']),
                    'TX结存': int(sim_stock['TX']),
                    'CG结存': int(sim_stock['CG']),
                })

            if sandbox_results:
                st.success(f"已推演至 {target_query}（{stage} 数据）")
                st.dataframe(pd.DataFrame(sandbox_results), use_container_width=True)
                st.caption(f"自洽性提示：当查询日期 = 销售截止日 ({sales_cutoff}) 时，"
                           f"沙盘累计跨区应等于主看板预估跨区订单数。")
