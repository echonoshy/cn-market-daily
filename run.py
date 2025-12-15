import json
import os
from datetime import date

import requests


def _normalize_date(value: str | None) -> tuple[str, str]:
    """
    返回 (api_date, filename_date)

    - api_date: 供 akshare 使用的 YYYYMMDD
    - filename_date: 供落盘文件名使用的 YYYY-MM-DD
    """

    if value is None:
        filename_date = date.today().isoformat()
        return filename_date.replace("-", ""), filename_date

    value = value.strip()

    if "-" in value:
        parts = value.split("-")
        if len(parts) != 3:
            raise ValueError("date 必须是 YYYYMMDD 或 YYYY-MM-DD")
        y, m, d = parts
        if (
            len(y) != 4
            or len(m) != 2
            or len(d) != 2
            or not y.isdigit()
            or not m.isdigit()
            or not d.isdigit()
        ):
            raise ValueError("date 必须是 YYYYMMDD 或 YYYY-MM-DD")
        return f"{y}{m}{d}", f"{y}-{m}-{d}"

    if len(value) == 8 and value.isdigit():
        return value, f"{value[:4]}-{value[4:6]}-{value[6:8]}"

    raise ValueError("date 必须是 YYYYMMDD 或 YYYY-MM-DD")


_SSE_SEGMENT_NAME_BY_PRODUCT_CODE: dict[str, str] = {
    "17": "股票",
    "01": "主板A",
    "02": "主板B",
    "03": "科创板",
    "11": "股票回购",
}

_SSE_METRICS: list[tuple[str, str]] = [
    ("挂牌数", "LIST_NUM"),
    ("市价总值", "TOTAL_VALUE"),
    ("流通市值", "NEGO_VALUE"),
    ("成交金额", "TRADE_AMT"),
    ("成交量", "TRADE_VOL"),
    ("平均市盈率", "AVG_PE_RATE"),
    ("换手率", "TOTAL_TO_RATE"),
    ("流通换手率", "NEGO_TO_RATE"),
]


def _parse_number(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip()
        if s in {"", "-", "--", "null", "None"}:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def _fetch_sse_deal_daily_result(api_date: str) -> list[dict[str, object]]:
    """
    直接请求上交所 commonQuery 接口，返回 JSON 的 result 列表。

    说明：AkShare 在接口返回空数据时会在内部设置列名时报 Length mismatch；
    这里做结构校验，避免崩溃并给出明确错误信息。
    """

    url = "https://query.sse.com.cn/commonQuery.do"
    params = {
        "sqlId": "COMMON_SSE_SJ_GPSJ_CJGK_MRGK_C",
        "PRODUCT_CODE": "01,02,03,11,17",
        "type": "inParams",
        "SEARCH_DATE": "-".join([api_date[:4], api_date[4:6], api_date[6:]]),
    }
    headers = {
        "Referer": "https://www.sse.com.cn/market/stockdata/overview/day/",
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "X-Requested-With": "XMLHttpRequest",
    }

    r = requests.get(url, params=params, headers=headers, timeout=20)
    r.raise_for_status()
    data_json = r.json()
    result = data_json.get("result")
    if result is None:
        raise RuntimeError(f"SSE 接口未返回 result 字段: keys={list(data_json.keys())}")
    if not isinstance(result, list):
        raise RuntimeError(f"SSE 接口 result 类型异常: {type(result)!r}")
    if not result:
        return []
    if not all(isinstance(x, dict) for x in result):
        raise RuntimeError("SSE 接口 result 结构异常：元素不是 dict")
    return result


def _build_sse_deal_daily_records(
    result: list[dict[str, object]],
) -> list[dict[str, object]]:
    """
    构造与 AkShare 输出一致的 records:
    [{"单日情况": "...", "股票": ..., "主板A": ..., "主板B": ..., "科创板": ..., "股票回购": ...}, ...]
    """

    if not result:
        return []

    by_segment: dict[str, dict[str, object]] = {}
    for item in result:
        code = str(item.get("PRODUCT_CODE", "")).strip()
        segment_name = _SSE_SEGMENT_NAME_BY_PRODUCT_CODE.get(code)
        if segment_name is None:
            continue
        by_segment[segment_name] = item

    columns = ["股票", "主板A", "主板B", "科创板", "股票回购"]
    records: list[dict[str, object]] = []
    for metric_name, field in _SSE_METRICS:
        row: dict[str, object] = {"单日情况": metric_name}
        for col in columns:
            row[col] = _parse_number(by_segment.get(col, {}).get(field))
        records.append(row)

    return records


def _default_output_path(filename_date: str, base_dir: str = "market_daily") -> str:
    month_dir = filename_date[:7]  # YYYY-MM
    return os.path.join(base_dir, month_dir, f"{filename_date}.json")


def fetch_sse_deal_daily(date_str: str | None = None) -> list[dict[str, object]]:
    """获取上交所成交统计(每日)并返回 records 列表。"""

    api_date, _ = _normalize_date(date_str)
    result = _fetch_sse_deal_daily_result(api_date)
    return _build_sse_deal_daily_records(result)


def save_sse_deal_daily(
    date_str: str | None = None, output_path: str | None = None
) -> str:
    """
    获取每日数据并保存到 output_path，返回输出文件路径。
    """

    api_date, filename_date = _normalize_date(date_str)
    if output_path is None:
        output_path = _default_output_path(filename_date)

    result = _fetch_sse_deal_daily_result(api_date)
    obj = _build_sse_deal_daily_records(result)

    payload = {"date": filename_date, "data": obj}

    output_dir = os.path.dirname(output_path) or "."
    os.makedirs(output_dir, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)
        fp.write("\n")

    return output_path


if __name__ == "__main__":
    from datetime import datetime

    today = datetime.today().strftime("%Y-%m-%d")
    print(save_sse_deal_daily(date_str=today))
