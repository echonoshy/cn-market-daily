import json
import os
from datetime import date

import akshare as ak


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


def fetch_sse_deal_daily(date_str: str | None = None) -> list[dict[str, object]]:
    """获取上交所成交统计(每日)并返回 records 列表。"""

    api_date, _ = _normalize_date(date_str)
    df = ak.stock_sse_deal_daily(date=api_date)
    return json.loads(df.to_json(orient="records"))


def save_sse_deal_daily(
    date_str: str | None = None, output_path: str = "market_daily/latest.json"
) -> str:
    """
    获取每日数据并保存到 output_path，返回输出文件路径。

    为了“只保留一个文件”，会在 output_path 所在目录内删除除目标文件之外的其它 .json。
    """

    api_date, filename_date = _normalize_date(date_str)
    df = ak.stock_sse_deal_daily(date=api_date)
    obj = json.loads(df.to_json(orient="records"))

    payload = {"date": filename_date, "data": obj}

    output_dir = os.path.dirname(output_path) or "."
    target_name = os.path.basename(output_path)
    os.makedirs(output_dir, exist_ok=True)

    for name in os.listdir(output_dir):
        if name.endswith(".json") and name != target_name:
            os.remove(os.path.join(output_dir, name))

    with open(output_path, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)
        fp.write("\n")

    return output_path


if __name__ == "__main__":
    from datetime import datetime

    today = datetime.today().strftime("%Y-%m-%d")
    output_file = f"market_daily/{today}.json"
    print(save_sse_deal_daily(output_path=output_file))
