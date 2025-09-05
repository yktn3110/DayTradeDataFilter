from __future__ import annotations
import re
import math
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Tuple, Optional

import pandas as pd
import numpy as np

from pathlib import Path
import sys
import pandas as pd
import time


_JNUM_RE = re.compile(r"^\s*\d{3,}\s*$")  # 注文番号（先頭セルが数字）
_CODE_RE = re.compile(r"(?P<code>\d{4})")
_PRICE_RE = re.compile(r"(-{1,2}|—|―|–|—|－|ー|—|–|—|—|—|—|—)|([0-9,]+(?:\.\d+)?)")
_INT_RE = re.compile(r"([0-9,]+)")
_DATE_SLASH_RE = re.compile(r"(\d{4})/(\d{1,2})/(\d{1,2})")  # 2025/9/1
# 「9月1日」「9:01:15」みたいな表記
_JP_MD_RE = re.compile(r"(\d{1,2})\s*月\s*(\d{1,2})\s*日")
_TIME_RE = re.compile(r"(\d{1,2}):(\d{2})(?::(\d{2}))?")

# 全角・半角混在スペースの統一
def _norm(s: Any) -> str:
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return ""
    return str(s).replace("\u00A0", " ").replace("\u3000", " ").strip()

def _to_int(s: str) -> Optional[int]:
    m = _INT_RE.search(s)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except ValueError:
        return None

def _to_float(s: str) -> Optional[float]:
    m = _PRICE_RE.search(s)
    if not m:
        return None
    val = m.group(2)
    if not val:
        return None
    try:
        return float(val.replace(",", ""))
    except ValueError:
        return None

def _parse_brand_block(s: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    例: '東京電力ホールディングス   9501   東証' や '六甲バター   2266   東証' を
    (銘柄名, コード, 市場) に分解
    """
    txt = re.sub(r"\s+", " ", s).strip()
    code = None
    market = None
    m = _CODE_RE.search(txt)
    if m:
        code = m.group("code")
        left = txt[:m.start()].strip()
        right = txt[m.end():].strip()
        # 右側に市場っぽい語があれば取る（東証, SOR 等）
        if right:
            # 先頭トークンだけ見る
            market = right.split()[0]
        name = left if left else None
        return name, code, market
    return (txt if txt else None), None, None

def _parse_date_yyyy_mm_dd(s: str) -> Optional[pd.Timestamp]:
    m = _DATE_SLASH_RE.search(s)
    if not m:
        return None
    y, mo, d = map(int, m.groups())
    try:
        return pd.Timestamp(year=y, month=mo, day=d)
    except Exception:
        return None

def _combine_jp_date_time(order_date: Optional[pd.Timestamp],
                          jp_md: Optional[Tuple[int,int]],
                          time_tuple: Optional[Tuple[int,int,int]]) -> Optional[pd.Timestamp]:
    """
    約定「9月1日」＋次行の「9:01:15」を、年は注文日から補完して Timestamp に。
    """
    if not jp_md:
        return None
    month, day = jp_md
    year = order_date.year if isinstance(order_date, pd.Timestamp) else pd.Timestamp.today().year
    hh, mm, ss = (time_tuple or (0,0,0))
    try:
        return pd.Timestamp(year=year, month=month, day=day, hour=hh, minute=mm, second=ss)
    except Exception:
        return None

@dataclass
class OrderRecord:
    注文番号: Optional[int] = None
    注文状況: Optional[str] = None
    注文状況補足: Optional[str] = None  # 例：(全部約定)
    注文種別: Optional[str] = None       # 通常注文/逆指値注文 など
    銘柄名: Optional[str] = None
    銘柄コード: Optional[str] = None
    市場: Optional[str] = None           # 銘柄行の市場 or 約定市場
    取引: Optional[str] = None           # 現物買/現物売/信新買(日計り) など
    注文日: Optional[pd.Timestamp] = None
    注文株数: Optional[int] = None
    執行条件: Optional[str] = None       # 指値/成行/--
    注文単価: Optional[float] = None
    約定市場: Optional[str] = None
    約定日: Optional[pd.Timestamp] = None
    約定時刻: Optional[str] = None       # 原文保持（任意）
    約定日時: Optional[pd.Timestamp] = None
    約定株数: Optional[int] = None
    約定単価: Optional[float] = None
    逆指値条件: Optional[str] = None
    取消フラグ: Optional[bool] = None
    訂正フラグ: Optional[bool] = None
    利用ポイント: Optional[str] = None
    関連番号: Optional[str] = None
    備考: Optional[str] = None           # パースしきれない情報

def parse_daytrade_sheet(
    path: str,
    sheet_name: Optional[str] = None,
    header: Optional[int] = None,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    指定シートから日計り（デイトレ）取引の生データを読み込み、注文ごとの正規化表に整形。
    戻り値: (DataFrame, warnings)
    """
    # 文字列化して読み込む（型は後で整形）
    df_raw = pd.read_excel(path, sheet_name=sheet_name, header=header, dtype=str)
    df_raw = df_raw.fillna("")

    # すべてのセルを文字列に正規化し、列を固定幅にしておく
    try:
        df = df_raw.map(_norm)          # pandas 2.2+ 推奨API
    except AttributeError:
        df = df_raw.applymap(_norm)     # フォールバック

    # 新規注文の開始判定：先頭列が数字（注文番号）
    def is_new_order(row: pd.Series) -> bool:
        c0 = row.iloc[0]
        return bool(_JNUM_RE.match(c0))

    records: List[OrderRecord] = []
    warnings: List[str] = []

    cur: Optional[OrderRecord] = None

    # 継続ブロックで拾う補助バッファ
    pending_jp_md: Optional[Tuple[int,int]] = None
    pending_time: Optional[Tuple[int,int,int]] = None

    n_cols = df.shape[1]

    for ridx, row in df.iterrows():
        cells = [row.iloc[i] if i < n_cols else "" for i in range(n_cols)]
        line = " ".join([c for c in cells if c]).strip()

        if not line:
            continue

        if is_new_order(row):
            # レコードを確定・保存
            if cur is not None:
                # 可能なら約定日時を確定
                if cur.約定日時 is None and pending_jp_md:
                    cur.約定日時 = _combine_jp_date_time(cur.注文日, pending_jp_md, pending_time)
                records.append(cur)

            cur = OrderRecord()
            pending_jp_md = None
            pending_time = None

            # ヘッダ1行目の基本情報を抜く
            try:
                cur.注文番号 = _to_int(row.iloc[0])
            except Exception:
                pass

            # 2列目: 注文状況
            if n_cols >= 2 and row.iloc[1]:
                cur.注文状況 = row.iloc[1]

            # 3列目: 注文種別
            if n_cols >= 3 and row.iloc[2]:
                cur.注文種別 = row.iloc[2]

            # 4列目以降に「銘柄 名称  コード  市場」が載るケースに対応
            brand_blob = " ".join(cells[3:7]).strip()
            if brand_blob:
                name, code, market = _parse_brand_block(brand_blob)
                cur.銘柄名 = name or cur.銘柄名
                cur.銘柄コード = code or cur.銘柄コード
                cur.市場 = market or cur.市場

            # 利用ポイント / 取引(取消/訂正) / 関連番号 が右側に並ぶケース
            tail_blob = " ".join(cells[7:]).strip()
            if "ポイント" in tail_blob:
                # 雑に最初の「〇ポイント」を拾う
                m = re.search(r"([0-9]+ポイント)", tail_blob)
                if m:
                    cur.利用ポイント = m.group(1)
            if "取消" in tail_blob:
                cur.取消フラグ = True
            if "訂正" in tail_blob:
                cur.訂正フラグ = True
            # 関連番号
            mrel = re.search(r"関連番号\s*([^\s]+)", tail_blob)
            if mrel:
                cur.関連番号 = mrel.group(1)

            continue

        # ここからは継続行の処理（cur が存在する前提）
        if cur is None:
            # 形式外：スキップしつつ警告
            warnings.append(f"行{ridx+1}: 注文ヘッダ外の孤立行を検出: {line[:50]}...")
            continue

        # 注文状況補足（例: "(全部約定)" が先頭列に来る）
        if row.iloc[0].startswith("(") and ")" in row.iloc[0] and not cur.注文状況補足:
            cur.注文状況補足 = row.iloc[0]

        # 取引種別（現物買/現物売/信新買 など）
        if "現物買" in line or "現物売" in line or "信新買" in line or "信新売" in line:
            # より短いセルを優先
            for c in cells:
                if any(k in c for k in ["現物買","現物売","信新買","信新売"]):
                    cur.取引 = c
                    break

        # 注文日 / 注文株数 / 執行条件 / 注文単価
        if "注文日" in line or _DATE_SLASH_RE.search(line):
            dt = _parse_date_yyyy_mm_dd(line)
            if dt:
                cur.注文日 = dt
        if "注文株数" in line:
            val = _to_int(line)
            if val is not None:
                cur.注文株数 = val
        if "執行条件" in line:
            # 指値/成行/--
            if "指値" in line:
                cur.執行条件 = "指値"
            elif "成行" in line:
                cur.執行条件 = "成行"
            elif "--" in line or "—" in line:
                cur.執行条件 = "--"
        if "注文単価" in line:
            price = _to_float(line)
            if price is not None:
                cur.注文単価 = price

        # 条件（特定 / 一般 / S など）は備考に寄せる
        if "特定" in line or "一般" in line:
            cur.備考 = " / ".join(filter(None, [cur.備考, line]))

        # 約定 市場 / 約定日時 / 約定株数 / 約定単価
        if "約定" in line and ("東証" in line or "名証" in line or "福証" in line or "SOR" in line):
            # 「約定 東証」など
            for c in cells:
                if "東証" in c or "名証" in c or "福証" in c or "SOR" in c:
                    cur.約定市場 = c
                    # 市場が未設定なら埋める
                    if not cur.市場:
                        cur.市場 = c
                    break

        # 「9月1日」などの和暦月日
        mmd = _JP_MD_RE.search(line)
        if mmd:
            pending_jp_md = (int(mmd.group(1)), int(mmd.group(2)))
            # 単独で日付だけなら、時刻は次行で拾う可能性があるため保留
            cur.約定日 = None  # いったん未確定（約定日時へ集約）

        # 次行が時刻なら拾って合成
        mt = _TIME_RE.search(line)
        if mt:
            hh = int(mt.group(1)); mm = int(mt.group(2)); ss = int(mt.group(3) or 0)
            pending_time = (hh, mm, ss)
            cur.約定時刻 = f"{hh:02d}:{mm:02d}:{ss:02d}"

        if "約定株数" in line:
            val = _to_int(line)
            if val is not None:
                cur.約定株数 = val
        if "約定単価" in line:
            price = _to_float(line)
            if price is not None:
                cur.約定単価 = price

        # 逆指値条件（自由文）
        if "逆指値" in line:
            cur.逆指値条件 = line

        # 取消/訂正フラグ（継続行にも現れる場合）
        if "取消" in line and cur.取消フラグ is None:
            cur.取消フラグ = True
        if "訂正" in line and cur.訂正フラグ is None:
            cur.訂正フラグ = True

        # 「銘柄 ... コード 市場」が継続行側に来る場合にも対応
        if "銘柄" in line and (cur.銘柄コード is None or cur.銘柄名 is None):
            name, code, market = _parse_brand_block(line.replace("銘柄", ""))
            cur.銘柄名 = cur.銘柄名 or name
            cur.銘柄コード = cur.銘柄コード or code
            cur.市場 = cur.市場 or market

        # 約定日時の確定（行ごと）
        if pending_jp_md and pending_time:
            cur.約定日時 = _combine_jp_date_time(cur.注文日, pending_jp_md, pending_time)
            cur.約定日 = cur.約定日時.normalize()

    # 最終レコードの確定
    if cur is not None:
        if cur.約定日時 is None and pending_jp_md:
            cur.約定日時 = _combine_jp_date_time(cur.注文日, pending_jp_md, pending_time)
            if cur.約定日時 is not None:
                cur.約定日 = cur.約定日時.normalize()
        records.append(cur)

    # DataFrame へ
    out = pd.DataFrame([asdict(r) for r in records])

    # 型整形（可能な範囲で）
    if not out.empty:
        # 日付列
        for col in ["注文日", "約定日", "約定日時"]:
            if col in out.columns:
                out[col] = pd.to_datetime(out[col], errors="coerce")

        # 数値列
        for col in ["注文番号", "注文株数", "約定株数"]:
            if col in out.columns:
                out[col] = pd.to_numeric(out[col], errors="coerce", downcast="integer")
        for col in ["注文単価", "約定単価"]:
            if col in out.columns:
                out[col] = pd.to_numeric(out[col], errors="coerce")

        # 真偽
        for col in ["取消フラグ", "訂正フラグ"]:
            if col in out.columns:
                out[col] = out[col].astype("boolean")\

        # 列の推奨順
        preferred = [
            "注文番号","注文状況","注文状況補足","注文種別",
            "銘柄名","銘柄コード","市場",
            "取引","注文日","注文株数","執行条件","注文単価",
            "約定市場","約定日時","約定株数","約定単価",
            "逆指値条件",
            "取消フラグ","訂正フラグ","利用ポイント","関連番号","備考"
        ]
        cols = [c for c in preferred if c in out.columns] + [c for c in out.columns if c not in preferred]
        out = out[cols]

    return out, warnings

def load_and_parse(file_path: str | Path, sheet_name: str = "元データ") -> pd.DataFrame:
    """
    Excelを読み込み、正規化したDataFrameを返す（ここではpandasのみ使用）
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"ファイルが見つかりません: {file_path.resolve()}")

    # シート存在チェック
    with pd.ExcelFile(file_path) as xls:
        if sheet_name not in xls.sheet_names:
            raise ValueError(f"指定シートが見つかりません: {sheet_name} / 既存: {xls.sheet_names}")

    # 実パース
    df, warnings = parse_daytrade_sheet(str(file_path), sheet_name=sheet_name, header=None)

    print("=== 正規化結果: 先頭5行 ===")
    with pd.option_context("display.max_columns", 200, "display.width", 200):
        print(df.head(5))

    if warnings:
        print("\n=== パース警告（要チェック） ===")
        for w in warnings:
            print("- ", w)

    print(f"\n読み込み完了: {file_path.name} / シート: {sheet_name} / レコード数: {len(df)}")
    return df


def drop_unwanted(df: pd.DataFrame) -> pd.DataFrame:
    """
    常時除外ルール:
      - 注文状況 = 取消完了
      - 取引 = 現物買 / 現物売
    """
    if df.empty:
        print("注意: 正規化結果が空のため除外処理はスキップします。")
        return df

    def _norm(s: str) -> str:
        if s is None:
            return ""
        return str(s).replace("\u00A0", " ").replace("\u3000", " ").strip()

    # 注文状況
    mask_cancel = pd.Series(False, index=df.index)
    if "注文状況" in df.columns:
        mask_cancel = df["注文状況"].astype(str).map(_norm).eq("取消完了")

    # 取引
    mask_genbutsu = pd.Series(False, index=df.index)
    if "取引" in df.columns:
        trade = df["取引"].astype(str).map(_norm)
        mask_genbutsu = trade.isin(["現物買", "現物売"])

    mask_drop = mask_cancel | mask_genbutsu
    removed = mask_drop.sum()
    kept = len(df) - removed
    print(f"常時除外: {removed}件（取消完了 + 現物買/現物売）/ 残り: {kept}件")

    return df.loc[~mask_drop].reset_index(drop=True)

def prune_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    指定された不要列を最終出力から除外する。
    （内部のフィルタには使ってもOK。）
    """
    drop_cols = [
        "注文日", "約定市場", "注文状況", "市場",
        "取消フラグ", "訂正フラグ", "利用ポイント", "関連番号", "備考", "執行条件", 
    ]
    exist = [c for c in drop_cols if c in df.columns]
    if exist:
        df = df.drop(columns=exist)
    return df


def _clean_dataframe_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """
    Excel（COM）書き込み前に NaT/NaN を安全な文字列へ。
    """
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    return out.replace({pd.NaT: "", pd.NA: ""}).fillna("")


def write_to_same_book_win32com(
    file_path: str | Path,
    df: pd.DataFrame,
    out_sheet: str = "正規化",
    table_name: str = "正規化tbl",
) -> None:
    try:
        import pythoncom
        import win32com.client as win32
    except Exception as e:
        raise RuntimeError("pywin32(win32com) が見つかりません。`pip install pywin32` を実行してください。") from e

    # ★ 絶対パスに正規化（Excel は相対だと迷子になりがち）
    path_abs = Path(file_path).resolve(strict=True)
    path_str = str(path_abs)
    path_long = r"\\?\{}".format(path_abs)  # 長いパス/一部の日本語に強い

    headers = list(df.columns)
    values = df.values.tolist() if not df.empty else []
    n_rows = (len(values) + 1) if headers else 0
    n_cols = len(headers)

    pythoncom.CoInitialize()
    excel = None
    wb = None
    try:
        excel = win32.gencache.EnsureDispatch("Excel.Application")
        excel.DisplayAlerts = False
        # excel.Visible = True  # デバッグ時に

        # ★ Open の再試行（相対→絶対→long path の順ではなく、最初から絶対で開く）
        def _open_excel(p):
            return excel.Workbooks.Open(
                Filename=p,
                ReadOnly=False,
                UpdateLinks=0,   # リンク更新しない
                IgnoreReadOnlyRecommended=True,
            )

        try:
            wb = _open_excel(path_str)
        except Exception as e1:
            # OneDrive 同期等で一瞬遅れる場合があるのでワンテンポ待つ
            time.sleep(0.3)
            try:
                wb = _open_excel(path_long)
            except Exception as e2:
                raise RuntimeError(
                    f"Excel でファイルを開けませんでした。\n"
                    f"- 試行1: {path_str}\n"
                    f"- 試行2: {path_long}\n"
                    f"元エラー: {e2}"
                ) from e1

        # シート取得/作成
        ws = None
        for sh in wb.Worksheets:
            if sh.Name == out_sheet:
                ws = sh
                break
        if ws is None:
            ws = wb.Worksheets.Add(After=wb.Worksheets(wb.Worksheets.Count))
            ws.Name = out_sheet
        else:
            ws.Cells.ClearContents()  # 値のみクリア（書式/テーブルは残す）

        # 書き込み
        if n_cols > 0:
            ws.Range(ws.Cells(1, 1), ws.Cells(1, n_cols)).Value = [headers]
            if n_rows > 1:
                ws.Range(ws.Cells(2, 1), ws.Cells(n_rows, n_cols)).Value = values

        # テーブル維持/作成
        lo = None
        try:
            lo = ws.ListObjects(table_name)
        except Exception:
            lo = None

        last_row = max(1, n_rows)
        last_col = max(1, n_cols)
        data_range = ws.Range(ws.Cells(1, 1), ws.Cells(last_row, last_col))

        if lo is None:
            if n_cols > 0:
                lo = ws.ListObjects.Add(1, data_range, None, 1)  # xlSrcRange=1, xlYes=1
                lo.Name = table_name
        else:
            lo.Resize(data_range)

        try:
            ws.Columns.AutoFit()
        except Exception:
            pass

        wb.Save()
        print(f"書き出し完了（win32com）: {path_abs.name} / シート: {out_sheet} / テーブル: {table_name}")

    finally:
        try:
            if wb is not None:
                wb.Close(SaveChanges=False)
        finally:
            if excel is not None:
                excel.Quit()
            pythoncom.CoUninitialize()

if __name__ == "__main__":
    default_file = "デイトレ20250901.xlsx"
    default_sheet = "元データ"

    file_arg = sys.argv[1] if len(sys.argv) >= 2 else default_file
    sheet_arg = sys.argv[2] if len(sys.argv) >= 3 else default_sheet

    try:
        # 1) 読み込み＆正規化
        df = load_and_parse(file_arg, sheet_arg)

        # 2) 常時除外（取消完了 / 現物買 / 現物売）
        df = drop_unwanted(df)

        # 3) 出力列を削る（指定の不要データは出さない）
        df = prune_columns(df)

        # 4) NaT/NaN を安全化（COMの astimezone エラー防止）
        df = _clean_dataframe_for_excel(df)

        # 5) win32com で同名ブック「正規化」に書き出し
        write_to_same_book_win32com(file_arg, df, out_sheet="正規化", table_name="正規化tbl")

    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
