from __future__ import annotations
import sys
from pathlib import Path
from datetime import datetime, time
import re
import pandas as pd

# =========================
# 固定マッピング版パーサ（標準ケース）
#   - 注文番号 r,0 が数値の行をブロック先頭とみなす
#   - 行位置は 2823 ブロックを基準に固定
# =========================

_CODE_RE = re.compile(r"(?P<code>\d{4})")

def _split_brand(blob: str):
    """ 'サンリオ   8136   東証' 的な塊 -> (銘柄名, コード, 市場) """
    if not isinstance(blob, str):
        return None, None, None
    txt = re.sub(r"\s+", " ", blob.replace("\u00A0", " ").replace("\u3000", " ")).strip()
    m = _CODE_RE.search(txt)
    if not m:
        return (txt or None), None, None
    code = m.group("code")
    name = txt[:m.start()].strip() or None
    right = txt[m.end():].strip()
    market = right.split()[0] if right else None
    return name, code, market

def parse_daytrade_sheet_fixed(path: str, sheet_name: str = "元データ") -> pd.DataFrame:
    """
    列位置固定の標準パターンを高速にパース。
    - 注文単価・約定単価・約定日時はセル位置で直接取得
    - 取消完了などのフィルタ用に「注文状況」も内部取得
    """
    df = pd.read_excel(path, sheet_name=sheet_name, header=None)
    n = df.shape[0]
    recs = []
    r = 0
    while r < n:
        v = df.iat[r, 0] if 0 <= r < n else None
        # ブロック開始: 注文番号が整数
        if pd.notna(v) and isinstance(v, (int, float)) and float(v).is_integer():
            base = r
            order_no = int(v)

            # ヘッダ行（r）
            status      = df.iat[base, 1] if pd.notna(df.iat[base, 1]) else None
            order_type  = df.iat[base, 2] if pd.notna(df.iat[base, 2]) else None
            brand_blob  = df.iat[base, 3] if pd.notna(df.iat[base, 3]) else None
            brand, code, _market_from_brand = _split_brand(brand_blob if isinstance(brand_blob, str) else "")

            # 詳細1行目（r+1）
            trade       = df.iat[base+1, 3] if base+1 < n and pd.notna(df.iat[base+1, 3]) else None
            order_qty   = df.iat[base+1, 6] if base+1 < n and pd.notna(df.iat[base+1, 6]) else None
            order_price = df.iat[base+1, 8] if base+1 < n and pd.notna(df.iat[base+1, 8]) else None
            # （執行条件や注文日などは出力不要のため省略：内部利用したくなればここで拾える）

            # 約定行（r+3）
            exec_qty    = df.iat[base+3, 6] if base+3 < n and pd.notna(df.iat[base+3, 6]) else None
            exec_price  = df.iat[base+3, 7] if base+3 < n and pd.notna(df.iat[base+3, 7]) else None
            excel_serial= df.iat[base+3, 5] if base+3 < n and pd.notna(df.iat[base+3, 5]) else None

            # 時刻行（r+4）
            tm          = df.iat[base+4, 5] if base+4 < n and pd.notna(df.iat[base+4, 5]) else None

            # 約定日時の合成（Excelシリアル日付 + time）
            exec_dt = None
            try:
                if isinstance(excel_serial, (int, float)):
                    exec_date = pd.Timestamp('1899-12-30') + pd.Timedelta(days=float(excel_serial))
                else:
                    exec_date = pd.to_datetime(excel_serial) if excel_serial is not None else None

                if isinstance(tm, time):
                    t = tm
                elif isinstance(tm, str):
                    t = pd.to_datetime(tm).time()
                elif isinstance(tm, (datetime, pd.Timestamp)):
                    t = (tm.time() if isinstance(tm, datetime) else tm.to_pydatetime().time())
                else:
                    t = None

                if exec_date is not None and t is not None:
                    exec_dt = pd.Timestamp.combine(exec_date.date(), t)
            except Exception:
                exec_dt = None

            recs.append({
                "注文番号": order_no,
                "注文種別": order_type,
                "銘柄名": brand,
                "銘柄コード": code,
                "取引": trade,
                "注文株数": order_qty,
                "注文単価": order_price,
                "約定株数": exec_qty,
                "約定単価": exec_price,
                "約定日時": exec_dt,
                # ↓ フィルタ用（出力しない）
                "_注文状況": status,
            })
            # 次ブロックへ（標準5行構成）
            r = base + 5
            continue
        r += 1

    out = pd.DataFrame(recs)

    # 型整形
    if not out.empty:
        for col in ["注文株数", "注文単価", "約定株数", "約定単価"]:
            if col in out.columns:
                out[col] = pd.to_numeric(out[col], errors="coerce")
        if "約定日時" in out.columns:
            out["約定日時"] = pd.to_datetime(out["約定日時"], errors="coerce")

    return out


# =========================
# 常時除外（取消完了 / 現物買 / 現物売）
# =========================

def _norm(s: str) -> str:
    if s is None:
        return ""
    return str(s).replace("\u00A0", " ").replace("\u3000", " ").strip()

def drop_unwanted(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        print("注意: 正規化結果が空のため除外処理はスキップします。")
        return df

    # 注文状況=取消完了（内部列 _注文状況）
    mask_cancel = pd.Series(False, index=df.index)
    if "_注文状況" in df.columns:
        mask_cancel = df["_注文状況"].astype(str).map(_norm).eq("取消完了")

    # 取引=現物買/現物売
    mask_genbutsu = pd.Series(False, index=df.index)
    if "取引" in df.columns:
        trade = df["取引"].astype(str).map(_norm)
        mask_genbutsu = trade.isin(["現物買", "現物売"])

    mask_drop = mask_cancel | mask_genbutsu
    removed = int(mask_drop.sum())
    kept = int(len(df) - removed)
    print(f"常時除外: {removed}件（取消完了 + 現物買/現物売）/ 残り: {kept}件")

    return df.loc[~mask_drop].reset_index(drop=True)


# =========================
# 出力列の削除（不要列）
#   ユーザ指定: 注文日/約定市場/注文状況/市場/取消フラグ/訂正フラグ/利用ポイント/関連番号/備考/執行条件
#   → 本パーサでは該当列を生成しないが、将来の拡張互換で drop も用意
# =========================

def prune_columns(df: pd.DataFrame) -> pd.DataFrame:
    drop_cols = [
        "注文状況補足", "注文日", "約定市場", "注文状況", "市場",
        "取消フラグ", "訂正フラグ", "利用ポイント", "約定日時", "関連番号", "備考", "執行条件", "逆指値条件", "約定日"
    ]
    exist = [c for c in drop_cols if c in df.columns]
    if exist:
        df = df.drop(columns=exist)
    return df


# =========================
# NaT/NaN の Excel 安全化（astimezone エラー対策）
# =========================

def clean_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    return out.replace({pd.NaT: "", pd.NA: ""}).fillna("")


# =========================
# win32com 書き出し（絶対パス/long path 対応、テーブル維持/作成）
# =========================

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

    path_abs = Path(file_path).resolve(strict=True)
    path_str = str(path_abs)
    path_long = r"\\?\{}".format(path_abs)

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

        def _open_excel(p):
            return excel.Workbooks.Open(
                Filename=p,
                ReadOnly=False,
                UpdateLinks=0,
                IgnoreReadOnlyRecommended=True,
            )

        try:
            wb = _open_excel(path_str)
        except Exception:
            wb = _open_excel(path_long)

        # シート取得 or 作成
        ws = None
        for sh in wb.Worksheets:
            if sh.Name == out_sheet:
                ws = sh
                break
        if ws is None:
            ws = wb.Worksheets.Add(After=wb.Worksheets(wb.Worksheets.Count))
            ws.Name = out_sheet
        else:
            ws.Cells.ClearContents()  # 値のみクリア（テーブル/書式は維持）

        # 値の一括書き込み
        if n_cols > 0:
            ws.Range(ws.Cells(1, 1), ws.Cells(1, n_cols)).Value = [headers]
            if n_rows > 1:
                ws.Range(ws.Cells(2, 1), ws.Cells(n_rows, n_cols)).Value = values

        # テーブル作成/リサイズ
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


# =========================
# メイン
# =========================

def main():
    default_file = "デイトレ20250901.xlsx"
    default_sheet = "元データ"

    file_arg = sys.argv[1] if len(sys.argv) >= 2 else default_file
    sheet_arg = sys.argv[2] if len(sys.argv) >= 3 else default_sheet

    # 1) 読み込み & 固定マッピングで正規化
    df = parse_daytrade_sheet_fixed(file_arg, sheet_name=sheet_arg)

    print("=== 正規化結果: 先頭5行 ===")
    with pd.option_context("display.max_columns", 200, "display.width", 200):
        print(df.head(5))

    # 2) 常時除外（取消完了 / 現物買 / 現物売）
    df = drop_unwanted(df)

    # 3) 出力列の削除（指定の不要列 + 内部列）
    df = prune_columns(df)

    # 4) NaT/NaN を Excel 向けに安全化
    df_out = clean_for_excel(df)

    # 5) win32com で同名ブックの「正規化」に書き込み
    write_to_same_book_win32com(file_arg, df_out, out_sheet="正規化", table_name="正規化tbl")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
