from __future__ import annotations
import sys
from pathlib import Path
import shutil
from datetime import datetime, time
import re
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
import re
import unicodedata  # 全角→半角の正規化

BACKUP_DEST_DIR = Path(r"C:\Users\yuki3\OneDrive\document\05_財務\投資\取引記録\デイトレ")

def default_daytrade_filename() -> str:
    """日本時間の今日の日付で 'デイトレYYYYMMDD.xlsx' を返す"""
    today_jst = datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%Y%m%d")
    return f"デイトレ{today_jst}.xlsx"

def ask_filename_with_default() -> Path:
    """
    コンソールでファイル名/日付を入力してもらう。
    - 空Enter → 既定の今日（JST）の日付ファイル
    - 8桁の数字（YYYYMMDD）→ 'デイトレYYYYMMDD.xlsx'
    - それ以外の文字列：
        - '.xlsx'で終わればそのまま
        - 終わらなければ拡張子を付ける
    """
    default_name = default_daytrade_filename()
    # ← メッセージに [既定値] を見える形で表示
    s = input(f"入力ファイル名を指定してください [{default_name}]: ").strip()

    if s == "":
        name = default_name
    elif re.fullmatch(r"\d{8}", s):  # 例: 20250916
        name = f"デイトレ{s}.xlsx"
    else:
        name = s if s.lower().endswith(".xlsx") else f"{s}.xlsx"

    p = Path(name).expanduser().resolve()
    if not p.exists():
        raise FileNotFoundError(f"指定ファイルが見つかりません: {p}")
    return p

# =========================
# 固定マッピング版パーサ（標準ケース）
#   - 注文番号 r,0 が数値の行をブロック先頭とみなす
#   - 行位置は 2823 ブロックを基準に固定
# =========================

_CODE_RE = re.compile(r"(?P<code>\d{3,4}[A-Z]?)")

def _split_brand(blob: str):
    """ 'サンリオ   8136   東証' 的な塊 -> (銘柄名, コード, 市場) """
    if not isinstance(blob, str):
        return None, None, None
    # 全角→半角正規化（NFKC）し、空白類を単一スペースへ
    norm = unicodedata.normalize("NFKC", blob).replace("\u00A0", " ").replace("\u3000", " ")
    txt = re.sub(r"\s+", " ", norm).strip()
    m = _CODE_RE.search(txt.upper())
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
            order_qty = df.iat[base+1, 6] if base+1 < n and pd.notna(df.iat[base+1, 6]) else None
            exec_qty  = int(order_qty) if order_qty is not None and not pd.isna(order_qty) else None            
            exec_cond = df.iat[base+1, 7] if base+1 < n and pd.notna(df.iat[base+1, 7]) else None
            exec_cond_str = str(exec_cond or "")
            order_price = df.iat[base+1, 8] if base+1 < n and pd.notna(df.iat[base+1, 8]) else None
            # （執行条件や注文日などは出力不要のため省略：内部利用したくなればここで拾える）
            # 数量は「注文株数」をそのまま約定株数として使う（優先）

            # 逆指値（「逆指」「逆指値」などを含む）なら約定/時刻の行を +1 ずらす
            order_type_str = str(order_type or "")
            offset = 1 if ("逆指" in order_type_str) else 0

            # 約定行（通常: base+3、逆指値: base+4）
            exec_row = base + 3 + offset
            # 時刻行（通常: base+4、逆指値: base+5）
            time_row = base + 4 + offset

            # 約定値/日付/時刻 取得（範囲外は None）
            exec_price   = df.iat[exec_row, 7] if exec_row < n and pd.notna(df.iat[exec_row, 7]) else None
            excel_serial = df.iat[exec_row, 5] if exec_row < n and pd.notna(df.iat[exec_row, 5]) else None
            tm           = df.iat[time_row, 5] if time_row < n and pd.notna(df.iat[time_row, 5]) else None
            
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
                "執行条件": exec_cond,
                "注文単価": order_price,
                "約定株数": exec_qty,
                "約定単価": exec_price,
                "約定日時": exec_dt,
                "_注文状況": status,     # フィルタ用（後で drop）
                "_base_row": base + 1,   # ★ Excelの1始まり行番号（エントリ行の先頭）
            })
            # 次の行へ（可変長ブロックや取消完了で短いケースでも安全）
            r = base + 1

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
#   ユーザ指定: 注文日/約定市場/注文状況/市場/取消フラグ/訂正フラグ/利用ポイント/関連番号/備考/
#   → 本パーサでは該当列を生成しないが、将来の拡張互換で drop も用意
# =========================

def prune_columns(df: pd.DataFrame) -> pd.DataFrame:
    drop_cols = [
        "注文状況補足", "注文日", "約定市場", "_注文状況", "市場",
        "取消フラグ", "訂正フラグ", "利用ポイント", "約定日時", "関連番号", "備考", "逆指値条件", "約定日"
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
        excel = win32.Dispatch("Excel.Application")
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

        # 既存テーブル名が異なる場合は単独テーブルを転用
        if lo is None and ws.ListObjects.Count == 1:
            lo = ws.ListObjects(1)
            try:
                lo.Name = table_name
            except Exception:
                pass

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

# ====== 2) ラウンドトリップ（往復）を生成 ======

def _side(entry_trade: str) -> str | None:
    """エントリー側の売買方向（ロング/ショート）を返す"""
    if not isinstance(entry_trade, str):
        return None
    t = entry_trade
    if "信新買" in t:
        return "LONG"
    if "信新売" in t:
        return "SHORT"
    return None

def _is_exit_for(side: str, trade: str) -> bool:
    """決済側が、指定sideの決済か判定"""
    if not isinstance(trade, str):
        return False
    if side == "LONG":
        return "信返売" in trade  # ロング決済
    if side == "SHORT":
        return "信返買" in trade  # ショート決済
    return False

def _fmt_hms(td_seconds: int) -> str:
    h = td_seconds // 3600
    m = (td_seconds % 3600) // 60
    s = td_seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def build_round_trips(df_norm: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """
    正規化DF→数量ベース集約の往復を生成（全銘柄横断で時間順採番）。
    戻り値: (統計用DataFrame, 出力列リスト)
    """
    cols_out = [
        "No", "エントリー時刻", "エグジット時刻", "時間",
        "銘柄", "買/売", "株数", "注文", "利確/損切",
        "損益（買）", "損益（売）", "損益", "累計損益",
    ]

    req = ["銘柄コード","銘柄名","取引","約定日時","約定単価","約定株数","_base_row","注文番号"]
    lacks = [c for c in req if c not in df_norm.columns]
    if lacks:
        raise ValueError(f"必要列が見つかりません: {lacks}")

    # 全銘柄横断で時間順（同秒は注文番号で安定化）
    df = df_norm.sort_values(["約定日時","注文番号"], na_position="last").reset_index(drop=True)

    from collections import defaultdict, deque

    def _side(t: str|None) -> str|None:
        t = "" if t is None else str(t)
        if "信新買" in t: return "LONG"
        if "信新売" in t: return "SHORT"
        return None

    def _is_exit_for(side: str, t: str|None) -> bool:
        t = "" if t is None else str(t)
        return (side=="LONG" and "信返売" in t) or (side=="SHORT" and "信返買" in t)

    def _fmt_hms(sec: int) -> str:
        h, m = divmod(sec, 3600)
        m, s = divmod(m, 60)
        return f"{h}:{m:02d}:{s:02d}"

    # キュー：銘柄×方向
    queues = defaultdict(lambda: {"LONG": deque(), "SHORT": deque()})
    trips: list[dict] = []

    for _, row in df.iterrows():
        code  = row["銘柄コード"]
        name  = row["銘柄名"]
        trade = row["取引"]
        ts    = row["約定日時"]

        # 元データから価格と数量を明確に分離
        exec_px = row.get("約定単価")       # 約定価格（数値）
        ord_px  = row.get("注文単価")       # 注文価格（成行だと空/記号あり得る）
        qty     = row.get("約定株数")       # 数量（正規化で注文株数→約定株数を採用しているはず）
        on      = row.get("注文番号")

        base_row = int(row["_base_row"]) if pd.notna(row.get("_base_row")) else None

        # --- 成行判定（ここで入れる） ---
        # ※ 前段の parse で「執行条件」を必ず保持しておくこと（pruneで削除しない）
        exec_cond = str(row.get("執行条件") or "").strip()

        # エントリー価格を決定：成行なら約定価格、その他は注文価格（なければ約定価格にフォールバック）
        def _num(x):
            try:
                return None if x is None or pd.isna(x) else float(x)
            except Exception:
                return None

        exec_px_num = _num(exec_px)
        ord_px_num  = _num(ord_px)

        entry_px_resolved = exec_px_num
        
        # ---------------------------------

        # 入力の最低限チェック（コード/数量/価格/時刻）
        if code is None or qty is None or pd.isna(qty) or entry_px_resolved is None or pd.isna(ts):
            continue

        qty = int(qty)  # 明示的に整数へ

        side = _side(trade)
        if side in ("LONG", "SHORT"):
            # 新規は貯める（価格: px / 残数量: qty_remaining）
            queues[code][side].append({
                "ts": ts,
                "px": float(entry_px_resolved),   # ← エントリー価格（成行なら約定価格）
                "qty_remaining": qty,             # ← 残数量
                "first_row": base_row,
                "entry_trade": trade,
            })
            continue

        # 返済：必要数量を先頭から吸い上げ、集約1行を作る
        for closing_side in ("LONG", "SHORT"):
            if not _is_exit_for(closing_side, trade):
                continue

            remain = qty
            if remain <= 0:
                break

            # 集約用バッファ
            agg_qty = 0
            agg_sum_px_qty = 0.0
            entry_time_first = None
            entry_trade_first = None
            entry_row_first = None

            while remain > 0 and queues[code][closing_side]:
                ent = queues[code][closing_side][0]
                use = min(remain, ent["qty_remaining"])
                remain -= use
                ent["qty_remaining"] -= use

                if entry_time_first is None:
                    entry_time_first  = ent["ts"]
                    entry_trade_first = ent["entry_trade"]
                    entry_row_first   = ent["first_row"]

                agg_qty += use
                # 価格×使用数量を逐次加算（常に正しい加重平均になる）
                agg_sum_px_qty += ent["px"] * use

                if ent["qty_remaining"] == 0:
                    queues[code][closing_side].popleft()

            # 充当できた分があれば1行を出力
            if agg_qty > 0:
                entry_avg_px = agg_sum_px_qty / agg_qty
                exit_px = _num(row.get("約定単価"))  # 返済側の価格は約定価格を使用（ここは必ずあるはず）
                entry_t = entry_time_first
                exit_t  = ts

                if closing_side == "LONG":
                    pnl = (exit_px - entry_avg_px) * agg_qty
                    pnl_buy, pnl_sell = pnl, ""   # 片側は空文字
                else:  # SHORT
                    pnl = (entry_avg_px - exit_px) * agg_qty
                    pnl_buy, pnl_sell = "", pnl

                trips.append({
                    "_entry_dt": entry_t,
                    "_exit_dt":  exit_t,
                    "_entry_row": entry_row_first,
                    "No": None,
                    "エントリー時刻": entry_t,
                    "エグジット時刻": exit_t,
                    "時間": _fmt_hms(int((exit_t - entry_t).total_seconds())) if pd.notna(entry_t) and pd.notna(exit_t) else "",
                    "銘柄": f"{name}  {code}",
                    "買/売": entry_trade_first,
                    "株数": agg_qty,
                    "注文": entry_avg_px,      # ← エントリー価格の加重平均（成行も正しく反映）
                    "利確/損切": exit_px,
                    "損益（買）": pnl_buy,
                    "損益（売）": pnl_sell,
                    "損益": pnl,
                })



    trips_df = pd.DataFrame(trips)

    if not trips_df.empty:
        # エントリー時刻で時間順に並べ替え → No採番
        trips_df = trips_df.sort_values(by=["_entry_dt", "銘柄", "注文"], kind="mergesort").reset_index(drop=True)
        trips_df["No"] = range(1, len(trips_df) + 1)

        # 累計損益（No順＝時間順での累積）
        # 損益は数値（float）想定だが念のため astype(float) で明示
        trips_df["累計損益"] = trips_df["損益"].astype(float).cumsum()

        # 表示用の時刻文字列化
        for c in ["エントリー時刻", "エグジット時刻"]:
            trips_df[c] = pd.to_datetime(trips_df[c], errors="coerce").dt.strftime("%H:%M:%S")

        # 損益（買/売）の“空欄”保証（COMでゴミ値化を防止）
        for c in ["損益（買）", "損益（売）"]:
            trips_df[c] = trips_df[c].apply(lambda v: "" if (v is None or (isinstance(v, float) and pd.isna(v))) else v)

    return trips_df, cols_out


# ====== 3) 統計シートへ書き出し（No列にハイパーリンク） ======
def write_statistics_win32com(
    file_path: str | Path,
    df_stats: pd.DataFrame,
    cols_out: list[str],
    out_sheet: str = "統計",
    link_sheet: str = "元データ",
    table_name: str = "統計tbl",
    anchor_cell: str = "A1",   # テーブルを置く起点。既存テーブルがあれば無視
) -> None:
    """
    - シート全体はクリアしない。
    - ListObject(table_name) だけを作成/リサイズして値を更新。
    - No列（テーブル1列目）の各セルに元データへのハイパーリンクを設定。
    """
    try:
        import pythoncom
        import win32com.client as win32
    except Exception as e:
        raise RuntimeError("pywin32(win32com) が見つかりません。`pip install pywin32` を実行してください。") from e

    path_abs = Path(file_path).resolve(strict=True)
    headers = cols_out[:]  # 出力ヘッダ
    values = df_stats[cols_out].values.tolist() if not df_stats.empty else []
    link_rows = df_stats["_entry_row"].tolist() if "_entry_row" in df_stats.columns else []

    pythoncom.CoInitialize()
    excel = None; wb = None
    try:
        excel = win32.Dispatch("Excel.Application")
        excel.DisplayAlerts = False
        wb = excel.Workbooks.Open(str(path_abs), ReadOnly=False, UpdateLinks=0, IgnoreReadOnlyRecommended=True)

        # 統計シート取得/作成（※シート全消去はしない）
        ws = None
        for sh in wb.Worksheets:
            if sh.Name == out_sheet:
                ws = sh; break
        if ws is None:
            ws = wb.Worksheets.Add(After=wb.Worksheets(wb.Worksheets.Count))
            ws.Name = out_sheet

        # 既存 ListObject を探す
        lo = None
        try:
            lo = ws.ListObjects(table_name)
        except Exception:
            lo = None

        if lo is None and ws.ListObjects.Count == 1:
            lo = ws.ListObjects(1)
            try:
                lo.Name = table_name
            except Exception:
                pass

        n_rows = len(values) + 1 if headers else 0
        n_cols = len(headers)

                # ★ 余分な旧データ行の削除（ヘッダは残す／列はいじらない）
        if lo is not None:
            # 目標データ行数（Excelのテーブルは最低1行必要なので max(1, ...) ）
            target_rows = max(len(values), 1)

            # 既存データ行は一旦すべて削除（テーブルは保持）
            _tot_prev2 = False
            try:
                _tot_prev2 = bool(lo.ShowTotals)
                if _tot_prev2:
                    lo.ShowTotals = False
            except Exception:
                pass
            try:
                cur_rows2 = lo.ListRows.Count
                for i in range(cur_rows2, 0, -1):
                    lo.ListRows(i).Delete()
                if lo.DataBodyRange is not None:
                    lo.DataBodyRange.ClearContents()
            except Exception:
                try:
                    if lo.DataBodyRange is not None:
                        lo.DataBodyRange.ClearContents()
                except Exception:
                    pass
            finally:
                try:
                    if _tot_prev2:
                        lo.ShowTotals = True
                except Exception:
                    pass

            # Totals 行があると削除が詰まることがあるので一時OFF
            totals_prev = False
            try:
                totals_prev = bool(lo.ShowTotals)
                if totals_prev:
                    lo.ShowTotals = False
            except Exception:
                pass

            try:
                cur_rows = lo.ListRows.Count
                if cur_rows > target_rows:
                    # 下から削除
                    for i in range(cur_rows, target_rows, -1):
                        lo.ListRows(i).Delete()
                elif cur_rows == 0:
                    # まれに0件扱い（テーブル壊れ気味）なら、DataBodyRangeクリアで保険
                    if lo.DataBodyRange is not None:
                        lo.DataBodyRange.ClearContents()
            except Exception:
                # 万一 ListRows が使えない場合の保険
                try:
                    if lo.DataBodyRange is not None:
                        # 先頭 target_rows 行を残して、以降を空に
                        if len(values) == 0:
                            lo.DataBodyRange.ClearContents()
                        else:
                            start_keep = lo.DataBodyRange.Cells(1, 1)
                            last_keep  = lo.DataBodyRange.Cells(target_rows, lo.Range.Columns.Count)
                            clear_from = lo.DataBodyRange.Cells(target_rows+1, 1)
                            clear_to   = lo.DataBodyRange.Cells(lo.DataBodyRange.Rows.Count, lo.Range.Columns.Count)
                            ws.Range(clear_from, clear_to).ClearContents()
                except Exception:
                    pass
            finally:
                try:
                    if totals_prev:
                        lo.ShowTotals = True
                except Exception:
                    pass


        # テーブルの作成 or リサイズ
        if lo is None:
            if n_cols == 0:
                # 何も書けないが、既存を壊さないため何もしない
                wb.Save(); return
            start_cell = ws.Range(anchor_cell)
            data_range = ws.Range(
                start_cell,
                ws.Cells(start_cell.Row + max(0, n_rows - 1), start_cell.Column + max(0, n_cols - 1))
            )
            lo = ws.ListObjects.Add(1, data_range, None, 1)  # xlSrcRange=1, xlYes=1
            lo.Name = table_name
        else:
            # 既存テーブルの左上（ヘッダ左上）を起点に、新しいサイズでリサイズ
            tl = lo.HeaderRowRange.Cells(1, 1)
            new_range = ws.Range(
                tl,
                ws.Cells(tl.Row + max(0, n_rows - 1), tl.Column + max(0, n_cols - 1))
            )
            lo.Resize(new_range)

        # ヘッダ行の更新
        if n_cols > 0:
            lo.HeaderRowRange.Value = [headers]

        # 既存データ本体を一旦空に（行数0に）するには、リサイズ済みなので DataBodyRange が None の可能性あり
        # 値の一括書き込み
        if len(values) > 0:
            # DataBodyRange はヘッダ直下の範囲
            body = lo.DataBodyRange
            ws.Range(body.Cells(1, 1), body.Cells(len(values), n_cols)).Value = values

        # No列（テーブル1列目）にハイパーリンク付与
        # 既存リンクはセルごとに上書き（重複防止で削除→追加）
        if len(values) > 0:
            body = lo.DataBodyRange
            first_col = body.Columns(1)
            for i, base_row in enumerate(link_rows, start=1):
                cell = first_col.Cells(i, 1)
                try:
                    # 既存リンク削除（あれば）
                    for hl in list(cell.Hyperlinks):
                        hl.Delete()
                except Exception:
                    pass
                if base_row and isinstance(base_row, int):
                    try:
                        ws.Hyperlinks.Add(
                            Anchor=cell,
                            Address="",
                            SubAddress=f"'{link_sheet}'!A{base_row}",
                            TextToDisplay=str(cell.Value)
                        )
                    except Exception:
                        pass

        try:
            lo.Range.Columns.AutoFit()
        except Exception:
            pass

        wb.Save()
        print(f"書き出し完了（統計・テーブルのみ更新）: {path_abs.name} / シート: {out_sheet} / 行数: {len(values)}")
    finally:
        if wb is not None:
            wb.Close(SaveChanges=False)
        if excel is not None:
            excel.Quit()
        pythoncom.CoUninitialize()


# ====== 4) スクリプト用シート（新シート） ======
def build_script_runs(df_stats: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """統計DFから「スクリプト用」シート向けのデータを生成。
    出力列: [No, エントリー時刻, エグジット時刻, 銘柄コード, 注文, 利確/損切, 損益]
    """
    cols_out = ["No", "エントリー時刻", "エグジット時刻", "銘柄コード", "注文", "利確/損切", "損益"]
    if df_stats is None or df_stats.empty:
        return pd.DataFrame(columns=cols_out), cols_out

    df = df_stats.copy()

    entry_dt = pd.to_datetime(df.get("_entry_dt"), errors="coerce")
    exit_dt = pd.to_datetime(df.get("_exit_dt"), errors="coerce")
    entry_str = entry_dt.dt.strftime("%H:%M:%S").where(entry_dt.notna(), "")
    exit_str = exit_dt.dt.strftime("%H:%M:%S").where(exit_dt.notna(), "")

    if "銘柄" in df.columns:
        code_src = df["銘柄"].astype(str)
    else:
        code_src = pd.Series([""] * len(df), index=df.index, dtype=object)
    code_extracted = code_src.str.extract(r"(\d{3,4}[A-Z]?)$", expand=False)
    code_numeric = pd.to_numeric(code_extracted, errors="coerce")
    code_final = code_numeric.where(code_numeric.notna(), code_extracted).fillna("")

    no_series = df["No"] if "No" in df.columns else pd.Series(range(1, len(df) + 1), index=df.index)

    out = pd.DataFrame({
        "No": no_series,
        "エントリー時刻": entry_str.fillna(""),
        "エグジット時刻": exit_str.fillna(""),
        "銘柄コード": code_final,
        "注文": pd.to_numeric(df.get("注文"), errors="coerce"),
        "利確/損切": pd.to_numeric(df.get("利確/損切"), errors="coerce"),
        "損益": pd.to_numeric(df.get("損益"), errors="coerce"),
    })

    out["銘柄コード"] = out["銘柄コード"].fillna("")

    return out[cols_out], cols_out


def write_script_sheet_win32com(
    file_path: str | Path,
    df_script: pd.DataFrame,
    cols_out: list[str],
    out_sheet: str = "スクリプト用",
    table_name: str = "スクリプト用tbl",
    anchor_cell: str = "A1",
) -> None:
    """
    「スクリプト用」シートにテーブルとして出力する。
    - 既存の ListObject を再利用しつつ ListRows を全削除してから貼り付け。
    - データ0件時もヘッダ+空行を維持。
    """
    try:
        import pythoncom
        import win32com.client as win32
    except Exception as e:
        raise RuntimeError("pywin32(win32com) が見つかりません。`pip install pywin32` を実行してください。") from e

    path_abs = Path(file_path).resolve(strict=True)
    headers = cols_out[:]
    values = df_script[cols_out].values.tolist() if not df_script.empty else []

    pythoncom.CoInitialize()
    excel = None
    wb = None
    try:
        excel = win32.Dispatch("Excel.Application")
        excel.DisplayAlerts = False
        wb = excel.Workbooks.Open(str(path_abs), ReadOnly=False, UpdateLinks=0, IgnoreReadOnlyRecommended=True)

        ws = None
        for sh in wb.Worksheets:
            if sh.Name == out_sheet:
                ws = sh
                break
        if ws is None:
            ws = wb.Worksheets.Add(After=wb.Worksheets(wb.Worksheets.Count))
            ws.Name = out_sheet

        lo = None
        try:
            lo = ws.ListObjects(table_name)
        except Exception:
            lo = None

        if lo is None and ws.ListObjects.Count == 1:
            lo = ws.ListObjects(1)
            try:
                lo.Name = table_name
            except Exception:
                pass

        n_rows = len(values) + 1 if headers else 0
        n_cols = len(headers)

        if lo is not None:
            totals_prev = False
            try:
                totals_prev = bool(lo.ShowTotals)
                if totals_prev:
                    lo.ShowTotals = False
            except Exception:
                pass
            try:
                cur = lo.ListRows.Count
                for i in range(cur, 0, -1):
                    lo.ListRows(i).Delete()
                if lo.DataBodyRange is not None:
                    lo.DataBodyRange.ClearContents()
            except Exception:
                try:
                    if lo.DataBodyRange is not None:
                        lo.DataBodyRange.ClearContents()
                except Exception:
                    pass
            finally:
                try:
                    if totals_prev:
                        lo.ShowTotals = True
                except Exception:
                    pass

        if lo is None:
            if n_cols == 0:
                wb.Save()
                return
            start_cell = ws.Range(anchor_cell)
            data_range = ws.Range(
                start_cell,
                ws.Cells(start_cell.Row + max(0, n_rows - 1), start_cell.Column + max(0, n_cols - 1))
            )
            lo = ws.ListObjects.Add(1, data_range, None, 1)
            lo.Name = table_name
        else:
            tl = lo.HeaderRowRange.Cells(1, 1)
            new_range = ws.Range(
                tl,
                ws.Cells(tl.Row + max(0, n_rows - 1), tl.Column + max(0, n_cols - 1))
            )
            lo.Resize(new_range)

        if n_cols > 0:
            lo.HeaderRowRange.Value = [headers]

        if len(values) > 0:
            body = lo.DataBodyRange
            ws.Range(body.Cells(1, 1), body.Cells(len(values), n_cols)).Value = values
        else:
            if lo.DataBodyRange is not None:
                lo.DataBodyRange.ClearContents()

        try:
            lo.Range.Columns.AutoFit()
        except Exception:
            pass

        wb.Save()
        print(f"書き出し完了（スクリプト用）: {path_abs.name} / シート: {out_sheet} / 行数: {len(values)}")
    finally:
        if wb is not None:
            wb.Close(SaveChanges=False)
        if excel is not None:
            excel.Quit()
        pythoncom.CoUninitialize()


# ====== 5) 銘柄ごとの損益集計（新シート） ======
def build_brand_pnl(df_stats: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """統計DFから銘柄ごとの損益集計を生成。
    出力列: [銘柄, 往復数, 総損益, 平均損益, 最大損益, 最小損益]
    """
    cols_out = ["銘柄", "往復数", "総損益", "平均損益", "最大損益", "最小損益"]
    if df_stats is None or df_stats.empty:
        return pd.DataFrame(columns=cols_out), cols_out

    need = ["銘柄", "損益"]
    lacks = [c for c in need if c not in df_stats.columns]
    if lacks:
        raise ValueError(f"銘柄損益の集計に必要な列が見つかりません: {lacks}")

    g = df_stats.groupby("銘柄", dropna=False)
    out = pd.DataFrame({
        "銘柄": g.size().index,
        "往復数": g.size().values,
        "総損益": g["損益"].sum().values,
        "平均損益": g["損益"].mean().values,
        "最大損益": g["損益"].max().values,
        "最小損益": g["損益"].min().values,
    })
    out = out.sort_values(by=["総損益", "銘柄"], ascending=[False, True]).reset_index(drop=True)
    return out, cols_out


def write_brand_pnl_win32com(
    file_path: str | Path,
    df_brand: pd.DataFrame,
    cols_out: list[str],
    out_sheet: str = "銘柄損益",
    table_name: str = "銘柄損益tbl",
    anchor_cell: str = "A1",
) -> None:
    """銘柄損益シートにテーブルで出力。
    - シート全体は消さず、ListObject のみ更新。
    - No列のハイパーリンクは不要。
    """
    try:
        import pythoncom
        import win32com.client as win32
    except Exception as e:
        raise RuntimeError("pywin32(win32com) が見つかりません。`pip install pywin32` を実行してください。") from e

    path_abs = Path(file_path).resolve(strict=True)
    headers = cols_out[:]
    values = df_brand[cols_out].values.tolist() if not df_brand.empty else []

    pythoncom.CoInitialize()
    excel = None
    wb = None
    try:
        excel = win32.Dispatch("Excel.Application")
        excel.DisplayAlerts = False
        wb = excel.Workbooks.Open(str(path_abs), ReadOnly=False, UpdateLinks=0, IgnoreReadOnlyRecommended=True)

        ws = None
        for sh in wb.Worksheets:
            if sh.Name == out_sheet:
                ws = sh
                break
        if ws is None:
            ws = wb.Worksheets.Add(After=wb.Worksheets(wb.Worksheets.Count))
            ws.Name = out_sheet

        lo = None
        try:
            lo = ws.ListObjects(table_name)
        except Exception:
            lo = None

        if lo is None and ws.ListObjects.Count == 1:
            lo = ws.ListObjects(1)
            try:
                lo.Name = table_name
            except Exception:
                pass

        n_rows = len(values) + 1 if headers else 0
        n_cols = len(headers)

        if lo is not None:
            totals_prev = False
            try:
                totals_prev = bool(lo.ShowTotals)
                if totals_prev:
                    lo.ShowTotals = False
            except Exception:
                pass
            try:
                cur = lo.ListRows.Count
                for i in range(cur, 0, -1):
                    lo.ListRows(i).Delete()
                if lo.DataBodyRange is not None:
                    lo.DataBodyRange.ClearContents()
            except Exception:
                try:
                    if lo.DataBodyRange is not None:
                        lo.DataBodyRange.ClearContents()
                except Exception:
                    pass
            finally:
                try:
                    if totals_prev:
                        lo.ShowTotals = True
                except Exception:
                    pass

        if lo is None:
            if n_cols == 0:
                wb.Save()
                return
            start_cell = ws.Range(anchor_cell)
            data_range = ws.Range(
                start_cell,
                ws.Cells(start_cell.Row + max(0, n_rows - 1), start_cell.Column + max(0, n_cols - 1))
            )
            lo = ws.ListObjects.Add(1, data_range, None, 1)
            lo.Name = table_name
        else:
            tl = lo.HeaderRowRange.Cells(1, 1)
            new_range = ws.Range(
                tl,
                ws.Cells(tl.Row + max(0, n_rows - 1), tl.Column + max(0, n_cols - 1))
            )
            lo.Resize(new_range)

        if n_cols > 0:
            lo.HeaderRowRange.Value = [headers]

        if len(values) > 0:
            body = lo.DataBodyRange
            ws.Range(body.Cells(1, 1), body.Cells(len(values), n_cols)).Value = values
        else:
            if lo.DataBodyRange is not None:
                lo.DataBodyRange.ClearContents()

        try:
            lo.Range.Columns.AutoFit()
        except Exception:
            pass

        wb.Save()
        print(f"書き出し完了（銘柄損益）: {path_abs.name} / シート: {out_sheet} / 行数: {len(values)}")
    finally:
        if wb is not None:
            wb.Close(SaveChanges=False)
        if excel is not None:
            excel.Quit()
        pythoncom.CoUninitialize()
# =========================
# メイン
# =========================

def main():
    # 既定シート名
    default_sheet = "元データ"

    # 1) ファイル決定ロジック
    if len(sys.argv) >= 2:
        # 既存挙動も維持: 引数1でフルパス/相対パス指定可、引数2でシート名
        file_arg = Path(sys.argv[1]).expanduser().resolve()
        if not file_arg.exists():
            raise FileNotFoundError(f"指定ファイルが見つかりません: {file_arg}")
        sheet_arg = sys.argv[2] if len(sys.argv) >= 3 else default_sheet
    else:
        # 追加: 対話で入力（既定は今日の 'デイトレYYYYMMDD.xlsx'）
        file_arg = ask_filename_with_default()
        sheet_arg = default_sheet

    # 2) 正規化（固定マッピング）
    df = parse_daytrade_sheet_fixed(str(file_arg), sheet_name=sheet_arg)

    print("=== 正規化結果: 先頭5行 ===")
    with pd.option_context("display.max_columns", 200, "display.width", 200):
        print(df.head(5))

    # 3) 常時除外（取消完了 / 現物買 / 現物売）
    df = drop_unwanted(df)

    # 4) 正規化シート出力
    df_norm_out = prune_columns(df)
    df_norm_out_clean = clean_for_excel(df_norm_out)
    write_to_same_book_win32com(
        file_path=str(file_arg),
        df=df_norm_out_clean,
        out_sheet="正規化",
        table_name="正規化tbl",
    )

    # 5) 統計（数量集約版）生成 & 出力（Noにハイパーリンク）
    df_stats, cols_out = build_round_trips(df)
    write_statistics_win32com(
        file_path=str(file_arg),
        df_stats=df_stats,
        cols_out=cols_out,
        out_sheet="統計",
        link_sheet=sheet_arg,
    )

    # 6) スクリプト用シート（スクリプト連携用テーブル）
    df_script, cols_script = build_script_runs(df_stats)
    write_script_sheet_win32com(
        file_path=str(file_arg),
        df_script=df_script,
        cols_out=cols_script,
        out_sheet="スクリプト用",
        table_name="スクリプト用tbl",
    )

    # 7) 銘柄ごとの損益集計を作成・出力
    df_brand, cols_brand = build_brand_pnl(df_stats)
    write_brand_pnl_win32com(
        file_path=str(file_arg),
        df_brand=df_brand,
        cols_out=cols_brand,
        out_sheet="銘柄損益",
        table_name="銘柄損益tbl",
    )

    # 8) 仕上げたファイルをバックアップ先へコピー保存
    try:
        BACKUP_DEST_DIR.mkdir(parents=True, exist_ok=True)
        dest_path = BACKUP_DEST_DIR / file_arg.name
        shutil.copy2(file_arg, dest_path)
        print(f"バックアップコピー完了: {dest_path}")
    except Exception as e:
        print(f"[WARN] バックアップコピーに失敗しました: {e}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
