#!/usr/bin/env python3
"""
NISC ISMS-P 심사일지 엑셀 → JSON 변환 (retrieve 최적화 버전)

템플릿: ((주)마이리얼트립) ISMS-P 심사일지(오광수).xlsx 형식
- 기본적으로 시트 '심사일지' 사용
- 일부 파일은 '예비결함' 시트로 결함사항이 분리될 수 있어 해당 시트도 지원

개선점
- retrieval_rows로 검색용 텍스트 정규화 (sections 필드는 출력하지 않음)
- 인터뷰 섹션은 interview row로 정규화
- 인증기준별 결함사항은 criteria row로 정규화
- 빈 criteria row(결함여부=0만 있고 나머지 비어있는 행)는 기본적으로 retrieval 제외
- 결함 합계는 일반 criteria row가 아니라 summary row로 분리
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import pandas as pd

DEFAULT_INPUT_PATH = "excel_test_file/template/NISC/NISC01.심사일지/((주)마이리얼트립) ISMS-P 심사일지(오광수).xlsx"
DEFAULT_OUTPUT_DIR = "excel_test_file/(step2)result_normalized_v2/NISC/NISC01.심사일지"



SHEET_NAME = "심사일지"
SHEET_NAME_DEFECT = "예비결함"
SECTION_TITLE_INTERVIEW = "서비스 및 인증기준별 심사원 확인사항"
SECTION_TITLE_CRITERIA = "인증기준별 결함사항"
SECTION_TITLE_DEFECT_SUMMARY = "결함 합계"


def _cell_str(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    return str(val).strip()


def _clean_text(text: str) -> str:
    text = _cell_str(text)
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _join_nonempty(parts: list[str], sep: str = " | ") -> str:
    return sep.join([p for p in parts if _cell_str(p)])


def read_excel(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_excel(path, sheet_name=SHEET_NAME, header=None)


def read_excel_optional_sheet(path: str | Path, sheet_name: str) -> pd.DataFrame | None:
    """
    지정한 시트가 존재하면 DataFrame을 반환하고, 없으면 None을 반환.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)

    xls = pd.ExcelFile(path)
    if sheet_name not in xls.sheet_names:
        return None
    return pd.read_excel(path, sheet_name=sheet_name, header=None)


def parse_metadata(df: pd.DataFrame) -> dict:
    meta = {
        "document_title": "",
        "심사원명": "",
        "인터뷰": [],
    }

    for i in range(0, min(6, len(df))):
        for j in range(df.shape[1]):
            v = df.iloc[i, j]
            if isinstance(v, str) and "서비스 및 인증기준별 심사원 확인사항" in v:
                meta["document_title"] = v.strip()

    # 심사원명: 위치가 고정되지 않을 수 있어 전체에서 스캔
    for i in range(min(30, len(df))):
        for j in range(df.shape[1]):
            v = _cell_str(df.iloc[i, j])
            if not v or "심사원명" not in v:
                continue
            inline = v.replace("심사원명", "", 1).strip().lstrip(":").strip()
            if inline:
                meta["심사원명"] = inline
                break
            # 오른쪽 셀 우선
            for jj in range(j + 1, df.shape[1]):
                cand = _cell_str(df.iloc[i, jj])
                if cand and "심사원명" not in cand:
                    meta["심사원명"] = cand
                    break
            if meta["심사원명"]:
                break
        if meta["심사원명"]:
            break

    # 인터뷰 블록: anchor 기반 파싱
    meta["인터뷰"] = parse_interview_sections(df)

    return meta


_INTERVIEW_BLOCK_ANCHOR = "구분"
_INTERVIEW_ROW_LABELS = ("인터뷰 부서 및 대상", "인터뷰 내용", "확인문서 또는 시스템")


def _label_matches(value: object, target: str) -> bool:
    return bool(_normalize_label(_cell_str(value))) and _normalize_label(
        target
    ) in _normalize_label(_cell_str(value))


def _row_has_any_label(df: pd.DataFrame, row_idx: int, labels: tuple[str, ...]) -> bool:
    for j in range(df.shape[1]):
        cell = df.iloc[row_idx, j]
        if any(_label_matches(cell, label) for label in labels):
            return True
    return False


def _row_looks_like_criteria_header(df: pd.DataFrame, row_idx: int) -> bool:
    values = [
        _normalize_label(_cell_str(df.iloc[row_idx, j])) for j in range(df.shape[1])
    ]
    return ("분야" in values) and ("항목" in values)


def _locate_interview_blocks(
    df: pd.DataFrame,
) -> list[tuple[int, int, dict[str, list[int]], dict[str, int], list[tuple[int, str]]]]:
    """
    앵커:
    - '구분'이 있는 행을 찾고
    - 아래쪽에서 '인터뷰 부서 및 대상/인터뷰 내용/확인문서 또는 시스템' 라벨 행을 찾고
    - '구분' 오른쪽부터 서비스명 컬럼들을 수집한다.
    """
    nrows, ncols = df.shape
    layouts: list[
        tuple[int, int, dict[str, list[int]], dict[str, int], list[tuple[int, str]]]
    ] = []

    for i in range(nrows):
        for j in range(ncols):
            if not _label_matches(df.iloc[i, j], _INTERVIEW_BLOCK_ANCHOR):
                continue

            # 라벨은 반복될 수 있음(특히 '인터뷰 내용')
            label_rows: dict[str, list[int]] = {}
            label_cols: dict[str, int] = {}
            for ii in range(i + 1, min(i + 15, nrows)):
                for jj in range(ncols):
                    cell_value = df.iloc[ii, jj]
                    for label in _INTERVIEW_ROW_LABELS:
                        if _label_matches(cell_value, label):
                            if label == "인터뷰 내용":
                                label_rows.setdefault(label, []).append(ii)
                                label_cols.setdefault(label, jj)
                            else:
                                if label not in label_rows:
                                    label_rows[label] = [ii]
                                    label_cols[label] = jj

            if not all(label in label_rows for label in _INTERVIEW_ROW_LABELS):
                continue

            search_start_col = max(j, max(label_cols.values())) + 1
            service_columns: list[tuple[int, str]] = []
            for jj in range(search_start_col, ncols):
                service_name = _clean_text(df.iloc[i, jj])
                if not service_name:
                    continue
                if any(
                    _label_matches(service_name, label)
                    for label in (_INTERVIEW_BLOCK_ANCHOR, *_INTERVIEW_ROW_LABELS)
                ):
                    continue
                service_columns.append((jj, service_name))

            if not service_columns:
                continue

            layouts.append((i, j, label_rows, label_cols, service_columns))

    return layouts


def parse_interview_sections(df: pd.DataFrame) -> list[dict]:
    layouts = _locate_interview_blocks(df)
    if not layouts:
        return []

    interviews: list[dict] = []

    def _collect_multiline(
        field_label: str, start_row: int, label_col: int, col_idx: int
    ) -> str:
        parts: list[str] = []
        r = start_row
        # 현재 셀 포함
        first = _clean_text(df.iloc[r, col_idx])
        if first:
            parts.append(first)

        # 아래로 이어지는 행들을 수집 (라벨 컬럼이 비어있고, 같은 컬럼에 값이 계속 나오는 형태)
        for r in range(start_row + 1, min(start_row + 80, df.shape[0])):
            if _row_looks_like_criteria_header(df, r):
                break
            if _row_has_any_label(
                df, r, (_INTERVIEW_BLOCK_ANCHOR, *_INTERVIEW_ROW_LABELS)
            ):
                break

            label_cell = (
                _clean_text(df.iloc[r, label_col]) if label_col < df.shape[1] else ""
            )
            if label_cell:
                break

            v = _clean_text(df.iloc[r, col_idx])
            if not v:
                # 연속 공백 2회면 종료(너무 길게 끌고 가지 않기 위함)
                # 단, 다른 서비스 컬럼에는 값이 있을 수 있으므로 여기서는 1회 공백만 허용
                continue
            parts.append(v)

        # 같은 내용이 셀 단위로 중복되는 경우를 줄이기 위해 join 후 정리
        return _clean_text("\n".join(parts))

    # 여러 인터뷰 블록이 있을 수 있어 모두 수집 후 service_name 기준으로 중복 제거
    seen = set()
    for block_row, block_col, label_to_rows, label_to_col, service_columns in layouts:
        for col_idx, service_name in service_columns:
            service_name_clean = _clean_text(service_name)

            dept_row = label_to_rows["인터뷰 부서 및 대상"][0]
            dept_col = label_to_col["인터뷰 부서 및 대상"]
            content_rows = label_to_rows["인터뷰 내용"]
            content_col = label_to_col["인터뷰 내용"]
            system_row = label_to_rows["확인문서 또는 시스템"][0]
            system_col = label_to_col["확인문서 또는 시스템"]

            dept = _collect_multiline(
                "인터뷰 부서 및 대상", dept_row, dept_col, col_idx
            )
            # '인터뷰 내용'은 라벨 행 자체가 여러 번 반복될 수 있어 모두 합친다.
            contents = []
            for r in content_rows:
                piece = _collect_multiline("인터뷰 내용", r, content_col, col_idx)
                if piece:
                    contents.append(piece)
            content = _clean_text("\n".join(contents))
            system = _collect_multiline(
                "확인문서 또는 시스템", system_row, system_col, col_idx
            )

            if not any([service_name_clean, dept, content, system]):
                continue

            key = (service_name_clean, dept, content, system)
            if key in seen:
                continue
            seen.add(key)

            interviews.append(
                {
                    "col_index": int(col_idx) + 1,  # 엑셀 표시 기준(1-based)
                    "서비스명": service_name_clean,
                    "인터뷰_부서_및_대상": dept,
                    "인터뷰_내용": content,
                    "확인문서_또는_시스템": system,
                }
            )

    return interviews


def _find_rightmost_flag_col(
    header_cells: list[str], col_item_name: int | None, ncols: int
) -> int | None:
    """헤더에 '결함여부'가 보이는 가장 오른쪽 열 (항목명 오른쪽만)."""
    if col_item_name is None:
        return None
    target = _normalize_label("결함여부")
    best: int | None = None
    for j in range(col_item_name + 1, min(ncols, len(header_cells))):
        lab = _normalize_label(header_cells[j])
        if not lab:
            continue
        if lab == target or target in lab:
            best = j
    return best


def _defect_column_range(
    col_item_name: int | None,
    col_flag: int | None,
    ncols: int,
    header_cells: list[str],
    *,
    right_of_item_only: bool,
) -> tuple[int, int, int | None]:
    """
    결함항목에 해당하는 열 구간 [start, end) 와, 결함여부 값을 읽을 열(있으면).

    right_of_item_only=True (통합 문서·시트 2개 이상):
      항목명 오른쪽부터 무조건 스캔. 결함여부 열이 있으면 그 직전까지가 결함항목.
      col_flag가 헤더 탐색에서 빠졌어도 헤더 재스캔으로 보완한다.
    """
    if col_item_name is None:
        return (0, 0, None)

    start = col_item_name + 1
    if start >= ncols:
        return (start, start, col_flag)

    flag_col = col_flag
    if right_of_item_only:
        if flag_col is None or flag_col <= col_item_name:
            flag_col = _find_rightmost_flag_col(header_cells, col_item_name, ncols)
        end = flag_col if (flag_col is not None and flag_col > start) else ncols
    else:
        if flag_col is None or flag_col <= col_item_name:
            return (start, start, None)
        end = flag_col

    if start >= end:
        return (start, start, flag_col)
    return (start, end, flag_col)


def _service_name_from_defect_header(raw_header: str) -> str:
    """
    항목명~결함여부 구간 열 헤더에서 서비스명 추출.
    병합 헤더(예: '서비스명' + '한컴닷컴')인 경우 접두 '서비스명'을 뗀다.
    """
    h = _clean_text(_cell_str(raw_header))
    if not h:
        return ""
    nl = _normalize_label(h)
    svc_prefix = _normalize_label("서비스명")
    if nl.startswith(svc_prefix) and len(nl) > len(svc_prefix):
        # '서비스명XXX' 또는 '서비스명 XXX' 형태
        for prefix in ("서비스명", "서비스 명"):
            if h.startswith(prefix):
                rest = h[len(prefix) :].lstrip(" :\t")
                return _clean_text(rest)
    return h


def _make_criteria_record(
    분야: str,
    분야명: str,
    항목: str,
    항목명: str,
    결함항목: list[str],
    결함여부: str,
) -> dict:
    분야_code = 분야.rstrip(".") if 분야 else ""
    return {
        "분야_code": _clean_text(분야_code),
        "분야_name": _clean_text(분야명),
        "항목_code": _clean_text(항목),
        "항목_name": _clean_text(항목명),
        "결함항목": 결함항목,
        "결함여부": _clean_text(결함여부),
    }


_DOMAIN_CODE_RE = re.compile(r"^\d+\.\d+$")
_ITEM_CODE_RE = re.compile(r"^\d+(?:\.\d+){2,}$")


def _normalize_label(text: str) -> str:
    return "".join(_cell_str(text).split())


def _find_header_col(header_cells: list[str], label: str) -> int | None:
    target = _normalize_label(label)
    if not target:
        return None

    for idx, value in enumerate(header_cells):
        if _normalize_label(value) == target:
            return idx

    for idx, value in enumerate(header_cells):
        if target in _normalize_label(value):
            return idx

    return None


def _combined_header_cells(df: pd.DataFrame, header_row: int) -> list[str]:
    """
    병합된 헤더(2줄 헤더 등)를 최대한 복원하기 위해
    header_row와 header_row-1을 합친 label list를 만든다.
    """
    ncols = df.shape[1]
    current = [_cell_str(df.iloc[header_row, j]) for j in range(ncols)]
    previous = (
        [_cell_str(df.iloc[header_row - 1, j]) for j in range(ncols)]
        if header_row > 0
        else [""] * ncols
    )

    merged: list[str] = []
    for prev, cur in zip(previous, current):
        prev = _cell_str(prev)
        cur = _cell_str(cur)
        if prev and cur and prev != cur:
            merged.append(f"{prev} {cur}".strip())
        else:
            merged.append(prev or cur)
    return merged


def parse_criteria_flexible(
    df: pd.DataFrame,
    *,
    defect_columns_right_of_item_name: bool = False,
) -> tuple[list[dict], dict]:
    """
    인증기준별 결함 테이블을 헤더 기반으로 동적 파싱.
    (심사일지/예비결함 시트 모두 지원)

    defect_columns_right_of_item_name:
      True이면 통합 문서(시트 2개 이상)에서 결함항목 열을 '항목명' 바로 오른쪽부터
      결함여부 열 직전(또는 결함여부 미식별 시 시트 끝까지)으로 고정한다.
    """
    nrows, ncols = df.shape
    if nrows == 0 or ncols == 0:
        return [], {}

    best = None
    best_score = -1

    ANCHOR_ITEM_CODE = "1.1.1"
    ANCHOR_ITEM_NAME = "경영진의 참여"

    for header_row in range(nrows):
        header_cells = _combined_header_cells(df, header_row)

        col_domain = _find_header_col(header_cells, "분야")
        col_item = _find_header_col(header_cells, "항목")
        if col_domain is None or col_item is None:
            continue

        col_domain_name = _find_header_col(header_cells, "분야명")
        if col_domain_name is None and col_domain + 1 < ncols:
            col_domain_name = col_domain + 1

        col_item_name = _find_header_col(header_cells, "항목명")
        if col_item_name is None and col_item + 1 < ncols:
            col_item_name = col_item + 1
        col_flag = _find_header_col(header_cells, "결함여부")

        # 최소 요건: code + name pair가 잡히면 가산점
        score = 0
        score += 2 if col_domain_name is not None else 0
        score += 2 if col_item_name is not None else 0
        score += 1 if col_flag is not None else 0
        if (
            col_item_name is not None
            and col_flag is not None
            and col_flag > col_item_name
        ):
            score += min(8, col_flag - col_item_name - 1)

        # 첫 데이터 행 추정:
        # - 우선 "1.1.1 / 경영진의 참여" 앵커 row를 찾는다.
        # - 없으면 코드 패턴이 나오는 첫 행을 사용한다.
        first_data_row = None
        anchor_row = None

        for i in range(header_row + 1, min(header_row + 60, nrows)):
            item_code = _cell_str(df.iloc[i, col_item]) if col_item < ncols else ""
            item_name = (
                _cell_str(df.iloc[i, col_item_name])
                if col_item_name is not None and col_item_name < ncols
                else ""
            )
            if item_code.rstrip(
                "."
            ) == ANCHOR_ITEM_CODE and ANCHOR_ITEM_NAME in _clean_text(item_name):
                anchor_row = i
                first_data_row = i
                break

        if first_data_row is None:
            for i in range(header_row + 1, min(header_row + 40, nrows)):
                domain_code = (
                    _cell_str(df.iloc[i, col_domain]) if col_domain < ncols else ""
                )
                item_code = _cell_str(df.iloc[i, col_item]) if col_item < ncols else ""
                if _DOMAIN_CODE_RE.match(
                    domain_code.rstrip(".")
                ) and _ITEM_CODE_RE.match(item_code.rstrip(".")):
                    first_data_row = i
                    break

        if first_data_row is None:
            continue

        score += 10 if anchor_row is not None else 5

        if (
            anchor_row is not None
            and col_item_name is not None
            and col_item_name < ncols
        ):
            anchor_item_name = _clean_text(df.iloc[anchor_row, col_item_name])
            if ANCHOR_ITEM_NAME not in anchor_item_name:
                score -= 5
        if score > best_score:
            best_score = score
            best = (
                header_row,
                first_data_row,
                col_domain,
                col_domain_name,
                col_item,
                col_item_name,
                col_flag,
            )

    if best is None:
        return [], {}

    (
        header_row,
        first_data_row,
        col_domain,
        col_domain_name,
        col_item,
        col_item_name,
        col_flag,
    ) = best

    header_cells_win = _combined_header_cells(df, header_row)
    d_start, d_end, flag_col_effective = _defect_column_range(
        col_item_name,
        col_flag,
        ncols,
        header_cells_win,
        right_of_item_only=defect_columns_right_of_item_name,
    )
    defect_col_specs: list[tuple[int, str]] = []
    for j in range(d_start, d_end):
        if j >= ncols:
            break
        raw = header_cells_win[j] if j < len(header_cells_win) else ""
        svc = _service_name_from_defect_header(_cell_str(raw))
        if not svc:
            svc = f"열{j + 1}"
        defect_col_specs.append((j, svc))

    records: list[dict] = []
    defect_summary: dict = {}

    prev_domain = ""
    prev_domain_name = ""
    prev_item = ""
    prev_item_name = ""

    def _get(row: int, col: int | None) -> str:
        if col is None or col >= ncols:
            return ""
        return _cell_str(df.iloc[row, col])

    for i in range(first_data_row, nrows):
        row_texts = [_cell_str(df.iloc[i, j]) for j in range(ncols)]
        if any("결함 합계" in x for x in row_texts if x):
            nums = [x for x in row_texts if re.fullmatch(r"\d+", x)]
            defect_summary = {
                "label": "결함 합계",
                "values": nums,
                "raw_cells": [x for x in row_texts if x],
            }
            break

        domain_raw = _get(i, col_domain)
        item_raw = _get(i, col_item)
        domain_name_raw = _get(i, col_domain_name)
        item_name_raw = _get(i, col_item_name)

        if domain_raw:
            prev_domain = domain_raw
        if item_raw:
            prev_item = item_raw
        if domain_name_raw:
            prev_domain_name = domain_name_raw
        if item_name_raw:
            prev_item_name = item_name_raw

        domain = domain_raw or prev_domain
        item = item_raw or prev_item
        domain_name = domain_name_raw or prev_domain_name
        item_name = item_name_raw or prev_item_name

        flag = _get(i, flag_col_effective if flag_col_effective is not None else col_flag)
        결함항목 = [_clean_text(_get(i, j)) for j, _svc in defect_col_specs]
        any_defect_cell = any(_clean_text(t) for t in 결함항목)

        if not (
            domain
            or item
            or domain_name
            or item_name
            or any_defect_cell
            or flag
        ):
            continue

        # 헤더 값이 그대로 들어오는 행(예: "분야", "항목")은 제외
        if _normalize_label(domain) == _normalize_label("분야") and _normalize_label(
            item
        ) == _normalize_label("항목"):
            continue

        records.append(
            {
                "row_index": int(i) + 1,  # 엑셀 표시 기준(1-based)
                **_make_criteria_record(
                    domain,
                    domain_name,
                    item,
                    item_name,
                    결함항목,
                    flag,
                ),
            }
        )

    return records, defect_summary


def parse_criteria(
    df: pd.DataFrame,
    *,
    defect_columns_right_of_item_name: bool = False,
) -> tuple[list[dict], dict]:
    """
    인증기준별 결함 테이블 파싱 (앵커 기반)
    return: (criteria_list, defect_summary)
    """
    return parse_criteria_flexible(
        df, defect_columns_right_of_item_name=defect_columns_right_of_item_name
    )


def _is_meaningful_criteria_row(row: dict) -> bool:
    """
    retrieval 대상으로 삼을 가치가 있는 criteria row인지 판단.
    인증기준 식별자만 있어도 retrieval_rows에 포함한다.
    """
    id_fields = [
        row.get("분야_code", ""),
        row.get("분야_name", ""),
        row.get("항목_code", ""),
        row.get("항목_name", ""),
    ]
    if any(_clean_text(x) for x in id_fields):
        return True

    for d in row.get("결함항목") or []:
        if isinstance(d, str) and _clean_text(d):
            return True
        if isinstance(d, dict) and _clean_text(d.get("내용", "")):
            return True

    defect_flag = _clean_text(row.get("결함여부", ""))
    if defect_flag:
        return True

    return False


def build_retrieval_rows(
    metadata: dict, criteria_rows: list[dict], defect_summary: dict
) -> list[dict]:
    rows: list[dict] = []

    doc_title = _clean_text(metadata.get("document_title", ""))
    auditor = _clean_text(metadata.get("심사원명", ""))

    # 1) interview rows
    interviews = metadata.get("인터뷰", [])
    for idx, item in enumerate(interviews):
        service_name = _clean_text(item.get("서비스명", ""))
        col_index = item.get("col_index", None)
        dept = _clean_text(item.get("인터뷰_부서_및_대상", ""))
        content = _clean_text(item.get("인터뷰_내용", ""))
        system = _clean_text(item.get("확인문서_또는_시스템", ""))

        if not any([service_name, dept, content, system]):
            continue

        prefix = (
            SECTION_TITLE_INTERVIEW
            if not doc_title
            else (
                doc_title
                if doc_title == SECTION_TITLE_INTERVIEW
                else _join_nonempty([doc_title, SECTION_TITLE_INTERVIEW])
            )
        )
        text = "\n".join(
            [
                prefix,
                f"서비스명: {service_name}" if service_name else "",
                f"인터뷰 대상: {dept}" if dept else "",
                f"인터뷰 내용: {content}" if content else "",
                f"확인문서 또는 시스템: {system}" if system else "",
                f"심사원명: {auditor}" if auditor else "",
            ]
        ).strip()

        rows.append(
            {
                "kind": "interview",
                "section": SECTION_TITLE_INTERVIEW,
                "col_index": int(col_index) if col_index is not None else None,
                "서비스명": service_name,
                "인터뷰_부서_및_대상": dept,
                "인터뷰_내용": content,
                "확인문서_또는_시스템": system,
                "text": text,
            }
        )

    # 2) criteria rows
    for idx, row in enumerate(criteria_rows):
        if not _is_meaningful_criteria_row(row):
            continue

        source_row_index = row.get("row_index", idx)
        field_code = _clean_text(row.get("분야_code", ""))
        field_name = _clean_text(row.get("분야_name", ""))
        item_code = _clean_text(row.get("항목_code", ""))
        item_name = _clean_text(row.get("항목_name", ""))
        defect_flag = _clean_text(row.get("결함여부", ""))
        결함항목_raw = row.get("결함항목") or []
        결함항목_out: list[str] = []
        for d in 결함항목_raw:
            if isinstance(d, str):
                결함항목_out.append(_clean_text(d))
            elif isinstance(d, dict):
                결함항목_out.append(_clean_text(d.get("내용", "")))

        crit_parts = [
            _join_nonempty([doc_title, SECTION_TITLE_CRITERIA]),
            _join_nonempty([field_code, field_name], sep=" "),
            _join_nonempty([item_code, item_name], sep=" "),
        ]
        결함_nonempty = [t for t in 결함항목_out if t]
        if 결함_nonempty:
            crit_parts.append("결함항목:\n" + "\n".join(결함_nonempty))
        if defect_flag:
            crit_parts.append(f"결함여부: {defect_flag}")
        text = "\n".join([p for p in crit_parts if p]).strip()

        rows.append(
            {
                "kind": "criteria",
                "section": SECTION_TITLE_CRITERIA,
                "row_index": int(source_row_index)
                if source_row_index is not None
                else None,
                "분야_code": field_code,
                "분야_name": field_name,
                "항목_code": item_code,
                "항목_name": item_name,
                "결함항목": 결함항목_out,
                "결함여부": defect_flag,
                "text": text,
            }
        )

    # 3) defect summary row
    if defect_summary:
        summary_text = "\n".join(
            [
                _join_nonempty([doc_title, SECTION_TITLE_DEFECT_SUMMARY]),
                f"값: {', '.join(defect_summary.get('values', []))}"
                if defect_summary.get("values")
                else "",
                f"원본: {' | '.join(defect_summary.get('raw_cells', []))}"
                if defect_summary.get("raw_cells")
                else "",
            ]
        ).strip()

        rows.append(
            {
                "kind": "summary",
                "section": SECTION_TITLE_DEFECT_SUMMARY,
                "label": defect_summary.get("label", ""),
                "values": defect_summary.get("values", []),
                "raw_cells": defect_summary.get("raw_cells", []),
                "text": summary_text,
            }
        )

    return rows


def excel_to_json(path: str | Path) -> dict:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)

    xls = pd.ExcelFile(path)
    multi_sheet = len(xls.sheet_names) >= 2
    # 통합 문서(시트 2개 이상): 항목명 오른쪽 열을 결함항목으로 고정
    defect_cols_mode = multi_sheet

    df_main = pd.read_excel(xls, sheet_name=SHEET_NAME, header=None)
    df_defect = (
        pd.read_excel(xls, sheet_name=SHEET_NAME_DEFECT, header=None)
        if SHEET_NAME_DEFECT in xls.sheet_names
        else None
    )

    metadata = parse_metadata(df_main)

    # criteria(인증기준별 결함사항)는 기본적으로 '심사일지'에서 파싱하되,
    # 시트가 분리된 경우('예비결함')에는 그 시트에서 파싱 결과를 사용/보강한다.
    criteria_main, defect_summary_main = parse_criteria(
        df_main, defect_columns_right_of_item_name=defect_cols_mode
    )
    criteria_defect, defect_summary_defect = ([], {})
    if df_defect is not None:
        criteria_defect, defect_summary_defect = parse_criteria(
            df_defect, defect_columns_right_of_item_name=defect_cols_mode
        )

    criteria = criteria_main or criteria_defect
    defect_summary = defect_summary_main or defect_summary_defect

    data = {
        "source_file": str(Path(path).name),
        "sheet": SHEET_NAME,
        "sheets": {
            "main": SHEET_NAME,
            "defect": SHEET_NAME_DEFECT if df_defect is not None else "",
            "used_for_criteria": SHEET_NAME
            if criteria_main
            else (SHEET_NAME_DEFECT if criteria_defect else ""),
            "used_for_defect_summary": (
                SHEET_NAME
                if defect_summary_main
                else (SHEET_NAME_DEFECT if defect_summary_defect else "")
            ),
        },
        "metadata": {
            "document_title": metadata.get("document_title", ""),
            "심사원명": metadata.get("심사원명", ""),
        },
        "retrieval_rows": build_retrieval_rows(metadata, criteria, defect_summary),
    }
    return data


def build_output_data(data: dict) -> dict:
    """
    배치 러너(step2_2024_2025_batch_runner.py) 호환을 위한 출력 데이터 구성.
    summary_doc로 감싸기 전에 저장할 최소 필드만 남긴다.
    """
    output = dict(data) if isinstance(data, dict) else {}
    output.pop("source_file", None)
    return output


def main() -> None:
    parser = argparse.ArgumentParser(
        description="NISC ISMS-P 심사일지 엑셀(심사일지 시트) → JSON/JSONL 변환",
    )
    parser.add_argument(
        "input",
        nargs="*",
        default=None,
        help="입력 엑셀 파일 경로 (.xlsx). 미지정 시 기본 경로 사용",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="출력 JSON 파일 경로 (미지정 시 기본 경로 또는 stdout)",
    )
    parser.add_argument(
        "--format",
        choices=["json", "jsonl"],
        default="json",
        help="출력 형식: json(단일 객체), jsonl(한 줄에 한 레코드)",
    )
    parser.add_argument(
        "--no-summary-doc",
        action="store_true",
        help="summary_doc를 포함하지 않습니다. (기본값: 포함, json 출력에서만 적용)",
    )
    args = parser.parse_args()

    if not args.input:
        args.input = [DEFAULT_INPUT_PATH]
        if args.output is None:
            out_name = Path(DEFAULT_INPUT_PATH).stem + ".json"
            args.output = str(Path(DEFAULT_OUTPUT_DIR) / out_name)

    path = Path(args.input[0])
    if not path.is_file():
        print(f"오류: 파일을 찾을 수 없습니다. {path}", file=sys.stderr)
        sys.exit(1)

    try:
        data = excel_to_json(path)
    except Exception as e:
        print(f"오류: 변환 실패 - {e}", file=sys.stderr)
        sys.exit(1)

    data_for_output = data
    if isinstance(data, dict):
        data_for_output = dict(data)
        data_for_output.pop("source_file", None)

    if args.format == "jsonl":
        text = json.dumps(data_for_output, ensure_ascii=False)
    else:
        if args.no_summary_doc:
            payload = data_for_output
        else:
            from summary_doc import build_summary_doc

            summary_doc = build_summary_doc(
                data,
                template="NISC01",
                source_file=str(Path(path).name),
            )
            payload = {
                "template": "NISC01",
                "summary_doc": summary_doc if isinstance(summary_doc, dict) else {},
                "data": data_for_output,
            }
        text = json.dumps(payload, ensure_ascii=False, indent=2)

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text, encoding="utf-8")
        retrieval_count = len(data.get("retrieval_rows", []))
        print(
            f"저장됨: {out_path} (retrieval_rows {retrieval_count}건)", file=sys.stderr
        )
    else:
        print(text)


if __name__ == "__main__":
    main()
