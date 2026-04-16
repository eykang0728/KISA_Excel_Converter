#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

if __package__ in {None, ""}:
    repo_root = Path(__file__).resolve().parents[2]
    src_root = repo_root / "src"
    sys.path.insert(0, str(src_root))

from summary_doc import build_summary_doc

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
DEFAULT_INPUT_ROOT = REPO_ROOT / "excel_test_file" / "template" / "step2_2024-2025_all"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "excel_test_file" / "(step2)result_normalized_v3" / "step2_2024-2025_all_v2"
COMMON_CONVERTER_PATH = SRC_ROOT / "step2_common_audit_log_excel_to_json.py"
EXCEL_SUFFIXES = {".xls", ".xlsx", ".xlsm"}


@dataclass(frozen=True)
class TemplateRunner:
    folder_name: str
    template: str
    converter: Callable[[str | Path], dict]


def _load_converter_module():
    spec = importlib.util.spec_from_file_location("step2_common_audit_log", COMMON_CONVERTER_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"공통 파서 모듈을 불러올 수 없습니다: {COMMON_CONVERTER_PATH}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_COMMON_MODULE = _load_converter_module()
common_excel_to_json = getattr(_COMMON_MODULE, "excel_to_json")
build_output_data = getattr(_COMMON_MODULE, "build_output_data")


RUNNERS: dict[str, TemplateRunner] = {
    "NISC": TemplateRunner(
        folder_name="NISC",
        template="NISC01",
        converter=common_excel_to_json,
    ),
    "OPA": TemplateRunner(
        folder_name="OPA",
        template="OPA02",
        converter=common_excel_to_json,
    ),
}


def detect_runner(input_root: Path, relative_path: Path) -> TemplateRunner:
    folder_candidates = [input_root.name.upper()]
    if relative_path.parts:
        folder_candidates.append(relative_path.parts[0].upper())

    folder_name = next((name for name in folder_candidates if name in RUNNERS), "")
    runner = RUNNERS.get(folder_name)
    if runner is None:
        raise ValueError(f"지원하지 않는 최상위 폴더입니다: {folder_name}")
    return runner


def iter_excel_files(input_root: Path) -> list[Path]:
    files: list[Path] = []
    for path in sorted(input_root.rglob("*")):
        if path.is_file() and path.suffix.lower() in EXCEL_SUFFIXES:
            files.append(path)
    return files


def build_output_path(
    input_file: Path,
    input_root: Path,
    output_root: Path,
    output_format: str,
) -> Path:
    relative_path = input_file.relative_to(input_root)
    suffix = ".jsonl" if output_format == "jsonl" else ".json"
    return (output_root / relative_path).with_suffix(suffix)


def build_payload(
    input_file: Path,
    runner: TemplateRunner,
    output_format: str,
    include_summary_doc: bool,
) -> str:
    data = runner.converter(input_file)
    data_for_output = data
    if isinstance(data, dict):
        data_for_output = build_output_data(data)

    if output_format == "jsonl":
        return json.dumps(data_for_output, ensure_ascii=False)

    if not include_summary_doc:
        payload = data_for_output
    else:
        summary_doc = build_summary_doc(
            data,
            template=runner.template,
            source_file=input_file.name,
        )
        payload = {
            "template": runner.template,
            "summary_doc": summary_doc if isinstance(summary_doc, dict) else {},
            "data": data_for_output,
        }
    return json.dumps(payload, ensure_ascii=False, indent=2)


def process_batch(
    input_root: Path,
    output_root: Path,
    output_format: str = "json",
    include_summary_doc: bool = True,
    continue_on_error: bool = True,
) -> tuple[int, list[str]]:
    if not input_root.is_dir():
        raise FileNotFoundError(f"입력 폴더를 찾을 수 없습니다: {input_root}")

    files = iter_excel_files(input_root)
    if not files:
        raise FileNotFoundError(f"처리할 엑셀 파일이 없습니다: {input_root}")

    success_count = 0
    failures: list[str] = []

    for input_file in files:
        relative_path = input_file.relative_to(input_root)
        runner = detect_runner(input_root, relative_path)
        output_path = build_output_path(
            input_file=input_file,
            input_root=input_root,
            output_root=output_root,
            output_format=output_format,
        )

        try:
            payload = build_payload(
                input_file=input_file,
                runner=runner,
                output_format=output_format,
                include_summary_doc=include_summary_doc,
            )
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(payload, encoding="utf-8")
            success_count += 1
            print(f"[{runner.template}] 저장됨: {output_path}", file=sys.stderr)
        except Exception as exc:  # noqa: BLE001
            message = f"[{runner.template}] 실패: {input_file} - {exc}"
            failures.append(message)
            print(message, file=sys.stderr)
            if not continue_on_error:
                break

    return success_count, failures


def main() -> None:
    parser = argparse.ArgumentParser(
        description="step2_2024-2025 폴더를 순회하며 공통 심사일지 파서를 자동 실행합니다.",
    )
    parser.add_argument(
        "input_root",
        nargs="?",
        default=str(DEFAULT_INPUT_ROOT),
        help="입력 루트 폴더. 기본값: services/excel_converter/excel_test_file/template/step2_2024-2025_all",
    )
    parser.add_argument(
        "-o",
        "--output-root",
        default=str(DEFAULT_OUTPUT_ROOT),
        help="출력 루트 폴더. 입력 구조를 유지한 채 JSON이 저장됩니다.",
    )
    parser.add_argument(
        "--format",
        choices=["json", "jsonl"],
        default="json",
        help="출력 형식",
    )
    parser.add_argument(
        "--no-summary-doc",
        action="store_true",
        help="summary_doc를 포함하지 않습니다. (json 출력에서만 적용)",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="파일 하나라도 실패하면 즉시 중단합니다.",
    )
    args = parser.parse_args()

    input_root = Path(args.input_root)
    output_root = Path(args.output_root)

    try:
        success_count, failures = process_batch(
            input_root=input_root,
            output_root=output_root,
            output_format=args.format,
            include_summary_doc=not args.no_summary_doc,
            continue_on_error=not args.fail_fast,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"오류: 배치 실행 실패 - {exc}", file=sys.stderr)
        sys.exit(1)

    print(
        f"완료: 성공 {success_count}건, 실패 {len(failures)}건",
        file=sys.stderr,
    )
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
