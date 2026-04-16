#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from step1_common_operating_spec_excel_to_json import (
    excel_to_structured as common_excel_to_structured,
)
from summary_doc import build_summary_doc


HERE = Path(__file__).resolve().parent
DEFAULT_INPUT_ROOT = HERE / "excel_test_file" / "template" / "step1_2024-2025_all"
DEFAULT_OUTPUT_ROOT = HERE / "excel_test_file" / "(step1)result_normalized_v3" / "step1_2024-2025_all_v2"
EXCEL_SUFFIXES = {".xls", ".xlsx", ".xlsm"}


@dataclass(frozen=True)
class TemplateRunner:
    folder_name: str
    template: str
    converter: Callable[[str | Path], dict]


RUNNERS: dict[str, TemplateRunner] = {
    "KAIT": TemplateRunner(
        folder_name="KAIT",
        template="COMMON_OPERATING_SPEC",
        converter=common_excel_to_structured,
    ),
    "NISC": TemplateRunner(
        folder_name="NISC",
        template="COMMON_OPERATING_SPEC",
        converter=common_excel_to_structured,
    ),
    "OPA": TemplateRunner(
        folder_name="OPA",
        template="COMMON_OPERATING_SPEC",
        converter=common_excel_to_structured,
    ),
}


def detect_runner_from_relative_path(relative_path: Path) -> TemplateRunner:
    if not relative_path.parts:
        raise ValueError("상대 경로가 비어 있습니다.")

    folder_name = relative_path.parts[0].upper()
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
    use_common_converter: bool,
) -> str:
    converter = common_excel_to_structured if use_common_converter else runner.converter
    data = converter(input_file)
    data_for_output = data
    if isinstance(data, dict):
        data_for_output = dict(data)
        data_for_output.pop("source_file", None)

    if output_format == "jsonl":
        return json.dumps(data_for_output, ensure_ascii=False)

    if not include_summary_doc:
        payload = data_for_output
    else:
        summary_doc = build_summary_doc(
            data,
            template=("COMMON_OPERATING_SPEC" if use_common_converter else runner.template),
            source_file=input_file.name,
        )
        payload = {
            "template": ("COMMON_OPERATING_SPEC" if use_common_converter else runner.template),
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
    use_common_converter: bool = False,
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
        runner = detect_runner_from_relative_path(relative_path)
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
                use_common_converter=use_common_converter,
            )
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(payload, encoding="utf-8")
            success_count += 1
            print(
                f"[{'COMMON_OPERATING_SPEC' if use_common_converter else runner.template}] 저장됨: {output_path}",
                file=sys.stderr,
            )
        except Exception as exc:  # noqa: BLE001
            message = (
                f"[{'COMMON_OPERATING_SPEC' if use_common_converter else runner.template}] "
                f"실패: 파일명={input_file.name}, 경로={input_file} - {exc}"
            )
            failures.append(message)
            print(message, file=sys.stderr)
            if not continue_on_error:
                break

    return success_count, failures


def main() -> None:
    parser = argparse.ArgumentParser(
        description="step1_2024-2025 폴더를 순회하며 KAIT05/NISC03/OPA04 파서를 자동 실행합니다.",
    )
    parser.add_argument(
        "input_root",
        nargs="?",
        default=str(DEFAULT_INPUT_ROOT),
        help="입력 루트 폴더. 기본값: services/excel_converter/excel_test_file/template/step1_2024-2025",
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
        "--use-common",
        action="store_true",
        help="기관별 파서 대신 공통 운영명세서 파서(step1_common_operating_spec_excel_to_json)를 사용합니다.",
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
            use_common_converter=args.use_common,
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
