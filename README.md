## Excel Converter (엑셀 → JSON/JSONL)

기관/템플릿별 엑셀 파일을 순회하며 **구조화된 JSON(또는 JSONL)** 로 변환하는 스크립트 모음입니다.

- **Step1**: 운영명세서(공통 운영명세서 파서 기반)
- **Step2**: 심사일지(공통 심사일지 파서 기반)
- 선택적으로 출력에 `summary_doc`(템플릿/회사명/문서유형/연도 힌트 등 최소 메타)를 포함할 수 있습니다.

## 요구사항

- **Python**: 3.9 이상
- **패키지/실행 관리**: `uv` 사용

## 빠른 시작 (uv)

### 1) uv 설치

Linux:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

설치 후 새 셸을 열거나 아래 중 하나를 실행하세요.

```bash
source "$HOME/.cargo/env" 2>/dev/null || true
export PATH="$HOME/.local/bin:$PATH"
```

### 2) 가상환경/의존성 설치

프로젝트 루트에서:

```bash
uv sync
```

> `uv.lock`이 함께 관리되므로, 동일한 의존성 버전으로 재현 가능한 환경이 만들어집니다.

### 3) 실행

#### Step1 배치 실행

```bash
uv run step1-batch
```

#### Step2 배치 실행

```bash
uv run step2-batch
```

옵션 확인:

```bash
uv run step1-batch -h
uv run step2-batch -h
```

## 입력/출력 폴더 (기본값)

### Step1

- **기본 입력 폴더**: `excel_test_file/template/step1_2024-2025_all/`
- **기본 출력 폴더**: `excel_test_file/(step1)result_normalized_v3/step1_2024-2025_all_v2/`

### Step2

- **기본 입력 폴더**: `excel_test_file/template/step2_2024-2025_all/`
- **기본 출력 폴더**: `excel_test_file/(step2)result_normalized_v3/step2_2024-2025_all_v2/`

둘 다 **입력 폴더 구조를 유지한 채** 출력 폴더에 JSON/JSONL 파일을 생성합니다.

## CLI 사용 예시

### 다른 입력 폴더 지정

```bash
uv run step1-batch /path/to/input_root
uv run step2-batch /path/to/input_root
```

### 출력 폴더 지정

```bash
uv run step1-batch /path/to/input_root -o /path/to/output_root
uv run step2-batch /path/to/input_root -o /path/to/output_root
```

### JSONL로 출력

```bash
uv run step1-batch --format jsonl
uv run step2-batch --format jsonl
```

### summary_doc 제외 (JSON 출력에서만 적용)

```bash
uv run step1-batch --no-summary-doc
uv run step2-batch --no-summary-doc
```

### 실패 시 즉시 중단 (fail-fast)

```bash
uv run step1-batch --fail-fast
uv run step2-batch --fail-fast
```

## 출력 포맷

### JSON (`--format json`, 기본)

`summary_doc` 포함 시 대략 아래 형태로 저장됩니다.

```json
{
  "template": "COMMON_OPERATING_SPEC",
  "summary_doc": {
    "template": "COMMON_OPERATING_SPEC",
    "source_file": "원본파일명.xlsx",
    "company": "회사명",
    "doc_type": "문서유형",
    "year_hint": "2024 ..."
  },
  "data": { "..." : "..." }
}
```

### JSONL (`--format jsonl`)

파일 1개당 **한 줄(JSON 1개)** 로 저장합니다. (`summary_doc`를 포함하지 않고 데이터만 기록)

## 입력 폴더 구조 규칙 (중요)

### Step1

`input_root` 아래 최상위 폴더명이 다음 중 하나여야 합니다.

- `KAIT/`
- `NISC/`
- `OPA/`

예:

```text
excel_test_file/template/step1_2024-2025_all/
  KAIT/...
  NISC/...
  OPA/...
```

### Step2

Step2는 `input_root` 자체 이름 또는 그 하위 첫 폴더명에서 아래를 감지합니다.

- `NISC`
- `OPA`

## 의존성/엔진 참고

- 엑셀 파일 포맷에 따라 `openpyxl`(xlsx) / `xlrd`(xls) 엔진이 필요할 수 있어 기본 의존성에 포함되어 있습니다.

## 문제 해결

- **엑셀 파일이 없다고 나올 때**: `input_root` 경로가 실제로 존재하는지, 내부에 `.xls/.xlsx/.xlsm` 파일이 있는지 확인하세요.
- **지원하지 않는 최상위 폴더 오류**: Step1은 `KAIT/NISC/OPA` 폴더 구조가 필요합니다.
- **의존성 꼬임/재설치**: 아래로 깨끗이 재구성할 수 있습니다.

```bash
rm -rf .venv
uv sync
```
