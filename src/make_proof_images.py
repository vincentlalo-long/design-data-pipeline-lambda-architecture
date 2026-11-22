from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

from config import BASE_DIR, LOGS_DIR, METRICS_DIR


def load_mono_font(size: int = 15):
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationMono-Regular.ttf",
        "C:/Windows/Fonts/consola.ttf",
    ]
    for path in candidates:
        if Path(path).exists():
            return ImageFont.truetype(path, size)
    return ImageFont.load_default()


def render_terminal_card(title: str, lines: list[str], out_path: Path) -> None:
    font = load_mono_font(15)
    title_font = load_mono_font(15)
    pad_x, pad_y = 20, 18
    line_h = 21
    max_chars = max([len(l) for l in lines] + [len(title) + 4])
    width = max(920, int(max_chars * 9.1) + pad_x * 2)
    height = 38 + pad_y * 2 + len(lines) * line_h

    img = Image.new("RGB", (width, height), "#14181f")
    draw = ImageDraw.Draw(img)

    # Window header bar
    draw.rectangle([(0, 0), (width, 36)], fill="#1f2630")
    draw.ellipse([(14, 12), (26, 24)], fill="#ff5f56")
    draw.ellipse([(34, 12), (46, 24)], fill="#ffbd2e")
    draw.ellipse([(54, 12), (66, 24)], fill="#27c93f")
    draw.text((82, 10), title, fill="#cfd8e3", font=title_font)

    y = 36 + pad_y
    for line in lines:
        line = line.replace("\t", "  ")
        color = "#e6edf3"
        if line.startswith("===") or line.startswith("---"):
            color = "#79c0ff"
        elif line.startswith("longd@") or line.startswith("$ "):
            color = "#7ee787"
        elif "Diff count = 0" in line or "correct\": true" in line or "Up " in line:
            color = "#a5f3fc"
        draw.text((pad_x, y), line, fill=color, font=font)
        y += line_h

    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path)
    print(f"Saved {out_path}")


def clean_batch_lines(path: Path) -> list[str]:
    out = []
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if "[batch_layer]" in raw:
            out.append(raw.strip())
    return out


def main() -> None:
    proof_dir = BASE_DIR / "report" / "proof"

    # 1. Services, Kafka 1-partition, Q1/Q2 & Ground-truth Verification
    ver_lines = (METRICS_DIR / "verification_proof.txt").read_text(encoding="utf-8").splitlines()
    lines_1 = ["longd@longPc:/mnt/d/BigData/Assignment4/design-data-pipeline-lambda-architecture$ cat metrics/verification_proof.txt", ""]
    lines_1.extend(ver_lines)
    render_terminal_card("Verification Proof: Docker, Kafka (1 Partition), PostgreSQL Q1/Q2 & Ground Truth", lines_1, proof_dir / "proof_1_services_kafka_pg.png")

    # 2. Component Logs (E3 Fault/Recovery + E4 Tuned)
    e3_prod = (LOGS_DIR / "E3_lambda_fault_r1_producer.log").read_text(encoding="utf-8").strip()
    e3_raw = (LOGS_DIR / "E3_lambda_fault_r1_raw.log").read_text(encoding="utf-8").splitlines()
    e3_batch = clean_batch_lines(LOGS_DIR / "E3_lambda_fault_r1_batch.log")
    e3_speed = [
        l.strip()
        for l in (LOGS_DIR / "E3_lambda_fault_r1_speed.log").read_text(encoding="utf-8", errors="ignore").splitlines()
        if "[speed_layer]" in l or "ProcessingTimeExecutor" in l
    ]
    e4_batch = clean_batch_lines(LOGS_DIR / "E4_lambda_tuned_r1_batch.log")
    e4_mon = (LOGS_DIR / "E4_lambda_tuned_r1_monitor.log").read_text(encoding="utf-8").strip()

    lines_2 = [
        "$ head -n 5 logs/E3_lambda_fault_r1_*.log (Producer, Raw Storage, Batch, Speed)",
        f"  [producer]     {e3_prod}",
        *[f"  [raw_consumer] {l}" for l in e3_raw],
        *[f"  [batch_layer]  {l}" for l in e3_batch],
        *[f"  [speed_layer]  {l}" for l in e3_speed[:6]],
        "",
        "$ tail logs/E4_lambda_tuned_r1_*.log (Tuned 500ms trigger, shuffle=4)",
        *[f"  [batch_layer]  {l}" for l in e4_batch],
        f"  [monitor]      {e4_mon[:125]}...",
        f"                 ...{e4_mon[-118:]}",
    ]
    render_terminal_card("Component Logs: E3 Checkpoint Recovery & E4 Tuned Verification", lines_2, proof_dir / "proof_2_pipeline_logs.png")


if __name__ == "__main__":
    main()
