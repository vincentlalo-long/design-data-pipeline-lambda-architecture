import os
import re
import subprocess
import markdown
import pypdf

script_dir = r"D:\BigData\Assignment4\design-data-pipeline-lambda-architecture\report"
md_path = os.path.join(script_dir, "HW4_Report.md")
html_path = os.path.join(script_dir, "HW4_Report.html")
pdf_path = os.path.join(script_dir, "HW4_Report.pdf")

with open(md_path, "r", encoding="utf-8") as f:
    md_text = f.read()

lines = md_text.split("\n")
content_lines = []
skip_header = True
for line in lines:
    if line.strip().startswith("## 1."):
        skip_header = False
    if not skip_header:
        content_lines.append(line)

content_md = "\n".join(content_lines)
html_body = markdown.markdown(content_md, extensions=["tables", "fenced_code"])

# Normalize standalone markdown image + italic caption into img-card
img_pattern = re.compile(
    r'<p><img\s+alt=["\'](.*?)["\']\s+src=["\'](.*?)["\']\s*/?>\s*<em>(.*?)</em></p>',
    re.DOTALL,
)


def img_repl(m):
    return f'<div class="img-card"><img src="{m.group(2)}" alt="{m.group(1)}"><p class="caption">{m.group(3)}</p></div>'


html_body, count = img_pattern.subn(img_repl, html_body)
print(f"Formatted {count} standalone images.")

html_full = f"""<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="utf-8">
<title>Báo cáo Assignment 4 - Nguyễn Đăng Long - 202400057</title>
<style>
  @page {{
    size: A4;
    margin: 0.9cm 1.1cm 0.9cm 1.1cm;
  }}
  body {{
    font-family: 'Times New Roman', Times, serif;
    font-size: 9.8pt;
    line-height: 1.24;
    color: #111;
    margin: 0;
  }}
  .header-block {{
    margin-bottom: 5px;
    text-align: center;
  }}
  .header-block h1 {{
    font-size: 12.8pt;
    font-weight: bold;
    text-transform: uppercase;
    margin: 1px 0 2px 0;
    color: #000;
  }}
  .header-block .sub-topic {{
    font-size: 9.8pt;
    font-style: italic;
    margin: 1px 0 5px 0;
    color: #222;
  }}
  .meta-info {{
    font-size: 9.2pt;
    line-height: 1.25;
    text-align: left;
    display: inline-block;
    margin: 1px auto;
  }}
  .meta-info p {{
    margin: 1px 0;
  }}
  hr.divider {{
    border: none;
    border-top: 1.5px solid #333;
    margin: 4px 0 6px 0;
  }}
  h2 {{
    font-size: 10.6pt;
    font-weight: bold;
    color: #000;
    margin-top: 7px;
    margin-bottom: 3px;
    break-after: avoid;
  }}
  h3 {{
    font-size: 9.6pt;
    font-weight: bold;
    color: #000;
    margin-top: 5px;
    margin-bottom: 2px;
    break-after: avoid;
  }}
  p {{
    margin-top: 2px;
    margin-bottom: 2.5px;
    text-align: justify;
  }}
  ul, ol {{
    margin-top: 2px;
    margin-bottom: 3px;
    padding-left: 16px;
  }}
  li {{
    margin-bottom: 1.5px;
    text-align: justify;
  }}
  pre {{
    background-color: #f7f7f7;
    border: 1px solid #ccc;
    padding: 4px 7px;
    font-family: Consolas, 'Courier New', monospace;
    font-size: 7.8pt;
    line-height: 1.18;
    white-space: pre-wrap;
    margin: 4px 0;
  }}
  code {{
    font-family: Consolas, 'Courier New', monospace;
    font-size: 8.2pt;
    background-color: #f2f2f2;
    padding: 0.5px 2px;
    border-radius: 2px;
  }}
  pre code {{
    background-color: transparent;
    padding: 0;
  }}
  table {{
    border-collapse: collapse;
    width: 100%;
    margin: 5px 0;
    font-size: 8pt;
  }}
  tr {{
    break-inside: avoid;
  }}
  th, td {{
    border: 1px solid #333;
    padding: 3px 4.5px;
    vertical-align: middle;
  }}
  th {{
    background-color: #efefef;
    font-weight: bold;
    text-align: center;
  }}
  .img-card {{
    text-align: center;
    margin: 6px auto;
    break-inside: avoid;
  }}
  .img-card img {{
    max-width: 90%;
    max-height: 225px;
    object-fit: contain;
    border: 1px solid #888;
    display: block;
    margin: 0 auto;
  }}
  .img-card .caption {{
    font-size: 8.3pt;
    font-style: italic;
    color: #333;
    text-align: center;
    margin-top: 3px;
    margin-bottom: 4px;
    line-height: 1.18;
  }}
</style>
</head>
<body>

<div class="header-block">
  <h1>BÁO CÁO ASSIGNMENT 4</h1>
  <p class="sub-topic">Thiết kế và Đánh giá Data Pipeline theo Kiến trúc Lambda (Batch-only vs. Lambda)</p>
  <div style="text-align: left; width: 100%;">
    <div class="meta-info">
      <p><strong>Mã HP:</strong> IT4931 - BigData Storage and Processing</p>
      <p><strong>Họ và tên:</strong> Nguyễn Đăng Long &nbsp;|&nbsp; <strong>MSSV:</strong> 202400057</p>
    </div>
  </div>
</div>

{html_body}

</body>
</html>
"""

with open(html_path, "w", encoding="utf-8") as f:
    f.write(html_full)

edge_exe = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
cmd = [
    edge_exe,
    "--headless",
    "--disable-gpu",
    "--no-pdf-header-footer",
    f"--print-to-pdf={pdf_path}",
    html_path,
]

res = subprocess.run(cmd, capture_output=True, text=True)
if os.path.exists(pdf_path):
    reader = pypdf.PdfReader(pdf_path)
    print(f"SUCCESS: {pdf_path} | Size={os.path.getsize(pdf_path):,} bytes | Pages={len(reader.pages)}")
else:
    print("FAILED:", res.stderr)
