from prep_warc_files import warc_to_html, warc_to_pdf
from combine_domains import combine_domains_by_timestamp
from html_combined_to_markdown import convert_html_combined_to_markdown
from pdf_combined_to_markdown import convert_pdf_combined_to_markdown

WARC_DIR = "/capstor/store/cscs/swissai/infra01/admin_chat/"
EXCEL    = "data/2025-11-20_19945_topics.xlsx"

warc_to_html(WARC_DIR, "output/html_raw")
warc_to_pdf(WARC_DIR, "output/pdf_raw")
combine_domains_by_timestamp("output/html_raw", "output/html_combined", excel_path=EXCEL)
combine_domains_by_timestamp("output/pdf_raw",  "output/pdf_combined",  excel_path=EXCEL)
convert_html_combined_to_markdown("output/html_combined", "output/markdown", EXCEL, "output/mappings/html.json")
convert_pdf_combined_to_markdown("output/pdf_combined",   "output/markdown", EXCEL, "output/mappings/pdf.json")

print("Done! Markdown files are in: output/markdown/")
