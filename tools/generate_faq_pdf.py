#!/usr/bin/env python3
"""Simple converter: docs/FAQ.md -> docs/FAQ.pdf

This script uses reportlab to produce a readable PDF from the markdown FAQ.
It implements a minimal markdown-to-PDF transformation (headings, paragraphs, horizontal rules).
"""
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, HRFlowable
from reportlab.lib.units import mm
import os

INPUT = os.path.join(os.path.dirname(__file__), '..', 'docs', 'FAQ.md')
OUTPUT = os.path.join(os.path.dirname(__file__), '..', 'docs', 'FAQ.pdf')

styles = getSampleStyleSheet()
# Use unique custom style names to avoid conflicts with built-in stylesheet
if 'MyHeading1' not in styles:
    styles.add(ParagraphStyle(name='MyHeading1', parent=styles['Heading1'], fontSize=18, leading=22))
if 'MyHeading2' not in styles:
    styles.add(ParagraphStyle(name='MyHeading2', parent=styles['Heading2'], fontSize=14, leading=18))
if 'MyCode' not in styles:
    styles.add(ParagraphStyle(name='MyCode', parent=styles['Code'], fontName='Courier', fontSize=9))


def md_to_flowables(md_text):
    lines = md_text.splitlines()
    flowables = []
    buffer = []

    def flush_paragraph():
        nonlocal buffer
        if not buffer:
            return
        text = ' '.join(line.strip() for line in buffer)
        flowables.append(Paragraph(text, styles['Normal']))
        flowables.append(Spacer(1, 6))
        buffer = []

    for line in lines:
        if line.startswith('# '):
            flush_paragraph()
            flowables.append(Paragraph(line[2:].strip(), styles['MyHeading1']))
            flowables.append(Spacer(1, 6))
        elif line.startswith('## '):
            flush_paragraph()
            flowables.append(Paragraph(line[3:].strip(), styles['MyHeading2']))
            flowables.append(Spacer(1, 4))
        elif line.startswith('---'):
            flush_paragraph()
            flowables.append(HRFlowable(width='100%', thickness=1, color=colors.grey))
            flowables.append(Spacer(1, 6))
        elif line.strip() == '':
            flush_paragraph()
        elif line.startswith('    ') or line.startswith('\t'):
            flush_paragraph()
            flowables.append(Paragraph('<pre>%s</pre>' % line.strip(), styles['MyCode']))
        else:
            buffer.append(line)
    flush_paragraph()
    return flowables


def main():
    with open(INPUT, 'r', encoding='utf-8') as f:
        md_text = f.read()

    doc = SimpleDocTemplate(OUTPUT, pagesize=A4, leftMargin=20*mm, rightMargin=20*mm, topMargin=18*mm, bottomMargin=18*mm)
    flowables = md_to_flowables(md_text)
    doc.build(flowables)
    print(f'Wrote PDF: {OUTPUT}')


if __name__ == '__main__':
    main()
