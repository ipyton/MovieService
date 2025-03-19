import datetime

import fitz  # PyMuPDF
import pdfplumber

import os
import fitz  # fitz就是pip install PyMuPDF

from paddleocr import PaddleOCR,draw_ocr
ocr = PaddleOCR(lang='ch')


import os
from openai import OpenAI

client = OpenAI(
    # 若没有配置环境变量，请用百炼API Key将下行替换为：api_key="sk-xxx",
    api_key=os.getenv("DASHSCOPE_API_KEY"),  # 如何获取API Key：https://help.aliyun.com/zh/model-studio/developer-reference/get-api-key
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1"
)

completion = client.chat.completions.create(
    model="deepseek-v3",  # 此处以 deepseek-r1 为例，可按需更换模型名称。
    messages=[
        {'role': 'user', 'content': '9.9和9.11谁大'}
    ]
)

# 通过reasoning_content字段打印思考过程
print("思考过程：")
print(completion.choices[0].message.reasoning_content)

# 通过content字段打印最终答案
print("最终答案：")
print(completion.choices[0].message.content)
def pdf2png(pdfPath, baseImagePath):

    imagePath = os.path.join(baseImagePath,os.path.basename(pdfPath).split('.')[0])
    startTime_pdf2img = datetime.datetime.now()  # 开始时间
    print("imagePath=" + imagePath)
    if not os.path.exists(imagePath):
        os.makedirs(imagePath)
    pdfDoc = fitz.open(pdfPath)
    totalPage = pdfDoc.page_count
    for pg in range(totalPage):
        page = pdfDoc[pg]
        rotate = int(0)
        zoom_x = 2
        zoom_y = 2
        mat = fitz.Matrix(zoom_x, zoom_y).prerotate(rotate)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        print(f'正在保存{pdfPath}的第{pg + 1}页，共{totalPage}页')
        pix.save(imagePath + '/' + f'images_{pg + 1}.png')
    endTime_pdf2img = datetime.datetime.now()
    print(f'{pdfDoc}-pdf2img-花费时间={(endTime_pdf2img - startTime_pdf2img).seconds}秒')
    return totalPage

def get_pdf_files(folder_path):
    """
    获取文件夹下所有PDF文件的路径
    :param folder_path: 文件夹路径
    :return: PDF文件路径列表
    """
    pdf_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.lower().endswith(".pdf"):  # 检查文件扩展名是否为.pdf
                pdf_files.append(os.path.join(root, file))
    return pdf_files


def is_text_based_pdf(pdf_path):
    """
    判断PDF是否为文字版
    :param pdf_path: PDF文件路径
    :return: True（文字版）或 False（扫描版）
    """
    # 使用PyMuPDF检查是否有文本
    doc = fitz.open(pdf_path)
    has_text = False

    for page_num in range(len(doc)):
        page = doc.load_page(page_num)
        text = page.get_text()
        if text.strip():  # 如果页面有文本
            has_text = True
            break

    if has_text:
        return True  # 文字版

    # 使用pdfplumber进一步检查
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            if page.extract_text():  # 如果页面有文本
                return True  # 文字版
            if len(page.images) > 0:  # 如果页面包含图像
                return False  # 扫描版

    return False  # 默认认为是扫描版

def processText(text):

    print()


def ocr_pngs(folder_path, totalPages):

    os.chdir(folder_path)
    result = ""
    for i in range(1,totalPages + 1):
        result = ocr.ocr(f"images_{i}.png", cls=False)

        for idx in range(len(result)):
            res = result[idx]
            if res == None:
                break
            for line in res:
                result += line[1][0]
                print(line[1][0])
    return result
    # os.chdir("..")

def processScannedPdf(folder_path, pdf):
    totalPages = pdf2png(pdf, folder_path)
    # print(pdf + ".png")
    result = ocr_pngs(pdf.split(".")[0], totalPages)
    path = os.path.join(folder_path, pdf + ".txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write(result)
        f.close()

folder_path = r"D:\books"  # 替换为你的文件夹路径
pdf_files = get_pdf_files(folder_path)
for pdf in pdf_files:
    if is_text_based_pdf(pdf):
        processText(pdf)
    else:
        processScannedPdf(folder_path,pdf)



# # pdf_path = "your_pdf_file.pdf"  # 替换为你的PDF文件路径
# if is_text_based_pdf(pdf_path):
#     print("这是文字版PDF")
# else:
#     print("这是扫描版PDF")