from pathlib import Path
from jinja2 import Environment, PackageLoader, select_autoescape
from nbformat import NotebookNode
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from pandoc import read as pandoc_read, write as pandoc_write


def get_table_image(url, fn: Path, out_dir: Path, name: str, delay=5):
    """Render HTML file in browser and grab a screenshot."""
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    browser = webdriver.Chrome(options=chrome_options)

    browser.get(url)

    image_path = out_dir.joinpath(name + ".png")

    # Wait for it to load
    table = WebDriverWait(browser, delay).until(
        lambda x: x.find_element(By.CLASS_NAME, "dataframe")
    )

    table.screenshot(str(image_path))
    browser.quit()
    os.remove(fn)

    from PIL import Image

    image = Image.open(image_path)

    image_box = image.getbbox()
    cropped = image.crop(image_box)
    cropped.save(image_path)

    return image_path


def get_table_png(table_html, name: str, out_dir: Path):
    fn = out_dir.joinpath(name + ".html")
    temp_url = "file://{fn}".format(fn=fn)
    with open(fn, "w") as out:
        out.write(table_html)
    return get_table_image(temp_url, fn, out_dir, name)


def sql_result_to_png(cell: NotebookNode, name: str, out_dir: Path):
    if "tags" in cell["metadata"] and "sql_result" in cell["metadata"]["tags"]:
        env = Environment(
            loader=PackageLoader("jupytersqlconverter"), autoescape=select_autoescape()
        )
        template = env.get_template("table_image.jinja")
        html = template.render(table=cell["source"])
        get_table_png(html, name, out_dir)
        return True
    return False


def include_notebook(main: NotebookNode, included: NotebookNode) -> NotebookNode:
    # TODO: merge
    pass


def index_solution_cells(cells: [NotebookNode]) -> [str]:
    cell_index = []
    was_solution = False
    for cell in cells:
        if (
            (not was_solution)
            and "tags" in cell["metadata"]
            and "correction" in cell["metadata"]["tags"]
        ):
            cell_index.append("solution_start")
            was_solution = True
        elif (
            was_solution
            and "tags" in cell["metadata"]
            and "correction" in cell["metadata"]["tags"]
        ):
            cell_index.append("solution")
        elif was_solution and (
            (
                "tags" in cell["metadata"]
                and "correction" not in cell["metadata"]["tags"]
            )
            or "tags" not in cell["metadata"]
        ):
            cell_index[-1] = "solution_end"
            cell_index.append(None)
            was_solution = False
        else:
            cell_index.append(None)
    return cell_index


def preprocess_cells_latex(
    nb: NotebookNode, output_path: str, image_name: str
) -> [NotebookNode]:
    cells = []
    i = 0
    cell_index = index_solution_cells(nb["cells"])
    cell_number = 0
    if cell_index[-1] == "solution":
        cell_index[-1] = "solution_end"
    for cell in nb["cells"]:
        c = cell.copy()
        if cell_index[cell_number] is not None:
            c["type"] = cell_index[cell_number]
        cell_number += 1
        if (
            cell["cell_type"] == "markdown"
            and "tags" in cell["metadata"]
            and "sql_source" in cell["metadata"]["tags"]
        ):
            p = pandoc_read(cell["source"])
            out = pandoc_write(p, format="latex")
            out = out.replace("\\begin{Highlighting}[]", "\\emph{")
            out = out.replace("\\end{Highlighting}", "}")
            c["source"] = out
            cells.append(c)
        elif (
            cell["cell_type"] == "markdown"
            and "tags" in cell["metadata"]
            and "sql_result" in cell["metadata"]["tags"]
        ):
            i += 1
            c[
                "source"
            ] = f"\\begin{{center}}\n\includegraphics[width=\maxwidth{{\linewidth}}]{{{output_path}/images/{image_name}_{i}.png}}\n\end{{center}}"
            cells.append(c)
        else:
            p = pandoc_read(cell["source"])
            out = pandoc_write(p, format="latex")
            c["source"] = out
            cells.append(c)
    return cells


def preprocess_cells_markdown(
    nb: NotebookNode, output_path: str, image_name: str
) -> [NotebookNode]:
    cells = []
    i = 0
    cell_index = index_solution_cells(nb["cells"])
    cell_number = 0
    for cell in nb["cells"]:
        c = cell.copy()
        if cell_index[cell_number] is not None:
            c["type"] = cell_index[cell_number]
        cell_number += 1
        if (
            cell["cell_type"] == "markdown"
            and "tags" in cell["metadata"]
            and "sql_source" in cell["metadata"]["tags"]
        ):
            cells.append(c)
        elif (
            cell["cell_type"] == "markdown"
            and "tags" in cell["metadata"]
            and "sql_result" in cell["metadata"]["tags"]
        ):
            i += 1
            c["source"] = f"![{image_name}]({output_path}/images/{image_name}_{i}.png)"
            cells.append(c)
        else:
            p = pandoc_read(cell["source"])
            out = pandoc_write(p, format="markdown")
            c["source"] = out
            cells.append(c)
    return cells


def preprocess_cells_markdown_html(nb: NotebookNode) -> [NotebookNode]:
    cells = []
    cell_index = index_solution_cells(nb["cells"])
    cell_number = 0
    for cell in nb["cells"]:
        c = cell.copy()
        if cell_index[cell_number] is not None:
            c["type"] = cell_index[cell_number]
        cell_number += 1
        if (
            cell["cell_type"] == "markdown"
            and "tags" in cell["metadata"]
            and "sql_source" in cell["metadata"]["tags"]
        ):
            cells.append(c)
        elif (
            cell["cell_type"] == "markdown"
            and "tags" in cell["metadata"]
            and "sql_result" in cell["metadata"]["tags"]
        ):
            cells.append(c)
        else:
            p = pandoc_read(cell["source"])
            out = pandoc_write(p, format="markdown")
            c["source"] = out
            cells.append(c)
    return cells
