import datetime as dt
from typing import Optional
import nbformat
import typer
import re
from pathlib import Path
from typing_extensions import Annotated
from enum import Enum
from .preprocessor import (
    SQLExecuteProcessor,
    CleanupProcessor,
    StudentPreprocessor,
    TranscludePreprocessor,
)
from jinja2 import (
    Environment,
    PackageLoader,
    FileSystemLoader,
    select_autoescape,
    Undefined,
)
from .utils import (
    preprocess_cells_latex,
    preprocess_cells_markdown,
    preprocess_cells_markdown_html,
    sql_result_to_png,
)

app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False,
    rich_markup_mode="markdown",
)

NB_EXT = ".ipynb"


class ConvertMode(str, Enum):
    latex = "latex"
    markdown = "markdown"
    mdhtml = "md+html"

    def __str__(self):
        return self.value


@app.command("eval-sql")
def evaluate_sql(
    db: Annotated[
        str,
        typer.Argument(
            help="Connection string used by SQLAlchemy to connect to the database."
        ),
    ],
    notebook: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=True,
            dir_okay=False,
            resolve_path=True,
            help="Path to the notebook to evaluate.",
        ),
    ],
    output_path: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=False,
            dir_okay=True,
            resolve_path=True,
            help="Output path where the evaluated notebook will be saved",
        ),
    ] = "./",
    output_file: Annotated[
        Optional[str],
        typer.Option(
            "--out",
            "-o",
            help="File name for the evaluated notebook. If not specified, will suffix the filename with _evaluated.",
        ),
    ] = None,
):
    nb = nbformat.read(notebook, as_version=4)
    ep = SQLExecuteProcessor(timeout=600, cnx_uri=db)
    ep.preprocess(nb, {"metadata": {"path": output_path}})

    cp = CleanupProcessor()

    cp.preprocess(nb)

    if output_file is None:
        fname = notebook.name
        fname = fname.replace(NB_EXT, "_evaluated.ipynb")
    else:
        fname = output_file
        if not fname.endswith(NB_EXT):
            fname += NB_EXT

    with open(output_path.joinpath(fname), "w", encoding="utf-8") as f:
        nbformat.write(nb, f)
        print(f"Successfully evaluated {notebook.name} and saved it into {fname}.")


@app.command("convert")
def convert_exercise(
    notebook: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=True,
            dir_okay=False,
            resolve_path=True,
            help="Path to the notebook to extract.",
        ),
    ],
    output_path: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=False,
            dir_okay=True,
            resolve_path=False,
            help="Output path where the extracted notebook will be saved",
        ),
    ] = "./",
    template: Annotated[
        Optional[Path],
        typer.Option(
            "--template",
            "-t",
            exists=True,
            file_okay=True,
            dir_okay=False,
            resolve_path=True,
        ),
    ] = None,
    conversion_target: Annotated[
        ConvertMode,
        typer.Option(
            "--mode",
            "-m",
            help="""Extraction mode :
- latex: will extract the exercise as a single latex file using *template* if specified, and a default template otherwise. Results of sql queries will be inserted as image paths.
- markdown: will extract the exercise as a single markdown file using *template* if specified, and a default template otherwise. Results of sql queries will be inserted as image paths.
- md+html: same as above, but results of queries will be inserted as html tables.
""",
        ),
    ] = ConvertMode.markdown,
):
    nb = nbformat.read(notebook, as_version=4)
    image_name = notebook.name
    image_name = image_name.replace(NB_EXT, "")

    if conversion_target == ConvertMode.latex:
        output_path = output_path.resolve()
        cells = preprocess_cells_latex(nb, output_path, image_name)
    elif conversion_target == ConvertMode.markdown:
        cells = preprocess_cells_markdown(nb, output_path, image_name)
    else:
        cells = preprocess_cells_markdown_html(nb)

    title = Undefined()
    name = notebook.stem
    date = dt.datetime.now()
    author = Undefined()
    categories = []
    exercise_type = Undefined()
    status = Undefined()
    tags = []
    description = Undefined()

    if template is None:
        env = Environment(
            loader=PackageLoader("jupytersqlconverter"), autoescape=select_autoescape()
        )
        if conversion_target in [ConvertMode.markdown, ConvertMode.mdhtml]:
            template = env.get_template("markdown.jinja")

        elif conversion_target == ConvertMode.latex:
            template = env.get_template("latex.jinja")
    else:
        t = template.name
        env = Environment(loader=FileSystemLoader(template.parents[0]))
        template = env.get_template(t)

    output = template.render(
            {
                "title": title,
                "name": name,
                "author": author,
                "date": date,
                "categories": categories,
                "exercise_type": exercise_type,
                "status": status,
                "tags": tags,
                "description": description,
                "cells": cells,
            }
        )
    output = output.replace('    \n', '\n')
    output = re.sub('\n\n+', '\n\n', output).rstrip()
    if conversion_target == ConvertMode.latex:
        out_file = output_path.joinpath(notebook.stem + '.tex')
    if conversion_target in [ConvertMode.markdown, ConvertMode.mdhtml]:
        out_file =  output_path.joinpath(notebook.stem + '.md')
    with open(out_file, 'w') as f:
        f.write(output)


@app.command(
    "extract",
    help="This will extract the results of the queries in the exercise as png images.",
)
def extract_images(
    notebook: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=True,
            dir_okay=False,
            resolve_path=True,
            help="Path to the notebook to extract.",
        ),
    ],
    output_path: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=False,
            dir_okay=True,
            resolve_path=True,
            help="Output path where the extracted notebook will be saved",
        ),
    ] = "./",
    template: Annotated[
        Optional[Path],
        typer.Option(
            "--template",
            "-t",
            exists=True,
            file_okay=True,
            dir_okay=False,
            resolve_path=True,
        ),
    ] = None,
):
    nb = nbformat.read(notebook, as_version=4)
    image_name = notebook.name
    image_name = image_name.replace(NB_EXT, "")
    i = 0
    for cell in nb["cells"]:
        i += 1
        if not sql_result_to_png(cell, image_name + "_" + str(i), output_path):
            i -= 1


@app.command("student")
def extract_student_version(
    notebook: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=True,
            dir_okay=False,
            resolve_path=True,
            help="Path to the notebook to convert.",
        ),
    ],
    output_path: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=False,
            dir_okay=True,
            resolve_path=True,
            help="Output path where the student notebook will be saved",
        ),
    ] = "./",
    output_file: Annotated[
        Optional[str],
        typer.Option(
            "--out",
            "-o",
            help="File name for the student notebook. If not specified, will suffix the filename with _student.",
        ),
    ] = None,
):
    nb = nbformat.read(notebook, as_version=4)
    ep = StudentPreprocessor(timeout=600)
    ep.preprocess(nb, {"metadata": {"path": output_path}})

    if output_file is None:
        fname = notebook.name
        fname = fname.replace(NB_EXT, "_student.ipynb")
    else:
        fname = output_file
        if not fname.endswith(NB_EXT):
            fname += NB_EXT

    with open(output_path.joinpath(fname), "w", encoding="utf-8") as f:
        nbformat.write(nb, f)
        print(
            f"Successfully extracted the student version from {notebook.name} and saved it into {fname}."
        )

@app.command("transclude")
def transclude(
    notebook: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=True,
            dir_okay=False,
            resolve_path=True,
            help="Path to the notebook to preprocess for transclusion.",
        ),
    ],
    output_path: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=False,
            dir_okay=True,
            resolve_path=True,
            help="Output path where the student notebook will be saved",
        ),
    ] = "./",
    output_file: Annotated[
        Optional[str],
        typer.Option(
            "--out",
            "-o",
            help="File name for the new notebook. If not specified, will suffix the filename with _transcluded.",
        ),
    ] = None,
):
    nb = nbformat.read(notebook, as_version=4)
    ep = TranscludePreprocessor()
    ep.preprocess(nb, notebook.parent)

    if output_file is None:
        fname = notebook.name
        fname = fname.replace(NB_EXT, "_transcluded.ipynb")
    else:
        fname = output_file
        if not fname.endswith(NB_EXT):
            fname += NB_EXT

    with open(output_path.joinpath(fname), "w", encoding="utf-8") as f:
        nbformat.write(nb, f)
        print(
            f"Successfully transcluded from {notebook.name} and saved it into {fname}."
        )

if __name__ == "__main__":
    # calling the main function
    app()
