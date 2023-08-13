from typing import Optional
import nbformat
import typer
from pathlib import Path
from typing_extensions import Annotated
from enum import Enum
from .preprocessor import (
    SQLExecuteProcessor,
    CleanupProcessor,
    StudentPreprocessor,
)
from rich.markdown import Markdown

app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False,
    rich_markup_mode="markdown",
)

NB_EXT = ".ipynb"


class ExtractMode(str, Enum):
    jupyter = "jupyter"
    latex = "latex"
    images = "images"
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


@app.command("extract")
def extract_exercise(
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
    exercise: Annotated[
        str,
        typer.Argument(
            help="Exercise name to extract. In the notebook, cell tags will be searched for that name and all cells matching the tag will be extracted. In image mode, only cells also tagged with 'result' will be extracted and converted to images."
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
    extraction_mode: Annotated[
        ExtractMode,
        typer.Option(
            "--mode",
            "-m",
            help="""Extraction mode :
- jupyter: will extract the exercise as *exercise*.ipynb.
- latex: will extract the exercise as a single latex file using *template* if specified, and a default template otherwise. Results of sql queries will be inserted as image paths.
- images: will extract the results of the queries in the exercise as png images.
- markdown: will extract the exercise as a single markdown file using *template* if specified, and a default template otherwise. Results of sql queries will be inserted as image paths.
- md+html: same as above, but results of queries will be inserted as html tables.
""",
        ),
    ] = ExtractMode.jupyter,
):
    # TODO: complete
    print(
        f"Input: {notebook.name}, exercise: {exercise}, out path: {output_path}, template: {template}, mode: {extraction_mode.value}"
    )


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
            help="Output path where the converted notebook will be saved",
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


if __name__ == "__main__":
    # calling the main function
    app()
