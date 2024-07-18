from pathlib import Path
from typing import Any, Tuple
from jupyter_client.manager import KernelManager
from nbconvert.preprocessors import ExecutePreprocessor, Preprocessor
from nbformat import NotebookNode, from_dict as nb_from_dict

import re
import fnmatch
import nbformat


class SQLExecuteProcessor(ExecutePreprocessor):
    def __init__(self, cnx_uri, **kw):
        super().__init__(**kw)
        self.import_str = (
            "import pandas as pd\nfrom sqlalchemy import create_engine, text"
        )
        self.db_cnx = f"engine = create_engine('{cnx_uri}')"
        self.db_query = """conn = engine.connect()
conn.execute(text(\"ALTER SESSION SET NLS_DATE_FORMAT = '{dateformat}'\"))
df = pd.read_sql(sql=\"\"\"{source}\"\"\", con=conn)
df.index += 1
{limiter}
"""

    def preprocess(
        self, nb: NotebookNode, resources: Any = None, km: KernelManager | None = None
    ) -> Tuple[NotebookNode, dict]:
        cells = nb["cells"][:]
        nb["cells"] = []
        for c in cells:
            if "tags" in c["metadata"] and "sql" in c["metadata"]["tags"]:
                if "hideinput" not in c["metadata"]["tags"]:
                    pre = {
                        "cell_type": "markdown",
                        "metadata": {},
                        "source": "```sql\n" + c["source"] + "\n```",
                    }
                    pre["metadata"]["tags"] = c["metadata"]["tags"][:]
                    if "enum:end" in pre["metadata"]["tags"]:
                        pre["metadata"]["tags"].remove("enum:end")
                    pre["metadata"]["tags"].append("sql_source")
                    nb["cells"].append(nb_from_dict(pre))
                c["metadata"]["tags"].append("sql_execute")
            if "tags" in c["metadata"] and "ignore" in c["metadata"]["tags"]:
                continue
            else:
                nb["cells"].append(c)
        return super().preprocess(nb, resources, km)

    def preprocess_cell(self, cell, resources, index):
        if (
            "tags" in cell["metadata"]
            and "sql" in cell["metadata"]["tags"]
            and "sql_execute" in cell["metadata"]["tags"]
        ):
            limit = fnmatch.filter(cell["metadata"]["tags"], "limit:*")
            if len(limit) > 0:
                limiter = f"df.head({int(limit[0].split(':')[1])}).to_html()"
            else:
                limiter = "df.to_html()"
            dateformat = fnmatch.filter(cell["metadata"]["tags"], "dateformat:*")
            if len(dateformat) > 0:
                dateformat = dateformat[0].split(":")[1]
            else:
                dateformat = "YYYY-MM-DD"
            cell["source"] = (
                self.import_str
                + "\n"
                + self.db_cnx
                + "\n"
                + self.db_query.format(
                    source=cell["source"], limiter=limiter, dateformat=dateformat
                )
            )
            cell["metadata"]["tags"].remove("sql_execute")
            cell["metadata"]["tags"].append("sql_executed")
        return super().preprocess_cell(cell, resources, index)


class CleanupProcessor(ExecutePreprocessor):
    def __init__(self, **kw):
        super().__init__(**kw)

    def preprocess(
        self, nb: NotebookNode, resources: Any = None, km: KernelManager | None = None
    ) -> Tuple[NotebookNode, dict]:
        cells = nb["cells"][:]
        nb["cells"] = []
        for c in cells:
            if (
                "tags" in c["metadata"]
                and "outputs" in c
                and "sql_executed" in c["metadata"]["tags"]
            ):
                c["metadata"]["tags"].remove("sql_executed")
                c["metadata"]["tags"].append("sql_result")
                output = c["outputs"][0]["data"]["text/plain"]
                output2 = str(output).replace("\\n", "")
                output2 = output2.replace("\\'", "'")
                pre = {
                    "cell_type": "markdown",
                    "metadata": {"tags": c["metadata"]["tags"]},
                    "source": output2[1:-1],
                }
                nb["cells"].append(nb_from_dict(pre))
            else:
                nb["cells"].append(c)
        return super().preprocess(nb, resources, km)


class StudentPreprocessor(ExecutePreprocessor):
    def __init__(self, **kw):
        super().__init__(**kw)

    def preprocess(
        self, nb: NotebookNode, resources: Any = None, km: KernelManager | None = None
    ) -> Tuple[NotebookNode, dict]:
        cells = nb["cells"][:]
        nb["cells"] = []
        for c in cells:
            if not ("tags" in c["metadata"] and "correction" in c["metadata"]["tags"]):
                nb["cells"].append(c)
        return super().preprocess(nb, resources, km)


class TranscludePreprocessor(Preprocessor):
    def __init__(self, **kw):
        super().__init__(**kw)

    def preprocess(
        self, nb: NotebookNode, path: Path, resources: Any = None
    ) -> Tuple[NotebookNode, dict]:
        expr = re.compile(r"{{(?P<file>.*?)}}", re.M)
        cells = nb["cells"][:]
        nb["cells"] = []
        for c in cells:
            if c["cell_type"] in ["raw", "markdown"]:
                source = c["source"][:]
                source = source.strip()
                match = expr.match(source)
                if match:
                    target = match.group("file")
                    if not target.endswith(".ipynb"):
                        target += ".ipynb"
                    transcluded_path = path.joinpath(target).resolve()
                    transcluded_nb = nbformat.read(transcluded_path, as_version=4)
                    nb["cells"].extend(transcluded_nb["cells"])
                else:
                    nb["cells"].append(c)
            else:
                nb["cells"].append(c)
        return super().preprocess(nb, resources)

    def preprocess_cell(self, cell, resources, _):
        # Do nothing, juste pass it on
        return cell, resources
