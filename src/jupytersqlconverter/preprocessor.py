from pathlib import Path
from typing import Any, Tuple
from jupyter_client.manager import KernelManager
from nbconvert.preprocessors import ExecutePreprocessor, Preprocessor
from nbformat import NotebookNode, from_dict as nb_from_dict

import re
import fnmatch
import nbformat


class SQLExecuteProcessor(ExecutePreprocessor):

    date_fmt = {
        "DD/MM/YYYY" : "%d/%m/%Y",
        "YYYY-MM-DD" : "%Y-%m-%d",
        "DD/MM/RR": "%d/%m/%y",
    }

    def __init__(self, cnx_uri, **kw):
        super().__init__(**kw)
        self.import_str = (
            "import pandas as pd\nfrom sqlalchemy import create_engine, text\nfrom sqlalchemy.exc import DatabaseError"
        )
        self.db_cnx = f"""if 'engine' not in locals():
    engine = create_engine('{cnx_uri}')
"""
        self.db_query = """with engine.connect() as conn:
    conn.execute(text(\"ALTER SESSION SET NLS_TERRITORY = FRANCE\"))
    conn.execute(text(\"ALTER SESSION SET NLS_LANGUAGE = FRENCH\"))
    conn.execute(text(\"ALTER SESSION SET NLS_DATE_FORMAT = '{dateformat}'\"))
    df = pd.read_sql(sql=\"\"\"{source}\"\"\", con=conn)
    for x in df.select_dtypes(include=['datetime64']).columns.tolist():
        df[x] = df[x].dt.strftime('{dateformat_str}')
    for x in df.select_dtypes(include=['float64']).columns.tolist():
        df[x] = df[x].apply(lambda v: '{{:.9g}}'.format(v))
    df.fillna("(null)",inplace=True)
    df = df.replace("nan", "(null)")
    df.index += 1
{limiter}
"""
        self.db_query_except = """with engine.connect() as conn:
    conn.execute(text(\"ALTER SESSION SET NLS_TERRITORY = FRANCE\"))
    conn.execute(text(\"ALTER SESSION SET NLS_LANGUAGE = FRENCH\"))
    conn.execute(text(\"ALTER SESSION SET NLS_DATE_FORMAT = '{dateformat}'\"))
    try:
        df = pd.read_sql(sql=\"\"\"{source}\"\"\", con=conn)
        for x in df.select_dtypes(include=['datetime64']).columns.tolist():
            df[x] = df[x].dt.strftime('{dateformat_str}')
        for x in df.select_dtypes(include=['float64']).columns.tolist():
            df[x] = df[x].apply(lambda v: '{{:.9g}}'.format(v))
        df.fillna("(null)",inplace=True)
        df = df.replace("nan", "(null)")
        df.index += 1
    except Exception as e:
        err, = e.orig.args
        print(err.message)
"""

        self.no_result_query = """with engine.connect() as conn:
    conn.execute(text(\"ALTER SESSION SET NLS_TERRITORY = FRANCE\"))
    conn.execute(text(\"ALTER SESSION SET NLS_LANGUAGE = FRENCH\"))
    conn.execute(text(\"ALTER SESSION SET NLS_DATE_FORMAT = '{dateformat}'\"))
    conn.execute(text(\"\"\"{source}\"\"\"))
    conn.commit()
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
            elif "tags" in c["metadata"] and "sql" in c["metadata"]["tags"] and "plsql" not in c["metadata"]["tags"]:
                import copy
                if ";" in c["source"].rstrip().rstrip(";"):
                    for s in c["source"].rstrip().rstrip(";").split(";"):
                        if s.strip() != "":
                            c_split = copy.deepcopy(c)
                            c_split["source"] = s
                            nb["cells"].append(c_split)
                else:
                    nb["cells"].append(c)
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
                dateformat = ":".join(dateformat[0].split(":")[1:])
            else:
                dateformat = "YYYY-MM-DD"
            query = cell["source"]
            if not ("tags" in cell["metadata"] and "sql" in cell["metadata"]["tags"] and "plsql" in cell["metadata"]["tags"]):
                query = query.rstrip().rstrip(";")
            else:
                query = query.rstrip().rstrip("/").rstrip()
            if "noresult" in cell["metadata"]["tags"]:
                cell["source"] = (
                    self.import_str
                    + "\n"
                    + self.db_cnx
                    + "\n"
                    + self.no_result_query.format(
                        source=query, dateformat=dateformat
                    )
                )
            elif "except" in cell["metadata"]["tags"]:
                cell["source"] = (
                    self.import_str
                    + "\n"
                    + self.db_cnx
                    + "\n"
                    + self.db_query_except.format(
                        source=query, limiter=limiter, dateformat=dateformat, dateformat_str=self.date_fmt[dateformat]
                    )
                )
            else:
                cell["source"] = (
                    self.import_str
                    + "\n"
                    + self.db_cnx
                    + "\n"
                    + self.db_query.format(
                        source=query, limiter=limiter, dateformat=dateformat, dateformat_str=self.date_fmt[dateformat]
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
                if len(c["outputs"]) > 0 and "noresult" not in c["metadata"]["tags"] and "except" not in c["metadata"]["tags"]:
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
                elif len(c["outputs"]) > 0 and "noresult" not in c["metadata"]["tags"] and "except" in c["metadata"]["tags"]:
                    c["metadata"]["tags"].remove("sql_executed")
                    output = c["outputs"][0]["text"]
                    pre = {
                        "cell_type": "markdown",
                        "metadata": {"tags": c["metadata"]["tags"]},
                        "source": "```console\n" + output + "```",
                    }
                    pre["metadata"]["tags"].append("sql_source")
                    if "oracle" in c["metadata"]["tags"]:
                        pre["metadata"]["tags"].remove("oracle")
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
