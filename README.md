# jupyter-sql-converter

NBConvert-based tools to evaluate SQL cells and produce teacher/student exercise sheets.

This will allow to turn a Jupyter notebook into individual exercises with:

- a teacher version with the solution and SQL code executed where necessary
- a student version without the solution

Exercises will be saved as individual notebooks and can be converted to markdown and/or latex, and can be combined to produce exercise sheets for teachers (with the solutions and other teacher-only information) and for students (with code cells to save their own answer before submitting them in the case of notebooks).

This tool can also be used to execute the student submission and display the result.
