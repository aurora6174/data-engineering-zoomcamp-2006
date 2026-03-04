import marimo

__generated_with = "0.11.0"
app = marimo.App()


@app.cell
def _():
    import dlt
    import ibis
    import marimo as mo
    return dlt, ibis, mo


@app.cell
def _(dlt, mo):
    mo.md("# Top 10 authors by book count")
    return


@app.cell
def _(dlt):
    dataset = dlt.pipeline("open_library_pipeline").dataset()
    conn = dataset.ibis()
    return conn, dataset


@app.cell
def _(conn, ibis):
    books = conn.table("books")
    authors = conn.table("books__authors")

    top_authors = (
        authors.join(books, authors["_dlt_parent_id"] == books["_dlt_id"])
        .group_by(author=authors["name"])
        .aggregate(book_count=books["_dlt_id"].nunique())
        .filter(lambda t: t["author"].notnull())
        .order_by([ibis.desc("book_count"), "author"])
        .limit(10)
    )
    return top_authors


@app.cell
def _(mo, top_authors):
    df = top_authors.execute()
    mo.ui.table(df)
    return df


@app.cell
def _(df, mo):
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.barh(df["author"], df["book_count"], color="#1f77b4")
    ax.invert_yaxis()
    ax.set_title("Top 10 Authors by Book Count")
    ax.set_xlabel("Book Count")
    ax.set_ylabel("Author")
    fig.tight_layout()

    mo.mpl.interactive(fig)
    return fig


if __name__ == "__main__":
    app.run()
