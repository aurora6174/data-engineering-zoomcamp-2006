import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md("""
    # NYC Taxi Data Explorer

    This marimo notebook reads data loaded by `taxi_pipeline` and visualizes the full
    `rides` table from `taxi_pipeline_dataset`.

    Reference: https://dlthub.com/docs/general-usage/dataset-access/marimo
    """)
    return


@app.cell
def _(mo):
    import dlt

    try:
        import altair as alt
    except ImportError:
        alt = None

    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="taxi_pipeline_dataset",
    )
    dataset = pipeline.dataset()

    # Returning ibis connection registers a datasource in marimo's Datasources panel.
    ibis_con = dataset.ibis()

    rides = ibis_con.table("rides")

    total_rows = rides.count().execute()
    date_bounds = rides.aggregate(
        min_pickup=rides.trip_pickup_date_time.min(),
        max_pickup=rides.trip_pickup_date_time.max(),
    ).execute()

    summary = mo.md(
        f"""
        **Rows:** {int(total_rows):,}  
        **Pickup range:** {date_bounds['min_pickup'].iloc[0]} to {date_bounds['max_pickup'].iloc[0]}
        """
    )
    return rides, summary, total_rows


@app.cell
def _(summary):
    summary
    return


@app.cell
def _(mo, total_rows):
    row_limit = mo.ui.slider(100, int(total_rows), step=100, value=min(2000, int(total_rows)))
    row_limit
    return (row_limit,)


@app.cell
def _(rides, row_limit):
    table_view = rides.limit(row_limit.value).execute()
    table_view
    return


@app.cell
def _(rides):
    trips_by_day = (
        rides.mutate(pickup_day=rides.trip_pickup_date_time.date())
        .group_by("pickup_day")
        .aggregate(trips=rides.count())
        .order_by("pickup_day")
        .execute()
    )

    payment_mix = (
        rides.group_by("payment_type")
        .aggregate(trips=rides.count(), avg_fare=rides.fare_amt.mean())
        .order_by("trips", ascending=False)
        .execute()
    )

    distance_fare = (
        rides.filter(rides.trip_distance.notnull() & rides.fare_amt.notnull())
        .select("trip_distance", "fare_amt", "payment_type", "vendor_name")
        .limit(5000)
        .execute()
    )
    return


app._unparsable_cell(
    r"""
    if alt is None:
        mo.md(
            "Install chart deps to render plots: `pip install altair pandas`"
        )
        return

    day_chart = (
        alt.Chart(trips_by_day)
        .mark_line(point=True)
        .encode(
            x=alt.X("pickup_day:T", title="Pickup day"),
            y=alt.Y("trips:Q", title="Trips"),
            tooltip=["pickup_day:T", "trips:Q"],
        )
        .properties(title="Trips by Day", width=700, height=280)
    )

    payment_chart = (
        alt.Chart(payment_mix)
        .mark_bar()
        .encode(
            x=alt.X("payment_type:N", title="Payment type"),
            y=alt.Y("trips:Q", title="Trips"),
            color="payment_type:N",
            tooltip=["payment_type:N", "trips:Q", alt.Tooltip("avg_fare:Q", format=".2f")],
        )
        .properties(title="Trips by Payment Type", width=350, height=280)
    )

    fare_distance_chart = (
        alt.Chart(distance_fare)
        .mark_circle(size=28, opacity=0.35)
        .encode(
            x=alt.X("trip_distance:Q", title="Trip distance"),
            y=alt.Y("fare_amt:Q", title="Fare amount"),
            color=alt.Color("payment_type:N", title="Payment type"),
            tooltip=["trip_distance:Q", "fare_amt:Q", "payment_type:N", "vendor_name:N"],
        )
        .properties(title="Fare vs Distance (sample)", width=700, height=320)
    )

    mo.md("## Charts")
    day_chart
    payment_chart
    fare_distance_chart
    """,
    name="_"
)


@app.cell
def _(mo):
    mo.md("""
    ## How to use

    1. Start notebook: `marimo edit taxi_marimo_notebook.py`
    2. Use the Datasources panel to inspect the `ibis_con` connection and generate SQL cells.
    3. Adjust the row-limit slider to browse as much of the `rides` table as needed.
    """)
    return


if __name__ == "__main__":
    app.run()
