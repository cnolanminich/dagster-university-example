from dagster import asset
from . import constants
from ..partitions import weekly_partition
from datetime import datetime, timedelta 
import plotly.express as px
import plotly.io as pio
import geopandas as gpd
import pandas as pd
from dagster_duckdb import DuckDBResource


from . import constants

@asset(
    deps=["taxi_trips", "taxi_zones"]
)
def manhattan_stats(database: DuckDBResource):
    query = """
        select
            zones.zone,
            zones.borough,
            zones.geometry,
            count(1) as num_trips,
        from trips
        left join zones on trips.pickup_zone_id = zones.zone_id
        where borough = 'Manhattan' and geometry is not null
        group by zone, borough, geometry
    """

    with database.get_connection() as conn:
        trips_by_zone = conn.execute(query).fetch_df()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_zone.to_json())

@asset(
    deps=["manhattan_stats"],
)
def manhattan_map():
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig = px.choropleth_mapbox(trips_by_zone,
        geojson=trips_by_zone.geometry.__geo_interface__,
        locations=trips_by_zone.index,
        color='num_trips',
        color_continuous_scale='Plasma',
        mapbox_style='carto-positron',
        center={'lat': 40.758, 'lon': -73.985},
        zoom=11,
        opacity=0.7,
        labels={'num_trips': 'Number of Trips'}
    )

    pio.write_image(fig, constants.MANHATTAN_MAP_FILE_PATH)


@asset(
    deps=["taxi_trips"],
    partitions_def=weekly_partition,
)
def trips_by_week(context,database: DuckDBResource):
    
    result = pd.DataFrame()

    current_date_str = context.asset_partition_key_for_output()
    query = f"""
        select
            vendor_id, total_amount, trip_distance, passenger_count
        from trips
        where date_trunc('week', pickup_datetime) = date_trunc('week', '{current_date_str}'::date)
    """
    
    with database.get_connection() as conn:
        data_for_week = conn.execute(query).fetch_df()

    aggregate = data_for_week.agg({
        "vendor_id": "count",
        "total_amount": "sum",
        "trip_distance": "sum",
        "passenger_count": "sum"
    }).rename({"vendor_id": "num_trips"}).to_frame().T # type: ignore

    aggregate["period"] = current_date_str

    result = pd.concat([result, aggregate])


    # clean up the formatting of the dataframe
    result['num_trips'] = result['num_trips'].astype(int)
    result['passenger_count'] = result['passenger_count'].astype(int)
    result['total_amount'] = result['total_amount'].round(2).astype(float)
    result['trip_distance'] = result['trip_distance'].round(2).astype(float)
    result = result[["period", "num_trips", "total_amount", "trip_distance", "passenger_count"]]
    result = result.sort_values(by="period")

    result.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)

    # current_date = datetime.strptime("2023-03-01", constants.DATE_FORMAT)
    # end_date = datetime.strptime("2023-04-01", constants.DATE_FORMAT)
    # while current_date < end_date:
    #     current_date_str = current_date.strftime(constants.DATE_FORMAT)
    #     query = f"""
    #         COPY(
    #         select
    #             date_trunc('week', pickup_datetime) as period,
    #             count(*) as num_trips,
    #             sum(passenger_count) as passenger_count,
    #             sum(total_amount) as total_amount,
    #             sum(trip_distance) as trip_distance,
    #         from trips
    #         where period = date_trunc('week','{current_date_str}'::date)
    #         group by 1
    #         )
    #         to '{constants.TRIPS_BY_WEEK_FILE_PATH}' (HEADER, DELIMITER ',');
    #     """
    #     current_date += timedelta(days=7)
    #     print(query)

    #     conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
    #     conn.execute(query)
