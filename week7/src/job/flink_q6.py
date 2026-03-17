from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

# CREATE TABLE processed_events (
#             window_start TIMESTAMP(3),
#             total_tip_amount DOUBLE PRECISION,
#             PRIMARY KEY (window_start)
#         );


def create_processed_events_sink_postgres(t_env):
    table_name = 'processed_events'
    sink_ddl = f"""
        CREATE TABLE processed_events (
            window_start TIMESTAMP(3),
            total_tip_amount DOUBLE PRECISION,
            PRIMARY KEY (window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


# CREATE TABLE events (
#              PULocationID INTEGER,
#              DOLocationID INTEGER,
#              lpep_pickup_datetime VARCHAR,
#              lpep_dropoff_datetime VARCHAR,
#              passenger_count INTEGER,
#              trip_distance DOUBLE PRECISION,
#              total_amount DOUBLE PRECISION,
#              tip_amount DOUBLE PRECISION,
#              event_timestamp TIMESTAMP,
#              WATERMARK TIMESTAMP
#          );

def create_events_source_kafka(t_env):
    table_name = "events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            passenger_count INTEGER,
            trip_distance DOUBLE PRECISION,
            total_amount DOUBLE  PRECISION,
            tip_amount DOUBLE PRECISION,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def log_processing():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    try:
        # Create Kafka table
        source_table = create_events_source_kafka(t_env)
    except Exception as e:
        print("create source failed:", str(e))

    try:
        aggregated_table = create_processed_events_sink_postgres(t_env)
    except Exception as e:
        print("create sink failed:", str(e))

    try:
        # write records to postgres
        t_env.execute_sql(
            f"""
                    INSERT INTO {aggregated_table}
                    SELECT
                        window_start,
                        SUM(tip_amount) AS total_tip_amount
                    FROM TABLE(
                        TUMBLE(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '1' HOUR)
                    )
                    GROUP BY window_start;
                    """
        ).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_processing()
