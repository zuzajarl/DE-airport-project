from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

path= "gs://krk-flights-bucket/krk_arrivals_raw/"


def create_kafka_source_table(t_env):
    table_name = "krk_arrivals_table"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            flight_number STRING,
            status STRING,
            airline_name STRING,
            origin_name STRING,
            scheduled_arrival_utc STRING,
            revised_arrival_utc STRING,
            origin_icao STRING,
            airline_icao STRING,
            delay_minutes DOUBLE,
            status_bucket STRING,
            ingested_at STRING
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'krk_arrivals',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def create_sink_table(t_env):
    table_name = 'krk_arrivals_gcs'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            flight_number STRING,
            status STRING,
            airline_name STRING,
            origin_name STRING,
            scheduled_arrival_utc STRING,
            revised_arrival_utc STRING,
            origin_icao STRING,
            airline_icao STRING,
            delay_minutes DOUBLE,
            status_bucket STRING,
            ingested_at STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{path}',
            'format' = 'json'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name



def log_processing():
    # Set up the table environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(5000)
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env, environment_settings=settings)
    try:
        # Create Kafka table
        source_table = create_kafka_source_table(t_env)
        gcs_sink = create_sink_table(t_env)
        # write records to GCS
        t_env.execute_sql(
            f"""
                    INSERT INTO {gcs_sink}
                    SELECT
                        flight_number,
                        status,
                        airline_name,
                        origin_name,
                        scheduled_arrival_utc,
                        revised_arrival_utc,
                        origin_icao,
                        airline_icao,
                        delay_minutes,
                        status_bucket,
                        ingested_at
                    FROM {source_table}
                    """
        ).wait()

    except Exception as e:
        print("Writing records from Kafka to GCS failed:", str(e))



if __name__ == '__main__':
    log_processing()
