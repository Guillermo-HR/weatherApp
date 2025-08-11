import pandas as pd
import logging

class Load:
    def __init__(self, logger: logging.Logger, engine):
        self.engine = engine
        self.logger = logger

    def load_data(self, data: dict, table: str) -> tuple[int, int]:
        successful = 0
        failed = 0

        try:
            df = pd.DataFrame(data)
            df['recorded_at'] = pd.to_datetime(df['recorded_at'], unit='s', utc=True)
            df.to_sql(table, self.engine, if_exists='append', index=False)
            successful = len(df)
        except Exception as e:
            self.logger.critical(f"Failed to load data into {table}: {e}")
            failed = len(data)
        
        return successful, failed