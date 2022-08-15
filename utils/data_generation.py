from faker import Faker
import pandas as pd
import datetime
import os
from pathlib import Path
import shutil

def create_faker_profile_data(n_rows: int, drop_cols: list, seed: int) -> pd.DataFrame:
    """Create Pandas DataFrame from Faker(locale='en_US').profile() data with given specifications."""
    Faker.seed(seed)
    fake = Faker(locale='en_US')
    fake_workers = [fake.profile() for x in range(n_rows)]
    df = pd.DataFrame(fake_workers)
    if drop_cols is not None:
        df = df.drop(drop_cols, axis=1)
    return df

class FakerProfileDataSnapshot:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.landing_dir = data_dir + '/landing'
        self.bronze_dir = data_dir + '/bronze'
        self.silver_dir = data_dir + '/silver'
        self.silver_dir = data_dir + '/gold'
        
        self.columns_a = [
            'job', 'company', 'name', 'sex', 'address', 'mail', 'birthdate']

        self.columns_b = [
            'job', 'company', 'ssn', 'website', 'username', 'name', 'sex', 
            'address', 'mail', 'birthdate']

        self.columns_c = [
            'job', 'company', 'ssn', 'website', 'username', 'name', 
            'address', 'mail', 'birthdate']

        self.columns_all = [
            'job', 'company', 'ssn', 'residence', 'current_location', 'blood_group', 
            'website', 'username', 'name', 'sex', 'address', 'mail', 'birthdate']

    def land_batch(self, batch_id) -> str:
        # daily comes fridays every week with the date in the filename
        delivery_date = datetime.date.fromisoformat('2022-08-12') + datetime.timedelta(days=7 * batch_id)
        df = self.create_batch(batch_id)
        df.index.name = 'id'
        filepath = f'{self.landing_dir}/snapshot_{delivery_date}.csv'
        df.to_csv(filepath) #, index=None)
        return filepath

    def delete_batches_in_landing_dir(self) -> None:
        file_list = Path(self.landing_dir).glob('snapshot_*.csv')
        [os.remove(file) for file in file_list]

    def delete_bronze_dir(self) -> None:
        shutil.rmtree(self.bronze_dir)
        
    def create_batch(self, batch_id) -> pd.DataFrame:
        """Initial data"""
        if batch_id == 1:
            df = create_faker_profile_data(4, None, 1)[self.columns_a]
            
        elif batch_id == 2:
            """1) + two new records"""
            df = create_faker_profile_data(6, None, 1)[self.columns_a]

        elif batch_id == 3:
            """2) + 3 new records - 2 old records"""
            df = create_faker_profile_data(9, None, 1)[self.columns_a] \
                .drop([2, 5], axis=0) \
                #.reset_index(drop=True)

        elif batch_id == 4:
            """3) + 1 new records - 1 old records + two columns"""
            df = create_faker_profile_data(10, None, 1)[self.columns_b] \
                .drop([2, 5, 7], axis=0) \
                #.reset_index(drop=True)
        
        elif batch_id == 5:
            """4) + 2 new records"""
            df = create_faker_profile_data(12, None, 1)[self.columns_b] \
                .drop([2, 5, 7], axis=0) \
                #.reset_index(drop=True)
        
        elif batch_id == 6:
            """5) + 2 new records - 1 column"""
            df = create_faker_profile_data(14, None, 1)[self.columns_c] \
                .drop([2, 5, 7], axis=0) \
                # .reset_index(drop=True)

        elif batch_id == 7:
            """5) + 6 new records - 3 records"""
            df = create_faker_profile_data(20, None, 1)[self.columns_c] \
                .drop([2, 5, 7, 8, 10, 11], axis=0) \
                #.reset_index(drop=True)

        return df
