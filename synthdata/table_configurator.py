import os
import random
import pandas as pd
from datetime import datetime
from typing import List, NoReturn
from dataclasses import dataclass, field


@dataclass
class ConfigConstructor:
    num_rows: int = field(default=1500, metadata={'doc': 'Number of transactions'})
    bukrs_list: List[str] = field(default_factory=lambda: ['0100', '0101'])
    start_date: datetime = datetime(2019, 9, 25)
    end_date: datetime = datetime(2022, 12, 31)
    saktiv_list: List[int] = field(default_factory=lambda: [2, 3, 1])
    rfhazul_list: List[int] = field(default_factory=lambda: [2, 3, 1])
    rfhazu_list: List[int] = field(default_factory=lambda: [2, 3, 1])
    sfhaart_list: List[int] = field(default_factory=lambda: ['100', '200'])
    zz_sfhaart_list: List[int] = field(default_factory=lambda: ['100', '200', '102', '202', '301', '110'])
    wgschft_list: List[str] = field(default_factory=lambda: ['RUB', 'USD', 'EUR'])
    codes_list: List[str] = field(default_factory=lambda: ['RUB', 'USD', 'EUR', 'GBP', 'CNY', 'ZAR',
                                                           'HKD', 'JPY'])
    waers_list: List[str] = field(default_factory=lambda: ['RUB', 'USD', 'EUR', 'CNY', 'HKD', 'GBP'])
    waers_b_list: List[str] = field(default_factory=lambda: ['RUB', 'EUR', 'UD00', 'UE00', 'USD',
                                                             'XUSD', 'GBP', 'UP00', 'XEUR', 'UD03'])
    butxt_list: List[str] = field(default_factory=lambda: ['НТЭК', 'НЛО', 'Другой банк', 'КРП', 'СПУТНИК',
                                                           'Девелопмент', 'АШАН', 'КБ', 'Озон-протон',
                                                           'Вкусно и точка'])
    limit_currenc_list: List[str] = field(default_factory=lambda: ['RUB', 'USD', 'EUR', 'CNY', 'GBP'])
    zz_num_reg: List[str] = field(default_factory=lambda: ['НН', 'ЗФ', 'НОК', 'НК', 'ГО', 'МК'])
    sgsart_list: List[str] = field(default_factory=lambda: ['A30', 'A20', 'A25', 'A26', 'A31', 'A22'])
    zz_vvsart: List[str] = field(default_factory=lambda: ['A30', 'A05', 'A02', 'A01', 'B02', 'A07',
                                                          'C10', 'C01', 'E10', 'C21', 'A31', 'C11', 'A06',
                                                          'A03', 'E20', 'D01', 'A10', 'A26', '01A', 'V01',
                                                          'B01'])
    sfhazba_list: List[str] = field(default_factory=lambda: ['G220', 'P110', 'ZP01', 'G210', 'G110',
                                                             'G100', 'I004', 'G120', 'G111', 'C003',
                                                             'G200', 'A104'])
    zz_bankname: List[str] = field(default_factory=lambda: ['СИБИРСКИЙ ФИЛИАЛ ПАО РОСБАНК', 'ПАО СБЕРБАНК',
                                                            'БАНК ГПБ (АО)', 'КАЛУЖСКОЕ ПАО СБЕРБАНК',
                                                            'АО "АЛЬФА-БАНК"', 'JPMORGAN BANK, N.A.',
                                                            'КРАСНОДАРСКОЕ ПАО СБЕРБАНК', 'BANK SARAN LTD',
                                                            'THE BANK OF NEW YORK', 'АО ЮНИКРЕДИТ БАНК',
                                                            'УРАЛЬСКИЙ ПАО СБЕРБАНК', 'Банк ГПБ (АО)'])

    def generate_zuond(self, used_zuond) -> str:
        FIXED_ZUOND_PART = '10000'
        RANDOM_ZUOND_RANGE = (100000, 999999)
        return self.generate_random_number(used_zuond, FIXED_ZUOND_PART, RANDOM_ZUOND_RANGE)

    def generate_kontrh(self, used_kontrh) -> str:
        FIXED_KONTRH_PART = '80000'
        RANDOM_KONTRH_RANGE = (1000, 9999)
        return self.generate_random_number(used_kontrh, FIXED_KONTRH_PART, RANDOM_KONTRH_RANGE)

    def get_random_date(self) -> str:
        start_timestamp = self.start_date.timestamp()
        end_timestamp = self.end_date.timestamp()
        random_timestamp = random.uniform(start_timestamp, end_timestamp)
        return datetime.fromtimestamp(random_timestamp).strftime('%Y-%m-%d')

    def get_random_time(self) -> str:
        random_timestamp = random.uniform(self.start_date.timestamp(), self.end_date.timestamp())
        random_datetime = datetime.fromtimestamp(random_timestamp)
        formatted_time = str(random_datetime.strftime('%H:%M:%S'))
        return formatted_time

    def get_random_delta_date(self, dcrdat: datetime) -> datetime:
        dcr_datetime = pd.to_datetime(dcrdat)
        random_timedelta = random.choice(pd.timedelta_range(start='30D', end='365D', freq='D'))
        random_datetime = dcr_datetime + random_timedelta
        return random_datetime.to_pydatetime()

    def generate_random_number(self, used_numbers, fixed_part, random_range) -> str:
        while True:
            random_part = str(random.randint(*random_range))
            number = f'{fixed_part}{random_part}'
            if number not in used_numbers:
                used_numbers.add(number)
                return number

    def generate_rfha(self, used_rfha) -> str:
        FIXED_RFHA_PART = '100000'
        RANDOM_RFHA_RANGE = (1000, 9999)
        random_part = ''.join(random.sample('0123456789', 4))
        rfha = f'{FIXED_RFHA_PART}{random_part}'
        while rfha in used_rfha:
            random_part = ''.join(random.sample('0123456789', 4))
            rfha = f'{FIXED_RFHA_PART}{random_part}'
        used_rfha.add(rfha)
        return rfha

    def save_dataframe_to_csv(self, dataframe, filename) -> NoReturn:
        directory = os.path.join(os.getcwd(), 'data')

        if not os.path.exists(directory):
            os.makedirs(directory)

        filepath = os.path.join(directory, filename)
        dataframe.to_csv(filepath, index=False, sep=';')
        print(f"Dataframe has been saved to {filepath}")